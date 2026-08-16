"""Gold layer: conformed trip fact enriched with zone geography and window features.

Three distributed computation decisions are made here and each one is evidenced rather
than asserted:

  1. the zone dimension is broadcast, so the 44 million row fact table never moves across
     the network to meet a 265 row lookup
  2. window functions run at two different grains, row level over the fact table and
     aggregate level over the far smaller zone hour table, because computing a rolling
     demand curve by sorting the fact table would be wasteful
  3. the fact table is partitioned by pickup date, and the alternative is written out
     alongside it so the difference can be measured instead of claimed
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from nyc_taxi.config import (
    BRONZE_ZONES_DIR,
    GOLD_ANALYTICS_DIR,
    GOLD_TRIPS_DIR,
    GOLD_TRIPS_UNPARTITIONED_DIR,
    SILVER_DIR,
    PipelineConfig,
)
from nyc_taxi.spark import (
    capture_explain,
    directory_stats,
    timed,
    write_evidence,
    write_text_evidence,
)


def read_silver(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(str(SILVER_DIR))


def read_zones(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(str(BRONZE_ZONES_DIR))


def enrich_with_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Join the trip fact to the zone dimension twice, on pickup and on dropoff.

    F.broadcast is explicit here even though the dimension is under the auto broadcast
    threshold and would likely be broadcast anyway. Relying on the optimiser to notice is
    fine until someone changes a threshold or the statistics go stale on a table this
    join cannot afford to shuffle. Stating the intent makes the plan stable.
    """
    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone_name").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone_name").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )
    return trips.join(F.broadcast(pickup_zones), on="pickup_location_id", how="left").join(
        F.broadcast(dropoff_zones), on="dropoff_location_id", how="left"
    )


def add_trip_window_features(trips: DataFrame) -> DataFrame:
    """Row level window features over the fact table.

    Partitioned by pickup zone and date rather than by zone alone. Zone alone would put
    every trip of the loaded period for the busiest Manhattan zones into a single window
    partition, roughly two million rows that one task has to sort. Adding the date splits
    that into 181 independent sorts and keeps the work spread.
    """
    zone_day = Window.partitionBy("pickup_location_id", "pickup_date").orderBy("pickup_ts")

    return (
        trips.withColumn("trip_seq_in_zone_day", F.row_number().over(zone_day))
        .withColumn(
            "seconds_since_prev_pickup_in_zone",
            F.col("pickup_ts").cast("long")
            - F.lag(F.col("pickup_ts").cast("long")).over(zone_day),
        )
        .withColumn(
            "prev_trip_fare_in_zone",
            F.lag("fare_amount").over(zone_day),
        )
    )


def build_zone_hour_demand(trips: DataFrame) -> DataFrame:
    """Aggregate to zone and hour, then run the rolling windows on that.

    The order matters. Rolling demand is a property of the hour, not of the trip, so the
    aggregation happens first and the window runs over roughly half a million grouped
    rows instead of forty three million detail rows.
    """
    hourly = (
        trips.withColumn("pickup_hour_ts", F.date_trunc("hour", F.col("pickup_ts")))
        .groupBy(
            "pickup_location_id",
            "pickup_borough",
            "pickup_zone",
            "pickup_service_zone",
            "pickup_hour_ts",
        )
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum("passenger_count").alias("passengers"),
            F.avg("trip_distance_miles").alias("avg_distance_miles"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("implied_speed_mph").alias("avg_speed_mph"),
        )
    )

    # rangeBetween over an epoch second column, so gaps in the hour series are handled as
    # real time gaps. A rowsBetween window would quietly treat a zone's 3am and its next
    # recorded 9am as adjacent.
    hour_epoch = F.col("pickup_hour_ts").cast("long")
    hourly = hourly.withColumn("hour_epoch", hour_epoch)

    rolling_3h = (
        Window.partitionBy("pickup_location_id")
        .orderBy("hour_epoch")
        .rangeBetween(-2 * 3600, 0)
    )
    rolling_24h = (
        Window.partitionBy("pickup_location_id")
        .orderBy("hour_epoch")
        .rangeBetween(-23 * 3600, 0)
    )
    zone_order = Window.partitionBy("pickup_location_id").orderBy("hour_epoch")

    return (
        hourly.withColumn("trips_rolling_3h", F.sum("trips").over(rolling_3h))
        .withColumn("trips_rolling_24h", F.sum("trips").over(rolling_24h))
        .withColumn("avg_fare_rolling_24h", F.avg("avg_fare").over(rolling_24h))
        .withColumn("trips_prev_hour", F.lag("trips").over(zone_order))
        .withColumn(
            "trips_vs_prev_hour_pct",
            F.round(
                100.0 * (F.col("trips") - F.col("trips_prev_hour")) / F.col("trips_prev_hour"), 2
            ),
        )
        .withColumn(
            "zone_hour_rank_by_trips",
            F.dense_rank().over(
                Window.partitionBy("pickup_location_id").orderBy(F.desc("trips"))
            ),
        )
        .withColumn("pickup_date", F.to_date("pickup_hour_ts"))
        .withColumn("pickup_hour", F.hour("pickup_hour_ts"))
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_hour_ts"))
        .drop("hour_epoch")
    )


def capture_join_plans(trips: DataFrame, zones: DataFrame, spark: SparkSession) -> dict:
    """Capture both join strategies so the broadcast decision can be shown, not asserted.

    The same logical join is planned twice, once with broadcasting available and once
    with it disabled, which is what a sort merge join of a 44 million row table against a
    265 row table would have cost.
    """
    sample = trips.select("pickup_location_id", "dropoff_location_id", "fare_amount")
    dim = zones.select(F.col("location_id").alias("pickup_location_id"), F.col("borough"))

    broadcast_plan = capture_explain(sample.join(F.broadcast(dim), on="pickup_location_id"))

    original = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    try:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        shuffle_plan = capture_explain(sample.join(dim, on="pickup_location_id"))
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(original))

    text = (
        "Broadcast join, as the pipeline runs it\n"
        "=======================================\n"
        "The dimension is sent to every executor. The fact table stays where it is and no\n"
        "Exchange appears on its side of the join.\n\n"
        f"{broadcast_plan}\n\n\n"
        "Sort merge join, with broadcasting disabled\n"
        "===========================================\n"
        "Both sides are hash partitioned and sorted on the join key. That is a full\n"
        "Exchange over every trip row, to meet a 265 row table.\n\n"
        f"{shuffle_plan}\n"
    )
    write_text_evidence("join_plans.txt", text)

    return {
        "broadcast": {
            "uses_broadcast_hash_join": "BroadcastHashJoin" in broadcast_plan,
            "shuffle_exchanges": count_plan_nodes(broadcast_plan, "Exchange"),
            "broadcast_exchanges": count_plan_nodes(broadcast_plan, "BroadcastExchange"),
            "sorts": count_plan_nodes(broadcast_plan, "Sort"),
        },
        "sort_merge": {
            "uses_sort_merge_join": "SortMergeJoin" in shuffle_plan,
            "shuffle_exchanges": count_plan_nodes(shuffle_plan, "Exchange"),
            "broadcast_exchanges": count_plan_nodes(shuffle_plan, "BroadcastExchange"),
            "sorts": count_plan_nodes(shuffle_plan, "Sort"),
        },
    }


def count_plan_nodes(plan: str, node_type: str) -> int:
    """Count operators of one type in a formatted physical plan.

    A substring count will not do. "Exchange" also matches "BroadcastExchange", and a
    broadcast is precisely the shuffle this design is avoiding, so counting them together
    would overstate the very number the design is judged on. The formatted plan numbers
    each operator once as "(14) Exchange", which is what this matches.
    """
    return len(re.findall(rf"^\(\d+\) {re.escape(node_type)}$", plan, flags=re.MULTILINE))


def run_gold(spark: SparkSession, cfg: PipelineConfig, write: bool = True) -> dict:
    timings: dict = {}
    metrics: dict = {"stage": "gold"}

    silver = read_silver(spark)
    zones = read_zones(spark)

    metrics["zone_dimension_rows"] = zones.count()
    metrics["join_plans"] = capture_join_plans(silver, zones, spark)

    enriched = enrich_with_zones(silver, zones)

    # One shuffle serves both the window functions and the partitioned write, and the
    # ordering is what makes that possible.
    #
    # Repartitioning by pickup_date first puts every row of a given date in one partition.
    # The window then asks to be clustered by (pickup_location_id, pickup_date), and a
    # partitioning on a subset of the required clustering keys already satisfies that
    # distribution, so Spark reuses the existing exchange instead of adding its own. The
    # write's partitionBy is likewise already satisfied. Applying the window first and
    # repartitioning afterwards computes the same answer and pays for two full shuffles of
    # 44 million rows to do it. The exchange count is asserted below.
    #
    # By range rather than by hash. Hashing 181 dates into 64 buckets hands each task an
    # arbitrary scatter of three or four unrelated dates, and every one of them becomes a
    # separate output directory the task has to write. Range partitioning gives each task a
    # contiguous slice of the calendar, so it writes close to a single date directory.
    partitioned_once = enriched.repartitionByRange(
        cfg.spark.gold_date_partitions, F.col("pickup_date")
    )
    with_windows = add_trip_window_features(partitioned_once)

    plan = capture_explain(with_windows)
    metrics["gold_plan"] = {
        "shuffle_exchanges": count_plan_nodes(plan, "Exchange"),
        "broadcast_exchanges": count_plan_nodes(plan, "BroadcastExchange"),
        "window_operators": count_plan_nodes(plan, "Window"),
        "sorts": count_plan_nodes(plan, "Sort"),
    }
    write_text_evidence("gold_write_plan.txt", plan)

    if write:
        with timed("gold: write partitioned trips", timings):
            (
                with_windows.write.mode("overwrite")
                .partitionBy("pickup_date")
                .parquet(str(GOLD_TRIPS_DIR))
            )

        # The same table without partition directories, written only so the pruning
        # comparison has a real baseline. It is copied from the finished gold table rather
        # than recomputed from silver, so the window functions are not paid for twice.
        with timed("gold: write unpartitioned baseline", timings):
            (
                spark.read.parquet(str(GOLD_TRIPS_DIR))
                .write.mode("overwrite")
                .option("maxRecordsPerFile", cfg.spark.bronze_max_rows_per_file)
                .parquet(str(GOLD_TRIPS_UNPARTITIONED_DIR))
            )

        with timed("gold: build zone hour demand", timings):
            demand = build_zone_hour_demand(spark.read.parquet(str(GOLD_TRIPS_DIR)))
            demand.repartition(8).write.mode("overwrite").parquet(
                str(GOLD_ANALYTICS_DIR / "zone_hour_demand")
            )

    # Measurement reads from disk rather than from the frames above, so it describes what
    # was actually written. It sits outside the write branch on purpose: running the stage
    # with --dry-run then refreshes the metrics against existing output without rebuilding
    # a layer that takes a quarter of an hour to produce.
    if GOLD_TRIPS_DIR.exists():
        gold_trips = spark.read.parquet(str(GOLD_TRIPS_DIR))
        with timed("gold: measure output", timings):
            metrics["gold_trip_rows"] = gold_trips.count()
            metrics["trips_with_unmatched_pickup_zone"] = gold_trips.filter(
                F.col("pickup_zone").isNull()
            ).count()
            metrics["layout"] = {
                "partitioned": directory_stats(GOLD_TRIPS_DIR),
                "unpartitioned_baseline": directory_stats(GOLD_TRIPS_UNPARTITIONED_DIR),
            }
        demand_path = GOLD_ANALYTICS_DIR / "zone_hour_demand"
        if demand_path.exists():
            metrics["zone_hour_rows"] = spark.read.parquet(str(demand_path)).count()

    metrics["timings_seconds"] = timings
    write_evidence("gold_metrics", metrics)
    return metrics
