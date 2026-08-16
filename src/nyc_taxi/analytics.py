"""Business analytics computed on the gold layer.

Three deliverables for the operations analyst persona:

  1. demand patterns by zone, hour and day of week
  2. a rule based fare anomaly screen
  3. how the prevalence of each data quality defect moved across the loaded months

The anomaly screen is the piece that needs its method stated plainly. It compares each
trip's fare against its own peer group, defined as the pickup zone crossed with a distance
band crossed with a duration band, and scores the distance from that group's median in
units of the group's robust spread, taken from its interquartile range.

A median and an IQR rather than a mean and a standard deviation, because the thing being
detected would itself inflate a mean and a standard deviation, which is how a naive z score
ends up hiding the outliers it was built to find. See peer_group_statistics for why the
spread is an IQR rather than a median absolute deviation, which is a decision about how
many times the fact table has to be read.

All three of zone, distance and duration are in the peer key, and the duration term is
there because leaving it out was measurably wrong. An earlier version compared fare per
mile within zone and distance alone, and flagged 2.73 percent of every sub one mile trip in
the dataset. It was not finding overcharging, it was finding traffic: the New York meter
charges by time whenever the cab is moving slowly, so a short crawl legitimately costs more
per mile than a short clear run. Comparing a trip only against others of similar length
*and* similar duration removes that entire class of false positive.

Everything is computed as Spark aggregations and broadcast joins. No trip is ever pulled
to the driver to be scored.
"""

from __future__ import annotations

import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nyc_taxi.config import (
    GOLD_ANALYTICS_DIR,
    GOLD_TRIPS_DIR,
    REPORTS_ROOT,
    AnomalyThresholds,
    PipelineConfig,
)
from nyc_taxi.spark import timed, write_evidence

# The interquartile range of a standard normal. Dividing a group's IQR by it converts a
# robust spread into something on the same scale as a standard deviation, so a threshold of
# 5 still reads as five sigmas rather than as an arbitrary constant.
IQR_TO_SIGMA = 1.349

# Accuracy argument for percentile_approx, which trades memory for precision. Spark builds
# one sketch per group per column, and this pipeline asks for three percentiles across
# roughly 6,600 peer groups, so the sketches are the memory cost of the whole stage. At
# 1000 the stage thrashed and stalled on an 8 GB machine. At 100 the relative error on a
# median is around one percent, a few cents on a typical fare, which is far finer than any
# decision made from it, and the stage completes comfortably.
PERCENTILE_ACCURACY = 100


def read_gold_trips(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(str(GOLD_TRIPS_DIR))


def read_zone_hour_demand(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(str(GOLD_ANALYTICS_DIR / "zone_hour_demand"))


def _band(column: str, edges: tuple[float, ...]) -> "F.Column":
    """Bucket a numeric column into left closed, right open labelled bands."""
    expr = F.lit(f"{edges[-2]:g}+")
    for i in range(len(edges) - 2, 0, -1):
        low, high = edges[i - 1], edges[i]
        expr = F.when(F.col(column) < high, F.lit(f"{low:g}-{high:g}")).otherwise(expr)
    return expr


def distance_band(thresholds: AnomalyThresholds) -> "F.Column":
    """Bucket a trip by distance, so fares are compared against trips of similar length.

    A flat rate per mile screen would flag most short trips, where the initial charge
    dominates, and miss genuinely overcharged long ones.
    """
    return _band("trip_distance_miles", thresholds.distance_band_edges)


def duration_band(thresholds: AnomalyThresholds) -> "F.Column":
    """Bucket a trip by how long it took.

    Without this the screen is measuring traffic. The New York meter charges by time
    whenever the cab is moving slowly, so two trips of identical distance can carry very
    different legitimate fares depending on the hour. The first version of this screen
    omitted duration and flagged 2.73 percent of all sub one mile trips as a result.
    """
    return _band("trip_duration_minutes", thresholds.duration_band_edges)


def with_peer_bands(trips: DataFrame, thresholds: AnomalyThresholds) -> DataFrame:
    return trips.withColumn("distance_band", distance_band(thresholds)).withColumn(
        "duration_band", duration_band(thresholds)
    )


# The columns that together define a peer group.
PEER_KEYS = ["pickup_location_id", "distance_band", "duration_band"]


def peer_group_statistics(trips: DataFrame, thresholds: AnomalyThresholds) -> DataFrame:
    """Robust centre and spread of fare, per peer group, in a single pass.

    The spread is the interquartile range rather than the median absolute deviation, and
    the reason is the shape of the computation rather than the statistics.

    A MAD is defined in terms of the median it is measured against, so it cannot be
    computed alongside that median. It needs the median first, then a second full pass over
    all 44 million rows to measure every row's deviation from it, then a second grouped
    percentile. The quartiles come out of the same sketch as the median, so the whole thing
    is one aggregation and one scan.

    Both are standard robust scale estimators and both ignore the tails that a standard
    deviation would swallow. MAD has the higher breakdown point, 50 percent against 25, and
    that margin does not matter here: this screen is looking for a fraction of a percent of
    unusual fares, nowhere near a quarter of any peer group. Halving the work does matter.

    IQR is divided by 1.349 because that is the interquartile range of a standard normal,
    which puts the result on the same scale as a standard deviation, so the threshold still
    reads as a number of sigmas.
    """
    banded = with_peer_bands(trips, thresholds)

    quartiles = F.expr(
        f"percentile_approx(fare_amount, array(0.25, 0.5, 0.75), {PERCENTILE_ACCURACY})"
    )

    return (
        banded.groupBy(*PEER_KEYS)
        .agg(
            quartiles.alias("fare_quartiles"),
            F.count(F.lit(1)).alias("peer_group_size"),
            F.avg("fare_amount").alias("peer_mean_fare"),
            F.expr(f"percentile_approx(fare_per_mile, 0.5, {PERCENTILE_ACCURACY})").alias(
                "peer_median_fare_per_mile"
            ),
        )
        .select(
            *PEER_KEYS,
            F.col("fare_quartiles")[0].alias("peer_q1_fare"),
            F.col("fare_quartiles")[1].alias("peer_median_fare"),
            F.col("fare_quartiles")[2].alias("peer_q3_fare"),
            "peer_group_size",
            "peer_mean_fare",
            "peer_median_fare_per_mile",
        )
        .withColumn(
            "peer_iqr_fare", F.col("peer_q3_fare") - F.col("peer_q1_fare")
        )
        .withColumn("peer_robust_sigma", F.col("peer_iqr_fare") / IQR_TO_SIGMA)
    )


def flag_fare_anomalies(
    trips: DataFrame, stats: DataFrame, thresholds: AnomalyThresholds
) -> DataFrame:
    """Attach the peer comparison and the anomaly flag to every trip."""
    banded = with_peer_bands(trips, thresholds)
    joined = banded.join(F.broadcast(stats), on=PEER_KEYS, how="left")

    # The scale is the group's robust spread, floored twice. See AnomalyThresholds for why:
    # flat rate airport groups have an interquartile range of exactly zero, every fare in
    # them being identical, and without a floor any fare that is not precisely the flat
    # rate scores as extreme.
    scale = F.greatest(
        F.col("peer_robust_sigma"),
        F.lit(thresholds.relative_scale_floor) * F.col("peer_median_fare"),
        F.lit(thresholds.min_scale),
    )
    score = (F.col("fare_amount") - F.col("peer_median_fare")) / scale

    scored = joined.withColumn("fare_robust_z", F.round(score, 3)).withColumn(
        "peer_group_scored",
        F.col("peer_group_size") >= F.lit(thresholds.min_peer_group_size),
    )

    return scored.withColumn(
        "fare_anomaly_flag",
        F.col("peer_group_scored")
        & (F.abs(F.col("fare_robust_z")) > F.lit(thresholds.robust_z_threshold)),
    ).withColumn(
        "fare_anomaly_direction",
        F.when(~F.col("fare_anomaly_flag"), F.lit(None).cast("string"))
        .when(F.col("fare_robust_z") > 0, F.lit("above_peer_group"))
        .otherwise(F.lit("below_peer_group")),
    )


def trip_weighted_avg(column: str) -> "F.Column":
    """Weighted mean of a per zone hour average, weighted by that slot's trip count.

    The zone hour table stores averages, so a plain avg() over it is the mean of means.
    That silently gives a zone with 3 trips in an hour the same say as one with 3,000, and
    the answer stops being the average fare a passenger paid. Weighting by trips restores
    the figure the question was actually asking for.
    """
    return F.sum(F.col(column) * F.col("trips")) / F.sum("trips")


def demand_by_zone_hour_dow(demand: DataFrame) -> DataFrame:
    """Average demand profile by zone, hour of day and day of week."""
    return (
        demand.groupBy(
            "pickup_location_id", "pickup_zone", "pickup_borough", "pickup_day_of_week", "pickup_hour"
        )
        .agg(
            F.sum("trips").alias("total_trips"),
            F.avg("trips").alias("avg_trips_per_hour_slot"),
            trip_weighted_avg("avg_fare").alias("avg_fare"),
            F.sum("total_revenue").alias("total_revenue"),
        )
        .withColumn("avg_trips_per_hour_slot", F.round("avg_trips_per_hour_slot", 2))
        .withColumn("avg_fare", F.round("avg_fare", 2))
        .withColumn("total_revenue", F.round("total_revenue", 2))
    )


def _write_report_csv(df: DataFrame, name: str, limit: int | None = None) -> str:
    """Write a small aggregate to reports/ as a single readable CSV.

    Only aggregates land here. These files are small enough to commit, which is what lets
    every figure quoted in the README be traced back to an artefact of an actual run.

    Doubles are cast to decimal on the way out. Spark's CSV writer renders a large double
    in scientific notation, so a revenue total lands in the file as 6.6314362998E8. These
    files exist to be opened and read by a person, and that is not a readable number.
    """
    readable = df
    for field in df.schema.fields:
        if field.dataType.simpleString() == "double":
            readable = readable.withColumn(field.name, F.col(field.name).cast("decimal(18,2)"))

    tables = REPORTS_ROOT / "tables"
    staging = tables / f"_{name}"
    frame = readable.limit(limit) if limit else readable
    frame.coalesce(1).write.mode("overwrite").option("header", True).csv(str(staging))

    # Spark writes a directory of part files plus its own markers. These are small
    # aggregates meant to be opened and read, and committed alongside the README they back
    # up, so the single part file is lifted out to reports/tables/<name>.csv and the
    # scaffolding is dropped.
    destination = tables / f"{name}.csv"
    part = next(staging.glob("part-*.csv"), None)
    if part is None:
        raise RuntimeError(f"no part file written for report {name}")
    destination.unlink(missing_ok=True)
    part.rename(destination)
    shutil.rmtree(staging, ignore_errors=True)
    return str(destination)


def run_analytics(spark: SparkSession, cfg: PipelineConfig) -> dict:
    timings: dict = {}
    metrics: dict = {"stage": "analytics"}
    trips = read_gold_trips(spark)
    demand = read_zone_hour_demand(spark)

    # Demand patterns.
    with timed("analytics: demand profile", timings):
        profile = demand_by_zone_hour_dow(demand)
        _write_report_csv(profile, "demand_by_zone_hour_dow")

        hour_of_day = (
            demand.groupBy("pickup_hour")
            .agg(
                F.sum("trips").alias("total_trips"),
                F.round(trip_weighted_avg("avg_fare"), 2).alias("avg_fare"),
                F.round(trip_weighted_avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
                F.round(trip_weighted_avg("avg_distance_miles"), 2).alias("avg_distance_miles"),
            )
            .orderBy("pickup_hour")
        )
        _write_report_csv(hour_of_day, "demand_by_hour_of_day")
        metrics["demand_by_hour"] = [r.asDict() for r in hour_of_day.collect()]

        dow = (
            demand.groupBy("pickup_day_of_week")
            .agg(
                F.sum("trips").alias("total_trips"),
                F.round(trip_weighted_avg("avg_fare"), 2).alias("avg_fare"),
                F.round(trip_weighted_avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
            )
            .orderBy("pickup_day_of_week")
        )
        _write_report_csv(dow, "demand_by_day_of_week")
        metrics["demand_by_day_of_week"] = [r.asDict() for r in dow.collect()]

        top_zones = (
            demand.groupBy("pickup_location_id", "pickup_zone", "pickup_borough")
            .agg(
                F.sum("trips").alias("total_trips"),
                F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
                F.round(trip_weighted_avg("avg_fare"), 2).alias("avg_fare"),
            )
            .orderBy(F.desc("total_trips"))
        )
        _write_report_csv(top_zones, "top_pickup_zones")
        metrics["top_pickup_zones"] = [r.asDict() for r in top_zones.limit(15).collect()]

        borough = (
            demand.groupBy("pickup_borough")
            .agg(
                F.sum("trips").alias("total_trips"),
                F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            )
            .orderBy(F.desc("total_trips"))
        )
        _write_report_csv(borough, "demand_by_borough")
        metrics["demand_by_borough"] = [r.asDict() for r in borough.collect()]

        peak = (
            demand.groupBy("pickup_day_of_week", "pickup_hour")
            .agg(F.sum("trips").alias("total_trips"))
            .orderBy(F.desc("total_trips"))
        )
        _write_report_csv(peak, "demand_by_dow_hour")
        metrics["busiest_dow_hour_slots"] = [r.asDict() for r in peak.limit(10).collect()]

    # Fare anomaly screen.
    with timed("analytics: peer group statistics", timings):
        stats = peer_group_statistics(trips, cfg.anomaly).cache()
        metrics["peer_groups"] = stats.count()
        _write_report_csv(stats.orderBy(F.desc("peer_group_size")), "fare_peer_group_stats")

    with timed("analytics: fare anomaly screen", timings):
        scored = flag_fare_anomalies(trips, stats, cfg.anomaly)

        # Every headline figure comes out of this one grouped pass. Aggregating by band
        # and summing the bands on the driver gives the same totals as a separate overall
        # aggregation would, for one scan instead of two, and removes any chance of the
        # two disagreeing.
        band_rows = (
            scored.groupBy("distance_band")
            .agg(
                F.count(F.lit(1)).alias("trips"),
                F.sum(F.when(F.col("peer_group_scored"), 1).otherwise(0)).alias("trips_in_scored_groups"),
                F.sum(F.when(F.col("fare_anomaly_flag"), 1).otherwise(0)).alias("flagged"),
                F.sum(F.when(F.col("fare_anomaly_direction") == "above_peer_group", 1).otherwise(0)).alias(
                    "flagged_above_peers"
                ),
                F.sum(F.when(F.col("fare_anomaly_direction") == "below_peer_group", 1).otherwise(0)).alias(
                    "flagged_below_peers"
                ),
            )
            .orderBy("distance_band")
            .collect()
        )

        summary = {
            "trips_scored_input": sum(r["trips"] for r in band_rows),
            "trips_in_scored_groups": sum(r["trips_in_scored_groups"] for r in band_rows),
            "trips_flagged": sum(r["flagged"] for r in band_rows),
            "flagged_above_peers": sum(r["flagged_above_peers"] for r in band_rows),
            "flagged_below_peers": sum(r["flagged_below_peers"] for r in band_rows),
        }
        total = summary["trips_scored_input"]
        summary["flagged_pct_of_gold"] = round(100.0 * summary["trips_flagged"] / total, 4) if total else 0.0
        metrics["fare_anomaly_summary"] = summary
        metrics["fare_anomaly_rate_by_band"] = [
            {
                "distance_band": r["distance_band"],
                "trips": r["trips"],
                "flagged": r["flagged"],
                "flagged_pct": round(100.0 * r["flagged"] / r["trips"], 4) if r["trips"] else 0.0,
            }
            for r in band_rows
        ]

        # The flagged set is a few thousand rows out of 44 million. Materialising it once
        # means the zone, month and example tables below are built from memory rather than
        # from three more scans of the fact table.
        anomalies = scored.filter(F.col("fare_anomaly_flag")).cache()
        metrics["fare_anomaly_rows_materialised"] = anomalies.count()
        by_zone = (
            anomalies.groupBy("pickup_location_id", "pickup_zone", "pickup_borough")
            .agg(
                F.count(F.lit(1)).alias("flagged_trips"),
                F.round(F.avg("fare_per_mile"), 2).alias("avg_flagged_fare_per_mile"),
                F.round(F.avg("peer_median_fare"), 2).alias("avg_peer_median_fare"),
                F.round(F.max("fare_amount"), 2).alias("max_flagged_fare"),
            )
            .orderBy(F.desc("flagged_trips"))
        )
        _write_report_csv(by_zone, "fare_anomalies_by_zone")
        metrics["fare_anomalies_top_zones"] = [r.asDict() for r in by_zone.limit(15).collect()]

        by_month = (
            anomalies.groupBy("pickup_month")
            .agg(F.count(F.lit(1)).alias("flagged_trips"))
            .orderBy("pickup_month")
        )
        _write_report_csv(by_month, "fare_anomalies_by_month")
        metrics["fare_anomalies_by_month"] = [r.asDict() for r in by_month.collect()]

        examples = (
            anomalies.select(
                "pickup_date",
                "pickup_zone",
                "dropoff_zone",
                "distance_band",
                "duration_band",
                "trip_distance_miles",
                "trip_duration_minutes",
                "fare_amount",
                "peer_median_fare",
                "peer_group_size",
                "fare_per_mile",
                "fare_robust_z",
                "payment_type",
            )
            .orderBy(F.desc("fare_robust_z"))
            .limit(25)
        )
        _write_report_csv(examples, "fare_anomaly_examples")
        metrics["fare_anomaly_examples"] = [r.asDict() for r in examples.collect()]

        band_rate_frame = spark.createDataFrame(
            [
                (r["distance_band"], r["trips"], r["flagged"], r["flagged_pct"])
                for r in metrics["fare_anomaly_rate_by_band"]
            ],
            "distance_band string, trips long, flagged long, flagged_pct double",
        )
        _write_report_csv(band_rate_frame, "fare_anomaly_rate_by_band")

    anomalies.unpersist()
    stats.unpersist()

    # Data quality trend across months, read back from the ledger the silver stage wrote.
    with timed("analytics: quality trend", timings):
        metrics["quality_trend"] = _quality_trend()

    metrics["timings_seconds"] = timings
    write_evidence("analytics_metrics", metrics)
    return metrics


def _quality_trend() -> dict:
    """Reshape the silver ledger's per month figures into a trend view."""
    import json

    ledger_path = REPORTS_ROOT / "evidence" / "silver_ledger.json"
    if not ledger_path.exists():
        return {"available": False, "reason": f"{ledger_path} not found, run the silver stage"}

    ledger = json.loads(ledger_path.read_text())["ledger"]
    per_month = ledger["per_source_month"]
    months = sorted(per_month)

    rule_ids = sorted({rid for m in per_month.values() for rid in m["flagged_pct"]})
    trend = {
        rid: {month: per_month[month]["flagged_pct"].get(rid, 0.0) for month in months}
        for rid in rule_ids
    }

    movements = {}
    for rid, series in trend.items():
        first, last = series[months[0]], series[months[-1]]
        movements[rid] = {
            "first_month": months[0],
            "first_month_pct": first,
            "last_month": months[-1],
            "last_month_pct": last,
            "change_pct_points": round(last - first, 6),
            "relative_change_pct": round(100.0 * (last - first) / first, 2) if first else None,
        }

    return {
        "available": True,
        "months": months,
        "excluded_pct_by_month": {m: per_month[m]["excluded_pct"] for m in months},
        "flagged_pct_by_rule_by_month": trend,
        "movement_first_to_last_month": movements,
    }
