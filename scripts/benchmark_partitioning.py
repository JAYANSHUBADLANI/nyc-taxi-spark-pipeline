"""Measure the file layout and partitioning decisions instead of asserting them.

Three experiments, all against the real data:

  1. read parallelism, the source layout against the bronze layout
  2. write strategy, a repartition shuffle against splitting files on write
  3. shuffle cost, pulled from the live Spark UI's own REST API

The Spark UI numbers are read from http://localhost:4040/api/v1 while the session is
still alive. That is the same data the Stages tab renders, captured in a form that can be
committed to the repository and re-derived by anyone running this.

Run: make benchmark
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from nyc_taxi.config import BRONZE_DIR, DEFAULT_CONFIG, DATA_ROOT, RAW_TRIPS_DIR  # noqa: E402
from nyc_taxi.schemas import YELLOW_TRIP_SCHEMA  # noqa: E402
from nyc_taxi.spark import (  # noqa: E402
    build_spark,
    capture_explain,
    directory_stats,
    write_evidence,
    write_text_evidence,
)

BENCH_DIR = DATA_ROOT / "benchmark"


def spark_ui_stages(spark: SparkSession) -> list[dict]:
    """Read completed stage metrics from the running Spark UI's REST API."""
    app_id = spark.sparkContext.applicationId
    url = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{app_id}/stages"
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            return json.loads(response.read())
    except Exception as exc:  # the UI is best effort, the benchmark still has wall times
        print(f"  could not reach the Spark UI REST API: {exc}")
        return []


def stage_metrics_since(spark: SparkSession, known_ids: set[int]) -> tuple[dict, set[int]]:
    """Aggregate the metrics of every stage that completed since the last checkpoint."""
    stages = spark_ui_stages(spark)
    fresh = [s for s in stages if s.get("stageId") not in known_ids and s.get("status") == "COMPLETE"]
    totals = {
        "stages": len(fresh),
        "tasks": sum(s.get("numCompleteTasks", 0) for s in fresh),
        "shuffle_write_bytes": sum(s.get("shuffleWriteBytes", 0) for s in fresh),
        "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in fresh),
        "input_bytes": sum(s.get("inputBytes", 0) for s in fresh),
        "spill_disk_bytes": sum(s.get("diskBytesSpilled", 0) for s in fresh),
        "executor_run_time_ms": sum(s.get("executorRunTime", 0) for s in fresh),
    }
    totals["shuffle_write_mb"] = round(totals["shuffle_write_bytes"] / 1024**2, 1)
    totals["shuffle_read_mb"] = round(totals["shuffle_read_bytes"] / 1024**2, 1)
    totals["spill_disk_mb"] = round(totals["spill_disk_bytes"] / 1024**2, 1)
    return totals, {s.get("stageId") for s in stages}


def timed_action(label: str, action) -> tuple[float, object]:
    start = time.perf_counter()
    result = action()
    elapsed = round(time.perf_counter() - start, 2)
    print(f"  {label}: {elapsed:,.2f}s")
    return elapsed, result


def experiment_read_parallelism(spark: SparkSession) -> dict:
    """The same aggregation over the source layout and over the bronze layout.

    The source files hold one Parquet row group each, and a row group is the smallest
    unit Spark can hand to a task. Six files therefore means six tasks, on any machine,
    with any number of cores. Bronze rewrites the same rows into more files.
    """
    print("\nExperiment 1: read parallelism")

    def aggregate(df):
        return lambda: df.groupBy("PULocationID").agg(
            F.count(F.lit(1)).alias("trips"), F.sum("fare_amount").alias("revenue")
        ).collect()

    source = spark.read.schema(YELLOW_TRIP_SCHEMA).parquet(str(RAW_TRIPS_DIR))
    bronze = spark.read.parquet(str(BRONZE_DIR))

    source_partitions = source.rdd.getNumPartitions()
    bronze_partitions = bronze.rdd.getNumPartitions()

    # Planned partition count overstates the real parallelism on the source layout. Spark
    # plans splits by byte range, but a Parquet row group cannot be divided, so a task
    # whose range holds no row group midpoint reads nothing. Counting rows per partition
    # shows how many tasks actually carry work.
    def partitions_with_rows(df) -> dict:
        counts = (
            df.groupBy(F.spark_partition_id().alias("partition"))
            .agg(F.count(F.lit(1)).alias("rows"))
            .collect()
        )
        non_empty = [r["rows"] for r in counts if r["rows"] > 0]
        return {
            "partitions_carrying_rows": len(non_empty),
            "max_rows_in_one_partition": max(non_empty) if non_empty else 0,
            "min_rows_in_one_partition": min(non_empty) if non_empty else 0,
        }

    source_distribution = partitions_with_rows(source.select("PULocationID"))
    bronze_distribution = partitions_with_rows(bronze.select("PULocationID"))

    # Warm the file system cache equally for both sides before timing.
    source.select("PULocationID").limit(1).collect()
    bronze.select("PULocationID").limit(1).collect()

    source_seconds, _ = timed_action("source layout", aggregate(source))
    bronze_seconds, _ = timed_action("bronze layout", aggregate(bronze))

    return {
        "source_layout": {
            "files": len(list(RAW_TRIPS_DIR.glob("*.parquet"))),
            "spark_partitions_planned": source_partitions,
            **source_distribution,
            "aggregation_seconds": source_seconds,
            "stats": directory_stats(RAW_TRIPS_DIR),
        },
        "bronze_layout": {
            "spark_partitions_planned": bronze_partitions,
            **bronze_distribution,
            "aggregation_seconds": bronze_seconds,
            "stats": directory_stats(BRONZE_DIR),
        },
        "speedup": round(source_seconds / bronze_seconds, 2) if bronze_seconds else None,
    }


def experiment_write_strategy(spark: SparkSession) -> dict:
    """Two ways to turn six big files into many smaller ones, priced against each other.

    Both produce a comparable output layout. Only one of them moves every row across the
    network to do it. Run on a single month so the experiment is cheap enough to repeat.
    """
    print("\nExperiment 2: write strategy, one month")
    one_month = sorted(RAW_TRIPS_DIR.glob("*.parquet"))[0]
    df = spark.read.schema(YELLOW_TRIP_SCHEMA).parquet(str(one_month))
    rows = df.count()
    target_files = 6
    print(f"  input {one_month.name}, {rows:,} rows")

    _, seen = stage_metrics_since(spark, set())

    shuffle_path = BENCH_DIR / "write_repartition"
    shuffle_seconds, _ = timed_action(
        "repartition then write",
        lambda: df.repartition(target_files).write.mode("overwrite").parquet(str(shuffle_path)),
    )
    shuffle_stage_metrics, seen = stage_metrics_since(spark, seen)

    split_path = BENCH_DIR / "write_max_records"
    split_seconds, _ = timed_action(
        "maxRecordsPerFile write",
        lambda: df.write.mode("overwrite")
        .option("maxRecordsPerFile", rows // target_files + 1)
        .parquet(str(split_path)),
    )
    split_stage_metrics, seen = stage_metrics_since(spark, seen)

    return {
        "input_rows": rows,
        "repartition_shuffle": {
            "seconds": shuffle_seconds,
            "spark_ui_metrics": shuffle_stage_metrics,
            "output": directory_stats(shuffle_path),
        },
        "max_records_per_file": {
            "seconds": split_seconds,
            "spark_ui_metrics": split_stage_metrics,
            "output": directory_stats(split_path),
        },
    }


def experiment_partition_pruning(spark: SparkSession, probe_date: str = "2019-03-13") -> dict:
    """Price an identical single day query against both gold layouts.

    The measurement is Spark's own stage level inputBytes counter, read back from the UI
    REST API. An earlier version of this used DataFrame.inputFiles() and produced numbers
    that were confidently backwards: inputFiles() reports every file belonging to the
    relation, before any partition pruning is applied, so the partitioned table appeared to
    open 181 files against the baseline's 39. Bytes actually read is the honest measure and
    it is what the Stages tab shows.
    """
    print("\nExperiment 4: partition pruning")
    from nyc_taxi.config import GOLD_TRIPS_DIR, GOLD_TRIPS_UNPARTITIONED_DIR

    results = {"probe_date": probe_date}
    for label, path in (
        ("partitioned_by_pickup_date", GOLD_TRIPS_DIR),
        ("unpartitioned", GOLD_TRIPS_UNPARTITIONED_DIR),
    ):
        if not path.exists():
            results[label] = {"skipped": f"{path} not present"}
            continue

        df = spark.read.parquet(str(path))
        filtered = df.filter(F.col("pickup_date") == F.lit(probe_date))
        plan = capture_explain(filtered)

        _, seen = stage_metrics_since(spark, set())
        seconds, rows = timed_action(label, lambda f=filtered: f.count())
        stage_totals, _ = stage_metrics_since(spark, seen)

        results[label] = {
            "rows_returned": rows,
            "seconds": seconds,
            "input_bytes_read": stage_totals["input_bytes"],
            "input_mb_read": round(stage_totals["input_bytes"] / 1024**2, 2),
            "tasks": stage_totals["tasks"],
            "plan_uses_partition_filter": "PartitionFilters: [isnotnull(pickup_date" in plan
            or "PartitionFilters: [(pickup_date" in plan,
            "layout": directory_stats(path),
        }

    both = [results[k] for k in results if isinstance(results[k], dict) and "input_bytes_read" in results[k]]
    if len(both) == 2:
        partitioned, baseline = both
        results["bytes_read_ratio"] = (
            round(baseline["input_bytes_read"] / partitioned["input_bytes_read"], 2)
            if partitioned["input_bytes_read"]
            else None
        )
    write_text_evidence("pruning_plans.txt", plan)
    return results


def experiment_join_shuffle_cost(spark: SparkSession) -> dict:
    """Price the zone join both ways, using the Spark UI's own shuffle counters."""
    print("\nExperiment 3: broadcast join against sort merge join")
    from nyc_taxi.bronze import read_zone_lookup
    from nyc_taxi.config import SILVER_DIR

    if not SILVER_DIR.exists():
        print("  silver layer not built yet, skipping")
        return {"skipped": "silver layer not present"}

    # One month rather than all six. A sort merge join of the full table against a 265 row
    # dimension shuffles several gigabytes, which is the point being made and also more
    # than this machine will absorb without swapping. A single month makes the same
    # comparison at a sixth of the cost, and both sides of the comparison pay it equally.
    benchmark_month = "2019-03"
    trips = (
        spark.read.parquet(str(SILVER_DIR))
        .filter(F.col("source_month") == benchmark_month)
        .select("pickup_location_id", "fare_amount", "trip_distance_miles")
    )
    zones = read_zone_lookup(spark).select(
        F.col("location_id").alias("pickup_location_id"), "borough"
    )

    def run(join_df):
        return lambda: join_df.groupBy("borough").agg(F.count(F.lit(1))).collect()

    _, seen = stage_metrics_since(spark, set())

    broadcast_seconds, _ = timed_action(
        "broadcast join", run(trips.join(F.broadcast(zones), on="pickup_location_id"))
    )
    broadcast_metrics, seen = stage_metrics_since(spark, seen)

    original = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        smj_seconds, _ = timed_action(
            "sort merge join", run(trips.join(zones, on="pickup_location_id"))
        )
        smj_metrics, seen = stage_metrics_since(spark, seen)
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", original)

    return {
        "scope": f"single month {benchmark_month}, both strategies measured on the same rows",
        "broadcast": {"seconds": broadcast_seconds, "spark_ui_metrics": broadcast_metrics},
        "sort_merge": {"seconds": smj_seconds, "spark_ui_metrics": smj_metrics},
        "shuffle_bytes_avoided": smj_metrics["shuffle_write_bytes"]
        - broadcast_metrics["shuffle_write_bytes"],
        "shuffle_mb_avoided": round(
            (smj_metrics["shuffle_write_bytes"] - broadcast_metrics["shuffle_write_bytes"]) / 1024**2,
            1,
        ),
    }


def main() -> int:
    cfg = DEFAULT_CONFIG
    spark = build_spark(cfg.spark, app_suffix="benchmark", keep_ui_alive=True)
    print(f"Spark UI at {spark.sparkContext.uiWebUrl}")

    results = {
        "spark_version": spark.version,
        "master": cfg.spark.master,
        "driver_memory": cfg.spark.driver_memory,
        "read_parallelism": experiment_read_parallelism(spark),
        "write_strategy": experiment_write_strategy(spark),
        "partition_pruning": experiment_partition_pruning(spark),
        "join_shuffle_cost": experiment_join_shuffle_cost(spark),
    }

    write_evidence("partitioning_benchmark", results)
    spark.stop()

    import shutil

    shutil.rmtree(BENCH_DIR, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
