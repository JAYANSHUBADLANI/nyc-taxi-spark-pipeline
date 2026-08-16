"""Bronze layer: ingest the raw TLC files under an enforced schema contract.

Bronze answers one question only, is this record structurally usable. Business judgement
about whether a trip is plausible belongs to silver. The split keeps each layer's failure
modes separable when something goes wrong in production.

What bronze does:
  1. checks every source file against the declared schema contract and stops on an unsafe
     difference rather than coercing silently
  2. stamps provenance, the source file and the month the file claims to cover
  3. routes structurally unusable records to a rejected path with the reason attached, and
     counts them, so nothing disappears without a number against it
  4. repartitions, because each source file holds a single Parquet row group and would
     otherwise pin the whole load to one task per file
"""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nyc_taxi.config import (
    BRONZE_DIR,
    BRONZE_REJECTED_DIR,
    BRONZE_ZONES_DIR,
    RAW_TRIPS_DIR,
    ZONE_LOOKUP_CSV,
    PipelineConfig,
)
from nyc_taxi.schemas import (
    YELLOW_TRIP_SCHEMA,
    ZONE_LOOKUP_SCHEMA,
    assert_contract,
)
from nyc_taxi.spark import directory_stats, timed, write_evidence

MONTH_PATTERN = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$")

# A record whose pickup falls outside the loaded window cannot belong to any file that was
# pulled, so it cannot be reconciled against a source month. The window is derived from the
# configured month list.
REJECT_REASON_OUT_OF_WINDOW = "pickup_outside_load_window"
REJECT_REASON_NULL_KEY = "null_structural_key"


def _load_window(months: list[str]) -> tuple[str, str]:
    """Inclusive start and exclusive end of the loaded period."""
    first, last = min(months), max(months)
    year, month = (int(p) for p in last.split("-"))
    end_year, end_month = (year + 1, 1) if month == 12 else (year, month + 1)
    return f"{first}-01 00:00:00", f"{end_year}-{end_month:02d}-01 00:00:00"


def check_source_contracts(spark: SparkSession, trips_dir: Path = RAW_TRIPS_DIR) -> dict:
    """Validate every source file's own schema against the contract before reading data."""
    files = sorted(trips_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"no trip files in {trips_dir}. Run `make download` first."
        )
    reports = {}
    for path in files:
        inferred = spark.read.parquet(str(path)).schema
        reports[path.name] = assert_contract(inferred, source=path.name)
    return reports


def read_raw_trips(spark: SparkSession, trips_dir: Path = RAW_TRIPS_DIR) -> DataFrame:
    """Read the raw files under the declared schema, never under inference."""
    return (
        spark.read.schema(YELLOW_TRIP_SCHEMA)
        .parquet(str(trips_dir))
        .withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
        # Widened after the read rather than during it, so the stored bronze schema matches
        # later years where this field carries real money. See schemas.py.
        .withColumn("airport_fee", F.col("airport_fee").cast("double"))
    )


def add_provenance(df: DataFrame) -> DataFrame:
    """Attach the month each source file claims to cover and the month the row is in."""
    file_month = F.regexp_extract(F.col("source_file"), r"yellow_tripdata_(\d{4}-\d{2})", 1)
    return df.withColumn("source_month", file_month).withColumn(
        "pickup_month", F.date_format("tpep_pickup_datetime", "yyyy-MM")
    )


def classify_structural_validity(df: DataFrame, months: list[str]) -> DataFrame:
    """Label each row with a rejection reason, or null when structurally usable."""
    window_start, window_end = _load_window(months)
    pickup = F.col("tpep_pickup_datetime")

    null_key = (
        pickup.isNull()
        | F.col("tpep_dropoff_datetime").isNull()
        | F.col("PULocationID").isNull()
        | F.col("DOLocationID").isNull()
    )
    out_of_window = (pickup < F.lit(window_start).cast("timestamp")) | (
        pickup >= F.lit(window_end).cast("timestamp")
    )

    reason = (
        F.when(null_key, F.lit(REJECT_REASON_NULL_KEY))
        .when(out_of_window, F.lit(REJECT_REASON_OUT_OF_WINDOW))
        .otherwise(F.lit(None).cast("string"))
    )
    return df.withColumn("reject_reason", reason)


def run_bronze(spark: SparkSession, cfg: PipelineConfig, write: bool = True) -> dict:
    """Execute the bronze stage and return the metrics it measured."""
    timings: dict = {}
    metrics: dict = {"stage": "bronze"}

    with timed("bronze: schema contract check", timings):
        metrics["schema_contract"] = check_source_contracts(spark)

    raw = add_provenance(read_raw_trips(spark))
    classified = classify_structural_validity(raw, cfg.months)

    # Read parallelism before any repartition. Each source file carries a single Parquet
    # row group, so this is one partition per file no matter how many cores exist.
    partitions_on_read = raw.rdd.getNumPartitions()

    # One scan produces the whole bronze count set: rows per source month, rows per
    # rejection reason, and cross month drift. The dataset is deliberately not cached,
    # 44.7 million rows across 19 columns does not fit in the storage memory of a local
    # session and the resulting spill costs more than re-scanning Parquet.
    with timed("bronze: classify and count", timings):
        by_reason = (
            classified.groupBy(
                "source_month",
                "reject_reason",
                (F.col("pickup_month") != F.col("source_month")).alias("month_drift"),
            )
            .agg(F.count(F.lit(1)).alias("rows"))
            .collect()
        )

    per_month: dict = {}
    total_raw = 0
    total_rejected = 0
    drift = 0
    reject_reasons: dict = {}
    for row in by_reason:
        month, reason, n = row["source_month"], row["reject_reason"], row["rows"]
        total_raw += n
        bucket = per_month.setdefault(month, {"raw_rows": 0, "accepted_rows": 0, "rejected_rows": 0})
        bucket["raw_rows"] += n
        if reason is None:
            bucket["accepted_rows"] += n
            # Rows sitting in a file covering a different month than their own pickup
            # date. Kept, they are valid trips inside the load window, but measured
            # because this is what quietly duplicates rows in a naive month by month load.
            if row["month_drift"]:
                drift += n
        else:
            bucket["rejected_rows"] += n
            total_rejected += n
            reject_reasons[reason] = reject_reasons.get(reason, 0) + n

    accepted = classified.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = classified.filter(F.col("reject_reason").isNotNull())

    if write:
        # maxRecordsPerFile rather than repartition, and the difference is not cosmetic.
        # The goal is more output files so later stages can scan wide, and a repartition
        # buys that by pushing all 44.7 million rows through a network shuffle. Splitting
        # on write reaches the same layout with no exchange at all: each task simply rolls
        # to a new file when it hits the row cap. The measured comparison is in
        # docs/partitioning.md.
        with timed("bronze: write accepted", timings):
            accepted.write.mode("overwrite").option(
                "maxRecordsPerFile", cfg.spark.bronze_max_rows_per_file
            ).parquet(str(BRONZE_DIR))
        with timed("bronze: write rejected", timings):
            rejected.coalesce(1).write.mode("overwrite").parquet(str(BRONZE_REJECTED_DIR))
        with timed("bronze: write zone reference", timings):
            read_zone_lookup(spark).coalesce(1).write.mode("overwrite").parquet(
                str(BRONZE_ZONES_DIR)
            )

    metrics.update(
        {
            "raw_rows": total_raw,
            "accepted_rows": total_raw - total_rejected,
            "rejected_rows": total_rejected,
            "rejected_pct": round(100.0 * total_rejected / total_raw, 6) if total_raw else 0.0,
            "reject_reasons": reject_reasons,
            "per_source_month": dict(sorted(per_month.items())),
            "cross_month_file_drift_rows": drift,
            "partitions_on_read": partitions_on_read,
            "max_rows_per_output_file": cfg.spark.bronze_max_rows_per_file,
            "timings_seconds": timings,
        }
    )
    if write:
        metrics["output"] = {
            "accepted": directory_stats(BRONZE_DIR),
            "rejected": directory_stats(BRONZE_REJECTED_DIR),
        }

    write_evidence("bronze_metrics", metrics)
    return metrics


def read_zone_lookup(spark: SparkSession, path: Path = ZONE_LOOKUP_CSV) -> DataFrame:
    """Read the zone dimension under an explicit schema, no inference."""
    if not path.exists():
        raise FileNotFoundError(f"zone lookup missing at {path}. Run `make download` first.")
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(ZONE_LOOKUP_SCHEMA)
        .csv(str(path))
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone_name")
        .withColumnRenamed("service_zone", "service_zone")
    )
