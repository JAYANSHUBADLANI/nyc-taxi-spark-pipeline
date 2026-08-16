"""Silver layer: apply the business quality rules and account for every row removed.

The output of this stage is two things, a cleaned trip table and a ledger. The ledger is
the part that matters. Anyone can write a filter chain, the discipline is being able to
hand someone a statement that starts at the raw row count, names every rule that removed
rows, and lands exactly on the kept count with no unexplained difference.

The ledger reports two counts per rule and they are different on purpose:

    rows_flagged          every row the rule considers a violation, rules overlap freely
    rows_first_excluded   rows this rule is the owner of, under the documented precedence

Only the second column is additive. A zero distance trip that also has a zero fare and
runs backwards in time is one lost row, not three, and the ledger has to say so.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nyc_taxi.config import (
    BRONZE_DIR,
    SILVER_DIR,
    SILVER_EXCLUDED_DIR,
    PipelineConfig,
    QualityThresholds,
)
from nyc_taxi.rules import (
    REJECT_RULES,
    SILVER_RULES,
    WARN_RULES,
    first_violated_reject_rule,
    implied_speed_mph,
    trip_duration_minutes,
)
from nyc_taxi.spark import directory_stats, timed, write_evidence


class ReconciliationError(RuntimeError):
    """Raised when the exclusion ledger does not add up to the input row count."""


def read_bronze(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(str(BRONZE_DIR))


def with_rule_evaluation(df: DataFrame, thresholds: QualityThresholds) -> DataFrame:
    """Attach one boolean per rule plus the owning rule for excluded rows.

    Every rule is evaluated in the same scan. This is the mechanical reason the ledger
    reconciles: the flags and the ownership assignment see identical input rows.
    """
    for rule in SILVER_RULES:
        df = df.withColumn(f"flag_{rule.id}", rule.violation(thresholds).eqNullSafe(True))
    return df.withColumn("owning_rule", first_violated_reject_rule(thresholds))


def build_ledger(df: DataFrame) -> list[dict]:
    """Collect the full ledger in a single shuffle.

    Grouping by source month as well as owning rule means the same aggregation that
    proves the reconciliation also produces the month by month quality trend, rather than
    paying for a second pass over 44 million rows to get it.
    """
    aggregations = [F.count(F.lit(1)).alias("rows")]
    aggregations += [
        F.sum(F.when(F.col(f"flag_{rule.id}"), 1).otherwise(0)).alias(f"flagged_{rule.id}")
        for rule in SILVER_RULES
    ]
    grouped = df.groupBy("source_month", "owning_rule").agg(*aggregations)
    return [row.asDict() for row in grouped.collect()]


def summarise_ledger(ledger_rows: list[dict], bronze_rows: int) -> dict:
    """Turn the raw grouped counts into the reconciliation statement."""
    kept_rows = sum(r["rows"] for r in ledger_rows if r["owning_rule"] == "kept")
    excluded_rows = sum(r["rows"] for r in ledger_rows if r["owning_rule"] != "kept")

    first_excluded = {rule.id: 0 for rule in REJECT_RULES}
    for row in ledger_rows:
        if row["owning_rule"] != "kept":
            first_excluded[row["owning_rule"]] += row["rows"]

    flagged_total = {
        rule.id: sum(r[f"flagged_{rule.id}"] for r in ledger_rows) for rule in SILVER_RULES
    }
    flagged_in_kept = {
        rule.id: sum(
            r[f"flagged_{rule.id}"] for r in ledger_rows if r["owning_rule"] == "kept"
        )
        for rule in SILVER_RULES
    }

    lines = []
    for rule in SILVER_RULES:
        lines.append(
            {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "severity": rule.severity.value,
                "description": rule.description,
                "rows_flagged": flagged_total[rule.id],
                "pct_of_input_flagged": round(100.0 * flagged_total[rule.id] / bronze_rows, 6)
                if bronze_rows
                else 0.0,
                "rows_first_excluded": first_excluded.get(rule.id, 0),
                "rows_flagged_within_kept": flagged_in_kept[rule.id],
            }
        )

    total_first_excluded = sum(first_excluded.values())
    reconciles = (kept_rows + total_first_excluded) == bronze_rows == (kept_rows + excluded_rows)

    # Per month prevalence for the quality trend, expressed against that month's own input
    # so months of different sizes stay comparable.
    per_month: dict = {}
    for row in ledger_rows:
        month = row["source_month"]
        bucket = per_month.setdefault(
            month,
            {"input_rows": 0, "kept_rows": 0, "excluded_rows": 0, "flagged": {r.id: 0 for r in SILVER_RULES}},
        )
        bucket["input_rows"] += row["rows"]
        if row["owning_rule"] == "kept":
            bucket["kept_rows"] += row["rows"]
        else:
            bucket["excluded_rows"] += row["rows"]
        for rule in SILVER_RULES:
            bucket["flagged"][rule.id] += row[f"flagged_{rule.id}"]

    for month, bucket in per_month.items():
        n = bucket["input_rows"]
        bucket["excluded_pct"] = round(100.0 * bucket["excluded_rows"] / n, 4) if n else 0.0
        bucket["flagged_pct"] = {
            rid: round(100.0 * cnt / n, 6) if n else 0.0 for rid, cnt in bucket["flagged"].items()
        }

    return {
        "input_rows": bronze_rows,
        "kept_rows": kept_rows,
        "excluded_rows": excluded_rows,
        "excluded_pct": round(100.0 * excluded_rows / bronze_rows, 4) if bronze_rows else 0.0,
        "sum_of_rows_first_excluded": total_first_excluded,
        "reconciles": reconciles,
        "reconciliation_difference": bronze_rows - (kept_rows + total_first_excluded),
        "lines": lines,
        "per_source_month": dict(sorted(per_month.items())),
    }


def conform(df: DataFrame) -> DataFrame:
    """Rename to the analytical vocabulary and derive the columns gold depends on.

    Bronze keeps the vendor's own column names for fidelity. Silver is where the table
    starts speaking the business's language instead.
    """
    unknown_passengers = F.col("flag_W01")
    return df.select(
        F.col("VendorID").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        # Nulled rather than trusted where the vendor did not report it. Keeping a zero
        # here would drag any average occupancy toward zero for reasons that have nothing
        # to do with how many people rode.
        F.when(unknown_passengers, F.lit(None).cast("int"))
        .otherwise(F.col("passenger_count").cast("int"))
        .alias("passenger_count"),
        F.col("trip_distance").alias("trip_distance_miles"),
        F.col("RatecodeID").cast("int").alias("ratecode_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("store_and_fwd_flag").alias("store_and_fwd_flag"),
        F.col("fare_amount").alias("fare_amount"),
        F.col("extra"),
        F.col("mta_tax").alias("mta_tax"),
        F.col("tip_amount").alias("tip_amount"),
        F.col("tolls_amount").alias("tolls_amount"),
        F.col("improvement_surcharge").alias("improvement_surcharge"),
        F.col("congestion_surcharge").alias("congestion_surcharge"),
        F.col("total_amount").alias("total_amount"),
        F.round(trip_duration_minutes(), 4).alias("trip_duration_minutes"),
        F.round(implied_speed_mph(), 4).alias("implied_speed_mph"),
        F.round(F.col("fare_amount") / F.col("trip_distance"), 4).alias("fare_per_mile"),
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        F.hour("tpep_pickup_datetime").alias("pickup_hour"),
        F.dayofweek("tpep_pickup_datetime").alias("pickup_day_of_week"),
        F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month"),
        F.col("source_month"),
        F.col("source_file"),
        *[F.col(f"flag_{rule.id}").alias(f"quality_{rule.name}") for rule in WARN_RULES],
    )


def run_silver(spark: SparkSession, cfg: PipelineConfig, write: bool = True) -> dict:
    timings: dict = {}
    bronze = read_bronze(spark)
    evaluated = with_rule_evaluation(bronze, cfg.quality)

    with timed("silver: build exclusion ledger", timings):
        ledger_rows = build_ledger(evaluated)

    bronze_rows = sum(r["rows"] for r in ledger_rows)
    ledger = summarise_ledger(ledger_rows, bronze_rows)

    if not ledger["reconciles"]:
        raise ReconciliationError(
            "exclusion ledger does not reconcile: "
            f"input {ledger['input_rows']}, kept {ledger['kept_rows']}, "
            f"attributed exclusions {ledger['sum_of_rows_first_excluded']}"
        )

    if write:
        kept = conform(evaluated.filter(F.col("owning_rule") == "kept"))
        with timed("silver: write kept trips", timings):
            kept.write.mode("overwrite").parquet(str(SILVER_DIR))

        # The excluded set is written too. A quality rule that turns out to be wrong is a
        # decision to be revisited, which is only possible if the rows it removed are
        # still on disk with the rule that took them.
        excluded = evaluated.filter(F.col("owning_rule") != "kept").select(
            F.col("owning_rule"),
            F.col("source_month"),
            F.col("tpep_pickup_datetime").alias("pickup_ts"),
            F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
            F.col("PULocationID").alias("pickup_location_id"),
            F.col("DOLocationID").alias("dropoff_location_id"),
            F.col("passenger_count"),
            F.col("trip_distance").alias("trip_distance_miles"),
            F.col("fare_amount"),
            F.col("total_amount"),
        )
        with timed("silver: write excluded trips", timings):
            excluded.write.mode("overwrite").partitionBy("owning_rule").parquet(
                str(SILVER_EXCLUDED_DIR)
            )

    metrics = {"stage": "silver", "ledger": ledger, "timings_seconds": timings}
    if write:
        metrics["output"] = {
            "kept": directory_stats(SILVER_DIR),
            "excluded": directory_stats(SILVER_EXCLUDED_DIR),
        }
    write_evidence("silver_ledger", metrics)
    return metrics
