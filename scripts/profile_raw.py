"""Profile the raw TLC feed before any rule thresholds are fixed.

This exists to justify the silver cleaning rules with observed prevalence rather than
assumed prevalence. It reads every downloaded month with the declared schema, checks the
schema contract, and counts how often each candidate defect appears.

Run: make profile
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import functions as F

from nyc_taxi.config import DEFAULT_CONFIG, RAW_TRIPS_DIR
from nyc_taxi.schemas import YELLOW_TRIP_SCHEMA, compare_to_contract
from nyc_taxi.spark import build_spark, timed, write_evidence


def main() -> None:
    cfg = DEFAULT_CONFIG
    spark = build_spark(cfg.spark, app_suffix="profile")

    files = sorted(RAW_TRIPS_DIR.glob("*.parquet"))
    print(f"profiling {len(files)} files")

    # Schema contract check per file, using the schema Spark infers from the footer.
    contract_reports = {}
    for f in files:
        inferred = spark.read.parquet(str(f)).schema
        contract_reports[f.name] = compare_to_contract(inferred)

    df = spark.read.schema(YELLOW_TRIP_SCHEMA).parquet(str(RAW_TRIPS_DIR))
    df = df.withColumn("source_file", F.input_file_name())

    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    duration_min = (dropoff.cast("long") - pickup.cast("long")) / 60.0
    speed_mph = F.when(duration_min > 0, F.col("trip_distance") / (duration_min / 60.0))

    checks = {
        "total_rows": F.lit(True),
        "null_pickup_ts": pickup.isNull(),
        "null_dropoff_ts": dropoff.isNull(),
        "null_pu_location": F.col("PULocationID").isNull(),
        "null_do_location": F.col("DOLocationID").isNull(),
        "null_passenger_count": F.col("passenger_count").isNull(),
        "null_ratecode": F.col("RatecodeID").isNull(),
        "null_congestion_surcharge": F.col("congestion_surcharge").isNull(),
        "pickup_outside_2019h1": (F.year(pickup) != 2019) | (F.month(pickup) > 6),
        "dropoff_before_pickup": dropoff < pickup,
        "dropoff_equals_pickup": dropoff == pickup,
        "duration_over_24h": duration_min > 24 * 60,
        "fare_negative": F.col("fare_amount") < 0,
        "fare_zero": F.col("fare_amount") == 0,
        "fare_over_1000": F.col("fare_amount") > 1000,
        "total_negative": F.col("total_amount") < 0,
        "distance_zero": F.col("trip_distance") == 0,
        "distance_negative": F.col("trip_distance") < 0,
        "distance_zero_with_fare": (F.col("trip_distance") <= 0) & (F.col("fare_amount") > 0),
        "distance_over_200mi": F.col("trip_distance") > 200,
        "passenger_zero": F.col("passenger_count") == 0,
        "passenger_over_6": F.col("passenger_count") > 6,
        "pu_location_out_of_range": ~F.col("PULocationID").between(1, 265),
        "do_location_out_of_range": ~F.col("DOLocationID").between(1, 265),
        "ratecode_undocumented": ~F.col("RatecodeID").isin([1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
        "speed_over_100mph": speed_mph > 100,
        "payment_type_undocumented": ~F.col("payment_type").isin([1, 2, 3, 4, 5, 6]),
    }

    aggs = [F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in checks.items()]
    aggs += [
        F.min(pickup).alias("min_pickup"),
        F.max(pickup).alias("max_pickup"),
        F.min("fare_amount").alias("min_fare"),
        F.max("fare_amount").alias("max_fare"),
        F.min("trip_distance").alias("min_distance"),
        F.max("trip_distance").alias("max_distance"),
        F.max("passenger_count").alias("max_passengers"),
    ]

    timings: dict = {}
    with timed("raw profile scan", timings):
        row = df.agg(*aggs).collect()[0].asDict()

    total = row["total_rows"]
    print(f"\n{'check':32} {'rows':>14} {'pct':>9}")
    prevalence = {}
    for name in checks:
        if name == "total_rows":
            continue
        n = row[name]
        pct = 100.0 * n / total if total else 0.0
        prevalence[name] = {"rows": int(n), "pct_of_raw": round(pct, 6)}
        print(f"{name:32} {n:>14,} {pct:>8.4f}%")

    ranges = {k: row[k] for k in row if k.startswith(("min_", "max_"))}
    print("\nranges:")
    for k, v in ranges.items():
        print(f"  {k}: {v}")

    write_evidence(
        "raw_profile",
        {
            "files_profiled": [f.name for f in files],
            "total_raw_rows": int(total),
            "schema_contract": contract_reports,
            "defect_prevalence": prevalence,
            "value_ranges": {k: str(v) for k, v in ranges.items()},
            "timings_seconds": timings,
        },
    )
    spark.stop()


if __name__ == "__main__":
    main()
