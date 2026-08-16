"""Explicit schema contracts for the raw TLC feed and the zone lookup.

The pipeline never relies on Parquet's embedded schema or on CSV inference. It declares
what it expects, checks the file against that declaration, and refuses to continue if the
contract is broken in a way that cannot be safely reconciled.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Contract for the yellow taxi trip records as published for 2019.
#
# Two column level notes, both verified against the downloaded files rather than assumed:
#   - passenger_count and RatecodeID are stored as double, not integer. The TLC re-release
#     widened them to a nullable floating type. They are read as double and cast during
#     the silver stage after the null and range rules have run.
#   - airport_fee exists in the 2019 files as a physically INT32 column holding nothing but
#     nulls. The field was introduced for later years and backfilled empty here. It is
#     declared as integer because that is what the file actually contains, then cast to
#     double in bronze so the stored table schema stays stable if later years are loaded.
#     Declaring it double here instead looks harmless and is not: Spark's vectorised
#     Parquet reader refuses an INT32 to double conversion at read time and fails the job.
YELLOW_TRIP_SCHEMA = StructType(
    [
        StructField("VendorID", LongType(), nullable=True),
        StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
        StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
        StructField("passenger_count", DoubleType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("RatecodeID", DoubleType(), nullable=True),
        StructField("store_and_fwd_flag", StringType(), nullable=True),
        StructField("PULocationID", LongType(), nullable=True),
        StructField("DOLocationID", LongType(), nullable=True),
        StructField("payment_type", LongType(), nullable=True),
        StructField("fare_amount", DoubleType(), nullable=True),
        StructField("extra", DoubleType(), nullable=True),
        StructField("mta_tax", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True),
        StructField("tolls_amount", DoubleType(), nullable=True),
        StructField("improvement_surcharge", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("congestion_surcharge", DoubleType(), nullable=True),
        StructField("airport_fee", IntegerType(), nullable=True),
    ]
)

# Columns that must be present for a record to be structurally usable at all. A record
# missing any of these cannot be placed in time or space and is rejected at bronze.
BRONZE_REQUIRED_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
)

ZONE_LOOKUP_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), nullable=False),
        StructField("Borough", StringType(), nullable=True),
        StructField("Zone", StringType(), nullable=True),
        StructField("service_zone", StringType(), nullable=True),
    ]
)


class SchemaContractError(RuntimeError):
    """Raised when a source file cannot be reconciled with the declared contract."""


def compare_to_contract(actual: StructType, expected: StructType = YELLOW_TRIP_SCHEMA) -> dict:
    """Compare a file's schema against the contract without raising.

    Returns a report describing missing columns, unexpected extra columns and type
    mismatches. Callers decide what is fatal.
    """
    actual_fields = {f.name: f.dataType.simpleString() for f in actual.fields}
    expected_fields = {f.name: f.dataType.simpleString() for f in expected.fields}

    missing = [c for c in expected_fields if c not in actual_fields]
    unexpected = [c for c in actual_fields if c not in expected_fields]
    mismatched = {
        c: {"expected": expected_fields[c], "actual": actual_fields[c]}
        for c in expected_fields
        if c in actual_fields and actual_fields[c] != expected_fields[c]
    }
    return {
        "missing_columns": missing,
        "unexpected_columns": unexpected,
        "type_mismatches": mismatched,
        "conforms": not missing and not unexpected and not mismatched,
    }


# A type mismatch is tolerable only when the source type can be widened into the contract
# type without losing information. Anything else stops the run.
#
# The timestamp_ntz to timestamp entry is a deliberate decision, not an oversight. The TLC
# files store pickup and dropoff as local New York wall clock readings with no zone
# offset. The pipeline runs with spark.sql.session.timeZone set to UTC, which preserves
# those wall clock digits exactly, so an hour of the day derived downstream is the local
# hour a dispatcher would recognise.
#
# The cost is daylight saving, and it is not hypothetical here. The spring transition of
# 10 March 2019 falls inside the loaded window: local time jumps from 01:59 to 03:00, so the
# 02:00 hour does not exist that day and any per hour demand series has a real hole in it. A
# trip running across that boundary also loses an hour of wall clock duration, which makes
# its implied speed read high. The ambiguous transition, where an hour repeats and a bare
# timestamp cannot say which one is meant, is the November one and sits outside this window.
# Nothing here corrects for either, because the source does not carry the offset that would
# be needed to do it. That is a property of the published data, stated rather than hidden.
#
# The set is deliberately narrow. It lists only conversions Spark's vectorised Parquet
# reader will actually perform, which is a smaller set than the conversions that look
# arithmetically lossless. int to double reads as obviously safe and is rejected by the
# reader, so it is not here.
SAFE_WIDENING = {
    ("int", "bigint"),
    ("float", "double"),
    ("void", "double"),
    ("void", "string"),
    ("void", "bigint"),
    ("void", "int"),
    ("timestamp_ntz", "timestamp"),
}


def assert_contract(actual: StructType, source: str, expected: StructType = YELLOW_TRIP_SCHEMA) -> dict:
    """Validate a source schema against the contract, raising on unsafe differences."""
    report = compare_to_contract(actual, expected)
    fatal: list[str] = []

    if report["missing_columns"]:
        fatal.append(f"missing columns {report['missing_columns']}")
    if report["unexpected_columns"]:
        fatal.append(f"unexpected columns {report['unexpected_columns']}")
    for column, types in report["type_mismatches"].items():
        if (types["actual"], types["expected"]) not in SAFE_WIDENING:
            fatal.append(
                f"column {column} is {types['actual']}, contract requires {types['expected']}"
            )

    if fatal:
        raise SchemaContractError(f"{source} violates the schema contract: " + "; ".join(fatal))
    return report
