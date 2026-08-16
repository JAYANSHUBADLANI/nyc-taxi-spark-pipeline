"""Shared fixtures.

The tests deliberately never touch the 44 million row dataset. They run against small
hand built frames whose expected answers can be worked out by hand, which is the only way
a transformation test tells you something you did not already assume. A single local
Spark session is shared across the whole session because starting a JVM per test would
dominate the runtime.
"""

from __future__ import annotations

import datetime as dt
import os
import time

# Pinned before the JVM starts, and it has to be. PySpark converts naive Python datetimes
# using the driver's system timezone while the session evaluates them in
# spark.sql.session.timeZone. On a machine set to anything other than UTC the two
# disagree, and every timestamp in these fixtures silently shifts by the offset. That is
# not a property of the pipeline, which reads wall clock timestamps out of Parquet and
# never crosses a timezone, so the tests pin both ends to UTC and measure the transform
# instead of the machine.
os.environ["TZ"] = "UTC"
time.tzset()

import pytest  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("nyc-taxi-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# Mirrors the bronze output schema, the input the silver rules actually see.
BRONZE_TEST_SCHEMA = StructType(
    [
        StructField("VendorID", LongType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", DoubleType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", DoubleType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", LongType()),
        StructField("DOLocationID", LongType()),
        StructField("payment_type", LongType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("airport_fee", DoubleType()),
        StructField("source_file", StringType()),
        StructField("source_month", StringType()),
        StructField("pickup_month", StringType()),
        StructField("case_id", StringType()),
    ]
)


def ts(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value)


def trip(
    case_id: str,
    pickup: str = "2019-03-13 09:00:00",
    dropoff: str = "2019-03-13 09:20:00",
    passenger_count: float | None = 2.0,
    trip_distance: float = 3.0,
    ratecode: float | None = 1.0,
    pu_location: int = 100,
    do_location: int = 200,
    payment_type: int = 1,
    fare_amount: float = 15.0,
    total_amount: float = 18.3,
) -> tuple:
    """One trip row, valid by default, with a single field overridden per test case."""
    return (
        2,
        ts(pickup),
        ts(dropoff),
        passenger_count,
        trip_distance,
        ratecode,
        "N",
        pu_location,
        do_location,
        payment_type,
        fare_amount,
        0.5,
        0.5,
        2.0,
        0.0,
        0.3,
        total_amount,
        0.0,
        None,
        "yellow_tripdata_2019-03.parquet",
        "2019-03",
        "2019-03",
        case_id,
    )


@pytest.fixture
def bronze_frame(spark: SparkSession):
    """Factory that builds a bronze shaped frame from trip() rows."""

    def _build(rows: list[tuple]):
        return spark.createDataFrame(rows, schema=BRONZE_TEST_SCHEMA)

    return _build
