"""Tests for schema contract enforcement and bronze structural classification."""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from nyc_taxi.bronze import (
    REJECT_REASON_NULL_KEY,
    REJECT_REASON_OUT_OF_WINDOW,
    _load_window,
    add_provenance,
    classify_structural_validity,
)
from nyc_taxi.schemas import (
    YELLOW_TRIP_SCHEMA,
    SchemaContractError,
    assert_contract,
    compare_to_contract,
)

MONTHS = ["2019-01", "2019-02", "2019-03", "2019-04", "2019-05", "2019-06"]


class TestSchemaContract:
    def test_the_real_contract_conforms_to_itself(self):
        report = compare_to_contract(YELLOW_TRIP_SCHEMA)
        assert report["conforms"] is True

    def test_missing_column_is_fatal(self):
        truncated = StructType([f for f in YELLOW_TRIP_SCHEMA.fields if f.name != "fare_amount"])
        with pytest.raises(SchemaContractError, match="missing columns"):
            assert_contract(truncated, source="truncated.parquet")

    def test_unexpected_column_is_fatal(self):
        """An unannounced new column means the publisher changed something. Stop and look."""
        widened = StructType([*YELLOW_TRIP_SCHEMA.fields, StructField("cbd_congestion_fee", DoubleType())])
        with pytest.raises(SchemaContractError, match="unexpected columns"):
            assert_contract(widened, source="widened.parquet")

    def test_unsafe_type_change_is_fatal(self):
        mangled = StructType(
            [
                StructField("fare_amount", StringType()) if f.name == "fare_amount" else f
                for f in YELLOW_TRIP_SCHEMA.fields
            ]
        )
        with pytest.raises(SchemaContractError, match="fare_amount is string"):
            assert_contract(mangled, source="mangled.parquet")

    def test_documented_safe_widening_is_allowed(self):
        """The published files really do carry these two differences.

        Timestamps arrive as timestamp_ntz and airport_fee arrives as int32. Both are
        deliberately tolerated, and the report still records that they differ.
        """
        from pyspark.sql.types import TimestampNTZType

        actual = StructType(
            [
                StructField(f.name, TimestampNTZType())
                if isinstance(f.dataType, TimestampType)
                else f
                for f in YELLOW_TRIP_SCHEMA.fields
            ]
        )
        report = assert_contract(actual, source="published.parquet")
        assert report["conforms"] is False
        assert "tpep_pickup_datetime" in report["type_mismatches"]

    def test_int_to_double_is_not_treated_as_safe(self):
        """Arithmetically lossless, and the vectorised Parquet reader still refuses it.

        This exact pair failed a real run on airport_fee, which is why the allowance list
        tracks what the reader does rather than what looks reasonable.
        """
        actual = StructType(
            [
                StructField("fare_amount", IntegerType()) if f.name == "fare_amount" else f
                for f in YELLOW_TRIP_SCHEMA.fields
            ]
        )
        with pytest.raises(SchemaContractError):
            assert_contract(actual, source="int_fare.parquet")


class TestLoadWindow:
    def test_window_spans_first_month_start_to_month_after_last(self):
        assert _load_window(MONTHS) == ("2019-01-01 00:00:00", "2019-07-01 00:00:00")

    def test_december_rolls_the_year(self):
        assert _load_window(["2019-11", "2019-12"]) == ("2019-11-01 00:00:00", "2020-01-01 00:00:00")


RAW_SCHEMA = StructType(
    [
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("PULocationID", LongType()),
        StructField("DOLocationID", LongType()),
        StructField("source_file", StringType()),
        StructField("case_id", StringType()),
    ]
)


def raw_row(case_id, pickup, dropoff="2019-03-13 09:20:00", pu=100, do=200, source="yellow_tripdata_2019-03.parquet"):
    return (
        dt.datetime.fromisoformat(pickup) if pickup else None,
        dt.datetime.fromisoformat(dropoff) if dropoff else None,
        pu,
        do,
        source,
        case_id,
    )


class TestStructuralClassification:
    @pytest.fixture
    def classified(self, spark):
        rows = [
            raw_row("in_window", "2019-03-13 09:00:00"),
            raw_row("first_instant", "2019-01-01 00:00:00"),
            raw_row("last_instant", "2019-06-30 23:59:59"),
            raw_row("year_2001", "2001-01-01 05:32:08"),
            raw_row("year_2088", "2088-01-24 05:55:39"),
            raw_row("month_july", "2019-07-01 00:00:00"),
            raw_row("month_december_prior", "2018-12-31 23:59:59"),
            raw_row("null_pickup", None),
            raw_row("null_dropoff", "2019-03-13 09:00:00", dropoff=None),
            raw_row("null_zone", "2019-03-13 09:00:00", pu=None),
        ]
        frame = spark.createDataFrame(rows, RAW_SCHEMA)
        result = classify_structural_validity(add_provenance(frame), MONTHS)
        return {r["case_id"]: r["reject_reason"] for r in result.collect()}

    def test_in_window_trips_are_accepted(self, classified):
        assert classified["in_window"] is None
        assert classified["first_instant"] is None
        assert classified["last_instant"] is None

    @pytest.mark.parametrize(
        "case_id", ["year_2001", "year_2088", "month_july", "month_december_prior"]
    )
    def test_pickups_outside_the_load_window_are_rejected(self, classified, case_id):
        assert classified[case_id] == REJECT_REASON_OUT_OF_WINDOW

    @pytest.mark.parametrize("case_id", ["null_pickup", "null_dropoff", "null_zone"])
    def test_missing_structural_keys_are_rejected(self, classified, case_id):
        assert classified[case_id] == REJECT_REASON_NULL_KEY

    def test_null_key_takes_precedence_over_window(self, spark):
        """A null pickup cannot be tested against a window, so the reason must say so."""
        frame = spark.createDataFrame([raw_row("null_and_unplaceable", None)], RAW_SCHEMA)
        result = classify_structural_validity(add_provenance(frame), MONTHS).collect()[0]
        assert result["reject_reason"] == REJECT_REASON_NULL_KEY


class TestProvenance:
    def test_source_month_is_read_from_the_file_name(self, spark):
        frame = spark.createDataFrame(
            [raw_row("t", "2019-03-13 09:00:00", source="yellow_tripdata_2019-03.parquet")], RAW_SCHEMA
        )
        row = add_provenance(frame).collect()[0]
        assert row["source_month"] == "2019-03"
        assert row["pickup_month"] == "2019-03"

    def test_pickup_month_can_differ_from_source_month(self, spark):
        """Cross month drift is real in the published files and must stay visible."""
        frame = spark.createDataFrame(
            [raw_row("drifted", "2019-02-28 23:50:00", source="yellow_tripdata_2019-03.parquet")],
            RAW_SCHEMA,
        )
        row = add_provenance(frame).collect()[0]
        assert row["source_month"] == "2019-03"
        assert row["pickup_month"] == "2019-02"
