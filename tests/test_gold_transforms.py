"""Tests for the gold layer: zone enrichment, window features and the demand rollup."""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import functions as F

from nyc_taxi.gold import add_trip_window_features, build_zone_hour_demand, enrich_with_zones


@pytest.fixture
def zones(spark):
    return spark.createDataFrame(
        [
            (100, "Manhattan", "Midtown Center", "Yellow Zone"),
            (200, "Queens", "JFK Airport", "Airports"),
            (300, "Brooklyn", "Park Slope", "Boro Zone"),
        ],
        "location_id int, borough string, zone_name string, service_zone string",
    )


SILVER_SCHEMA = (
    "pickup_location_id int, dropoff_location_id int, pickup_ts timestamp, "
    "passenger_count int, trip_distance_miles double, trip_duration_minutes double, "
    "fare_amount double, total_amount double, implied_speed_mph double, pickup_date date"
)


def silver_row(
    pu: int = 100,
    do: int = 200,
    pickup: str = "2019-03-13 09:15:00",
    passengers: int = 2,
    distance: float = 3.0,
    duration: float = 20.0,
    fare: float = 15.0,
    total: float = 18.3,
    speed: float = 9.0,
) -> tuple:
    pickup_ts = dt.datetime.fromisoformat(pickup)
    return (pu, do, pickup_ts, passengers, distance, duration, fare, total, speed, pickup_ts.date())


class TestZoneEnrichment:
    def test_pickup_and_dropoff_are_labelled_independently(self, spark, zones):
        trips = spark.createDataFrame([silver_row(pu=100, do=200)], SILVER_SCHEMA)
        row = enrich_with_zones(trips, zones).collect()[0]
        assert row["pickup_borough"] == "Manhattan"
        assert row["pickup_zone"] == "Midtown Center"
        assert row["dropoff_borough"] == "Queens"
        assert row["dropoff_zone"] == "JFK Airport"

    def test_join_is_left_so_an_unmatched_zone_never_loses_the_trip(self, spark, zones):
        """A trip with a zone id absent from the dimension must survive with nulls.

        An inner join here would silently shrink the fact table, and the row count would
        stop matching the silver ledger with nothing to explain the gap.
        """
        trips = spark.createDataFrame([silver_row(pu=999), silver_row(pu=100)], SILVER_SCHEMA)
        enriched = enrich_with_zones(trips, zones)
        assert enriched.count() == 2
        assert enriched.filter(F.col("pickup_zone").isNull()).count() == 1

    def test_join_plan_broadcasts_the_dimension(self, spark, zones):
        trips = spark.createDataFrame([silver_row()], SILVER_SCHEMA)
        plan = enrich_with_zones(trips, zones)._jdf.queryExecution().toString()
        assert "BroadcastHashJoin" in plan
        assert "SortMergeJoin" not in plan

    def test_enrichment_does_not_duplicate_rows(self, spark, zones):
        """The dimension is unique on location_id, so the join must be row preserving."""
        trips = spark.createDataFrame([silver_row() for _ in range(5)], SILVER_SCHEMA)
        assert enrich_with_zones(trips, zones).count() == 5


class TestTripWindowFeatures:
    @pytest.fixture
    def sequenced(self, spark, zones):
        trips = spark.createDataFrame(
            [
                silver_row(pu=100, pickup="2019-03-13 09:00:00", fare=10.0),
                silver_row(pu=100, pickup="2019-03-13 09:05:00", fare=20.0),
                silver_row(pu=100, pickup="2019-03-13 09:30:00", fare=30.0),
                # Same zone, different day. Must restart the sequence.
                silver_row(pu=100, pickup="2019-03-14 08:00:00", fare=40.0),
                # Different zone, same day. Must have its own sequence.
                silver_row(pu=300, pickup="2019-03-13 09:10:00", fare=50.0),
            ],
            SILVER_SCHEMA,
        )
        return add_trip_window_features(enrich_with_zones(trips, zones)).collect()

    def test_sequence_restarts_per_zone_and_day(self, sequenced):
        by_key = {(r["pickup_location_id"], str(r["pickup_ts"])): r for r in sequenced}
        assert by_key[(100, "2019-03-13 09:00:00")]["trip_seq_in_zone_day"] == 1
        assert by_key[(100, "2019-03-13 09:05:00")]["trip_seq_in_zone_day"] == 2
        assert by_key[(100, "2019-03-13 09:30:00")]["trip_seq_in_zone_day"] == 3
        assert by_key[(100, "2019-03-14 08:00:00")]["trip_seq_in_zone_day"] == 1
        assert by_key[(300, "2019-03-13 09:10:00")]["trip_seq_in_zone_day"] == 1

    def test_gap_to_previous_pickup_is_null_for_the_first_trip(self, sequenced):
        firsts = [r for r in sequenced if r["trip_seq_in_zone_day"] == 1]
        assert all(r["seconds_since_prev_pickup_in_zone"] is None for r in firsts)

    def test_gap_measures_real_elapsed_seconds(self, sequenced):
        second = [r for r in sequenced if r["pickup_location_id"] == 100 and r["trip_seq_in_zone_day"] == 2][0]
        third = [r for r in sequenced if r["pickup_location_id"] == 100 and r["trip_seq_in_zone_day"] == 3][0]
        assert second["seconds_since_prev_pickup_in_zone"] == 300
        assert third["seconds_since_prev_pickup_in_zone"] == 1500

    def test_lagged_fare_comes_from_the_previous_trip_in_the_same_zone(self, sequenced):
        third = [r for r in sequenced if r["pickup_location_id"] == 100 and r["trip_seq_in_zone_day"] == 3][0]
        assert third["prev_trip_fare_in_zone"] == pytest.approx(20.0)


class TestZoneHourDemand:
    @pytest.fixture
    def demand(self, spark, zones):
        rows = []
        # Zone 100: 3 trips at 09:00, 1 at 10:00, then a deliberate gap until 20:00.
        for minute in (0, 20, 40):
            rows.append(silver_row(pu=100, pickup=f"2019-03-13 09:{minute:02d}:00", fare=12.0))
        rows.append(silver_row(pu=100, pickup="2019-03-13 10:15:00", fare=20.0))
        rows.append(silver_row(pu=100, pickup="2019-03-13 20:00:00", fare=30.0))
        rows.append(silver_row(pu=300, pickup="2019-03-13 09:05:00", fare=8.0))
        trips = spark.createDataFrame(rows, SILVER_SCHEMA)
        result = build_zone_hour_demand(enrich_with_zones(trips, zones))
        return {(r["pickup_location_id"], r["pickup_hour"]): r for r in result.collect()}

    def test_trips_are_bucketed_into_the_right_hour(self, demand):
        assert demand[(100, 9)]["trips"] == 3
        assert demand[(100, 10)]["trips"] == 1
        assert demand[(300, 9)]["trips"] == 1

    def test_rolling_three_hour_window_accumulates_within_range(self, demand):
        assert demand[(100, 9)]["trips_rolling_3h"] == 3
        assert demand[(100, 10)]["trips_rolling_3h"] == 4

    def test_rolling_window_respects_time_gaps_not_row_order(self, demand):
        """The 20:00 hour is ten hours after the previous record for this zone.

        A rowsBetween window would treat it as adjacent to 10:00 and roll the earlier
        trips into it. A rangeBetween over epoch seconds correctly sees an empty window.
        """
        assert demand[(100, 20)]["trips_rolling_3h"] == 1
        assert demand[(100, 20)]["trips_rolling_24h"] == 5

    def test_previous_hour_comparison(self, demand):
        assert demand[(100, 9)]["trips_prev_hour"] is None
        assert demand[(100, 10)]["trips_prev_hour"] == 3
        assert demand[(100, 10)]["trips_vs_prev_hour_pct"] == pytest.approx(-66.67, abs=0.01)

    def test_aggregates_are_computed_per_zone_hour(self, demand):
        assert demand[(100, 9)]["avg_fare"] == pytest.approx(12.0)
        assert demand[(100, 10)]["avg_fare"] == pytest.approx(20.0)
        assert demand[(100, 9)]["passengers"] == 6

    def test_calendar_columns_are_derived(self, demand):
        row = demand[(100, 9)]
        assert str(row["pickup_date"]) == "2019-03-13"
        assert row["pickup_day_of_week"] == 4
