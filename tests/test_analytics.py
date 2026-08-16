"""Tests for the fare anomaly screen and the demand rollups."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from nyc_taxi.analytics import (
    demand_by_zone_hour_dow,
    distance_band,
    duration_band,
    flag_fare_anomalies,
    peer_group_statistics,
    trip_weighted_avg,
)
from nyc_taxi.config import AnomalyThresholds

THRESHOLDS = AnomalyThresholds()

TRIP_SCHEMA = (
    "pickup_location_id int, pickup_zone string, pickup_borough string, "
    "trip_distance_miles double, trip_duration_minutes double, fare_amount double, "
    "fare_per_mile double"
)

# Every helper trip sits in the same duration band by default, so a test that varies only
# distance or only fare keeps its peer group intact.
DEFAULT_DURATION = 12.0


def scored_trip(
    pu: int, distance: float, fare: float, duration: float = DEFAULT_DURATION
) -> tuple:
    return (
        pu,
        f"Zone {pu}",
        "Manhattan",
        distance,
        duration,
        fare,
        round(fare / distance, 4) if distance else None,
    )


class TestDistanceBand:
    @pytest.mark.parametrize(
        "distance,expected",
        [
            (0.5, "0-1"),
            (1.0, "1-2"),
            (1.9, "1-2"),
            (2.0, "2-3"),
            (4.9, "3-5"),
            (5.0, "5-10"),
            (12.0, "10-20"),
            (25.0, "20+"),
            (500.0, "20+"),
        ],
    )
    def test_bands_are_left_closed_and_cover_the_range(self, spark, distance, expected):
        frame = spark.createDataFrame([scored_trip(100, distance, 5.0)], TRIP_SCHEMA)
        assert frame.select(distance_band(THRESHOLDS)).collect()[0][0] == expected

    def test_every_trip_lands_in_exactly_one_band(self, spark):
        rows = [scored_trip(100, d / 4, 5.0) for d in range(1, 200)]
        frame = spark.createDataFrame(rows, TRIP_SCHEMA).withColumn("band", distance_band(THRESHOLDS))
        assert frame.filter(F.col("band").isNull()).count() == 0
        assert frame.count() == len(rows)


class TestDurationBand:
    @pytest.mark.parametrize(
        "duration,expected",
        [(1.0, "0-5"), (5.0, "5-10"), (12.0, "10-20"), (20.0, "20-40"), (95.0, "40+")],
    )
    def test_duration_bands_cover_the_range(self, spark, duration, expected):
        frame = spark.createDataFrame([scored_trip(100, 2.5, 12.0, duration)], TRIP_SCHEMA)
        assert frame.select(duration_band(THRESHOLDS)).collect()[0][0] == expected


class TestPeerGroupStatistics:
    def test_median_and_group_size_are_per_zone_distance_and_duration(self, spark):
        rows = [scored_trip(100, 2.5, 12.0) for _ in range(10)]
        rows += [scored_trip(100, 7.0, 25.0) for _ in range(6)]
        rows += [scored_trip(200, 2.5, 30.0) for _ in range(4)]
        # Same zone and distance as the first group, but a different duration band, so it
        # must form its own peer group rather than merging into it.
        rows += [scored_trip(100, 2.5, 40.0, duration=35.0) for _ in range(8)]

        stats = {
            (r["pickup_location_id"], r["distance_band"], r["duration_band"]): r
            for r in peer_group_statistics(
                spark.createDataFrame(rows, TRIP_SCHEMA), THRESHOLDS
            ).collect()
        }
        assert stats[(100, "2-3", "10-20")]["peer_group_size"] == 10
        assert stats[(100, "2-3", "10-20")]["peer_median_fare"] == pytest.approx(12.0, abs=0.01)
        assert stats[(100, "5-10", "10-20")]["peer_group_size"] == 6
        assert stats[(200, "2-3", "10-20")]["peer_median_fare"] == pytest.approx(30.0, abs=0.01)
        assert stats[(100, "2-3", "20-40")]["peer_group_size"] == 8
        assert stats[(100, "2-3", "20-40")]["peer_median_fare"] == pytest.approx(40.0, abs=0.01)

    def test_a_slow_trip_is_not_compared_against_fast_ones(self, spark):
        """The correction that matters, and the reason duration is in the peer key.

        A 2.5 mile crawl through traffic costs more than a 2.5 mile clear run, because the
        meter charges for time. Comparing the two treats congestion as fraud.
        """
        rows = [scored_trip(100, 2.5, 11.0, duration=8.0) for _ in range(150)]
        rows += [scored_trip(100, 2.5, 26.0, duration=35.0) for _ in range(150)]
        frame = spark.createDataFrame(rows, TRIP_SCHEMA)
        scored = flag_fare_anomalies(frame, peer_group_statistics(frame, THRESHOLDS), THRESHOLDS)
        assert scored.filter(F.col("fare_anomaly_flag")).count() == 0

    def test_a_flat_rate_group_does_not_flag_every_small_deviation(self, spark):
        """The JFK case, which broke a full run of the screen.

        A regulated flat rate produces a peer group where every fare is identical and the
        median absolute deviation is exactly zero. Scored against a token floor, a fare a
        few cents off the flat rate becomes a 67 sigma event, and the screen flagged 16.2
        percent of all 10 to 20 mile trips in the real data. The scale floor is
        proportional to the fare, so ordinary rounding survives and a genuinely wrong fare
        still does not.
        """
        rows = [scored_trip(132, 16.0, 52.0, duration=45.0) for _ in range(300)]
        rows.append(scored_trip(132, 16.0, 52.5, duration=45.0))  # trivially off, keep
        rows.append(scored_trip(132, 16.0, 55.0, duration=45.0))  # mildly off, keep
        rows.append(scored_trip(132, 16.0, 180.0, duration=45.0))  # genuinely wrong, flag
        frame = spark.createDataFrame(rows, TRIP_SCHEMA)
        scored = flag_fare_anomalies(frame, peer_group_statistics(frame, THRESHOLDS), THRESHOLDS)
        flagged = {r["fare_amount"] for r in scored.filter(F.col("fare_anomaly_flag")).collect()}

        assert 52.5 not in flagged
        assert 55.0 not in flagged
        assert 180.0 in flagged

    def test_median_resists_the_outliers_it_is_meant_to_detect(self, spark):
        """The reason the screen uses a median and a MAD rather than a mean and a sigma.

        Ten extreme fares in a group of 110 drag the mean well off the typical fare while
        leaving the median where it belongs.
        """
        rows = [scored_trip(100, 2.5, 12.0) for _ in range(100)]
        rows += [scored_trip(100, 2.5, 900.0) for _ in range(10)]
        stats = peer_group_statistics(spark.createDataFrame(rows, TRIP_SCHEMA), THRESHOLDS).collect()[0]
        assert stats["peer_median_fare"] == pytest.approx(12.0, abs=0.01)
        assert stats["peer_mean_fare"] > 80


class TestFareAnomalyFlag:
    @pytest.fixture
    def scored(self, spark):
        # A peer group large enough to be scored, with a little natural spread so the MAD
        # is not degenerate, plus one clear outlier in each direction.
        rows = []
        for i in range(120):
            rows.append(scored_trip(100, 2.5, 12.0 + (i % 5) * 0.25))
        rows.append(scored_trip(100, 2.5, 240.0))
        rows.append(scored_trip(100, 2.5, 0.5))
        # A second group below the minimum size, which must be left unscored.
        rows += [scored_trip(200, 2.5, 12.0) for _ in range(20)]
        rows.append(scored_trip(200, 2.5, 900.0))
        frame = spark.createDataFrame(rows, TRIP_SCHEMA)
        stats = peer_group_statistics(frame, THRESHOLDS)
        return flag_fare_anomalies(frame, stats, THRESHOLDS)

    def test_extreme_high_fare_is_flagged_above_peers(self, scored):
        row = scored.filter(F.col("fare_amount") == 240.0).collect()[0]
        assert row["fare_anomaly_flag"] is True
        assert row["fare_anomaly_direction"] == "above_peer_group"
        assert row["fare_robust_z"] > THRESHOLDS.robust_z_threshold

    def test_extreme_low_fare_is_flagged_below_peers(self, scored):
        row = scored.filter(F.col("fare_amount") == 0.5).collect()[0]
        assert row["fare_anomaly_flag"] is True
        assert row["fare_anomaly_direction"] == "below_peer_group"

    def test_ordinary_trips_are_not_flagged(self, scored):
        ordinary = scored.filter(
            (F.col("pickup_location_id") == 100) & F.col("fare_amount").between(12.0, 13.0)
        )
        assert ordinary.count() == 120
        assert ordinary.filter(F.col("fare_anomaly_flag")).count() == 0

    def test_small_peer_groups_are_not_scored(self, scored):
        """An outlier in a thin group stays unflagged, because the peer estimate is not
        trustworthy enough to accuse a trip on."""
        row = scored.filter(F.col("pickup_location_id") == 200).filter(
            F.col("fare_amount") == 900.0
        ).collect()[0]
        assert row["peer_group_scored"] is False
        assert row["fare_anomaly_flag"] is False
        assert row["fare_anomaly_direction"] is None

    def test_flagging_never_drops_or_duplicates_trips(self, scored):
        assert scored.count() == 143

    def test_direction_is_null_exactly_when_not_flagged(self, scored):
        mismatched = scored.filter(
            F.col("fare_anomaly_flag") != F.col("fare_anomaly_direction").isNotNull()
        )
        assert mismatched.count() == 0


class TestDemandRollup:
    def test_rollup_sums_trips_and_averages_fares(self, spark):
        demand = spark.createDataFrame(
            [
                (100, "Midtown", "Manhattan", 4, 9, 10, 20.0, 200.0),
                (100, "Midtown", "Manhattan", 4, 9, 30, 22.0, 660.0),
                (100, "Midtown", "Manhattan", 5, 9, 5, 18.0, 90.0),
            ],
            "pickup_location_id int, pickup_zone string, pickup_borough string, "
            "pickup_day_of_week int, pickup_hour int, trips long, avg_fare double, total_revenue double",
        )
        result = {
            (r["pickup_day_of_week"], r["pickup_hour"]): r
            for r in demand_by_zone_hour_dow(demand).collect()
        }
        assert result[(4, 9)]["total_trips"] == 40
        assert result[(4, 9)]["avg_trips_per_hour_slot"] == pytest.approx(20.0)
        # Trip weighted, not the mean of the two slot averages.
        # (20.00 * 10 + 22.00 * 30) / 40 = 21.50, where a plain mean would say 21.00 and
        # give the 10 trip slot the same weight as the 30 trip one.
        assert result[(4, 9)]["avg_fare"] == pytest.approx(21.5)
        assert result[(4, 9)]["total_revenue"] == pytest.approx(860.0)
        assert result[(5, 9)]["total_trips"] == 5

    def test_weighted_average_ignores_slot_count_and_follows_trip_count(self, spark):
        """One busy slot should dominate many quiet ones, which a mean of means will not do."""
        demand = spark.createDataFrame(
            [(1, 1000, 10.0), (2, 1, 100.0), (3, 1, 100.0)],
            "pickup_location_id int, trips long, avg_fare double",
        )
        weighted = demand.agg(trip_weighted_avg("avg_fare").alias("w")).collect()[0]["w"]
        plain = demand.agg(F.avg("avg_fare").alias("p")).collect()[0]["p"]
        assert weighted == pytest.approx((1000 * 10.0 + 100.0 + 100.0) / 1002)
        assert weighted < 11.0
        assert plain == pytest.approx(70.0)
