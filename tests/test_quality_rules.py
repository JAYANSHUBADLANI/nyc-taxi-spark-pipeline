"""Tests for the silver quality rules and the exclusion ledger.

The ledger tests are the important ones. A rule that fires on the wrong row is a bug you
find eventually. A ledger that silently fails to add up is a bug you never find, because
its output looks like a report.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from nyc_taxi.config import QualityThresholds
from nyc_taxi.rules import (
    REJECT_RULES,
    SILVER_RULES,
    Severity,
    implied_speed_mph,
    trip_duration_minutes,
)
from nyc_taxi.silver import build_ledger, conform, summarise_ledger, with_rule_evaluation

from conftest import trip

THRESHOLDS = QualityThresholds()


def owning_rules(frame) -> dict[str, str]:
    """Map each case id to the rule that owns it, or 'kept'."""
    evaluated = with_rule_evaluation(frame, THRESHOLDS)
    return {r["case_id"]: r["owning_rule"] for r in evaluated.select("case_id", "owning_rule").collect()}


class TestIndividualRules:
    def test_valid_trip_is_kept(self, bronze_frame):
        result = owning_rules(bronze_frame([trip("valid")]))
        assert result["valid"] == "kept"

    @pytest.mark.parametrize(
        "case_id,kwargs,expected_rule",
        [
            ("zero_fare", {"fare_amount": 0.0}, "R01"),
            ("negative_fare", {"fare_amount": -12.5}, "R01"),
            ("null_fare", {"fare_amount": None}, "R01"),
            ("absurd_fare", {"fare_amount": 943274.8}, "R02"),
            ("negative_total", {"total_amount": -18.3}, "R03"),
            ("zero_distance", {"trip_distance": 0.0}, "R04"),
            ("negative_distance", {"trip_distance": -1.0}, "R04"),
            ("absurd_distance", {"trip_distance": 45977.22, "dropoff": "2019-03-14 08:00:00"}, "R05"),
            ("backwards_clock", {"dropoff": "2019-03-13 08:40:00"}, "R06"),
            ("zero_duration", {"dropoff": "2019-03-13 09:00:00"}, "R06"),
            ("marathon_meter", {"dropoff": "2019-03-14 10:00:00"}, "R07"),
            ("teleporting", {"trip_distance": 90.0}, "R08"),
            ("seven_passengers", {"passenger_count": 7.0}, "R09"),
            ("zone_zero", {"pu_location": 0}, "R10"),
            ("zone_above_range", {"do_location": 266}, "R10"),
        ],
    )
    def test_reject_rule_owns_its_case(self, bronze_frame, case_id, kwargs, expected_rule):
        result = owning_rules(bronze_frame([trip(case_id, **kwargs)]))
        assert result[case_id] == expected_rule

    @pytest.mark.parametrize(
        "case_id,kwargs",
        [
            ("null_passengers", {"passenger_count": None}),
            ("zero_passengers", {"passenger_count": 0.0}),
            ("undocumented_ratecode", {"ratecode": 99.0}),
            ("undocumented_payment", {"payment_type": 0}),
        ],
    )
    def test_warn_rules_do_not_exclude(self, bronze_frame, case_id, kwargs):
        """A warned row stays in the kept set. This is the decision the project turns on."""
        result = owning_rules(bronze_frame([trip(case_id, **kwargs)]))
        assert result[case_id] == "kept"

    def test_boundary_values_are_inclusive_as_documented(self, bronze_frame):
        frame = bronze_frame(
            [
                trip("min_valid_fare", fare_amount=0.01),
                trip("max_valid_fare", fare_amount=1000.0),
                trip("min_valid_distance", trip_distance=0.01, dropoff="2019-03-13 09:01:00"),
                trip("max_valid_passengers", passenger_count=6.0),
                trip("lowest_zone", pu_location=1, do_location=1),
                trip("highest_zone", pu_location=265, do_location=265),
            ]
        )
        assert set(owning_rules(frame).values()) == {"kept"}

    def test_speed_rule_does_not_fire_without_positive_duration(self, bronze_frame):
        """A zero duration trip belongs to R06, not to the speed rule.

        Without the null guard in implied_speed_mph this row divides by zero, and the
        row gets attributed to whichever rule the optimiser happens to reach first.
        """
        frame = bronze_frame([trip("zero_duration_long_distance", dropoff="2019-03-13 09:00:00", trip_distance=50.0)])
        evaluated = with_rule_evaluation(frame, THRESHOLDS)
        row = evaluated.select("owning_rule", "flag_R08").collect()[0]
        assert row["owning_rule"] == "R06"
        assert row["flag_R08"] is False


class TestDerivedColumns:
    def test_duration_in_minutes(self, bronze_frame):
        frame = bronze_frame([trip("t", pickup="2019-03-13 09:00:00", dropoff="2019-03-13 09:20:00")])
        assert frame.select(trip_duration_minutes()).collect()[0][0] == pytest.approx(20.0)

    def test_implied_speed(self, bronze_frame):
        # 10 miles covered in 30 minutes is 20 mph.
        frame = bronze_frame(
            [trip("t", pickup="2019-03-13 09:00:00", dropoff="2019-03-13 09:30:00", trip_distance=10.0)]
        )
        assert frame.select(implied_speed_mph()).collect()[0][0] == pytest.approx(20.0)

    def test_implied_speed_is_null_when_duration_is_not_positive(self, bronze_frame):
        frame = bronze_frame([trip("t", dropoff="2019-03-13 09:00:00")])
        assert frame.select(implied_speed_mph()).collect()[0][0] is None


class TestExclusionLedger:
    @pytest.fixture
    def mixed_frame(self, bronze_frame):
        """Six kept rows and six excluded rows, one per rule family, plus overlaps."""
        return bronze_frame(
            [
                trip("keep_1"),
                trip("keep_2", passenger_count=1.0),
                trip("keep_3", trip_distance=0.5, fare_amount=5.0),
                trip("keep_warned_1", passenger_count=0.0),
                trip("keep_warned_2", ratecode=99.0),
                trip("keep_warned_3", passenger_count=None, payment_type=0),
                trip("drop_fare", fare_amount=0.0),
                trip("drop_total", total_amount=-5.0),
                trip("drop_distance", trip_distance=0.0),
                trip("drop_time", dropoff="2019-03-13 08:00:00"),
                trip("drop_passengers", passenger_count=9.0),
                # Violates R01, R04 and R06 at once. It must be counted as one lost row.
                trip("drop_triple_overlap", fare_amount=-1.0, trip_distance=0.0, dropoff="2019-03-13 08:00:00"),
            ]
        )

    def test_ledger_reconciles_exactly(self, mixed_frame):
        evaluated = with_rule_evaluation(mixed_frame, THRESHOLDS)
        ledger = summarise_ledger(build_ledger(evaluated), bronze_rows=12)

        assert ledger["reconciles"] is True
        assert ledger["reconciliation_difference"] == 0
        assert ledger["kept_rows"] == 6
        assert ledger["excluded_rows"] == 6
        assert ledger["sum_of_rows_first_excluded"] == 6

    def test_overlapping_violations_are_counted_once_in_exclusions(self, mixed_frame):
        """The whole point of the ownership column.

        The triple overlap row trips three rules. Flagged counts see it three times,
        exclusion counts must see it exactly once, under the first rule in precedence.
        """
        evaluated = with_rule_evaluation(mixed_frame, THRESHOLDS)
        ledger = summarise_ledger(build_ledger(evaluated), bronze_rows=12)
        lines = {line["rule_id"]: line for line in ledger["lines"]}

        assert lines["R01"]["rows_flagged"] == 2
        assert lines["R04"]["rows_flagged"] == 2
        assert lines["R06"]["rows_flagged"] == 2

        assert lines["R01"]["rows_first_excluded"] == 2
        assert lines["R04"]["rows_first_excluded"] == 1
        assert lines["R06"]["rows_first_excluded"] == 1

        total_flagged = sum(
            line["rows_flagged"] for line in ledger["lines"] if line["severity"] == "REJECT"
        )
        assert total_flagged > ledger["excluded_rows"]

    def test_warn_rules_never_contribute_exclusions(self, mixed_frame):
        evaluated = with_rule_evaluation(mixed_frame, THRESHOLDS)
        ledger = summarise_ledger(build_ledger(evaluated), bronze_rows=12)
        for line in ledger["lines"]:
            if line["severity"] == "WARN":
                assert line["rows_first_excluded"] == 0
                assert line["rows_flagged_within_kept"] > 0

    def test_ledger_reports_every_rule(self, mixed_frame):
        evaluated = with_rule_evaluation(mixed_frame, THRESHOLDS)
        ledger = summarise_ledger(build_ledger(evaluated), bronze_rows=12)
        assert {line["rule_id"] for line in ledger["lines"]} == {r.id for r in SILVER_RULES}

    def test_reconciliation_failure_is_detectable(self, mixed_frame):
        """Feed a wrong input count and the ledger must refuse to agree with itself."""
        evaluated = with_rule_evaluation(mixed_frame, THRESHOLDS)
        ledger = summarise_ledger(build_ledger(evaluated), bronze_rows=99)
        assert ledger["reconciles"] is False
        assert ledger["reconciliation_difference"] == 99 - 12


class TestConform:
    def test_unknown_passenger_count_is_nulled_not_zeroed(self, bronze_frame):
        frame = bronze_frame([trip("zero", passenger_count=0.0), trip("real", passenger_count=3.0)])
        evaluated = with_rule_evaluation(frame, THRESHOLDS)
        rows = {
            r["quality_passenger_count_unknown"]: r["passenger_count"]
            for r in conform(evaluated).collect()
        }
        assert rows[True] is None
        assert rows[False] == 3

    def test_conform_derives_calendar_and_rate_columns(self, bronze_frame):
        frame = bronze_frame(
            [trip("t", pickup="2019-03-13 14:30:00", dropoff="2019-03-13 15:00:00", trip_distance=6.0, fare_amount=24.0)]
        )
        row = conform(with_rule_evaluation(frame, THRESHOLDS)).collect()[0]
        assert row["pickup_hour"] == 14
        assert row["pickup_day_of_week"] == 4  # Spark numbers Sunday as 1, so Wednesday is 4
        assert row["pickup_month"] == "2019-03"
        assert row["trip_duration_minutes"] == pytest.approx(30.0)
        assert row["fare_per_mile"] == pytest.approx(4.0)

    def test_conform_output_carries_no_source_column_names(self, bronze_frame):
        frame = bronze_frame([trip("t")])
        columns = set(conform(with_rule_evaluation(frame, THRESHOLDS)).columns)
        assert not columns & {"VendorID", "PULocationID", "tpep_pickup_datetime", "RatecodeID"}


class TestRuleDefinitions:
    def test_rule_ids_are_unique(self):
        ids = [r.id for r in SILVER_RULES]
        assert len(ids) == len(set(ids))

    def test_every_rule_states_a_rationale(self):
        for rule in SILVER_RULES:
            assert len(rule.rationale) > 40, f"{rule.id} needs a written justification"

    def test_reject_rules_are_the_severity_they_claim(self):
        assert all(r.severity is Severity.REJECT for r in REJECT_RULES)
