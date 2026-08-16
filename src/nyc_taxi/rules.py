"""Data quality rules as declarative objects rather than an inline filter chain.

Each rule owns its identifier, severity, predicate and written rationale. The silver stage
evaluates every rule over the same scan, which is what makes the exclusion ledger
reconcile exactly instead of being reconstructed from separate counts.

Two severities, and the distinction matters:

    REJECT  the record cannot describe a real trip, so it leaves the kept set
    WARN    the trip is real but one attribute is unreliable, so the row is kept and
            flagged

The separation exists because of something the raw profile turned up. 785,404 trips carry
passenger_count = 0 and a further 197,782 carry null, yet those rows have valid
timestamps, distances and fares. They are metering and vendor reporting gaps, not phantom
trips. Excluding them would quietly bias the very demand series the gold layer is built to
produce, so they are kept and flagged instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import functions as F

from nyc_taxi.config import QualityThresholds


class Severity(str, Enum):
    REJECT = "REJECT"
    WARN = "WARN"


@dataclass(frozen=True)
class Rule:
    id: str
    name: str
    severity: Severity
    description: str
    rationale: str
    condition: Callable[[QualityThresholds], Column]

    def violation(self, thresholds: QualityThresholds) -> Column:
        """Column that is True exactly when the record violates this rule."""
        return self.condition(thresholds)


def _pickup() -> Column:
    return F.col("tpep_pickup_datetime")


def _dropoff() -> Column:
    return F.col("tpep_dropoff_datetime")


def trip_duration_minutes() -> Column:
    return (_dropoff().cast("long") - _pickup().cast("long")) / 60.0


def implied_speed_mph() -> Column:
    """Average speed implied by the odometer distance and the metered duration.

    Null when the duration is not positive, so the speed rule never double counts a
    record that the timestamp ordering rule already caught.
    """
    duration_hours = trip_duration_minutes() / 60.0
    return F.when(duration_hours > 0, F.col("trip_distance") / duration_hours)


# Rule order is significant. The ledger attributes each excluded row to the first REJECT
# rule it violates, so the ordering below is the documented precedence: money problems,
# then distance, then time, then derived physics, then dimensional integrity.
SILVER_RULES: tuple[Rule, ...] = (
    Rule(
        id="R01",
        name="fare_not_positive",
        severity=Severity.REJECT,
        description="fare_amount is null, zero or negative",
        rationale=(
            "The 2019 metered fare begins at a 2.50 USD initial charge, so a fare at or "
            "below zero is a voided, refunded or miskeyed record rather than a trip that "
            "was actually sold."
        ),
        condition=lambda t: F.col("fare_amount").isNull() | (F.col("fare_amount") < t.min_fare_amount),
    ),
    Rule(
        id="R02",
        name="fare_implausibly_high",
        severity=Severity.REJECT,
        description="fare_amount above the plausible ceiling",
        rationale=(
            "The observed maximum in the raw feed is 943,274.80 USD. The ceiling is set "
            "at roughly ten times the flat JFK to Manhattan rate so that a genuinely long "
            "or heavily tolled trip survives while a keying error does not."
        ),
        condition=lambda t: F.col("fare_amount") > t.max_fare_amount,
    ),
    Rule(
        id="R03",
        name="total_amount_negative",
        severity=Severity.REJECT,
        description="total_amount is negative",
        rationale=(
            "A negative total is the chargeback or correction side of a voided trip. It "
            "is a real accounting event but not a journey, and leaving it in makes any "
            "revenue aggregate understate reality."
        ),
        condition=lambda t: F.col("total_amount") < t.min_total_amount,
    ),
    Rule(
        id="R04",
        name="distance_not_positive",
        severity=Severity.REJECT,
        description="trip_distance is null, zero or negative",
        rationale=(
            "308,281 records pair a zero odometer distance with a positive fare, the "
            "classic TLC meter fault. The trip cannot be placed on the network, so it is "
            "unusable for the zone level demand and fare per mile work downstream."
        ),
        condition=lambda t: F.col("trip_distance").isNull()
        | (F.col("trip_distance") < t.min_trip_distance),
    ),
    Rule(
        id="R05",
        name="distance_implausibly_long",
        severity=Severity.REJECT,
        description="trip_distance above the plausible ceiling",
        rationale=(
            "The raw maximum is 45,977 miles, roughly twice the circumference of the "
            "earth. 200 miles comfortably clears any real yellow cab run out of the metro "
            "area."
        ),
        condition=lambda t: F.col("trip_distance") > t.max_trip_distance_miles,
    ),
    Rule(
        id="R06",
        name="dropoff_not_after_pickup",
        severity=Severity.REJECT,
        description="dropoff timestamp is not strictly after the pickup timestamp",
        rationale=(
            "Covers both the 54 records where the clock runs backwards and the 42,070 "
            "where pickup and dropoff are identical. Neither can yield a duration, which "
            "every speed and demand measure depends on."
        ),
        condition=lambda t: _dropoff().isNull() | _pickup().isNull() | (_dropoff() <= _pickup()),
    ),
    Rule(
        id="R07",
        name="duration_implausibly_long",
        severity=Severity.REJECT,
        description="metered duration longer than 24 hours",
        rationale=(
            "A meter left running overnight describes the meter, not the journey. The "
            "bound is deliberately loose so that genuine long distance hires survive."
        ),
        condition=lambda t: trip_duration_minutes() > t.max_trip_duration_minutes,
    ),
    Rule(
        id="R08",
        name="implied_speed_implausible",
        severity=Severity.REJECT,
        description="implied average speed above the plausible ceiling",
        rationale=(
            "A cross check that catches contradictions the single column rules miss: a "
            "distance and a duration that are each individually plausible but impossible "
            "together. 100 mph is unreachable as a trip average on New York streets."
        ),
        condition=lambda t: implied_speed_mph() > t.max_implied_speed_mph,
    ),
    Rule(
        id="R09",
        name="passenger_count_above_licensed_max",
        severity=Severity.REJECT,
        description="passenger_count above the licensed vehicle maximum",
        rationale=(
            "A yellow medallion vehicle is licensed to carry at most 6 passengers in its "
            "minivan configuration. Values of 7 to 9 appear in the feed and cannot be "
            "physically true."
        ),
        condition=lambda t: F.col("passenger_count") > t.max_passenger_count,
    ),
    Rule(
        id="R10",
        name="location_id_out_of_range",
        severity=Severity.REJECT,
        description="pickup or dropoff LocationID outside the published TLC zone range",
        rationale=(
            "Referential integrity against the zone lookup, which covers 1 to 265 with no "
            "gaps. An out of range identifier would silently drop the row at the gold "
            "join, so it is caught and counted here instead."
        ),
        condition=lambda t: F.col("PULocationID").isNull()
        | F.col("DOLocationID").isNull()
        | ~F.col("PULocationID").between(t.min_location_id, t.max_location_id)
        | ~F.col("DOLocationID").between(t.min_location_id, t.max_location_id),
    ),
    Rule(
        id="W01",
        name="passenger_count_unknown",
        severity=Severity.WARN,
        description="passenger_count is null or zero",
        rationale=(
            "Kept deliberately. These rows carry valid timestamps, distances and fares, "
            "so the trip happened and belongs in the demand series. Only the occupancy "
            "attribute is unreliable, and it is nulled and flagged rather than trusted."
        ),
        condition=lambda t: F.col("passenger_count").isNull()
        | (F.col("passenger_count") < t.min_passenger_count),
    ),
    Rule(
        id="W02",
        name="ratecode_undocumented",
        severity=Severity.WARN,
        description="RatecodeID is null or not in the published code list",
        rationale=(
            "Value 99 appears in the feed and is not in the TLC data dictionary. It "
            "affects how the fare should be interpreted but not whether the trip "
            "occurred, so the row stays and the code is flagged."
        ),
        condition=lambda t: F.col("RatecodeID").isNull()
        | ~F.col("RatecodeID").isin([float(v) for v in t.valid_ratecode_ids]),
    ),
    Rule(
        id="W03",
        name="payment_type_undocumented",
        severity=Severity.WARN,
        description="payment_type is null or not in the published code list",
        rationale=(
            "Concentrated in the same vendor reporting gap as W01. Relevant to revenue "
            "attribution, irrelevant to whether a passenger was carried."
        ),
        condition=lambda t: F.col("payment_type").isNull()
        | ~F.col("payment_type").isin(list(range(1, 7))),
    ),
)

REJECT_RULES = tuple(r for r in SILVER_RULES if r.severity is Severity.REJECT)
WARN_RULES = tuple(r for r in SILVER_RULES if r.severity is Severity.WARN)

RULES_BY_ID = {r.id: r for r in SILVER_RULES}


def first_violated_reject_rule(thresholds: QualityThresholds) -> Column:
    """Attribute each row to the first REJECT rule it violates, else 'kept'.

    This is what makes the ledger add up. Rules overlap heavily, a single record can be
    zero distance and zero fare and backwards in time at once, so a per rule count of
    violations will always exceed the number of rows removed. Assigning every excluded
    row to exactly one owning rule gives a partition of the raw row count.
    """
    expr = F.lit("kept")
    for rule in reversed(REJECT_RULES):
        expr = F.when(rule.violation(thresholds), F.lit(rule.id)).otherwise(expr)
    return expr


def any_reject_violation(thresholds: QualityThresholds) -> Column:
    condition = F.lit(False)
    for rule in REJECT_RULES:
        condition = condition | rule.violation(thresholds)
    return condition
