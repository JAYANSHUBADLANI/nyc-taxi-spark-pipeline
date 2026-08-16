"""Central configuration: paths, run parameters, and the data quality rule thresholds.

Every threshold that decides whether a trip record is kept or excluded lives here so the
assumptions are in one auditable place rather than scattered through the transform code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_ROOT = Path(os.environ.get("NYC_TAXI_DATA_ROOT", PROJECT_ROOT / "data"))
REPORTS_ROOT = Path(os.environ.get("NYC_TAXI_REPORTS_ROOT", PROJECT_ROOT / "reports"))

RAW_TRIPS_DIR = DATA_ROOT / "raw" / "trips"
RAW_REFERENCE_DIR = DATA_ROOT / "raw" / "reference"
ZONE_LOOKUP_CSV = RAW_REFERENCE_DIR / "taxi_zone_lookup.csv"

BRONZE_DIR = DATA_ROOT / "bronze" / "trips"
BRONZE_REJECTED_DIR = DATA_ROOT / "bronze" / "rejected"
BRONZE_ZONES_DIR = DATA_ROOT / "bronze" / "zones"

SILVER_DIR = DATA_ROOT / "silver" / "trips"
SILVER_EXCLUDED_DIR = DATA_ROOT / "silver" / "excluded"

GOLD_TRIPS_DIR = DATA_ROOT / "gold" / "trips"
GOLD_TRIPS_UNPARTITIONED_DIR = DATA_ROOT / "gold" / "trips_unpartitioned"
GOLD_ANALYTICS_DIR = DATA_ROOT / "gold" / "analytics"

EVIDENCE_DIR = REPORTS_ROOT / "evidence"

# Months pulled from the TLC public trip record archive. Confirmed available and
# schema identical across all six files at build time.
TRIP_MONTHS = ["2019-01", "2019-02", "2019-03", "2019-04", "2019-05", "2019-06"]

TLC_TRIP_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
)
TLC_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def _env(name: str, default: str) -> Callable[[], str]:
    """Defer an environment lookup to instantiation time.

    A bare `os.environ.get(...)` as a dataclass default is evaluated once, when the module
    is first imported, which binds the whole configuration to whatever the environment
    happened to be at import. It works when every variable is exported before the process
    starts, and it silently ignores anything set afterwards. Reading through a factory
    instead means a SparkConfig reflects the environment at the moment it is built.
    """
    return lambda: os.environ.get(name, default)


def _env_int(name: str, default: int) -> Callable[[], int]:
    return lambda: int(os.environ.get(name, str(default)))


@dataclass(frozen=True)
class SparkConfig:
    """Local Spark sizing. Tuned for an 8 core / 8 GB developer machine."""

    master: str = field(default_factory=_env("NYC_TAXI_SPARK_MASTER", "local[8]"))
    app_name: str = "nyc-taxi-pipeline"
    driver_memory: str = field(default_factory=_env("NYC_TAXI_DRIVER_MEMORY", "4g"))
    shuffle_partitions: int = field(default_factory=_env_int("NYC_TAXI_SHUFFLE_PARTITIONS", 64))
    # The source files carry a single Parquet row group each, so a plain read yields one
    # task per file no matter how many cores exist. Bronze fixes that by splitting its
    # output on write instead of shuffling: at 1.4 million rows per file the six source
    # files land as roughly 36 pieces, giving every later stage a wide scan. Measured
    # against the shuffle based alternative in docs/partitioning.md.
    bronze_max_rows_per_file: int = field(
        default_factory=_env_int("NYC_TAXI_BRONZE_ROWS_PER_FILE", 1_400_000)
    )
    # Partition count for the deliberately unpartitioned gold baseline, which exists only
    # as the comparison case for the pruning measurement.
    baseline_output_partitions: int = field(
        default_factory=_env_int("NYC_TAXI_BASELINE_PARTITIONS", 32)
    )
    # Gold is range partitioned on pickup date rather than hash partitioned. The loaded
    # window holds 181 dates, so a slightly larger number of range partitions gives each
    # write task a contiguous slice of roughly one day, which means one open Parquet
    # writer per task instead of one per date the task happens to hold.
    gold_date_partitions: int = field(
        default_factory=_env_int("NYC_TAXI_GOLD_DATE_PARTITIONS", 192)
    )
    local_dir: str | None = field(
        default_factory=lambda: os.environ.get("NYC_TAXI_SPARK_LOCAL_DIR")
    )


@dataclass(frozen=True)
class QualityThresholds:
    """Boundaries used by the silver cleaning rules.

    These are judgement calls, not facts published by the TLC. They are set to be
    permissive enough that a legitimate but unusual trip survives, and tight enough that
    a physically impossible one does not. Each is justified in docs/data_quality.md.
    """

    # Valid TLC taxi zone identifiers. The published lookup covers 1 to 265 with no gaps,
    # where 264 is "Unknown" and 265 is "Outside of NYC".
    min_location_id: int = 1
    max_location_id: int = 265

    # A yellow medallion cab is licensed for at most 6 passengers (minivan configuration).
    min_passenger_count: int = 1
    max_passenger_count: int = 6

    # Metered fare must be positive. The TLC initial charge alone is 2.50 USD in 2019, so
    # a zero or negative metered fare is a voided or miskeyed record, not a real trip.
    min_fare_amount: float = 0.01
    # Roughly ten times the flat JFK to Manhattan rate. Above this is a keying error.
    max_fare_amount: float = 1000.0

    # A charged trip must have moved. Zero distance with a positive fare is the classic
    # TLC meter fault.
    min_trip_distance: float = 0.01
    # Longer than any plausible in-service yellow cab trip inside the metro area.
    max_trip_distance_miles: float = 200.0

    # Duration bounds. Dropoff must follow pickup, and a metered trip should not run for
    # a full day.
    max_trip_duration_minutes: float = 24 * 60.0

    # Implied average speed. Above this the timestamps or the odometer are wrong.
    max_implied_speed_mph: float = 100.0

    # Total charged to the passenger cannot be negative.
    min_total_amount: float = 0.0

    # RatecodeID values documented by the TLC data dictionary. 99 appears in the raw feed
    # and is not a documented code.
    valid_ratecode_ids: tuple[int, ...] = (1, 2, 3, 4, 5, 6)


@dataclass(frozen=True)
class AnomalyThresholds:
    """Parameters for the rule based fare anomaly flag.

    This is a heuristic screen for review, not a validated fraud model.
    """

    # A trip's fare is compared against the distribution of fares within its own peer
    # group, defined as pickup zone crossed with distance band crossed with duration band.
    # Flagged when it sits this many robust standard deviations from that group's median.
    robust_z_threshold: float = 5.0
    # Peer groups smaller than this are not scored, the estimate would be unstable.
    min_peer_group_size: int = 100
    # Floor on the scale used to score a deviation, as a fraction of the group's own
    # median fare. Regulated flat rate journeys, JFK to Manhattan above all, produce peer
    # groups where every single fare is identical and the median absolute deviation is
    # exactly zero. Dividing by that, or by a token 0.01, declares a fare one cent off the
    # flat rate to be a 67 sigma event. Measured: with a 0.01 floor the screen flagged
    # 16.2 percent of all 10 to 20 mile trips and 25.8 percent of everything above 20
    # miles, which is the airport traffic and not fraud. A floor proportional to the fare
    # keeps a dollar of noise on a 7 dollar fare and a dollar of noise on a 52 dollar fare
    # in proportion to each other.
    relative_scale_floor: float = 0.05
    # Absolute floor underneath the relative one, so a very cheap peer group cannot drive
    # the scale to near zero either.
    min_scale: float = 0.50
    # Distance bands in miles used to build peer groups.
    distance_band_edges: tuple[float, ...] = (0.0, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 1e9)
    # Duration bands in minutes. The New York meter charges for time as well as distance,
    # 50 cents per minute below 12 mph in 2019, so a trip that is short in miles and long
    # in minutes has a legitimately high fare. Comparing on distance alone treats every
    # traffic jam as an anomaly, which the first version of this screen duly did.
    duration_band_edges: tuple[float, ...] = (0.0, 5.0, 10.0, 20.0, 40.0, 1e9)


@dataclass(frozen=True)
class PipelineConfig:
    spark: SparkConfig = field(default_factory=SparkConfig)
    quality: QualityThresholds = field(default_factory=QualityThresholds)
    anomaly: AnomalyThresholds = field(default_factory=AnomalyThresholds)
    months: list[str] = field(default_factory=lambda: list(TRIP_MONTHS))


DEFAULT_CONFIG = PipelineConfig()
