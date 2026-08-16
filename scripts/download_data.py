"""Download the TLC trip files and the zone lookup.

The TLC publishes these over plain HTTPS with no account and no API key, which is the
whole reason this dataset was chosen. Re-running is safe, a file already present with a
plausible size is left alone.

Run: make download
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi.config import (  # noqa: E402
    RAW_REFERENCE_DIR,
    RAW_TRIPS_DIR,
    TLC_TRIP_URL_TEMPLATE,
    TLC_ZONE_LOOKUP_URL,
    TRIP_MONTHS,
    ZONE_LOOKUP_CSV,
)

MIN_PLAUSIBLE_TRIP_BYTES = 50 * 1024**2


def download(url: str, destination: Path, min_bytes: int = 0) -> bool:
    """Fetch one file. Returns True if it was downloaded, False if already present."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size >= min_bytes:
        print(f"  present  {destination.name} ({destination.stat().st_size / 1024**2:.1f} MB)")
        return False

    print(f"  fetching {destination.name} from {url}")
    tmp = destination.with_suffix(destination.suffix + ".part")
    with urllib.request.urlopen(url) as response, tmp.open("wb") as handle:
        while chunk := response.read(1024 * 1024):
            handle.write(chunk)
    tmp.replace(destination)
    print(f"  done     {destination.name} ({destination.stat().st_size / 1024**2:.1f} MB)")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="download the raw TLC inputs")
    parser.add_argument("--months", nargs="*", default=TRIP_MONTHS, help="months as YYYY-MM")
    args = parser.parse_args()

    print(f"trip files -> {RAW_TRIPS_DIR}")
    for month in args.months:
        download(
            TLC_TRIP_URL_TEMPLATE.format(month=month),
            RAW_TRIPS_DIR / f"yellow_tripdata_{month}.parquet",
            min_bytes=MIN_PLAUSIBLE_TRIP_BYTES,
        )

    print(f"reference data -> {RAW_REFERENCE_DIR}")
    download(TLC_ZONE_LOOKUP_URL, ZONE_LOOKUP_CSV, min_bytes=1024)

    total = sum(p.stat().st_size for p in RAW_TRIPS_DIR.glob("*.parquet"))
    print(f"\n{len(list(RAW_TRIPS_DIR.glob('*.parquet')))} trip files, {total / 1024**2:.0f} MB total")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
