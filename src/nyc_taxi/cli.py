"""Single entrypoint for the pipeline. Runs one stage or the whole chain.

    spark-submit --driver-memory 4g src/nyc_taxi/cli.py --stage all
    python -m nyc_taxi.cli --stage silver

Every stage writes its measured metrics to reports/evidence as it goes, so a partial run
still leaves behind the numbers for the stages that completed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

# Allows the module to be handed straight to spark-submit as a file path, where the
# package will not already be on sys.path.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nyc_taxi.analytics import run_analytics  # noqa: E402
from nyc_taxi.bronze import run_bronze  # noqa: E402
from nyc_taxi.config import DEFAULT_CONFIG, REPORTS_ROOT  # noqa: E402
from nyc_taxi.gold import run_gold  # noqa: E402
from nyc_taxi.silver import run_silver  # noqa: E402
from nyc_taxi.spark import build_spark, timed  # noqa: E402

STAGES = ("bronze", "silver", "gold", "analytics")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC taxi bronze to gold pipeline")
    parser.add_argument(
        "--stage",
        default="all",
        choices=(*STAGES, "all"),
        help="stage to run, or all to run bronze through analytics in order",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="compute and report metrics without writing layer output",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = DEFAULT_CONFIG
    stages = STAGES if args.stage == "all" else (args.stage,)

    spark = build_spark(cfg.spark, app_suffix=args.stage)
    # Read back from the live session rather than from config. Under spark-submit the
    # command line wins, and the run should report what it is actually using.
    conf = spark.sparkContext.getConf()
    print(
        f"Spark {spark.version} on {conf.get('spark.master')}, "
        f"driver memory {conf.get('spark.driver.memory', 'default')}"
    )
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

    summary: dict = {}
    run_timings: dict = {}
    try:
        for stage in stages:
            with timed(f"STAGE {stage}", run_timings):
                if stage == "bronze":
                    summary[stage] = run_bronze(spark, cfg, write=not args.dry_run)
                elif stage == "silver":
                    summary[stage] = run_silver(spark, cfg, write=not args.dry_run)
                elif stage == "gold":
                    summary[stage] = run_gold(spark, cfg, write=not args.dry_run)
                elif stage == "analytics":
                    if args.dry_run:
                        print("analytics needs written gold output, skipped under --dry-run")
                        continue
                    summary[stage] = run_analytics(spark, cfg)
    finally:
        spark.stop()

    # Merged rather than overwritten. Stages are often run one at a time during
    # development, and a plain write would leave the file describing only whichever stage
    # happened to run last, which is not a record of the pipeline.
    timings_path = REPORTS_ROOT / "evidence" / "run_timings.json"
    timings_path.parent.mkdir(parents=True, exist_ok=True)
    recorded: dict = {}
    if timings_path.exists():
        try:
            recorded = json.loads(timings_path.read_text())
        except json.JSONDecodeError:
            recorded = {}
    stamp = dt.datetime.now().isoformat(timespec="seconds")
    for stage, elapsed in run_timings.items():
        recorded[stage] = {"seconds": elapsed, "recorded_at": stamp}
    timings_path.write_text(json.dumps(recorded, indent=2, sort_keys=True) + "\n")

    print("\n" + "=" * 72)
    print("RUN SUMMARY")
    print("=" * 72)
    for stage, elapsed in run_timings.items():
        print(f"  {stage:24} {elapsed:>8,.1f}s")
    if "bronze" in summary:
        b = summary["bronze"]
        print(f"\n  raw rows            {b['raw_rows']:>14,}")
        print(f"  bronze accepted     {b['accepted_rows']:>14,}")
        print(f"  bronze rejected     {b['rejected_rows']:>14,}")
    if "silver" in summary:
        led = summary["silver"]["ledger"]
        print(f"  silver kept         {led['kept_rows']:>14,}")
        print(f"  silver excluded     {led['excluded_rows']:>14,}")
        print(f"  ledger reconciles   {str(led['reconciles']):>14}")
    if "gold" in summary and "gold_trip_rows" in summary["gold"]:
        print(f"  gold trips          {summary['gold']['gold_trip_rows']:>14,}")
    if "analytics" in summary:
        fa = summary["analytics"]["fare_anomaly_summary"]
        print(f"  fares flagged       {fa['trips_flagged']:>14,}  ({fa['flagged_pct_of_gold']}%)")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
