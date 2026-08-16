"""SparkSession construction and small helpers shared by every layer."""

from __future__ import annotations

import contextlib
import json
import os
import time
from pathlib import Path
from typing import Iterator
from urllib.parse import quote

from pyspark.sql import DataFrame, SparkSession

from nyc_taxi.config import DEFAULT_CONFIG, EVIDENCE_DIR, PROJECT_ROOT, SparkConfig


def portable_evidence_text(text: str) -> str:
    """Remove the checkout-specific root from evidence intended for version control."""
    root = str(PROJECT_ROOT)
    return text.replace(root, "<PROJECT_ROOT>").replace(
        quote(root, safe="/"), "<PROJECT_ROOT>"
    )


def portable_path(path: Path) -> str:
    """Render paths inside the checkout relative to it, avoiding machine-local prefixes."""
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return portable_evidence_text(str(path))


def _portable_evidence_value(value):
    """Recursively make path-bearing strings safe to commit as JSON evidence."""
    if isinstance(value, str):
        return portable_evidence_text(value)
    if isinstance(value, dict):
        return {key: _portable_evidence_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_portable_evidence_value(item) for item in value]
    return value


def running_under_spark_submit() -> bool:
    """True when the JVM was already launched by spark-submit.

    PYSPARK_GATEWAY_PORT is set by the launcher and is how PySpark itself decides to attach
    to an existing gateway rather than starting one. It matters here because a job started
    through spark-submit already has the configuration the operator asked for on the command
    line: driver memory cannot be changed once the JVM is up, so setting it in code is
    silently ignored, and setting master in code would override the submitted value. Either
    way the run would report settings it is not using, which is the sort of small untruth
    that costs somebody an afternoon.
    """
    return "PYSPARK_GATEWAY_PORT" in os.environ


def build_spark(
    cfg: SparkConfig | None = None,
    app_suffix: str | None = None,
    keep_ui_alive: bool = False,
) -> SparkSession:
    """Create a local SparkSession sized by config.

    keep_ui_alive leaves the Spark UI reachable while the job runs so the SQL tab can be
    inspected for shuffle behaviour.
    """
    cfg = cfg or DEFAULT_CONFIG.spark
    name = cfg.app_name if app_suffix is None else f"{cfg.app_name}-{app_suffix}"

    builder = SparkSession.builder.appName(name)
    if not running_under_spark_submit():
        builder = builder.master(cfg.master).config("spark.driver.memory", cfg.driver_memory)

    builder = (
        builder.config("spark.sql.shuffle.partitions", cfg.shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        # Parquet written by older tools uses the legacy calendar. Fail loudly rather
        # than silently shifting dates.
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "EXCEPTION")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
        # Parquet buffers a whole row group in memory before flushing it, and that buffer
        # is invisible to Spark's memory manager. At the 128 MB default, eight concurrent
        # writer tasks reserve up to a gigabyte that the manager does not know it has lost,
        # and the window sort running alongside them fails to acquire a page it should
        # have had. This is not theoretical, it killed a full gold run with
        # UNABLE_TO_ACQUIRE_MEMORY. 32 MB row groups are still comfortably large enough to
        # read efficiently and leave the execution pool intact.
        .config("spark.hadoop.parquet.block.size", 32 * 1024 * 1024)
        # Keep one output file writer open per task. Spark then sorts by the partition
        # column and writes each directory to completion instead of holding several open.
        .config("spark.sql.maxConcurrentOutputFileWriters", "0")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.eventLog.enabled", "false")
    )
    if cfg.local_dir:
        builder = builder.config("spark.local.dir", cfg.local_dir)
    if keep_ui_alive:
        builder = builder.config("spark.ui.port", "4040")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextlib.contextmanager
def timed(label: str, sink: dict | None = None) -> Iterator[None]:
    """Time a block of work and record it, so README timings come from real runs."""
    start = time.perf_counter()
    print(f"[start] {label}", flush=True)
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"[done ] {label} in {elapsed:,.1f}s", flush=True)
        if sink is not None:
            sink[label] = round(elapsed, 1)


def write_evidence(name: str, payload: dict | list) -> Path:
    """Persist a metrics payload produced by an actual run."""
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / f"{name}.json"
    path.write_text(
        json.dumps(_portable_evidence_value(payload), indent=2, default=str) + "\n"
    )
    print(f"[evidence] wrote {portable_path(path)}", flush=True)
    return path


def write_text_evidence(name: str, text: str) -> Path:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / name
    text = portable_evidence_text(text)
    path.write_text(text if text.endswith("\n") else text + "\n")
    print(f"[evidence] wrote {portable_path(path)}", flush=True)
    return path


def capture_explain(df: DataFrame, mode: str = "formatted") -> str:
    """Return the physical plan as a string instead of printing it to stdout."""
    return df._jdf.queryExecution().explainString(
        df.sparkSession._jvm.org.apache.spark.sql.execution.ExplainMode.fromString(mode)
    )


def directory_stats(path: Path) -> dict:
    """File count, total bytes and per file sizes for a written Parquet directory."""
    if not path.exists():
        return {"path": portable_path(path), "exists": False}
    data_files = [p for p in path.rglob("*.parquet") if p.is_file()]
    sizes = sorted((p.stat().st_size for p in data_files), reverse=True)
    total = sum(sizes)
    leaf_dirs = {p.parent for p in data_files}
    return {
        "path": portable_path(path),
        "exists": True,
        "data_file_count": len(data_files),
        "partition_directory_count": len(leaf_dirs),
        "total_bytes": total,
        "total_mb": round(total / 1024**2, 1),
        "avg_file_mb": round(total / len(sizes) / 1024**2, 2) if sizes else 0.0,
        "largest_file_mb": round(sizes[0] / 1024**2, 2) if sizes else 0.0,
        "smallest_file_mb": round(sizes[-1] / 1024**2, 2) if sizes else 0.0,
    }
