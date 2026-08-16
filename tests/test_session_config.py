"""Tests for how the session decides which configuration wins.

These exist because of a real failure. `make pipeline` is documented as the entrypoint and
it was broken in two separate ways at once: spark-submit could not find its own
installation from inside an unactivated virtual environment, and once that was fixed the
application overrode the submitted master and driver memory with its own defaults, so the
run printed settings it was not using.
"""

from __future__ import annotations

import os

from nyc_taxi.config import PROJECT_ROOT, SparkConfig
from nyc_taxi.spark import (
    portable_evidence_text,
    portable_path,
    running_under_spark_submit,
)


class TestSparkSubmitDetection:
    def test_not_detected_for_a_plain_python_run(self, monkeypatch):
        monkeypatch.delenv("PYSPARK_GATEWAY_PORT", raising=False)
        assert running_under_spark_submit() is False

    def test_detected_when_the_launcher_has_started_a_gateway(self, monkeypatch):
        monkeypatch.setenv("PYSPARK_GATEWAY_PORT", "54321")
        assert running_under_spark_submit() is True

    def test_other_spark_variables_do_not_imply_submit(self, monkeypatch):
        """SPARK_HOME is set by the Makefile for every invocation, submitted or not.

        Keying the decision off it instead would disable the local defaults for a plain
        `python -m nyc_taxi.cli` run, which would then start with no master at all.
        """
        monkeypatch.delenv("PYSPARK_GATEWAY_PORT", raising=False)
        monkeypatch.setenv("SPARK_HOME", "/somewhere/pyspark")
        monkeypatch.setenv("PYSPARK_PYTHON", "/somewhere/python")
        assert running_under_spark_submit() is False


class TestSparkConfigOverrides:
    def test_defaults_are_sized_for_the_documented_machine(self, monkeypatch):
        for var in ("NYC_TAXI_SPARK_MASTER", "NYC_TAXI_DRIVER_MEMORY"):
            monkeypatch.delenv(var, raising=False)
        cfg = SparkConfig()
        assert cfg.master == "local[8]"
        assert cfg.driver_memory == "4g"

    def test_every_sizing_knob_reads_from_the_environment(self, monkeypatch):
        """The knobs are environment driven because one machine needed two sizings.

        The write heavy stages ran at local[8] with 4 GB. The aggregation heavy analytics
        stage drove the same machine into swap at that size and had to come down.
        """
        monkeypatch.setenv("NYC_TAXI_SPARK_MASTER", "local[6]")
        monkeypatch.setenv("NYC_TAXI_DRIVER_MEMORY", "3g")
        monkeypatch.setenv("NYC_TAXI_SHUFFLE_PARTITIONS", "32")
        monkeypatch.setenv("NYC_TAXI_BRONZE_ROWS_PER_FILE", "500000")
        monkeypatch.setenv("NYC_TAXI_GOLD_DATE_PARTITIONS", "96")

        cfg = SparkConfig()
        assert cfg.master == "local[6]"
        assert cfg.driver_memory == "3g"
        assert cfg.shuffle_partitions == 32
        assert cfg.bronze_max_rows_per_file == 500_000
        assert cfg.gold_date_partitions == 96


class TestEvidencePortability:
    def test_project_paths_are_relative(self):
        assert portable_path(PROJECT_ROOT / "data" / "gold" / "trips") == "data/gold/trips"

    def test_plain_and_url_encoded_roots_are_redacted(self):
        root = str(PROJECT_ROOT)
        encoded_root = root.replace(" ", "%20")
        evidence = portable_evidence_text(
            f"Location: file:{root}/data/gold\nURI: file:{encoded_root}/data/silver"
        )

        assert root not in evidence
        assert encoded_root not in evidence
        assert evidence.count("<PROJECT_ROOT>") == 2
