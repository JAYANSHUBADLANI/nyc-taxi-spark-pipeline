SHELL := /bin/bash
PYTHON ?= .venv/bin/python
SPARK_SUBMIT ?= .venv/bin/spark-submit

# PySpark needs a JDK 17 runtime. Set JAVA_HOME in the environment, or point it here.
JAVA_HOME ?= $(HOME)/.jdks/jdk-17.0.20+8/Contents/Home
export JAVA_HOME

# spark-submit is a shell script that shells back out to `python` from PATH to locate its
# own installation. With PySpark installed in a virtual environment that is not activated,
# that lookup finds an interpreter without pyspark and the script dies with a confusing
# "/bin/spark-class: No such file or directory". Resolving SPARK_HOME from the venv's own
# pyspark package removes the guesswork, and pinning PYSPARK_PYTHON makes the driver and
# the workers use the same interpreter as everything else here.
SPARK_HOME := $(shell $(PYTHON) -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))" 2>/dev/null)
PYSPARK_PYTHON := $(abspath $(PYTHON))
export SPARK_HOME
export PYSPARK_PYTHON
export PYSPARK_DRIVER_PYTHON = $(PYSPARK_PYTHON)

.PHONY: help setup download profile bronze silver gold analytics pipeline benchmark test clean clean-data

help:
	@echo "make setup      create the virtual environment and install the package"
	@echo "make download   fetch the TLC trip files and the zone lookup"
	@echo "make profile    profile the raw feed before any cleaning"
	@echo "make pipeline   run bronze through analytics on the full dataset"
	@echo "make bronze     run a single stage (also: silver, gold, analytics)"
	@echo "make benchmark  measure the partitioning and file layout choices"
	@echo "make test       run the unit tests against small in memory samples"
	@echo "make clean      remove derived layers, keeps the downloaded raw files"

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"

download:
	$(PYTHON) scripts/download_data.py

profile:
	$(PYTHON) scripts/profile_raw.py

# The documented full run. spark-submit is the entrypoint a scheduler would call, and the
# settings below win over the defaults in config.py because the application deliberately
# does not override a submitted configuration. See build_spark.
pipeline:
	$(SPARK_SUBMIT) \
		--master "local[8]" \
		--driver-memory 4g \
		--conf spark.sql.shuffle.partitions=64 \
		--conf spark.sql.adaptive.enabled=true \
		--name nyc-taxi-pipeline \
		src/nyc_taxi/cli.py --stage all

bronze silver gold analytics:
	$(PYTHON) -m nyc_taxi.cli --stage $@

benchmark:
	$(PYTHON) scripts/benchmark_partitioning.py

test:
	$(PYTHON) -m pytest tests -q

clean:
	rm -rf data/bronze data/silver data/gold

clean-data: clean
	rm -rf data/raw
