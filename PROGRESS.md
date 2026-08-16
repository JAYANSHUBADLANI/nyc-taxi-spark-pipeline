# Progress

Running notes on what is built, what is left, and what needs a decision from me.

## Environment

The machine this was built on is an 8 core, 8 GB Apple Silicon Mac. That constraint shaped
several design decisions and is worth remembering when reading the timings.

- JDK 17 (Temurin) unpacked to `~/.jdks/jdk-17.0.20+8`. Homebrew could not reach its own
  formula API from this network, so the JDK came straight from Adoptium. `make` exports
  `JAVA_HOME` pointing there, override it if the JDK lives somewhere else.
- PySpark 3.5.3 in `.venv`, project installed editable with `pip install -e ".[dev]"`.

## Done

### Phase 1, ingestion

- [x] Six months of TLC yellow taxi Parquet pulled, 2019-01 to 2019-06, 673 MB, 44,658,561
      rows. Schema verified identical across all six files rather than assumed.
- [x] Taxi zone lookup pulled, 265 zones, IDs 1 to 265 contiguous.
- [x] Raw profile run over the full 44.7 million rows before any rule was written, so the
      cleaning thresholds are set against observed prevalence. Output in
      `reports/evidence/raw_profile.json`.
- [x] Bronze ingestion with an enforced schema contract, provenance stamping, a counted
      rejected records path, and a file layout that restores read parallelism.

### Phase 2, silver

- [x] Thirteen quality rules as declarative objects, ten REJECT and three WARN.
- [x] Exclusion ledger that reconciles raw count to kept count, with per rule ownership so
      overlapping violations are never double counted.

### Phase 3, gold

- [x] Broadcast join against the zone dimension, with both plans captured.
- [x] Row level and aggregate level window functions.
- [x] Date partitioned output plus an unpartitioned baseline for the pruning measurement.

### Phase 4, analytics and packaging

- [x] Demand patterns, fare anomaly screen, quality trend across months.
- [x] Unit tests over the transformation logic, on small in memory frames.
- [x] `make pipeline` entrypoint via spark-submit.

## Decisions I made along the way

These are the ones worth being able to defend, not the routine ones.

1. **Zero and null passenger counts are kept, not excluded.** 785,404 trips report zero
   passengers and 197,782 report null, and all of them carry valid timestamps, distances
   and fares. The null cohort turned out to be entirely VendorID 2 and 5 with an
   undocumented payment type, so it is a vendor reporting gap rather than a phantom trip.
   Excluding roughly a million real trips would bias the demand series the gold layer
   exists to produce, so the attribute is nulled and flagged instead of the row being
   dropped. This is why the rules carry a severity.

2. **Bronze splits files on write rather than repartitioning.** The first version used
   `repartition(32)`, which ran past twelve minutes and spilled over 2.3 GB of shuffle
   before I abandoned it. `maxRecordsPerFile` reaches the same output layout with no
   exchange at all. Both are measured in `make benchmark`.

3. **The schema contract tolerates exactly two differences**, timestamp_ntz against
   timestamp and int32 against the declared airport_fee type, and both are documented in
   `schemas.py`. A third, int to double, looked safe and is not: Spark's vectorised Parquet
   reader refuses it at read time. That one cost a failed run.

4. **The fare anomaly screen went through three versions, and the first two were wrong in
   ways only the full data exposed.**

   Version one compared fare per mile within pickup zone and distance band. It flagged 2.73
   percent of every sub one mile trip, because the New York meter charges by time and a
   short crawl through traffic legitimately costs more per mile than a short clear run. It
   was detecting congestion. Adding a duration band to the peer key dropped that to 0.49
   percent.

   Version two then flagged 16.2 percent of 10 to 20 mile trips and 25.8 percent of
   everything above 20 miles. Those are the airport runs. JFK to Manhattan is a regulated
   flat fare, so those peer groups have a median of exactly 52.00 dollars and a median
   absolute deviation of exactly zero. Scoring against a 0.01 floor makes a fare one cent
   off the flat rate a 67 sigma event. The scale is now floored at 5 percent of the group's
   own median.

   Both are covered by tests now, so neither can come back quietly.

5. **Hourly and daily averages are trip weighted.** The zone hour table stores averages, so
   a plain `avg()` over it is a mean of means that gives a zone with 3 trips in an hour the
   same weight as one with 3,000. It was not a rounding difference: the average fare in the
   midnight hour moved from 16.10 to 13.57 dollars once weighted.

6. **Gold is range partitioned on pickup date, not hash partitioned, and Parquet row
   groups are cut to 32 MB.** The first attempt died with `UNABLE_TO_ACQUIRE_MEMORY` in the
   window operator. The cause was not the window. Parquet buffers a full row group in
   memory before flushing, that buffer is invisible to Spark's memory manager, and at the
   128 MB default eight concurrent writer tasks quietly took a gigabyte the manager still
   believed it had. Hash partitioning made it worse by scattering three or four unrelated
   dates into each task, so each task opened that many output directories at once. Range
   partitioning gives each task a contiguous slice of the calendar and therefore close to
   one open writer.

## Known weak spots

Stated here so nothing in an interview is a surprise. The README covers these in full.

- `local[*]` is not a distributed cluster. The code is cluster shaped, the evidence is
  single machine.
- Every cleaning threshold is a judgement call, not a TLC published standard.
- The fare anomaly flag is a rule based screen, not a validated fraud model. There are no
  labels in this data, so it has no measurable precision or recall.

## Final state

Everything in the four phases is built, run end to end on the full 44,658,561 rows, and the
figures in the README all come from `reports/evidence/`.

| Check | Result |
| --- | --- |
| Bronze reconciles | 44,658,561 = 44,657,636 + 925 |
| Silver ledger reconciles | 44,657,636 = 44,227,382 + 430,254, asserted at runtime |
| Gold preserves rows | 44,227,382, zero unmatched zones after the broadcast join |
| Tests | 101 passing in 28s |
| Full pipeline runtime | about 29 minutes |
| `make pipeline` via spark-submit | verified working, both launch paths |
| Daylight saving gap | verified, 02:00 on 2019-03-10 has 0 trips against 8,417 a week later |

## Two things caught at the very end

**`make pipeline` had never been run.** Every stage was exercised through
`python -m nyc_taxi.cli`, so the code was well tested and the documented entrypoint was not.
It failed twice over: spark-submit could not locate its own installation from an unactivated
virtual environment, and once past that, the application overrode the submitted master and
driver memory with its own defaults while printing settings it was not using. Both fixed,
both now covered by `tests/test_session_config.py`.

**The configuration was bound at import time.** Writing the test for the above turned up
that `os.environ.get(...)` as a dataclass default is evaluated once when the module is first
imported. It happened to work, because every run exported its variables before starting the
process, but anything set later was silently ignored. The fields now read the environment
when a config object is built.

## For me to do

- [ ] Review the rule thresholds in `src/nyc_taxi/config.py` and decide whether any are
      too aggressive for how I want to talk about the project. R02, the 1,000 dollar fare
      ceiling, is the weakest one and removes 48 rows out of 44 million.
- [ ] Decide whether to keep the unpartitioned gold baseline in `data/gold`. It exists only
      as the comparison case for the pruning measurement and costs 1.5 GB.
- [ ] Push to GitHub myself. Nothing in this build touched a remote, and no git repository
      was initialised here.
