# NYC Taxi Trip Pipeline (PySpark)

[![tests](https://github.com/JAYANSHUBADLANI/nyc-taxi-spark-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/JAYANSHUBADLANI/nyc-taxi-spark-pipeline/actions/workflows/tests.yml)

A layered bronze, silver and gold data pipeline over six months of real New York City yellow
taxi trip records: 44,658,561 rows of published TLC data, ingested under an enforced schema
contract, cleaned against documented rules that account for every row they remove, and
served as a partitioned fact table with zone geography and window features attached.

I built this as a data engineering project rather than a modelling one. The interesting
parts are the layer boundaries, the reconciliation discipline, and the distributed
computation decisions, not the analytics sitting on top.

Every number in this README came out of an actual run. Nothing is estimated, and the
evidence each figure came from is in `reports/evidence/`.

## Why Spark, honestly

The row count answers this on its own. 44.7 million trips arrive as 673 MB of Parquet and
reach 1.27 GB by the silver layer, on a machine with 8 GB of RAM. Nothing here fits in
memory at once, so every stage has to be designed around how the data is split rather than
around what fits in a dataframe.

The source layout forces the point immediately. Each published file contains exactly one
Parquet row group, and a row group cannot be divided between tasks, so the ingest is capped
at a handful of parallel readers until something rewrites the layout. That is a genuine
distributed systems problem, not a decoration on a small CSV.

I will be equally direct about the limit: this runs on `local[8]`, which is a single JVM
pretending to be a cluster. See [What this does not demonstrate](#what-this-does-not-demonstrate).

## Architecture

```
  TLC published Parquet                     taxi zone lookup
  6 files, 673 MB, 44,658,561 rows          265 zones, 12 KB
            |                                       |
            v                                       |
  +---------------------------+                     |
  |  BRONZE                   |                     |
  |  schema contract enforced |                     |
  |  provenance stamped       |                     |
  |  structural rejects split |                     |
  +---------------------------+                     |
            |                    \                  |
            |                     +--> rejected records, with reason
            v                                       |
  +---------------------------+                     |
  |  SILVER                   |                     |
  |  13 quality rules         |                     |
  |  exclusion ledger         |                     |
  |  conformed vocabulary     |                     |
  +---------------------------+                     |
            |                    \                  |
            |                     +--> excluded rows, partitioned by owning rule
            v                                       v
  +--------------------------------------------------+
  |  GOLD                                            |
  |  broadcast join to zone dimension                |
  |  window features, row level and aggregate level  |
  |  partitioned by pickup_date                      |
  +--------------------------------------------------+
            |
            v
  demand patterns  |  fare anomaly screen  |  quality trend
```

Each layer has one job and one failure mode. Bronze decides whether a record is
structurally usable, silver decides whether it describes a plausible trip, gold decides how
it is shaped for querying. When something breaks, the layer boundary tells you which
question was answered wrongly.

## Quick start

Requires Python 3.10 or newer and a JDK 17 runtime.

```bash
make setup && make download && make pipeline
```

| Target | What it does |
| --- | --- |
| `make setup` | Create the virtual environment and install the package |
| `make download` | Fetch the six trip files and the zone lookup, roughly 673 MB |
| `make profile` | Profile the raw feed before any cleaning is applied |
| `make pipeline` | Run bronze through analytics via `spark-submit` |
| `make test` | Run the unit tests, no large data required |
| `make benchmark` | Measure the partitioning and file layout decisions |

Individual stages run with `make bronze`, `make silver`, `make gold`, `make analytics`, or
directly:

```bash
python -m nyc_taxi.cli --stage silver
```

`JAVA_HOME` is exported by the Makefile and defaults to `~/.jdks/jdk-17.0.20+8`. Override it
if your JDK lives elsewhere.

## The data

NYC Taxi and Limousine Commission yellow taxi trip records, January to June 2019, published
as Parquet over plain HTTPS with no account or API key.

I chose 2019 deliberately: it predates the 2020 demand collapse, and I verified the schema
is byte for byte identical across all six months rather than assuming it. It is not:
`airport_fee` is present but entirely null, a field backfilled for later years, and the
timestamps are `timestamp_ntz`, local wall clock readings with no zone offset. Both are
handled explicitly in `src/nyc_taxi/schemas.py`.

| Month | Rows |
| --- | ---: |
| 2019-01 | 7,696,617 |
| 2019-02 | 7,049,370 |
| 2019-03 | 7,866,620 |
| 2019-04 | 7,475,949 |
| 2019-05 | 7,598,445 |
| 2019-06 | 6,971,560 |
| **Total** | **44,658,561** |

## Bronze: ingestion under contract

Parquet is self describing, so "schema enforcement" cannot mean parsing. It means declaring
what the pipeline expects, checking each file against that declaration, and refusing to
continue when the difference is one that cannot be safely reconciled.

The contract allows exactly the widenings Spark's vectorised reader will actually perform.
That list is narrower than the list of conversions that look lossless, which I found out the
hard way: `int` to `double` is arithmetically safe, and the reader rejects it outright. That
failure is now a test.

Bronze then splits records on structural validity alone:

| | Rows |
| --- | ---: |
| Raw input | 44,658,561 |
| Accepted | 44,657,636 |
| Rejected, pickup outside the load window | 925 |

The 925 rejects are records whose pickup timestamps fall in years from 2001 to 2088. They
cannot belong to any file that was pulled, so they cannot be reconciled to a source month.
They are written to `data/bronze/rejected` with the reason attached rather than dropped.

A further 1,972 rows sit in a file covering a different month than their own pickup date.
Those are kept, since they are valid trips inside the window, but the count is recorded
because cross month drift is exactly what silently duplicates rows in an incremental load.

## Silver: rules that account for themselves

Thirteen rules, each a declarative object with an identifier, a severity, a predicate and a
written justification. Ten reject, three warn.

The severity split is the design decision I would most want to be asked about. A REJECT rule
means the record cannot describe a real trip. A WARN rule means the trip happened but an
attribute is untrustworthy, so the row stays and the attribute is nulled and flagged.

The case that forced it: 785,404 trips report zero passengers and 197,782 report null, and
all of them carry valid timestamps, distances and fares. The null cohort turned out to be
entirely VendorID 2 and 5 with an undocumented payment type, averaging 42.69 USD over 10.45
miles. Those are real airport runs with missing metadata. Dropping nearly a million real
trips to enforce a passenger count bound would have biased the demand series the gold layer
exists to produce.

### The exclusion ledger

```
  44,657,636   rows into silver
-    430,254   rows excluded  (0.96%)
= 44,227,382   rows kept
```

| ID | Rule | Severity | Flagged | Owns | Kept and flagged |
| --- | --- | --- | ---: | ---: | ---: |
| R01 | fare_not_positive | REJECT | 85,480 | 85,480 | 0 |
| R02 | fare_implausibly_high | REJECT | 48 | 48 | 0 |
| R03 | total_amount_negative | REJECT | 68,173 | 0 | 0 |
| R04 | distance_not_positive | REJECT | 329,046 | 308,211 | 0 |
| R05 | distance_implausibly_long | REJECT | 26 | 23 | 0 |
| R06 | dropoff_not_after_pickup | REJECT | 42,119 | 2,169 | 0 |
| R07 | duration_implausibly_long | REJECT | 39 | 16 | 0 |
| R08 | implied_speed_implausible | REJECT | 35,471 | 34,152 | 0 |
| R09 | passenger_count_above_licensed_max | REJECT | 453 | 155 | 0 |
| R10 | location_id_out_of_range | REJECT | 0 | 0 | 0 |
| W01 | passenger_count_unknown | WARN | 983,186 | 0 | 983,186 |
| W02 | ratecode_undocumented | WARN | 199,359 | 0 | 199,359 |
| W03 | payment_type_undocumented | WARN | 197,782 | 0 | 197,782 |

Two count columns, because rules overlap and a single column would lie. The reject rules
flag 560,855 violations between them but only 430,254 rows were removed. The difference is
130,601 rows that break more than one rule at once.

**R03 is the line that proves the point.** It flags 68,173 rows and owns none of them: every
negative total also has a non-positive fare, so R01 already claimed them. Reporting flagged
counts as removals would credit R03 with removing 68,173 rows that a different rule removed.
Each excluded row is instead attributed to the first rule it violates under a documented
precedence, so only that column sums, and it sums exactly. The pipeline asserts the
reconciliation at runtime and raises if it ever fails.

**R10 fires on nothing.** I wrote the zone referential integrity rule expecting to catch
bad identifiers and there are none in this window. It stays, because it guards the gold join
where an out of range identifier would vanish into a left join and break the row count with
nothing to explain it. A guard reporting zero is doing its job, and I would rather show an
honest zero than delete the check.

Full detail, including every threshold and its reasoning, is in
[docs/data_quality.md](docs/data_quality.md).

## Gold: distributed computation decisions

Three decisions, each evidenced by a captured plan rather than asserted. Full working in
[docs/partitioning.md](docs/partitioning.md).

**The zone dimension is broadcast.** 265 rows against 44.2 million. Counting operators in
both captured plans, the broadcast join has 0 shuffle exchanges and 0 sorts; forcing a sort
merge join produces 2 exchanges and 2 sorts, hash partitioning and sorting the entire trip
table so a 12 KB lookup can be attached to it.

**One shuffle serves both window functions and the partitioned write.** Range partitioning
by `pickup_date` before applying the windows means the window's required clustering on
`(pickup_location_id, pickup_date)` is already satisfied, and so is the write's
`partitionBy`. The captured plan for the gold write contains 1 Exchange, 1 Sort, 2 Window
operators and 2 BroadcastHashJoins. Applying the window first and repartitioning afterwards
computes exactly the same answer and pays for two full shuffles of 44 million rows.

**Windows run at the grain the question lives at.** Trip sequence and the gap since the
previous pickup are row level, over `(zone, date)`. Rolling demand is a property of the
hour, not the trip, so the data is aggregated to zone and hour first and the rolling windows
run over roughly half a million grouped rows instead of 44 million detail rows. The rolling
windows use `rangeBetween` over epoch seconds rather than `rowsBetween`, so a zone's 3am and
its next recorded 9am are treated as six hours apart rather than adjacent. There is a test
for exactly that.

## Results

### Row counts through the layers

| Stage | Rows | Change |
| --- | ---: | --- |
| Raw source files | 44,658,561 | |
| Bronze accepted | 44,657,636 | 925 rejected, pickup outside the load window |
| Silver kept | 44,227,382 | 430,254 excluded by 10 reject rules |
| Gold trips | 44,227,382 | broadcast join preserved every row, 0 unmatched zones |

The gold count matching silver exactly is the check that the two left joins against the
zone dimension neither dropped nor duplicated a trip.

### Distributed computation, measured

Shuffle figures come from Spark's own stage counters, read back through the UI's REST API
by `make benchmark`. Full working in [docs/partitioning.md](docs/partitioning.md).

**Splitting files on write beats repartitioning, and it is not close.** Same 7,696,617 rows,
same resulting layout:

| | repartition(6) then write | maxRecordsPerFile |
| --- | ---: | ---: |
| Wall time | 26.97s | 14.87s |
| Shuffle written | 444.3 MB | **0.0 MB** |

**Partition pruning on a single day filter**, `pickup_date = '2019-03-13'`, 269,492 rows
returned from both layouts:

| | Partitioned | Unpartitioned |
| --- | ---: | ---: |
| Bytes read | **0.03 MB** | 4.08 MB |
| Tasks | 3 | 18 |
| Filter resolved as | `PartitionFilters` | pushed down `Filter` |

**Broadcast against sort merge**, one month of trips joined to the 265 row dimension:

| | Broadcast | Sort merge |
| --- | ---: | ---: |
| Shuffle written | **0.0 MB** | 7.6 MB |
| Stages | 3 | 4 |
| Wall time | 1.69s | 2.93s |

**Read parallelism** is the one where the honest answer is smaller than the headline. The
bronze rewrite turns 6 files into 35, but Spark packs small files up to 128 MB per task, so
reads go from 6 tasks to 8, not to 35. What actually improves is balance: the source layout
hands one task an entire 7,866,620 row file, while bronze produces 8 partitions within 3
percent of each other. Same aggregation, 2.31s against 1.66s, a 1.39x gain.

### Demand patterns

Computed from the zone and hour table, 597,524 zone hour rows covering 265 zones and 181
days. Averages are trip weighted.

| Hour | Trips | Avg fare | Avg speed (mph) | Avg distance (mi) |
| ---: | ---: | ---: | ---: | ---: |
| 04 | 344,641 | 15.41 | 18.60 | 4.37 |
| 05 | 411,403 | 16.58 | 19.54 | 4.83 |
| 09 | 2,071,007 | 12.62 | 10.18 | 2.66 |
| 15 | 2,467,118 | 13.80 | 9.63 | 3.03 |
| 18 | 2,917,367 | 12.43 | 10.03 | 2.70 |

Three things worth pointing at:

**Demand swings by a factor of 8.5 across the day**, from 344,641 trips in the 4am hour to
2,917,367 in the 6pm hour. At the finer grain the fleet actually plans against, a zone hour
slot, the spread is nearly 20 to 1: Thursday 6pm carries 461,750 trips against 23,271 on
Monday at 3am.

**Speed runs inversely to demand, and the fleet loses half its road speed at the peak.**
19.54 mph at 5am against 9.63 mph at 3pm. The busiest hours are also the least productive
per driver hour, which is the part a placement model has to price in: adding cars to a zone
that is already congested buys less than the trip count suggests.

**The 5am hour is a different business.** Highest average fare of the day at 16.58 USD on
the longest average trip at 4.83 miles, in the emptiest traffic. That is the airport run,
and it is invisible in a daily average.

Geographically the picture is stark. Manhattan accounts for 40,199,892 of 44,227,382 trips,
90.9 percent, and 663.1 million USD of total revenue. The busiest single pickup zone is
Upper East Side South with 1,904,532 trips, ahead of Midtown Center at 1,815,218.

### Fare anomaly screen

Each trip is compared against its own peer group, the pickup zone crossed with a distance
band crossed with a duration band, and scored on how far its fare sits from that group's
median in units of the group's robust spread. 6,598 peer groups, 44,149,326 of the
44,227,382 trips in groups large enough to score.

**155,781 trips flagged, 0.35 percent.** 137,723 above their peers and 18,058 below.

| Distance band | Trips | Flagged | Rate |
| --- | ---: | ---: | ---: |
| 0-1 mi | 11,055,205 | 42,064 | 0.38% |
| 1-2 mi | 14,998,264 | 15,956 | 0.11% |
| 2-3 mi | 6,767,218 | 8,038 | 0.12% |
| 3-5 mi | 4,898,221 | 21,475 | 0.44% |
| 5-10 mi | 3,715,058 | 18,547 | 0.50% |
| 10-20 mi | 2,473,148 | 29,047 | 1.17% |
| 20+ mi | 320,268 | 20,654 | 6.45% |

The trips at the top of the ranking are not subtle. The highest scoring is an 800 USD fare
for a 0.49 mile trip lasting 1 minute 34 seconds, in a zone where the peer median for that
distance and duration is 4.50 USD. Below it: 780 USD for half a mile, 495 USD for a trip
that recorded 0.01 miles.

By zone, JFK Airport leads with 28,444 flagged trips, which is what you would expect from a
regulated flat fare: any trip that did not charge the flat rate stands out sharply against
peers that all charged exactly the same.

**Two things I would say before anyone relies on this.** The 20+ mile band flags 6.45
percent, far above the rest, because very long trips are genuinely heterogeneous and a
single peer group covers everything from a Hamptons run to a mistake. And the screen has no
labels behind it, so its precision is unmeasured and unmeasurable from this data. It ranks
well. It does not know what fraud is.

The method took three attempts, and both of the wrong ones looked reasonable until they met
44 million rows. That story is in [docs/business_context.md](docs/business_context.md) and
in `PROGRESS.md`, because the corrections are more informative than the final number.

### Data quality trend across the six months

| Source month | Excluded % | Non positive fares % |
| --- | ---: | ---: |
| 2019-01 | 0.8807 | 0.1269 |
| 2019-02 | 0.9107 | 0.1751 |
| 2019-03 | 0.9088 | 0.1880 |
| 2019-04 | 0.9149 | 0.1986 |
| 2019-05 | 0.9977 | 0.2183 |
| 2019-06 | 1.1846 | 0.2460 |

The exclusion rate rises 34.5 percent relative over six months, and it is not spread evenly.
Non positive fares nearly double, from 0.127 to 0.246 percent, while the distance and speed
rules stay broadly flat. Something specific to how fares are recorded degraded over this
period.

The pipeline can establish that and cannot explain it. A vendor software change, a rise in
voided trips, and a change in TLC reporting practice all produce this signature. It is a
monitoring signal, and the value of it is that somebody gets to ask the question in month
two rather than discovering it in an annual review.

## Testing

```bash
make test
```

101 tests, 28 seconds, no large data required. They run against small hand built frames
whose expected answers can be worked out by hand, which is the only way a transformation
test tells you something you did not already assume.

The ledger tests are the ones that matter. A rule that fires on the wrong row is a bug you
eventually notice; a ledger that quietly fails to add up is a bug you never notice, because
its output looks like a report. So there is a test that a row violating three rules at once
is counted once in the exclusions and three times in the flags, and a test that feeding the
ledger a wrong input count makes it refuse to reconcile.

Several tests exist purely because of real failures, and those are the ones I would point an
interviewer at:

- The fixtures pin `TZ=UTC` before the JVM starts, because PySpark converts naive Python
  datetimes using the driver's system timezone while evaluating them in the session
  timezone. On a machine set to anything else, every fixture timestamp silently shifts and
  ten tests fail for reasons that have nothing to do with the pipeline.
- `int` to `double` is asserted **not** to be a safe schema widening, because Spark's
  vectorised Parquet reader refuses it and it killed a full run.
- A test asserts that a peer group of identical flat rate fares does not flag every trip a
  few cents off that rate, because an earlier version of the anomaly screen did exactly
  that to 16 percent of all 10 to 20 mile trips.
- A test asserts that a slow trip is compared only against other slow trips, because the
  first version of the screen was measuring traffic and calling it overcharging.
- `tests/test_session_config.py` exists because `make pipeline`, the entrypoint this README
  documents, was broken in two ways at once and I only found out by running it. See below.

### The entrypoint was broken and the tests did not know

Worth writing down because it is the most ordinary kind of failure. Every stage had been run
through `python -m nyc_taxi.cli`, so the code was thoroughly exercised and the documented
command had never once been executed.

`spark-submit` is a shell script that shells back out to `python` from `PATH` to find its
own installation. With PySpark inside a virtual environment that is not activated, that
lookup finds an interpreter without pyspark and the script dies with
`/bin/spark-class: No such file or directory`, which says nothing about the actual cause.
The Makefile now resolves `SPARK_HOME` from the venv's own pyspark package.

Fixing that exposed a second problem underneath it. The application called `.master()` and
set driver memory in code, so a job submitted with `--master local[4] --driver-memory 2g`
ran as `local[8]` with 4g and cheerfully printed the settings it was not using. Driver
memory cannot change after the JVM starts, so half of that was silently ignored anyway.
`build_spark` now detects a submitted session and leaves its configuration alone, and the
run summary reads the values back from the live session rather than from config.

## Runtime

Measured end to end on 8 cores and 8 GB of RAM. Stage timings come from
`reports/evidence/run_timings.json` and `reports/evidence/raw_profile.json`.

| Stage | Time | What dominates it |
| --- | ---: | --- |
| Raw profile | 10s | one scan of 44.7M rows |
| Bronze | 165s | writing 859 MB with provenance, no shuffle |
| Silver | 93s | one ledger aggregation, then writing 1.27 GB |
| Gold | 457s | the partitioned write of 181 date directories dominates it |
| Analytics | 66s | peer group statistics over 6,598 groups |
| **Total** | **~13 min** | |

Bronze and silver and gold ran at `local[8]` with a 4 GB driver. Analytics ran at `local[6]`
with 3 GB, after the larger setting drove the machine into swap. That is the reason every
sizing knob in `config.py` reads from an environment variable: an 8 GB laptop running a
44 million row pipeline has no slack, and the settings that work for a write heavy stage are
not the ones that work for an aggregation heavy one.

## What this does not demonstrate

Written before anyone asks, because being able to name your own project's limits is worth
more than the project pretending it has none.

**`local[8]` is not a distributed cluster.** This is the big one. Every shuffle here is a
write to local disk and a read back from local disk. Nothing crosses a network. That means
this project shows nothing about the problems that actually make production Spark hard:
executor loss and task retry, data locality, stragglers, network shuffle fetch failures,
skew across nodes, or sizing executors against a real cluster manager. The code is written
the way cluster code is written, and the reasoning about shuffle and broadcast transfers
directly, but the evidence behind every number is single machine. Where cluster behaviour
would differ, the difference is usually that things get worse, not better.

**Every cleaning threshold is my judgement, not a published standard.** The TLC does not
define a maximum plausible fare or a maximum implied speed. I set them, documented the
reasoning for each, and made them all configurable in one file. R01 and R04 account for 91
percent of exclusions and rest on documented fare rules, so I would defend those hardest.
R02, the 1,000 USD fare ceiling, is frankly arbitrary and removes 48 rows out of 44 million;
the pipeline would be indistinguishable without it.

**The fare anomaly flag is a rule based screen, not a validated model.** There are no labels
in this data. Nobody has recorded which trips were actually fraudulent or overcharged, so
the screen has no measurable precision or recall and cannot acquire one from this dataset.
It is a peer comparison that ranks trips by how far their fare per mile sits from other
trips in the same zone and distance band. That is genuinely useful for producing a review
queue and genuinely not a fraud detector. Calling it one would be the single most misleading
thing this project could claim.

**The data quality trend is a signal, not a diagnosis.** Exclusions rise from 0.88 to 1.18
percent across the six months, concentrated in the fare rules. The pipeline can show that
and cannot explain it. A vendor software change, a rise in voided trips, and a change in
reporting practice all look identical from here.

**Not built:** orchestration, incremental or idempotent partial loads, trip level
deduplication, a schema registry, or CI. The pipeline is a full reload each run.

**Timestamps are local wall clock throughout, and there is a hole in the demand series to
prove it.** The TLC publishes pickup and dropoff as `timestamp_ntz`, local New York readings
with no zone offset, and the pipeline preserves those digits exactly so that an hour of the
day means the hour a dispatcher would recognise. The cost shows up on 10 March 2019, when
clocks jumped from 01:59 to 03:00:

| Date | 00:00 | 01:00 | 02:00 | 03:00 | 04:00 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2019-03-10 | 13,876 | 11,797 | **0** | 10,619 | 6,778 |
| 2019-03-17 | 13,568 | 11,098 | 8,417 | 6,197 | 4,049 |

The 02:00 hour is the only hour in that entire day with no trips at all, and a week later
the same hour carries 8,417. Nothing here corrects for it, because the source carries no
offset that would allow a correction. The ambiguous transition, where an hour repeats and a
bare timestamp cannot say which one is meant, falls in November and outside this window.

## Repository layout

```
src/nyc_taxi/
  config.py      paths, Spark sizing, every quality threshold in one auditable place
  schemas.py     the schema contract and what counts as a safe difference
  rules.py       13 quality rules as declarative objects with written rationales
  bronze.py      contract enforcement, provenance, structural rejection
  silver.py      rule evaluation, the exclusion ledger, conformed output
  gold.py        broadcast join, window features, partitioning, plan capture
  analytics.py   demand patterns, fare anomaly screen, quality trend
  cli.py         single entrypoint, one stage or the whole chain
scripts/
  download_data.py           fetch the raw inputs
  profile_raw.py             profile the feed before rules are written
  benchmark_partitioning.py  measure the layout decisions
tests/                       88 tests on small in memory frames
docs/                        data quality, partitioning, business context
reports/evidence/            metrics and plans from actual runs
reports/tables/              aggregate CSVs behind the figures quoted here
```
