# Partitioning, file layout and shuffle

Every figure here was measured. The plans are captured in `reports/evidence/join_plans.txt`
and `reports/evidence/gold_write_plan.txt`, the benchmark numbers in
`reports/evidence/partitioning_benchmark.json`, all produced by running the pipeline.

## The problem the source data hands you

The six published files are around 110 MB each, and each one contains exactly one Parquet
row group.

That matters more than the file count. A row group is the smallest unit Spark can hand to a
task, so a file containing one row group cannot be divided between two tasks no matter how
large it is. Spark plans splits by byte range, and on this data it plans nine partitions,
but a task whose byte range contains no row group start reads nothing at all. The effective
parallelism is bounded by the number of files.

This is the honest version of "why Spark for this". Not because 44.7 million rows is
enormous by Spark's standards, but because the volume forces the layout question at all:
673 MB of source Parquet expands past 1.2 GB by silver, and the machine running it has 8 GB
of RAM, so nothing here can be held in memory at once and every stage has to be designed
around how the data is split.

## Fixing read parallelism without a shuffle

The obvious fix is `repartition(32)`, and it works. It also moves all 44.7 million rows
across the network to achieve a file layout. I tried it first: it ran past twelve minutes
and had written over 2.3 GB of shuffle data before I stopped it.

`maxRecordsPerFile` reaches the same layout with no exchange at all. Each writing task rolls
to a new file when it hits the row cap, which is a local decision requiring no coordination
between tasks.

Both were then measured properly, on a single month so the experiment is cheap to repeat.
Shuffle bytes come from Spark's own stage counters, read back from the UI's REST API.

**Writing 7,696,617 rows, two ways:**

| | repartition(6) then write | maxRecordsPerFile |
| --- | ---: | ---: |
| Wall time | 26.97s | **14.87s** |
| Shuffle written | **444.3 MB** | **0.0 MB** |
| Tasks | 12 | 6 |
| Output files | 6 | 7 |
| Average file size | 32.1 MB | 20.9 MB |

444 megabytes across the network, or none, for the same outcome. The repartition also costs
two stages instead of one, since a shuffle has to have a write side and a read side.

The bronze rewrite of the full six months, with `maxRecordsPerFile` at 1.4 million:

| | Source layout | Bronze layout |
| --- | --- | --- |
| Files | 6 | 35 |
| Row groups per file | 1 | several |
| Size on disk | 673 MB | 859 MB |
| Average file size | 104 MB | 24.5 MB |
| Shuffle to produce it | not applicable | none |

Bronze is larger than the source because it carries the provenance columns and the widened
`airport_fee`, not because anything was duplicated. The row count reconciles exactly.

### What this does and does not buy

An important correction to the intuition, and the measurement makes it concrete. More files
does not mean proportionally more tasks. Spark packs small files together up to
`spark.sql.files.maxPartitionBytes`, 128 MB by default, so 35 files of 24.5 MB are read by
eight tasks, not 35.

Running the same aggregation over each layout:

| | Source layout | Bronze layout |
| --- | ---: | ---: |
| Partitions planned | 6 | 8 |
| Partitions actually carrying rows | 6 | 8 |
| Largest partition | 7,866,620 rows | 5,600,000 rows |
| Smallest partition | 6,971,560 rows | 5,457,636 rows |
| Aggregation time | 2.31s | **1.66s** |

The real gain is visible in the middle rows rather than the last one. On the source layout
each task is handed an entire file, so the largest task carries 7.87 million rows and the
work is as lopsided as the files happen to be. After the rewrite the same data arrives as
eight partitions within 3 percent of each other in size. The job finishes when its slowest
task finishes, and balance is what moves that.

The speedup is 1.39x, not the 5x that "6 files becomes 35 files" might suggest. Pushing
parallelism higher would mean lowering `maxPartitionBytes`, which is a separate lever and
not a consequence of the file count.

## Broadcasting the zone dimension

The zone lookup is 265 rows and 12 KB. The trip fact is 44.2 million rows. There is exactly
one sensible way to join those, and the code says so explicitly with `F.broadcast()` rather
than relying on the optimiser noticing.

Both plans were captured against the real tables. Counting operator nodes in the physical
plans:

| | Broadcast join | Sort merge join |
| --- | ---: | ---: |
| Shuffle exchanges | 0 | 2 |
| Broadcast exchanges | 1 | 0 |
| Sorts | 0 | 2 |
| Join operator | BroadcastHashJoin | SortMergeJoin |

Running both against a single month of real trips, with the shuffle counters read from
Spark's stage metrics:

| | Broadcast join | Sort merge join |
| --- | ---: | ---: |
| Wall time | 1.69s | 2.93s |
| Shuffle written | **0.0 MB** | 7.6 MB |
| Shuffle read | 0.0 MB | 7.6 MB |
| Stages | 3 | 4 |
| Tasks | 13 | 19 |

Be precise about that 7.6 MB, because the honest number is smaller than the dramatic one.
The final aggregation only needs the join key, so Spark's column pruning strips the fare and
distance columns before the exchange and what crosses the network is one integer column,
compressed. A query that actually carried the trip payload through the join would shuffle
far more. The point that generalises is not the magnitude, it is that the broadcast side
shuffles exactly nothing regardless of how wide the fact table is, because the fact table
never moves.

The sort merge plan hash partitions and sorts **both** sides on the join key. One of those
sides is the entire trip table. The other is 265 rows. Every trip row crosses the network
and is sorted, so that a 12 KB dimension can be attached to it.

The auto broadcast threshold is 10 MB and the dimension is far below it, so Spark would very
likely have broadcast this anyway. The explicit hint is there because "very likely" is doing
load bearing work in that sentence. If someone lowers the threshold, or the dimension is
read through a path where Spark has no size statistics, the plan silently becomes the second
column of that table. Stating the intent makes the plan stable against configuration it does
not control.

## One shuffle for two windows and the write

The gold stage applies row level window functions and then writes partitioned by pickup
date. Done naively that is two full shuffles of 44 million rows: one to satisfy the window's
partitioning, one to group rows by date for the write.

It can be one, and the ordering of operations is what makes it one.

```python
partitioned_once = enriched.repartitionByRange(192, F.col("pickup_date"))
with_windows = add_trip_window_features(partitioned_once)   # partitions by (zone, date)
with_windows.write.partitionBy("pickup_date").parquet(...)
```

The window requires its input clustered by `(pickup_location_id, pickup_date)`. A
partitioning on `pickup_date` alone already guarantees that: if all rows for a date are in
one partition, then all rows for a given zone **and** date are too. Spark's
`EnsureRequirements` recognises that a partitioning on a subset of the required clustering
keys satisfies the distribution, so it reuses the exchange rather than adding one. The
write's `partitionBy` is satisfied by the same exchange.

Node counts from the captured plan of the actual gold write:

| Operator | Count |
| --- | ---: |
| Exchange (shuffle) | 1 |
| BroadcastExchange | 2 |
| Window | 2 |
| Sort | 1 |
| BroadcastHashJoin | 2 |

Two window operators and two joins over 44 million rows, with one shuffle and one sort
underneath all of them.

### Range, not hash, and why that turned out to matter

The first version used `repartition("pickup_date")`, hash partitioning into 64 buckets. It
died with `UNABLE_TO_ACQUIRE_MEMORY` inside the window operator.

The window was not the culprit. Hashing 181 dates into 64 buckets gives each task three or
four scattered, unrelated dates, and because the write is partitioned by date, each of those
becomes a separate output directory that the task has to open a Parquet writer for. Parquet
buffers an entire row group in memory before flushing, and that buffer is allocated outside
Spark's memory manager. At the 128 MB default, eight concurrent tasks holding several
writers each quietly consumed around a gigabyte that the memory manager still believed was
available. The window operator then asked for a 16 KB page and was refused.

Two changes fixed it, and both are about the writers rather than the window:

1. `repartitionByRange` gives each task a contiguous slice of the calendar, so it writes
   close to one date directory and holds close to one writer open.
2. `parquet.block.size` cut from 128 MB to 32 MB, reducing what each open writer reserves by
   a factor of four.

The lesson worth keeping: on a memory constrained executor, the Parquet writer's buffers are
part of your memory budget and Spark's accounting cannot see them. An out of memory error
inside an operator is not proof that the operator is what used the memory.

## Partition pruning, measured

Gold is written twice, once partitioned by `pickup_date` and once not, purely so the
difference can be measured rather than asserted. The probe is a single day filter,
`pickup_date = '2019-03-13'`, run identically against both layouts.

| | Partitioned by pickup_date | Unpartitioned baseline |
| --- | ---: | ---: |
| Files | 181 | 39 |
| Directories | 181 | 1 |
| Size on disk | 1,307 MB | 1,500 MB |
| Average file size | 7.2 MB | 38.5 MB |
| Filter appears in plan as | `PartitionFilters` | pushed down `Filter` |
| Rows returned | 269,492 | 269,492 |
| **Bytes read** | **0.03 MB** | **4.08 MB** |
| Tasks launched | 3 | 18 |
| Wall time | 0.11s | 0.32s |

**125 times less data read** for the same 269,492 rows, and 3 tasks instead of 18.

The mechanism is visible in the plan: on the partitioned table the filter is resolved
against directory names before a single byte of Parquet is opened. On the unpartitioned
table the same predicate becomes a pushed down Parquet predicate, which requires opening
files to evaluate.

Both absolute figures are small because the probe is a `count`, which Spark answers from
Parquet footer statistics rather than by reading row data. The ratio between them is the
meaningful part, and it is the ratio that scales to a query that actually reads columns.

### How I got this measurement wrong the first time

Worth recording, because the wrong version looked perfectly convincing.

The first implementation counted `DataFrame.inputFiles()` on the filtered frame. That call
returns every file belonging to the relation, before any partition pruning is applied. The
resulting table reported that the partitioned layout would open **181 files and 1.37 GB**
while the baseline opened **39 files and 1.57 GB**, which is backwards, and which I would
have published as evidence of pruning working.

The tell was that the numbers tracked the layouts' total file counts exactly, rather than
anything about the query. The replacement measures bytes actually read from Spark's stage
level `inputBytes` counter, which is the same figure the Stages tab in the Spark UI shows.

There is a second, more interesting trap in this comparison, and it is worth stating because
it makes the honest result smaller than the marketing version. The unpartitioned baseline is
written by copying the finished partitioned table, so its rows arrive in date order and
every Parquet row group carries tight min and max statistics on `pickup_date`. Predicate
pushdown can therefore skip nearly all of it without any partition directories at all. A
baseline written from randomly ordered data would look far worse. Partition pruning wins
here, but a good deal of what partitioning is usually credited with can be had from sort
order alone.

### The cost side, stated plainly

Date partitioning is not free and this grain is not automatically right.

181 days over six months means 181 directories holding roughly 8 MB each. That is well under
the 128 MB that reads efficiently, and it is the reason the partitioned layout has more
files and more total metadata than the unpartitioned one. A query that reads the whole
period pays for that, and gains nothing.

The grain is right here because the access pattern is genuinely date centric: an operations
analyst asks about last Tuesday, or last month, far more often than they scan two years. If
the dominant query were "this zone across all time", partitioning by zone, or by month with
zone as a secondary sort, would beat it. The right answer follows the queries, and that is
the part a portfolio project cannot fully demonstrate, since it has no real query log to
point at.
