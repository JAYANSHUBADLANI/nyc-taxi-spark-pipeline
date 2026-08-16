# Data quality rules and the exclusion ledger

Every number on this page comes from a run of the pipeline over all 44,658,561 source
rows. Nothing here is estimated. The machine readable version is
`reports/evidence/silver_ledger.json`.

## How the rules were chosen

The thresholds were set after profiling the raw feed, not before. `make profile` scans all
six months and counts each candidate defect, and the results decided both which rules were
worth writing and where their boundaries sit. Two of the issues I expected to find were not
there, and one I did not expect was.

What the profile found across 44.7 million raw rows:

| Observation | Rows | Comment |
| --- | ---: | --- |
| Pickup timestamps outside 2019 H1 | 925 | Years range from 2001 to 2088 |
| Dropoff earlier than pickup | 54 | The clock runs backwards |
| Dropoff identical to pickup | 42,070 | No duration at all |
| Negative fare | 68,191 | Minimum observed is -450.08 USD |
| Zero fare | 17,298 | |
| Fare above 1,000 USD | 48 | Maximum observed is 943,274.80 USD |
| Zero trip distance | 329,087 | Maximum observed is 45,977 miles |
| Zero distance paired with a positive fare | 308,281 | The classic meter fault |
| Passenger count of zero | 785,404 | |
| Passenger count above 6 | 453 | Up to 9 |
| Null passenger count | 197,782 | |
| Implied speed above 100 mph | 35,473 | |
| **Location ID outside the published zone range** | **0** | The rule still exists, see below |

Two findings changed the design:

**Invalid zone IDs do not occur in this window.** I wrote the referential integrity rule
expecting to catch some, and it fires on nothing. The rule stays because it guards the gold
join, where an out of range identifier would silently vanish into a left join and quietly
break the row count reconciliation. A guard that reports zero is doing its job. I would
rather show a rule with an honest zero against it than remove it and lose the check.

**The null passenger count rows are a vendor gap, not corrupt records.** All 197,782 of
them belong to VendorID 2 and 5, all carry an undocumented payment type of 0, and all have
valid timestamps, distances and fares, averaging 42.69 USD over 10.45 miles. These are real
airport runs with missing metadata. That, plus the 785,404 rows reporting zero passengers,
is nearly a million real trips. Excluding them would have biased the demand series that the
entire gold layer exists to produce.

## Severity, and why the rules have one

Rules carry a severity rather than all being filters.

- **REJECT** means the record cannot describe a real trip. The row leaves the kept set.
- **WARN** means the trip is real but one attribute is not trustworthy. The row stays, the
  attribute is nulled where it would mislead, and a `quality_*` flag column records it.

The passenger count case is the one that forced the distinction, and it splits across both
severities. A count above the licensed maximum of 6 is physically impossible, so that is a
REJECT. A count of zero or null is a reporting gap on a trip that certainly happened, so
that is a WARN. Collapsing the two into a single "passenger count bounds" filter would have
thrown away 983,186 valid trips to remove 453 impossible ones.

## The exclusion ledger

Input to silver is the 44,657,636 rows bronze accepted.

| ID | Rule | Severity | Rows flagged | Rows it owns | Kept and flagged |
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

Reconciliation:

```
  44,657,636   rows into silver
-    430,254   rows excluded, the sum of the "rows it owns" column
= 44,227,382   rows kept
```

The pipeline asserts this equality at runtime and raises `ReconciliationError` if it ever
fails, so a rule cannot be added without the arithmetic still closing.

### Why there are two count columns

Because rules overlap, and pretending otherwise produces a report that quietly lies.

The REJECT rules flag 560,815 violations between them, but only 430,254 rows were removed.
The difference is not an error, it is 130,561 rows that break more than one rule at once. To
keep the ledger additive, each excluded row is attributed to the first rule it violates
under a documented precedence, and only that column sums.

Three lines make the point sharply:

- **R03, total_amount_negative, flags 68,173 rows and owns none of them.** Every trip with a
  negative total also has a non-positive fare, so R01 has already claimed all of them. If I
  reported flagged counts as removals, R03 would appear to remove 68,173 rows that were
  in fact removed by a different rule.
- **R06, dropoff_not_after_pickup, flags 42,119 and owns 2,169.** Roughly 95 percent of the
  zero duration trips also have a zero distance or a broken fare.
- **R04, distance_not_positive, flags 329,046 and owns 308,211.** The gap is the zero
  distance trips whose fare was already invalid.

A naive filter chain reports whatever each stage happened to drop given what survived the
stage before it, which makes the numbers an artefact of the order the code was written in.
The ownership column makes the precedence an explicit, documented decision instead.

## Quality trend across the six months

Same rules, same thresholds, applied to each month independently.

| Source month | Rows in | Rows kept | Excluded % | R01 fare % | R04 distance % | W01 unknown passengers % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2019-01 | 7,696,166 | 7,628,383 | 0.8807 | 0.1269 | 0.7154 | 1.8977 |
| 2019-02 | 7,049,267 | 6,985,071 | 0.9107 | 0.1751 | 0.7062 | 2.1110 |
| 2019-03 | 7,866,565 | 7,795,075 | 0.9088 | 0.1880 | 0.6841 | 2.2069 |
| 2019-04 | 7,475,877 | 7,407,479 | 0.9149 | 0.1986 | 0.6817 | 2.3979 |
| 2019-05 | 7,598,380 | 7,522,573 | 0.9977 | 0.2183 | 0.7416 | 2.3239 |
| 2019-06 | 6,971,381 | 6,888,801 | 1.1846 | 0.2460 | 0.9047 | 2.2790 |

The exclusion rate rises from 0.88 percent to 1.18 percent over six months, a 34 percent
relative increase, and the drift is not evenly spread across the rules.

The clearest single movement is R01. Non-positive fares run at 0.127 percent of January and
0.246 percent of June, close to a doubling in half a year while the distance and speed rules
stay broadly flat. Unknown passenger counts climb steadily too, from 1.90 to 2.28 percent,
with the sharpest step between March and April.

Worth being precise about what this does and does not establish. It shows that the reported
data degraded over the period, concentrated in the money columns and in vendor supplied
metadata. It does not establish why, and this pipeline cannot tell you. A rise in voided
and corrected fares, a vendor software change, and a change in TLC reporting practice would
all look like this from here. What it is good for is exactly what a monitoring signal
should be: it is the number that should have triggered someone to go and ask.

## The thresholds are judgement calls

None of these are TLC published standards. They are mine, and an interviewer is entitled to
push on any of them.

| Rule | Threshold | Reasoning |
| --- | --- | --- |
| R01 | fare >= 0.01 | The 2019 metered initial charge is 2.50 USD, so anything at or below zero is a void or a correction |
| R02 | fare <= 1,000 | Roughly ten times the flat JFK to Manhattan rate |
| R04 | distance >= 0.01 | A charged trip has to have moved |
| R05 | distance <= 200 miles | Far beyond any real yellow cab run out of the metro area |
| R07 | duration <= 24 hours | A meter left running describes the meter, not the journey |
| R08 | implied speed <= 100 mph | Unreachable as a trip average on New York streets |
| R09 | passengers <= 6 | The licensed maximum for a medallion minivan |

The ones I would defend hardest are R01 and R04, which together account for 91 percent of
all exclusions and rest on documented TLC fare rules rather than taste. The one I would
least defend is R02: a 1,000 USD ceiling is arbitrary, and it removes 48 rows out of 44
million, so the pipeline would be indistinguishable without it. It stays because a fare of
943,274.80 USD reaching a business intelligence tool is a worse outcome than a rule that
looks slightly arbitrary in a document.

The exclusions are written to `data/silver/excluded`, partitioned by the owning rule, so
any of these decisions can be revisited against the actual rows it removed rather than
re-run from scratch.
