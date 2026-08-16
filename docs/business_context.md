# Business context

## Who this is for

Priya runs operations analytics for a fleet of medallion cabs in New York. Two questions
land on her desk most weeks.

The first is where to put cars. Drivers cluster where they already are, which is not always
where the next fare is. She wants to know which pickup zones run hot at which hours on which
days, and she needs it at a grain fine enough to act on, a zone and an hour, not a borough
and a month.

The second is which fares to look at. Occasionally a passenger disputes a charge, or a
driver's takings look out of line with their hours. She has no fraud system and does not
want one. She wants a shortlist short enough to review by hand.

Underneath both sits a problem she does not think of as hers: the raw trip feed is not
usable as it arrives. It contains trips that charge money for zero distance, dropoffs that
happen before pickups, and fares up to 943,274.80 US dollars. Every analyst who touches it
cleans it slightly differently, so two people asking the same question get two answers, and
the meeting turns into an argument about whose spreadsheet is right.

## What this pipeline gives her

**A trip history she can query without cleaning it first.** 44,227,382 trips over six
months, with the geography already joined on and the calendar columns already derived. The
cleaning rules are applied once, in one place, and are the same for everyone.

**An exclusion ledger that survives being challenged.** When somebody asks why the trip
count is 44.2 million rather than the 44.7 million in the source files, the answer is a
statement that names each rule, the rows it removed, and the reasoning behind its
threshold, and it reconciles to the row. The removed rows are kept on disk, partitioned by
the rule that removed them, so a rule can be argued with against the actual records it
touched.

**A demand profile at the grain of the decision.** Trips, revenue, average fare and average
speed by pickup zone and hour, with rolling three hour and 24 hour windows and an hour on
hour change, so a zone that is warming up looks different from one that is already busy.

**A fare screen, not a fraud model.** Each trip is compared against other trips that
started in the same zone, ran a similar distance, and took a similar length of time, then
scored on how far its fare sits from that group's median. The output is a ranked shortlist
for human review.

All three of those peer terms are load bearing, and I know because two of them were missing
at first. Comparing on zone and distance alone flagged 2.73 percent of every short trip,
which was congestion rather than overcharging: the meter charges by time, so a crawl costs
more per mile than a clear run. Adding duration fixed that and exposed the next problem, the
regulated JFK flat fare, where every trip in the peer group costs exactly the same and any
deviation at all looked infinitely unusual. Both are covered by tests now.

## The recommendation

Three things follow from what the pipeline found, in the order I would raise them.

**1. Treat the data quality trend as an operational signal, not a footnote.**

The exclusion rate is not stable. It runs at 0.88 percent of January and 1.18 percent of
June, and the movement is concentrated in the money columns: non-positive fares nearly
double over the six months, from 0.127 to 0.246 percent. Distance and speed defects stay
broadly flat over the same period, so this is not general degradation, it is something
specific to how fares are being recorded.

That is worth someone's attention while the trail is warm. This pipeline cannot say whether
it is a vendor software change, a rise in voided trips, or a reporting practice change,
and it should not pretend to. What it can do is put the number in front of somebody every
month. My recommendation is that the ledger is reviewed on each load, with the fare rules
tracked specifically, and a movement of more than about 0.1 percentage points month on
month treated as something to investigate rather than absorb.

**2. Use the demand profile for placement, and keep the flagged trips out of it.**

The demand tables are built from the kept set only, which matters more than it sounds.
Roughly a million trips carry an unreliable passenger count, and if occupancy is ever used
for planning, those rows have to be excluded from that particular calculation rather than
counted as carrying zero people. The `quality_passenger_count_unknown` flag is on the trip
table for exactly this, so a question about trip demand and a question about passenger
demand can be answered from the same table without either one quietly corrupting the other.

**3. Treat the fare flag as a work queue with a known false positive rate, and never as an
accusation.**

The screen has no labels behind it. Nobody has told this pipeline which trips were actually
fraudulent, so its precision is unknown and unknowable from this data. What it does
reliably is rank, and a shortlist ranked by how far a fare sits from its peers is a better
use of a reviewer's morning than a random sample.

The honest framing for anyone using it: this is a filter that turns 44 million trips into a
few thousand worth looking at, and some of those will have ordinary explanations. If it
ever starts driving a consequence for a driver rather than a question for a reviewer, it
needs labels and a real model first.

## What I would build next

- Feed the flagged trips back as labels. A few hundred reviewed outcomes would turn the
  screen into something measurable, which is the gap between this and a real anomaly model.
- Extend the load to the full year and check whether the fare quality drift continues or
  reverses. Six months is enough to see a trend and not enough to trust it.
- Add trip level deduplication. The published files leak 1,972 records across month
  boundaries, which is harmless at this volume but is the shape of a problem that gets
  worse with incremental daily loads.
