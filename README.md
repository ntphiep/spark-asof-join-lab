# spark-asof-join-lab

[Apache Spark 4.3.0](https://spark.apache.org/releases/spark-release-4-3-0.html), released on
2026-10-30, adds SQL [`ASOF JOIN`](https://spark.apache.org/docs/4.3.0/sql-ref-syntax-qry-select-asof-join.html).
This lab asks one question: is it faster than the patterns people used before it existed?

The task is "attach the price that was valid when each order was placed". Four queries in
`queries.py` do it:

| Query | How it works |
|---|---|
| `asof_join` | `LEFT ASOF JOIN ... MATCH_CONDITION (o.order_ts >= p.valid_from) ON product_id` |
| `join_then_rank` | join every earlier price change, keep `ROW_NUMBER() = 1` per order |
| `validity_range` | build `valid_to` with `LEAD()`, then join on the range |
| `union_last_value` | stack orders and price changes on one timeline, carry `LAST(price, true)` forward |

All four return the same 10M rows. `bench.py` compares row count, priced rows, total revenue
and a hash of `(order_id, price)` before timing anything (`results/*/checks.txt`).

## Setup

Every dataset has 10M orders and about 1M price changes over one year. Only the shape changes:
fewer products means more price changes per product.

Spark 4.3.0 from PyPI, `local[8]`, 6 GB driver, adaptive query execution on, 64 shuffle
partitions, Java 21, on an AMD Ryzen 7 5700G (8 cores) with 30 GB RAM. Each time is the median
of 3 runs after one warm-up, written to Spark's `noop` sink.

## What I found

| Results folder | Price changes per product | Order x earlier price pairs | `asof_join` | `validity_range` | `union_last_value` | `join_then_rank` |
|---|---|---|---|---|---|---|
| `p20000_c50` | 50 | 249,993,870 | 8.4 s | 2.6 s | 4.5 s | 47.6 s |
| `p20000_c50_no_broadcast` | 50 | 249,993,870 | 9.7 s | 2.5 s | 4.4 s | 43.9 s |
| `p5000_c200` | 200 | 999,961,329 | 24.1 s | 4.5 s | 5.3 s | not run |
| `p1000_c1000` | 1,000 | 4,999,804,807 | 90.2 s | 12.0 s | 5.0 s | not run |

- `asof_join` is 5.7x faster than `join_then_rank`, and much shorter to write.
- `asof_join` time grows with the number of price changes per product. The row counts stay
  the same, yet it goes from 8.4 s to 90.2 s. `union_last_value` stays near 5 s.
- `p20000_c50_no_broadcast` sets `spark.sql.autoBroadcastJoinThreshold=-1`. The order of the
  results does not change, so `validity_range` is not winning because of a broadcast join.
- A `BROADCAST(p)` hint does not change the plan: it stays `SortMergeAsOfJoin`
  (`results/broadcast_hint_plan.txt`).
- `join_then_rank` was skipped for the two larger shapes. It has to produce every pair in the
  "pairs" column first, and that is already 47.6 s at 250M pairs.

## Why `asof_join` slows down

The physical operator is
[`SortMergeAsOfJoinExec`](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeAsOfJoinExec.scala).
Both sides are shuffled and sorted by `(product_id, timestamp)`, and the right rows of the
current key are buffered. For a backward match (`left >= right`),
[`findBestBackwardForward`](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeAsOfJoinExec.scala#L385-L409)
calls `rightGroupBuffer.generateIterator()` for every left row. That overload starts at index 0
([`ExternalAppendOnlyUnsafeRowArray.scala:210`](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/ExternalAppendOnlyUnsafeRowArray.scala#L210)).
The scan stops after the match, so each order walks the price list of its product from the first
row up to its match. The total work follows the "pairs" column, which is what the timings show.

The left side is sorted too, so the next order of the same product could resume where the
previous one stopped. That is my reading of the code, not a patch I tested.

## Running it

You need Java 17 or 21 and Python 3.11 or later.

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python generate.py 10000000 20000 50                    # writes data/p20000_c50
.venv/bin/python bench.py 3

export LAB_DATA=p1000_c1000 LAB_TAG=p1000_c1000
.venv/bin/python generate.py 10000000 1000 1000
LAB_QUERIES=asof_join,union_last_value,validity_range .venv/bin/python bench.py 3

.venv/bin/python summarize.py              # terminal table
.venv/bin/python summarize.py --markdown   # the table above
```

`LAB_DATA` picks the folder under `data/`, `LAB_TAG` the folder under `results/`, and
`BROADCAST_THRESHOLD=-1` turns off broadcast joins. The data is about 180 MB per shape and is not
committed. All runs together take about 20 minutes on the machine above.

`flag_off.py` shows the error you get without `spark.sql.join.asofJoin.enabled=true`
(`results/flag_off.txt`).

## Layout

```
common.py             Spark session, data and results folders
generate.py           orders and price_changes as Parquet
queries.py            the four queries and the pair count
bench.py              checksum, plans and timings for one dataset
summarize.py          one table from every results/*/bench.json
broadcast_hint.py     plan of asof_join with a BROADCAST hint
flag_off.py           ASOF JOIN with the feature flag off
demo.sh               prints the table for the screenshot in the post
results/              checks, physical plans and timings from my runs
```

These are single-machine numbers in local mode. A cluster changes the absolute times, but each
order still walks its product's price list, so I expect the trend to hold. I have not tested that.
