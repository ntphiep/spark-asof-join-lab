"""Check that the queries agree, then time each one.

Usage: python bench.py [runs]
Writes to results/<LAB_TAG>/: checks.txt, bench.json and plans/<query>.txt.
"""
import json
import os
import statistics
import sys
import time

from common import RESULTS, register_tables, spark_session
from queries import CANDIDATE_PAIRS, QUERIES

RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
# LAB_QUERIES=asof_join,validity_range runs a subset of the queries.
SELECTED = os.environ.get("LAB_QUERIES", ",".join(QUERIES)).split(",")


def checksum(spark, sql):
    return spark.sql(
        f"""
        SELECT COUNT(*) AS rows_out,
               COUNT(price) AS priced,
               SUM(price * quantity) AS revenue,
               SUM(HASH(order_id, price)) AS row_hash
        FROM ({sql})
        """
    ).first().asDict()


def timed_run(spark, sql):
    start = time.perf_counter()
    spark.sql(sql).write.format("noop").mode("overwrite").save()
    return time.perf_counter() - start


def main():
    spark = spark_session()
    register_tables(spark)
    (RESULTS / "plans").mkdir(parents=True, exist_ok=True)

    orders = spark.table("orders").count()
    prices = spark.table("price_changes").count()
    pairs = spark.sql(CANDIDATE_PAIRS).first().pairs

    lines = [
        f"orders: {orders:,}",
        f"price_changes: {prices:,}",
        f"order x earlier price change pairs: {pairs:,}",
        "",
    ]
    sums = {}
    for name in SELECTED:
        sql = QUERIES[name]
        sums[name] = checksum(spark, sql)
        lines.append(f"{name}: {sums[name]}")
        plan = spark.sql(sql)._jdf.queryExecution().executedPlan().toString()
        (RESULTS / "plans" / f"{name}.txt").write_text(plan)
    same = all(s == sums[SELECTED[0]] for s in sums.values())
    lines += ["", f"{len(SELECTED)} queries return the same rows: {same}"]
    (RESULTS / "checks.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))

    timings = {}
    for name in SELECTED:
        sql = QUERIES[name]
        timed_run(spark, sql)  # warm-up
        runs = [round(timed_run(spark, sql), 2) for _ in range(RUNS)]
        timings[name] = {"runs_s": runs, "median_s": statistics.median(runs)}
        print(f"{name}: {runs} median {timings[name]['median_s']} s")

    conf = spark.sparkContext.getConf()
    out = {
        "spark_version": spark.version,
        "master": conf.get("spark.master"),
        "driver_memory": conf.get("spark.driver.memory"),
        "orders": orders,
        "price_changes": prices,
        "candidate_pairs": pairs,
        "timings": timings,
    }
    (RESULTS / "bench.json").write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main()
