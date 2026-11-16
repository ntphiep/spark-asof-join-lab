"""Generate orders and product price changes as Parquet.

Each product gets a price change roughly every few days over one year.
Orders land at random seconds in the same year, so most orders fall
between two price changes and some come before the first one.

Usage: python generate.py [orders] [products] [changes_per_product]
"""
import sys

from pyspark.sql import functions as F

from common import DATA, spark_session

ORDERS = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
PRODUCTS = int(sys.argv[2]) if len(sys.argv) > 2 else 20_000
CHANGES = int(sys.argv[3]) if len(sys.argv) > 3 else 50

YEAR_START = 1767225600  # 2026-01-01 00:00:00 UTC
YEAR_SECONDS = 365 * 24 * 3600


def main():
    spark = spark_session()

    prices = (
        spark.range(PRODUCTS * CHANGES)
        .select(
            (F.col("id") % PRODUCTS).alias("product_id"),
            F.timestamp_seconds(
                F.lit(YEAR_START) + (F.rand(1) * YEAR_SECONDS).cast("long")
            ).alias("valid_from"),
            F.round(F.lit(5) + F.rand(2) * 495, 2).cast("decimal(10,2)").alias("price"),
        )
        .dropDuplicates(["product_id", "valid_from"])
    )
    prices.repartition(16).write.mode("overwrite").parquet(str(DATA / "price_changes"))

    orders = spark.range(ORDERS).select(
        F.col("id").alias("order_id"),
        (F.abs(F.hash(F.col("id"), F.lit(7))) % PRODUCTS).cast("long").alias("product_id"),
        F.timestamp_seconds(
            F.lit(YEAR_START) + (F.rand(3) * YEAR_SECONDS).cast("long")
        ).alias("order_ts"),
        (F.lit(1) + (F.rand(4) * 5).cast("int")).alias("quantity"),
    )
    orders.repartition(32).write.mode("overwrite").parquet(str(DATA / "orders"))

    p = spark.read.parquet(str(DATA / "price_changes")).count()
    o = spark.read.parquet(str(DATA / "orders")).count()
    print(f"price_changes rows: {p:,}")
    print(f"orders rows: {o:,}")


if __name__ == "__main__":
    main()
