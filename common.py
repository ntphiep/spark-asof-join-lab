"""Shared Spark session and paths for the lab."""
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).parent
DATA = ROOT / "data" / os.environ.get("LAB_DATA", "p20000_c50")
RESULTS = ROOT / "results" / os.environ.get("LAB_TAG", "default")


def spark_session(asof_enabled=True):
    spark = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[8]"))
        .appName("spark-asof-join-lab")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.join.asofJoin.enabled", str(asof_enabled).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", os.environ.get("BROADCAST_THRESHOLD", "10MB"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def register_tables(spark):
    spark.read.parquet(str(DATA / "orders")).createOrReplaceTempView("orders")
    spark.read.parquet(str(DATA / "price_changes")).createOrReplaceTempView("price_changes")
