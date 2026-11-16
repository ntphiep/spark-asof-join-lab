"""Show what happens when ASOF JOIN runs without spark.sql.join.asofJoin.enabled."""
from common import register_tables, spark_session
from queries import ASOF

spark = spark_session(asof_enabled=False)
register_tables(spark)
print("spark.sql.join.asofJoin.enabled =", spark.conf.get("spark.sql.join.asofJoin.enabled"))
try:
    spark.sql(ASOF).count()
except Exception as e:  # noqa: BLE001
    print(type(e).__name__)
    print(next(line for line in str(e).splitlines() if line.strip()))
