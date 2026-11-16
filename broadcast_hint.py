"""Print the physical plan of ASOF JOIN with a BROADCAST hint on price_changes.

The plan stays SortMergeAsOfJoin, so both sides are still shuffled and sorted.
"""
from common import register_tables, spark_session
from queries import ASOF

spark = spark_session()
register_tables(spark)
sql = ASOF.replace("SELECT o.order_id", "SELECT /*+ BROADCAST(p) */ o.order_id", 1)
print(spark.sql(sql)._jdf.queryExecution().executedPlan().toString())
