"""Four ways to attach the price that was valid when each order was placed.

Every query keeps all orders (orders placed before the first price change
get a NULL price) and returns the same columns.
"""

ASOF = """
SELECT o.order_id, o.order_ts, o.product_id, o.quantity, p.price
FROM orders o
LEFT ASOF JOIN price_changes p
  MATCH_CONDITION (o.order_ts >= p.valid_from)
  ON o.product_id = p.product_id
"""

# Join every earlier price change, then keep the newest one per order.
JOIN_THEN_RANK = """
WITH candidates AS (
  SELECT o.order_id, o.order_ts, o.product_id, o.quantity, p.price,
         ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY p.valid_from DESC) AS rn
  FROM orders o
  LEFT JOIN price_changes p
    ON o.product_id = p.product_id AND p.valid_from <= o.order_ts
)
SELECT order_id, order_ts, product_id, quantity, price
FROM candidates
WHERE rn = 1
"""

# Turn price changes into validity ranges with LEAD, then range join.
VALIDITY_RANGE = """
WITH ranges AS (
  SELECT product_id, price, valid_from,
         LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS valid_to
  FROM price_changes
)
SELECT o.order_id, o.order_ts, o.product_id, o.quantity, r.price
FROM orders o
LEFT JOIN ranges r
  ON o.product_id = r.product_id
 AND o.order_ts >= r.valid_from
 AND (r.valid_to IS NULL OR o.order_ts < r.valid_to)
"""

# Stack both tables on one timeline and carry the last price forward.
# kind = 0 sorts a price change before an order with the same timestamp,
# which matches order_ts >= valid_from.
UNION_LAST_VALUE = """
WITH timeline AS (
  SELECT order_id, product_id, order_ts AS ts, quantity,
         CAST(NULL AS DECIMAL(10,2)) AS price, 1 AS kind
  FROM orders
  UNION ALL
  SELECT NULL, product_id, valid_from, NULL, price, 0
  FROM price_changes
),
filled AS (
  SELECT *, LAST(price, true) OVER (
           PARTITION BY product_id ORDER BY ts, kind
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_price
  FROM timeline
)
SELECT order_id, ts AS order_ts, product_id, quantity, last_price AS price
FROM filled
WHERE kind = 1
"""

QUERIES = {
    "asof_join": ASOF,
    "join_then_rank": JOIN_THEN_RANK,
    "validity_range": VALIDITY_RANGE,
    "union_last_value": UNION_LAST_VALUE,
}

# Number of (order, earlier price change) pairs that a plain range join
# has to produce. Counted with a running window so it stays cheap even when
# the pairs run into billions.
CANDIDATE_PAIRS = """
WITH timeline AS (
  SELECT product_id, order_ts AS ts, 1 AS kind FROM orders
  UNION ALL
  SELECT product_id, valid_from, 0 FROM price_changes
),
counted AS (
  SELECT kind, SUM(1 - kind) OVER (
           PARTITION BY product_id ORDER BY ts, kind
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS earlier_prices
  FROM timeline
)
SELECT SUM(earlier_prices) AS pairs
FROM counted
WHERE kind = 1
"""
