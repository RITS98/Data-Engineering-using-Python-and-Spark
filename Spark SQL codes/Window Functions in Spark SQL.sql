-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW daily_product_revenue
USING PARQUET
OPTIONS(
  path = 'dbfs:/public/retail_db/daily_product_revenue'
)

-- COMMAND ----------

SELECT
  *
FROM
  daily_product_revenue
ORDER BY
  1, 3 DESC;

-- COMMAND ----------

SELECT
  dpr.*,
  rank() over(order by revenue desc) as rnk
FROM
  daily_product_revenue dpr
WHERE
  dpr.order_date = '2013-07-26 00:00:00.0'
ORDER BY
  1, 3 DESC;

-- COMMAND ----------

SELECT
  dpr.*,
  rank() over(partition by order_date order by revenue desc) as rnk
FROM daily_product_revenue dpr
ORDER BY 1, 3 DESC

-- COMMAND ----------

SELECT
  *
FROM (
  SELECT
  dpr.*,
  rank() over(partition by order_date order by revenue desc) as rnk
FROM daily_product_revenue dpr
ORDER BY 1, 3 DESC
) AS subquery
WHERE
  subquery.rnk <= 3