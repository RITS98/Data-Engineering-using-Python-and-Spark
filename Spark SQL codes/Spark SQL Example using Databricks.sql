-- Databricks notebook source
SELECT current_date;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/public/retail_db/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The ORDERS table

-- COMMAND ----------

SELECT * FROM TEXT.`dbfs:/public/retail_db/orders/` LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders(
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
) USING CSV
OPTIONS (
  path='/public/retail_db/orders/',
  sep=',' --optional if it is a common seperator
);

-- COMMAND ----------

DESCRIBE orders; -- metadata about orders

-- COMMAND ----------

SELECT * FROM orders LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The ORDER_ITEMS table

-- COMMAND ----------

SELECT * FROM TEXT.`dbfs:/public/retail_db/order_items/` LIMIT 10

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW order_items(
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal FLOAT,
  order_item_product_price FLOAT
) USING CSV
OPTIONS(
  path='dbfs:/public/retail_db/order_items/',
  sep=','
);

-- COMMAND ----------

DESCRIBE order_items;

-- COMMAND ----------

SELECT * FROM order_items LIMIT 10;

-- COMMAND ----------

SELECT DISTINCT order_status FROM orders;

-- COMMAND ----------

SELECT
  COUNT(*)
FROM orders
WHERE
order_status IN ('COMPLETE', 'CLOSED');

-- COMMAND ----------

SELECT
  *
FROM orders AS o
JOIN
  order_items AS oi
  ON
    o.order_id = oi.order_item_order_id
  WHERE
    o.order_status IN ('COMPLETE', 'CLOSED');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Compute Daily Product Revenue

-- COMMAND ----------

SELECT
  o.order_date,
  oi.order_item_product_id,
  round(sum(oi.order_item_subtotal), 2) As revenue
FROM orders AS o
JOIN
  order_items AS oi
  ON
    o.order_id = oi.order_item_order_id
  WHERE
    o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY
  1, 2
ORDER BY 1 ASC, 3 DESC;
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert the above table back into DBFS as parquet file

-- COMMAND ----------

INSERT OVERWRITE DIRECTORY 'dbfs:/public/retail_db/daily_product_revenue'
USING PARQUET
SELECT
  o.order_date,
  oi.order_item_product_id,
  round(sum(oi.order_item_subtotal), 2) As revenue
FROM orders AS o
JOIN
  order_items AS oi
  ON
    o.order_id = oi.order_item_order_id
  WHERE
    o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY
  1, 2
ORDER BY 1 ASC, 3 DESC;
  

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/public/retail_db/daily_product_revenue

-- COMMAND ----------

SELECT * FROM PARQUET.`dbfs:/public/retail_db/daily_product_revenue`
ORDER BY order_date ASC, revenue DESC 
LIMIT 10;