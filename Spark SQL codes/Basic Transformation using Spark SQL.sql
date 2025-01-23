-- Databricks notebook source
DROP DATABASE itversity_retail_db CASCADE

-- COMMAND ----------

CREATE DATABASE itversity_retail_db

-- COMMAND ----------

USE itversity_retail_db

-- COMMAND ----------

CREATE OR REPLACE TABLE orders (
  order_id INT,
  order_data DATE,
  order_customer_id INT,
  order_status STRING
)

-- COMMAND ----------

DESCRIBE FORMATTED orders;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders_v(
  order_id INT,
  order_data DATE,
  order_customer_id INT,
  order_status STRING
) USING CSV
OPTIONS (
  path = 'dbfs:/public/retail_db/orders'
)

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM orders_v;

-- COMMAND ----------

CREATE OR REPLACE TABLE order_items (
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal FLOAT,
  order_item_product_price FLOAT
)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW order_items_v(
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal FLOAT,
  order_item_product_price FLOAT
) USING CSV
OPTIONS (
  path = 'dbfs:/public/retail_db/order_items'
)

-- COMMAND ----------

INSERT INTO order_items
SELECT * FROM order_items_v;

-- COMMAND ----------

SHOW tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filtering Data

-- COMMAND ----------

SELECT * FROM orders
WHERE order_status = 'COMPLETE'

-- COMMAND ----------

SELECT *
FROM orders
WHERE order_status LIKE 'C%';

-- COMMAND ----------

SELECT *
FROM orders
where order_status IN ('PENDING')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aggregraion of data

-- COMMAND ----------

-- sum, min, max, avg

SELECT
  sum(o.order_item_subtotal) as total_sales,
  avg(o.order_item_subtotal) as avg_sales,
  min(o.order_item_subtotal) as min_sales,
  max(o.order_item_subtotal) as max_sales
FROM 
  order_items o

-- COMMAND ----------

SELECT
  o.order_item_product_id as product_id,
  sum(o.order_item_subtotal) as total_sales
FROM
  order_items o
GROUP BY o.order_item_product_id
ORDER BY 1 DESC

-- COMMAND ----------

SELECT
  order_data,
  count(*) AS order_count
FROM
  orders
WHERE
  date_format(order_data, 'yyyyMM') = 201401
AND order_status IN ('COMPLETE', 'CLOSED')
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

ALTER TABLE orders RENAME COLUMN order_data TO order_date

-- COMMAND ----------

-- Order of execution
-- SELECT
-- FROM
-- WHERE
-- GROUP BY
-- HAVING
-- ORDER BY
-- LIMIT


SELECT 
  order_data,
  count(*) AS order_count
FROM
  orders
WHERE
  date_format(order_data, 'yyyyMM') = 201401
  AND order_status IN ('COMPLETE', 'CLOSED')
GROUP BY 1
HAVING order_count > 100
ORDER BY 2 DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JOINS in SQL

-- COMMAND ----------

SELECT
  *
FROM
  orders as o
JOIN
  order_items AS ot
ON
  o.order_id = ot.order_item_order_id

-- COMMAND ----------

SELECT
  o.*,
  oi.order_item_product_id,
  oi.order_item_quantity,
  oi.order_item_subtotal
FROM 
  orders as o
LEFT OUTER JOIN 
  order_items as oi
ON
  o.order_id = oi.order_item_order_id
ORDER BY  o.order_id;

-- COMMAND ----------

SELECT
  o.*,
  oi.order_item_product_id,
  oi.order_item_quantity,
  oi.order_item_subtotal
FROM 
  orders as o
RIGHT OUTER JOIN 
  order_items as oi
ON
  o.order_id = oi.order_item_order_id
ORDER BY  o.order_id;

-- COMMAND ----------

SELECT
  o.order_id,
  o.order_data,
  o.order_customer_id,
  o.order_status,
  nvl(round(sum(oi.order_item_subtotal), 2), 0) AS order_revenue
FROM
  orders AS o
LEFT OUTER JOIN
  order_items AS oi
ON 
  o.order_id = oi.order_item_order_id
WHERE 
  date_format(order_data, 'yyyyMM') = 201401
  AND o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY 1, 2, 3, 4
HAVING 
  order_revenue > 1000 OR order_revenue = 0
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sorting Data

-- COMMAND ----------

SELECT
  *
FROM 
  orders
ORDER BY
  order_customer_id ASC, order_status DESC NULLS LAST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Coping Data

-- COMMAND ----------

SELECT
  o.order_data,
  round(sum(oi.order_item_subtotal), 2) AS order_revenue
FROM
  orders AS o
JOIN
  order_items AS oi
  ON 
    o.order_id = oi.order_item_order_id
WHERE
  o.order_status IN ('COMPLETE' , 'CLOSED')
GROUP BY
  o.order_data
ORDER BY
  o.order_data

-- COMMAND ----------

CREATE OR REPLACE TABLE daily_revenue
AS
SELECT
  o.order_data,
  round(sum(oi.order_item_subtotal), 2) AS order_revenue
FROM
  orders AS o
JOIN
  order_items AS oi
  ON 
    o.order_id = oi.order_item_order_id
WHERE
  o.order_status IN ('COMPLETE' , 'CLOSED')
GROUP BY
  o.order_data
ORDER BY
  o.order_data

-- COMMAND ----------

SELECT * FROM daily_revenue;

-- COMMAND ----------

DROP TABLE daily_revenue;

-- COMMAND ----------

CREATE OR REPLACE TABLE daily_revenue(
  order_date DATE,
  order_revenue DOUBLE
)

-- COMMAND ----------

INSERT INTO daily_revenue
SELECT
  o.order_data,
  round(sum(oi.order_item_subtotal), 2) AS order_revenue
FROM
  orders AS o
JOIN
  order_items AS oi
  ON 
    o.order_id = oi.order_item_order_id
WHERE
  o.order_status IN ('COMPLETE' , 'CLOSED')
GROUP BY
  o.order_data
ORDER BY
  o.order_data

-- COMMAND ----------

SELECT * FROM daily_revenue;

-- COMMAND ----------

