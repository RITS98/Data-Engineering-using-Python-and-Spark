-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Overview of Supported Providers

-- COMMAND ----------

DROP DATABASE IF EXISTS itversity_retail_db CASCADE

-- COMMAND ----------

SET spark.sql.warehouse.dir

-- COMMAND ----------

DROP DATABASE itversity_retail_db

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS itversity_retail_db;

-- COMMAND ----------

DESCRIBE DATABASE itversity_retail_db

-- COMMAND ----------

-- CREATE TABLE orders (
--   order_id INT,
--   order_date DATE,
--   order_customer_id INT,
--   order_status STRING
-- ) USING DELTA -- PARQUET, CSV, JSON

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS itversity_retail_db
MANAGED LOCATION 'dbfs:/public/warehouse/itversity_retail_db'

-- COMMAND ----------

USE itversity_retail_db

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

CREATE OR REPLACE TABLE itversity_retail_db.orders (
  order_id INT,
  order_date TIMESTAMP,
  order_customer_id INT,
  order_status STRING
) USING DELTA

-- Delta table is the default data table format in Databricks and is a feature of the Delta Lake open source data framework. Delta tables are typically used for data lakes, where data is ingested via streaming or in large batches.

-- We can also use PARQUET, CSV, JSON in place of DELTA


-- COMMAND ----------

DESCRIBE orders;

-- COMMAND ----------

-- More metadata about the table
DESCRIBE FORMATTED orders;

-- COMMAND ----------

-- SHOW the SQL for creating the orders table
SHOW CREATE TABLE orders;

-- COMMAND ----------

SELECT 
  *
FROM
  PARQUET.`dbfs:/public/retail_db_parquet/orders`
LIMIT 5;

-- COMMAND ----------

COPY INTO orders
FROM
'dbfs:/public/retail_db_parquet/orders'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

TRUNCATE TABLE ORDERS;

-- COMMAND ----------

SELECT * FROM orders
LIMIT 10;

-- COMMAND ----------

SELECT
  count(
    *
  )
FROM 
orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create and review more such table 

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TABLE order_items(
  order_item_id BIGINT,
  order_item_order_id BIGINT,
  order_item_product_id BIGINT,
  order_item_quantity BIGINT,
  order_item_subtotal DOUBLE,
  order_item_product_price DOUBLE
)
USING DELTA

-- COMMAND ----------

DESCRIBE FORMATTED order_items;

-- COMMAND ----------

INSERT INTO order_items
SELECT
  *
FROM
  PARQUET.`dbfs:/public/retail_db_parquet/order_items`

-- COMMAND ----------

SELECT * FROM order_items LIMIT 5

-- COMMAND ----------

SELECT
  order_item_order_id,
  round(sum(order_item_subtotal), 2) AS order_revenue
FROM
  order_items
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CRUD demo on DELTA tables

-- COMMAND ----------

CREATE TABLE crud_demo (
  user_id INT,
  user_fname STRING,
  user_lname STRING,
  user_email STRING
) USING DELTA

-- COMMAND ----------

INSERT INTO crud_demo
VALUES
(1, 'Scott', 'Tiger', 'stiger@gmail.com'),
(2, 'Donald', 'Duck', 'dduck@gmail.com')

-- COMMAND ----------

SELECT * FROM crud_demo;

-- COMMAND ----------

DELETE FROM crud_demo
WHERE user_email = null;

-- COMMAND ----------

UPDATE crud_demo 
SET user_email = 'tjerry@gmail.com'
WHERE
user_id = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merge statements

-- COMMAND ----------

CREATE OR REPLACE TABLE crud_demo_stg (
  user_id INT, 
  user_fname STRING,
  user_lname STRING,
  user_email STRING
) USING DELTA;

-- COMMAND ----------

INSERT INTO crud_demo_stg
VALUES
(3, 'Durga', 'Gadiraju', 'durga@gmail.com'),
(5, 'ITVersity', 'Inc', 'iinc@gmail.com'),
(6, 'Analytics', 'Inc', 'ainc@gmail.com')

-- COMMAND ----------

-- Used to Update records as well as insert  new records into an existing table
MERGE INTO crud_demo AS cd
USING crud_demo_stg AS cdg
ON
  cd.user_id = cdg.user_id
WHEN MATCHED THEN UPDATE SET * 
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- WHEN source column and target column names are different
MERGE INTO crud_demo AS cd
USING crud_demo_stg AS cdg
ON
  cd.user_id = cdg.user_id
WHEN MATCHED THEN UPDATE SET 
  cd.user_fname = cdg.user_fname,
  cd.user_lname = cdg.user_lname,
  cd.user_email = cdg.user_email
WHEN NOT MATCHED THEN INSERT 
(
  cd.user_id,
  cd.user_fname,
  cd.user_lname,
  cd.user_email
)
VALUES
(
  cdg.user_id,
  cdg.user_fname,
  cdg.user_lname,
  cdg.user_email
)