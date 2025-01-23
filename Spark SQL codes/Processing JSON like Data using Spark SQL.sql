-- Databricks notebook source
USE itversity_retail_db

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ARRAY

-- COMMAND ----------

CREATE OR REPLACE TABLE users (
  user_id INT,
  user_fname STRING,
  user_lanme STRING,
  user_phones ARRAY<STRING>
)

-- COMMAND ----------

INSERT INTO users
VALUES
  (1, 'Scott', 'Tiger', ARRAY('+1 (234) 567 8901', '+1 (123) 456 7890')),
  (2, 'Donald', 'Duck', NULL),
  (3, 'Mickey', 'Mouse', ARRAY('+1 (456) 789 0123'))


-- COMMAND ----------

SELECT * FROM users;

-- COMMAND ----------

-- size, explode, explode_outer

SELECT
  user_id, 
  size(user_phones) as count_phones,
  nvl2(user_phones, size(user_phones), 0) as count_ph
FROM users;

-- COMMAND ----------

SELECT user_id,
  explode(user_phones) AS user_phone
FROM users


-- COMMAND ----------

SELECT user_id,
  explode_outer(user_phones) AS user_phone
FROM users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## STRUCT

-- COMMAND ----------

CREATE OR REPLACE TABLE users(
  user_id INT,
  user_fname STRING,
  user_lanme STRING,
  user_phones STRUCT<home: STRING, mobile: STRING>
)

-- COMMAND ----------

INSERT INTO users
VALUES
  (1, 'Scott', 'Tiger', STRUCT('+1 (234) 567 8901', '+1 (123) 456 7890')),
  (2, 'Donald', 'Duck', STRUCT(NULL, NULL)),
  (3, 'Mickey', 'Mouse', STRUCT('+1 (456) 789 0123', NULL))

-- COMMAND ----------

SELECT user_id, 
  user_phones.home AS user_phone_home,
  user_phones.mobile AS user_phone_mobile
FROM users


-- COMMAND ----------

SELECT order_item_id,
  order_item_order_id,
  order_item_product_id,
  order_item_subtotal,
  STRUCT(order_item_quantity, order_item_product_price) AS order_item_trans_details
FROM order_items


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## STRUCT inside ARRAY

-- COMMAND ----------

CREATE OR REPLACE TABLE users (
  user_id INT,
  user_fname STRING,
  user_lname STRING,
  user_phones ARRAY<STRUCT<phone_type: STRING, phone_number: STRING>>
)


-- COMMAND ----------

INSERT INTO users
VALUES
  (1, 'Scott', 'Tiger', ARRAY(STRUCT('home', '+1 (234) 567 8901'), STRUCT('mobile', '+1 (123) 456 7890'))),
  (2, 'Donald', 'Duck', NULL),
  (3, 'Mickey', 'Mouse', ARRAY(STRUCT('home', '+1 (123) 456 9012')));

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

WITH user_phones_exploded AS (
  SELECT user_id,
    explode_outer(user_phones) AS user_phone
  FROM users
) 

SELECT user_id, user_phone.* FROM user_phones_exploded


-- COMMAND ----------

WITH user_phones_exploded AS (
  SELECT user_id,
    explode_outer(user_phones) AS user_phone
  FROM users
) 

SELECT user_id, 
  user_phone.phone_type AS user_phone_type,
  user_phone.phone_number AS user_phone_number
FROM user_phones_exploded

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## COLLECT

-- COMMAND ----------

CREATE OR REPLACE TABLE users (
  user_id INT,
  user_fname STRING,
  user_lname STRING,
  user_phone_type STRING,
  user_phone_number STRING
);

-- COMMAND ----------

INSERT INTO users
VALUES
  (1, 'Scott', 'Tiger', 'home', '+1 (234) 567 8901'),
  (1, 'Scott', 'Tiger', 'mobile', '+1 (123) 456 7890'),
  (2, 'Donald', 'Duck', NULL, NULL),
  (3, 'Mickey', 'Mouse', 'home', '+1 (123) 456 9012')

-- COMMAND ----------

SELECT user_id,
  user_fname,
  user_lname,
  collect_list(user_phone_number) AS user_phones
FROM users
GROUP BY 1, 2, 3


-- COMMAND ----------


SELECT user_id,
  user_fname,
  user_lname,
  concat_ws(',', collect_list(user_phone_number)) AS user_phones
FROM users
GROUP BY 1, 2, 3

-- COMMAND ----------

SELECT user_id,
  user_fname,
  user_lname,
  STRUCT(user_phone_type, user_phone_number) AS user_phone
FROM users

-- COMMAND ----------

SELECT user_id,
  user_fname,
  user_lname,
  nvl2(user_phone_number, STRUCT(user_phone_type, user_phone_number), NULL) AS user_phone
FROM users


-- COMMAND ----------

SELECT user_id,
  user_fname,
  user_lname,
  collect_list(nvl2(user_phone_number, STRUCT(user_phone_type, user_phone_number), NULL)) AS user_phones
FROM users
GROUP BY 1, 2, 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SPLIT with EXPLODE_OUTER

-- COMMAND ----------

CREATE OR REPLACE TABLE users (
  user_id INT,
  user_fname STRING,
  user_lname STRING,
  user_phones STRING
);

-- COMMAND ----------

INSERT INTO users
VALUES
  (1, 'Scott', 'Tiger', '+1 (234) 567 8901,+1 (123) 456 7890'),
  (2, 'Donald', 'Duck', NULL),
  (3, 'Mickey', 'Mouse', '+1 (123) 456 9012')

-- COMMAND ----------

SELECT
  user_id,
  user_fname,
  user_lname,
  explode_outer(split(user_phones, ',')) AS user_phone
FROM users