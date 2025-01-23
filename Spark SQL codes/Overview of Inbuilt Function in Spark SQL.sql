-- Databricks notebook source
DESCRIBE FUNCTION substr

-- COMMAND ----------

SHOW FUNCTIONS

-- COMMAND ----------

SELECT current_date AS current_data

-- COMMAND ----------

SELECT substr('Hello World', 1, 5) AS result;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## STRING MANIPULATION FUNCTIONS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Case Conversioon and Length

-- COMMAND ----------

SELECT
  lower('hELLo WorlD') AS lowe_res,
  upper('hELLo WorlD') AS upper_res,
  initcap('hELLo WorlD') AS initcap_res;

-- COMMAND ----------

SELECT
  length('Hello World') AS length_res

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extracting Data - substr, split

-- COMMAND ----------

SELECT
  substr('Hello World', 1, 5) As first_str,
  substr('Hello World', -5) AS last_str

-- COMMAND ----------

SELECT
  split('Hello World', ' ') AS split_res

-- COMMAND ----------

SELECT
  split('123456789, 1235299u49', ',')[1] AS second_num

-- COMMAND ----------

-- exploding single i/p into multiple rows
SELECT explode(split('123456789, 12402u39350', ',')) AS phone_numbers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Trimming and Padding

-- COMMAND ----------

SELECT
  ltrim('             Hello World') as ltrim_res,
  rtrim('Hello World              ') as rtrim_res,
  trim('             Hello World              ') as trim_rem

-- COMMAND ----------

SELECT
  trim(LEADING 'a' FROM 'aaaaaaHelloWorldbbbbbb') AS res1,
  trim(TRAILING 'b' FROM 'aaaaaaHelloWorldbbbbbb') AS res2

-- COMMAND ----------

SELECT
  lpad('Hello World', 5, '*') AS res1,
  lpad('Hello World', 20, '*') AS res2

-- COMMAND ----------

SELECT
  rpad('Hello World', 5, '*') AS res1,
  rpad('Hello World', 20, '*') AS res2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reverse and concatenate multiple string

-- COMMAND ----------

SELECT
  reverse('Hellow World') as res1

-- COMMAND ----------

SELECT 
  concat('Hello','World') As res2,
  concat_ws(' ', 'Hello', 'Wor;d') as res3, -- first argument is delimiter
  concat_ws('&', 'Hello', 'World') as res4

-- COMMAND ----------

SELECT 
ARRAY('123456', 'adafag', '235k2jntk') AS original,
CONCAT_WS(' - ', ARRAY('123456', 'adafag', '235k2jntk')) AS res

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Date Manipulation

-- COMMAND ----------

SELECT current_date, current_timestamp;

-- COMMAND ----------

SELECT
  current_date as current_date,
  date_add(current_date, 32) AS result


-- COMMAND ----------

SELECT
  date_add(current_date, -730) as res1,
  date_sub(current_date, 730) as res2

-- COMMAND ----------

SELECT
  datediff('2024-02-12', '2023-06-24') as date_diff_res

-- COMMAND ----------

SELECT
  add_months('2024-02-12', 2) as add_months_res,
  date_add('2024-02-12', 4 * 365) as add_years_res

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Trunc Date

-- COMMAND ----------

-- trunc Only applied on month and year
SELECT
  trunc('2025-02-25', 'MM') as beginning_of_month,
  trunc('2025-02-25', 'YY') as beginning_of_year

-- COMMAND ----------

SELECT
  EXTRACT(MONTH FROM date_trunc('WEEK', '2025-02-23')) as res1,
  EXTRACT(YEAR FROM date_trunc('WEEK', '2025-02-23')) as res2,
  EXTRACT(DAY FROM date_trunc('WEEK', '2025-02-23')) as res3,
  

-- COMMAND ----------

SELECT
  current_timestamp as current_timestamp,
  date_format(current_timestamp, 'yyyy') as rs1,
  date_format(current_timestamp, 'MM') as rs2,
  date_format(current_timestamp, 'dd') as rs3,
  date_format(current_timestamp, 'HH') as rs4,
  date_format(current_timestamp, 'mm') as rs5,
  date_format(current_timestamp, 'SS') as rs6,
  date_format(current_timestamp, 'EEEE') as rs7,
  date_format(current_timestamp, 'MMM') as rs8,
  date_format(current_timestamp, 'EE') as rs9,
  date_format(current_timestamp, 'MMMM') as rs10

-- COMMAND ----------

SELECT
  date_format('2025-02-23', 'yyyyMM') as year_mon,
  date_format('2025-02-23', 'yyyy-MM-dd') as res2,
  date_format('2025-02-23', 'dd MMMM, yyyy') as res3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing Non-Standard Dates and Timestamp

-- COMMAND ----------

SELECT
  to_date('2022/1/16', 'yyyy/M/dd') as res,
  to_timestamp('2022/1/16 12:30', 'yyyy/M/dd HH:mm') as res2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extracting day, month, year, dayofmonth, weekofyear

-- COMMAND ----------

SELECT
  day(current_date) as day,
  month(current_date) as month,
  year(current_date) as year,
  dayofmonth(current_date) as day_of_month,
  weekofyear(current_date) as week_of_year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dealing with unix_timestamp

-- COMMAND ----------

SELECT
  from_unixtime(1556662731, 'yyyyMM') as res,
  to_unix_timestamp('2019-05-01 18:05:31') as unixtime

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Numeric Functions

-- COMMAND ----------

SELECT
  abs(-1) as res1,
  abs(10) as res2

-- COMMAND ----------

USE itversity_retail_db;
SELECT current_database();

-- COMMAND ----------

SELECT
  sum(order_item_subtotal) as total_sales,
  avg(order_item_subtotal) as avg_sales,
  ceil(avg(order_item_subtotal)) as rounded_avg_sales,
  floor(avg(order_item_subtotal)) as rounded_avg_sales2,
  round(avg(order_item_subtotal), 2) as rounded_avg_sales3
FROM 
  order_items

-- COMMAND ----------

SELECT
  pow(13, 2) as res

-- COMMAND ----------

SELECT
  count_if(order_item_subtotal > 100) as res,
  count_if(order_item_subtotal < 100) as res2,
  count_if(order_item_subtotal = 100) as res3,
  count(*) as tot_count
FROM
  order_items

-- COMMAND ----------

SELECT
  cume_dist() over (order by order_item_subtotal) as res
FROM
  order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Type Casting

-- COMMAND ----------

SELECT 
  '09'::INT as num1,
  current_timestamp::date as date1,
  'True'::BOOLEAN as bool1,


-- COMMAND ----------

SELECT
  cast('0.64' AS FLOAT) as f1,
  cast('3.984' AS INT) as i1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Handling Null values

-- COMMAND ----------

SELECT
  nvl(1, 0) as nv,
  coalesce(1, 0) as co

-- COMMAND ----------

SELECT
  nvl(NULL, 0) nv,
  COALESCE(NULL, 3241) AS coa

-- COMMAND ----------

-- nvl2 uses conditional logic to return a value based on a condition

-- Example -> SELECT Name, NVL2(Bonus, Bonus, 100) AS Effective_Bonus FROM Employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CASE and WHEN statement

-- COMMAND ----------

SELECT * FROM orders;

-- COMMAND ----------

SELECT
  o.*,
  CASE
    WHEN o.order_status IN ('COMPLETE', 'CLOSED') THEN 'COMPLETED'
    WHEN o.order_status IN ('PENDING_PAYMENT', 'OPEN') THEN 'PENDING'
    ELSE o.order_status
    END AS status
FROM
  orders o
ORDER BY 
  o.order_id DESC

-- COMMAND ----------

SELECT
  CASE
    WHEN o.order_status IN ('COMPLETE', 'CLOSED') THEN 'COMPLETED'
    WHEN o.order_status IN ('PENDING', 'PENDING_PAYMENT', 'PROCESSING', 'PAYMENT_REVIEW') THEN 'PENDING'
    ELSE 'OTHER' 
    END AS updated_order_status,
    count(*) as order_count
FROM
  orders o
GROUP BY 1
ORDER BY 2 DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Word Count

-- COMMAND ----------

CREATE TABLE lines(
  s STRING
)

-- COMMAND ----------

INSERT INTO lines
VALUES 
  ('Hello World'),
  ('How are you'),
  ('Let us perform the word count'),
  ('The definition of word count'),
  ('to get the count of each word from this data')

-- COMMAND ----------

Select * from lines

-- COMMAND ----------

SELECT split(s, ' ') AS word_array FROM lines

-- COMMAND ----------

SELECT
  count(1)
FROM
  (SELECT
    explode(split(s, ' ')) as words  FROM lines)

-- COMMAND ----------

SELECT
  words, count(1) 
FROM
  (SELECT
    explode(split(s, ' ')) AS words FROM lines)
GROUP BY 1
ORDER BY 2 DESC;

-- COMMAND ----------

SELECT s, SIZE(SPLIT(s, ' ')) FROM lines