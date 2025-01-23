# Databricks notebook source
print("hello world")

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_date

# COMMAND ----------

spark.sql('SELECT current_date').show()

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db/orders

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TEMPORARY VIEW orders(
# MAGIC   order_id INT,
# MAGIC   order_date STRING,
# MAGIC   order_customer_id INT,
# MAGIC   order_status STRING
# MAGIC ) USING CSV
# MAGIC OPTIONS(
# MAGIC   path = 'dbfs:/public/retail_db/orders/'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC SELECT * FROM orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_status,
# MAGIC count(*) AS order_count
# MAGIC FROM orders
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC;