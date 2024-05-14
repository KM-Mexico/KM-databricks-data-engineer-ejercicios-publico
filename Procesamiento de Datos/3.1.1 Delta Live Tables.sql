-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Delta Live Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ejecutar antes el notebook 3.1.0 Init Delta Live Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creación de tabla streaming a partir de una tabla Delta creada en el ejercicio pasado

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE streaming_orders_bronze
AS
SELECT * FROM STREAM(databricks_de.orders_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creación de tabla streaming a partir de Autoloader

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE streaming_orders_bronze_from_autoloader
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("dbfs:/mnt/demo-datasets/bookstore/orders-json-raw", "json",
                             map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creación de vista materializada de customers

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW streaming_customers_mv
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables
-- MAGIC
-- MAGIC #### orders_cleaned

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE streaming_orders_silver (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.streaming_orders_bronze) o
  LEFT JOIN LIVE.streaming_customers_mv c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Tables

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW streaming_cn_daily_customer_books_gold_mv
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.streaming_orders_silver
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW streaming_fr_daily_customer_books_gold_mv
COMMENT "Daily number of books per customer in France"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.streaming_orders_silver
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
