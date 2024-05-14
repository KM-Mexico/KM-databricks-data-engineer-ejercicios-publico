-- Databricks notebook source
SELECT order_date as fecha, sum(books_counts) as libros 
FROM hive_metastore.databricks_de.streaming_fr_daily_customer_books_gold_mv
GROUP BY ALL

-- COMMAND ----------

SELECT order_date as fecha, count(customer_id) as clientes
FROM hive_metastore.databricks_de.streaming_cn_daily_customer_books_gold_mv
GROUP BY ALL
