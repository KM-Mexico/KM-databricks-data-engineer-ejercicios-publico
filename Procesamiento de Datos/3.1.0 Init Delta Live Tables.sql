-- Databricks notebook source
-- MAGIC %fs 
-- MAGIC rm -r /mnt/demo-datasets/bookstore/orders-raw

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS databricks_de

-- COMMAND ----------

-- MAGIC %run "/Workspace/Repos/Databricks Data Engineer 2024/Databricks_Data_Engineer_2024/Procesamiento de Datos/3.0 Arquitectura Multi Hop"
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS databricks_de.streaming_customers_mv;
DROP TABLE IF EXISTS databricks_de.streaming_orders_bronze;
DROP TABLE IF EXISTS databricks_de.streaming_orders_bronze_from_autoloader;
DROP TABLE IF EXISTS databricks_de.streaming_orders_silver;
DROP TABLE IF EXISTS databricks_de.streaming_cn_daily_customer_books_gold_mv;
DROP TABLE IF EXISTS databricks_de.streaming_fr_daily_customer_books_gold_mv;
