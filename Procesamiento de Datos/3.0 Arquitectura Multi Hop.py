# Databricks notebook source
# MAGIC %md
# MAGIC Correr el setup para los sets de datos

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /mnt/demo-datasets/bookstore/orders-raw

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS databricks_de

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Revisar directorio de parquets
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
    .load("dbfs:/mnt/demo-datasets/bookstore/orders-raw")
    .createOrReplaceTempView("orders_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Agregar metadata a orders raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC   SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creacion de tabla Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_de.orders_bronze

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /mnt/demo/checkpoints/orders_bronze

# COMMAND ----------

(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
      .outputMode("append")
      .table("databricks_de.orders_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC Revisamos cuantos registros existen en la tabla destino

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM databricks_de.orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Cargamos otro archivo

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Revisamos como a cambiado la data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM databricks_de.orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creación de Vista Estática

# COMMAND ----------

(spark.read
      .format("json")
      .load("dbfs:/mnt/demo-datasets/bookstore/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creación de tabla Silver

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos vista a partir de tabla bronce

# COMMAND ----------

(spark.readStream
  .table("databricks_de.orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

# MAGIC %md
# MAGIC Transformamos los datos de bronce

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_de.orders_silver

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /mnt/demo/checkpoints/orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Cargamos a tabla en capa Silver

# COMMAND ----------

(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("databricks_de.orders_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC Revisamos los registros

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM databricks_de.orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Cargamos mas datos a la ruta de archivos parquet

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Revisamos como ha cambiado la data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM databricks_de.orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creación de tabla Gold

# COMMAND ----------

(spark.readStream
  .table("databricks_de.orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %md
# MAGIC Realizamos las agregaciones de capa Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_de.daily_customer_books

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /mnt/demo/checkpoints/daily_customer_books

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("databricks_de.daily_customer_books"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.daily_customer_books

# COMMAND ----------

# MAGIC %md
# MAGIC Cargamos mas datos a la ruta de archivos parquet

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Revisamos como ha cambiado la data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.daily_customer_books

# COMMAND ----------

# MAGIC %md
# MAGIC La data no ha cambiado ya que el stream hacia Gold tiene un trigger que una vez ejecutado el stream se termina. Se tiene que volver a ejecutar el stream

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("databricks_de.daily_customer_books"))

# COMMAND ----------

# MAGIC %md
# MAGIC Revisamos como ha cambiado la data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.daily_customer_books

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Paramos los streams activos streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
