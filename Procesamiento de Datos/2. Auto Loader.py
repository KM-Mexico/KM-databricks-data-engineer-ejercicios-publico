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
# MAGIC # Autoloader para carga de parquets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Revisar directorio de parquets

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Crear Stream Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC Limpiamos datos de ejecuciones previas en caso de que existan

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_de.orders_updates

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /mnt/demo/orders_checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos crear el Stream de Auto Loader en la misma sentencia de readStream y writeStream

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load("dbfs:/mnt/demo-datasets/bookstore/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("databricks_de.orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC Checamos cuantos registros trae el primer archivo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM databricks_de.orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Se agregan nuevos archivos

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos observar que en la ruta de parquets existe un nuevo archivo

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Una vez que se agrega el parquet en el directorio, vemos que automaticamente el Stream Autoloader carga el archivo a la tabla destino

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM databricks_de.orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Explorar el historial de versiones

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY databricks_de.orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC Agregamos un archivo mas

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirmamos el directorio

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Veemos como cambia la data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM databricks_de.orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY databricks_de.orders_updates
