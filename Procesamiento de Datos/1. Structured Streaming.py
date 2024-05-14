# Databricks notebook source
# MAGIC %md
# MAGIC Correr el setup para los sets de datos

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS databricks_de

# COMMAND ----------

# MAGIC %md
# MAGIC # Crear Streams a partir de diferentes fuentes de datos "infinitas"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tablas o Vistas

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una vista temporal que apunte a los CSV poniendo su estructura

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW tmp_csv
# MAGIC    (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "mnt/demo-datasets/bookstore/books-csv/export_*.csv",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una tabla delta ya que es mas rapido hacer el ejercicio con tablas delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_de.books;
# MAGIC CREATE TABLE IF NOT EXISTS databricks_de.books AS
# MAGIC SELECT * FROM tmp_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.books limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos el stream a traves de la tabla

# COMMAND ----------

stream_df_table = spark.readStream.table("databricks_de.books")

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos consultarlo en formato de dataframe

# COMMAND ----------

stream_df_table.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos consultarlo en formato de vista SQL

# COMMAND ----------

stream_df_table.createOrReplaceTempView("books_table_stream_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_table_stream_vw limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archivos

# COMMAND ----------

# MAGIC %md
# MAGIC Para crear el stream necesitamos la estructura de los datos, para eso primero tenemos que seleccionar un archivo para no cargar todos

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/demo-datasets/bookstore/books-csv/

# COMMAND ----------

# MAGIC %md
# MAGIC Cargamos ese csv a un dataframe y de ahi obtenemos su estructura

# COMMAND ----------

df = spark.read.format("csv").options(header="true", delimiter = ";", path = "dbfs:/mnt/demo-datasets/bookstore/books-csv/export_001.csv").load()

csv_schema = df.schema
print(csv_schema)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos crear el stream con la estructura y la ruta de los CSVs

# COMMAND ----------

stream_df_csv = spark.readStream.format("csv") \
            .schema(csv_schema) \
            .options(header="true", delimiter=";", path="dbfs:/mnt/demo-datasets/bookstore/books-csv/") \
            .load()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver los resultados en formato de dataframe

# COMMAND ----------

stream_df_csv.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver los resultados del stream tambien en SQL con una vista temporal

# COMMAND ----------

stream_df_csv.createOrReplaceTempView("stream_books_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stream_books_vw LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC # Escribir Streams

# COMMAND ----------

# MAGIC %md
# MAGIC ##Escribir Stream desde una tabla o vista

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_de.books_stream_from_view

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /data_curso/structured_streaming/books_stream_from_view

# COMMAND ----------

spark.table("books_table_stream_vw") \
    .writeStream \
    .trigger(processingTime='60 seconds') \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/data_curso/structured_streaming/books_stream_from_view") \
    .table("databricks_de.books_stream_from_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.books_stream_from_view LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Escribir Stream desde un dataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_de.books_stream_from_df

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /data_curso/structured_streaming/books_stream_from_df

# COMMAND ----------

stream_df_table.writeStream \
    .trigger(processingTime='60 seconds') \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/data_curso/structured_streaming/books_stream_from_df") \
    .table("databricks_de.books_stream_from_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.books_stream_from_df LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Escribir un Stream con agregaciones

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una vista de streaming apartir de la tabla de books

# COMMAND ----------

spark.readStream.table("databricks_de.books").createOrReplaceTempView("stream_books_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stream_books_view

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una vista de streaming con agregaciones

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_table_stream_agg_vw AS
# MAGIC SELECT author, count(*) as libros_por_autor
# MAGIC FROM stream_books_view
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver la data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_table_stream_agg_vw

# COMMAND ----------

# MAGIC %md
# MAGIC Escribimos la data en modo complete

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /data_curso/structured_streaming/books_stream_from_view_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_de.books_stream_agg

# COMMAND ----------

spark.table("books_table_stream_agg_vw") \
    .writeStream \
    .trigger(processingTime='4 seconds') \
    .outputMode("complete") \
    .option("checkpointLocation", "dbfs:/data_curso/structured_streaming/books_stream_from_view_agg") \
    .table("databricks_de.books_stream_agg")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.books_stream_agg

# COMMAND ----------

# MAGIC %md
# MAGIC Linaje:
# MAGIC - databricks_de.books - stream_books_view - books_table_stream_agg_vw - databricks_de.books_stream_agg

# COMMAND ----------

# MAGIC %md
# MAGIC #Agregar registros a la fuente

# COMMAND ----------

# MAGIC %md
# MAGIC ##Agregar registros a books

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricks_de.books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Revisamos resultados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.books_stream_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricks_de.books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_de.books_stream_agg
