# Databricks notebook source
spark.read.json("/mnt/formula1dl612/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceID, count(1)
# MAGIC FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

spark.read.json("/mnt/formula1dl612/raw/2021-03-28/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceID, count(1)
# MAGIC FROM results_w1
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

spark.read.json("/mnt/formula1dl612/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceID, count(1)
# MAGIC FROM results_w2
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest drivers.json file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StructField, DateType, StringType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                   StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("code", StringType(), True),
                                      StructField("name", name_schema),
                                      StructField("dob", DateType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True),
                                      ])

# COMMAND ----------

# https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrameReader.json.html
drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) \
.withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

display(drivers_final_df.head(5))

# COMMAND ----------

drivers_dropped_df = drivers_final_df.drop(col('url'))
display(drivers_dropped_df.head(5))

# COMMAND ----------

drivers_dropped_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")
