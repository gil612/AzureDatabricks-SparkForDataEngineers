# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrameReader.json.html
constructors_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")


# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructors_ref") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) 


# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors").head(5))

# COMMAND ----------

dbutils.notebook.exit("Success")
