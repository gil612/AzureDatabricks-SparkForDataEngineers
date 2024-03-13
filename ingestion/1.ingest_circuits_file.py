# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### A schema is the description of the structure of your data (which together create a Dataset in Spark SQL). It can be implicit (and inferred at runtime) or explicit (and known at compile time).

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# CircuitId is a primary key
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# Fix header
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html#pyspark.sql.DataFrameReader.csv
# https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html
# Add .option("header", True) before .csv("dbfs:/...")
# inferSchema : str or bool, optional
# infers the input schema automatically from data. It requires one extra pass over the data. If None is set, it uses the default value, false.

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Change country column to race_country
circuits_selected_df = circuits_df.select(col("circuitId"),
                                          col("circuitRef"),
                                          col("name"),
                                          col("location"),
                                          col("country") \
                                          #.alias("race_country")
                                          ,
                                          col("lat"),
                                          col("lng"),
                                          col("alt"))
display(circuits_selected_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longtitue") \
.withColumnRenamed("alt", "altitude") \
.withColumn("env",lit( "Production")) \
.withColumn("data_source", lit(v_data_source)) 
display(circuits_renamed_df.head(5))      

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------



# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5 - Write datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits").head(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")
