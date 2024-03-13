# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, concat
from pyspark.sql.functions import to_timestamp, lit, current_timestamp

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# CircuitId is a primary key
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/races.csv")
display(races_df.head(5))

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_selected_df = races_df.select("raceId", "year", "round", "circuitId", "name", "time", "date")
display(races_selected_df.head(5))

# COMMAND ----------

# lit(' ')
races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("year","race_year") \
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
.withColumn("data_source", lit(v_data_source))    
display(races_renamed_df.head(5))

# COMMAND ----------

races_final_df = add_ingestion_date(races_renamed_df)
display(races_final_df.head(5))

# COMMAND ----------

# races_final_df = races_ingestion_Date_df.select(col("raceId").alias('race_id'),
#                                           col("year").alias('race_year'),
#                                           col("round"),
#                                           col("circuitId").alias("circuit_id"),
#                                           col("name"),
#                                           col("ingestion_date"),
#                                           col("race_timestamp"),
#                                           )
# display(races_final_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Partitioning

# COMMAND ----------

# races_final.write.mode("overwrite").parquet("/mnt/formula1dl612/processed/races")
races_final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl612/processed/races

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races").head(5))

# COMMAND ----------

dbutils.notebook.exit("Success")
