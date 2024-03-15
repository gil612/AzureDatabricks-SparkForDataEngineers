# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StructField, StringType

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("stop", IntegerType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)
                                        ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source))
display(lap_times_renamed_df)

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")
overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
