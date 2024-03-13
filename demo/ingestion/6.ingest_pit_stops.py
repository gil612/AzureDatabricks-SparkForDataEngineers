# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json
# MAGIC #### Multi line json

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StructField, DateType, StringType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields = [
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                                    ])

# COMMAND ----------

pit_stops_df = spark.read \
.option("multiline", True) \
.schema(pit_stops_schema) \
.json(f"{raw_folder_path}/pit_stops.json")
display(pit_stops_df)

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source))
display(pit_stops_renamed_df)

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_renamed_df)

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")
