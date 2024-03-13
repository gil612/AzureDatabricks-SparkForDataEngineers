# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder
# MAGIC #### Multi line json

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

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True)
                                    ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumn("data_source", lit(v_data_source))
display(qualifying_renamed_df)

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
