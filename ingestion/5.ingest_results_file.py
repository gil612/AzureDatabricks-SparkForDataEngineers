# Databricks notebook source
# spark.read.json("/mnt/formula1dl612/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# %sql
# SELECT raceID, count(1)
# FROM results_cutover
# GROUP BY raceId
# ORDER BY raceId DESC;

# COMMAND ----------

# spark.read.json("/mnt/formula1dl612/raw/2021-03-28/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# %sql
# SELECT raceID, count(1)
# FROM results_w1
# GROUP BY raceId
# ORDER BY raceId DESC;

# COMMAND ----------

# spark.read.json("/mnt/formula1dl612/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# %sql
# SELECT raceID, count(1)
# FROM results_w2
# GROUP BY raceId
# ORDER BY raceId DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StructField, DateType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", FloatType(), True),
                                      StructField("statusId", StringType(), True)
                                      ])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import lit, col

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_Lap_speed") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_final_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_dropped_df = results_final_df.drop(col('statusId'))
display(results_dropped_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Method 1

# COMMAND ----------

# Be careful!!
# collect() takes all the data and put it into the driver node's memory.
# We should collect small amount of data

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Method 2

# COMMAND ----------

def re_arrange_partition_column (input_df,  partition_column):

    column_list = []
    for column_name in results_final_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column) 

    print(column_list)

    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

# output_df = re_arrange_partition_column(results_final_df, 'race_id')

# COMMAND ----------

def overwrite_partition (input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")   

# COMMAND ----------

overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------


