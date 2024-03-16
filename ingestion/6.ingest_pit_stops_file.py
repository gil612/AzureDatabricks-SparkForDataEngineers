# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json
# MAGIC #### Multi line json

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.pit_stops;

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

from pyspark.sql.types import StructType, IntegerType, StructField, DateType, StringType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields = [
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                                    ])

# COMMAND ----------

pit_stops_df = spark.read \
.option("multiline", True) \
.schema(pit_stops_schema) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")
display(pit_stops_df)

# COMMAND ----------

pit_stops_ingest_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

display(pit_stops_ingest_df)

# COMMAND ----------

from pyspark.sql.functions import lit, col

# COMMAND ----------

pit_stops_renamed_df = pit_stops_ingest_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))
display(pit_stops_renamed_df)

# COMMAND ----------

pit_stops_renamed_df.schema.names

# COMMAND ----------

pit_stops_renamed_df.printSchema()

# COMMAND ----------

# def re_arrange_partition_column(input_df, partition_column):
#   column_list = []
#   for column_name in input_df.schema.names:
#     if column_name != partition_column:
#       column_list.append(column_name)
#   column_list.append(partition_column)
#   output_df = input_df.select(column_list)
#   return output_df

# COMMAND ----------

# def overwrite_partition(input_df, db_name, table_name, partition_column):
#   output_df = re_arrange_partition_column(input_df, partition_column)
#   spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
#   if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#     output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#   else:
#     output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# partition_column = 'race_id'
# column_list = []
# for column_name in pit_stops_renamed_df.schema.names:
#     if column_name != partition_column:
#         column_list.append(column_name)
# column_list.append(partition_column)
# output_df = pit_stops_renamed_df.select(column_list)
# display(output_df)

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# db_name = 'f1_processed'
# table_name = 'pit_stops'

# if (spark._jsparkSession.catalog().tableExists('f1_processed.pit_stops')):
#     output_df.write.mode("overwrite").insertInto('f1_processed.pit_stops')
# else:
#     output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

# pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
overwrite_partition(pit_stops_renamed_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")
