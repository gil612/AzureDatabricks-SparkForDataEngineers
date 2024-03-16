# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def column_to_list (path, column_name, v_file_date):
    # Use collect() to get a list
    row_list = spark.read.parquet(path) \
    .filter(f"file_date = '{v_file_date}'") \
    .select(column_name) \
    .distinct()\
    .collect()
    column_value_list = [row[column_name] for row in row_list]
    output_df = spark.read.parquet(path).filter(col (column_name).isin(column_value_list))
    return output_df

# COMMAND ----------

def f1(path):
    return spark.read.parquet(path)


# COMMAND ----------


