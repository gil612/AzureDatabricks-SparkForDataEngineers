# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental Load
# MAGIC ##### 1. Amending race_results notebook
# MAGIC ##### 2. Add
# MAGIC %run "../includes/common_functions"
# MAGIC ##### 3. Add
# MAGIC dbutils.widgets.text("p_file_date", "2021-03-21")\
# MAGIC v_file_date = dbutils.widgets.get("p_file_date")
# MAGIC ##### 4. Find race years which the data is to be reprocessed
# MAGIC ##### 5. Run p_file_date 3 times (2021-03-21, 2021-03-28, 20021-04-18)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Produce driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Incremental Load] Find race years which the data is to be reprocessed

# COMMAND ----------

# Use collect() to get a list
race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") \
.select('race_year') \
.distinct()\
.collect()

# COMMAND ----------

race_results_list

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)
    

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col ("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

# COMMAND ----------

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.driver_standings;
