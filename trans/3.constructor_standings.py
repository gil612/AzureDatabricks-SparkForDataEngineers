# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce constructor standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

path = f"{presentation_folder_path}/race_results"
race_results_df = column_to_list(path, 'race_year',v_file_date)
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))
display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

# display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')
