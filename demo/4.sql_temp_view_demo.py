# Databricks notebook source
# MAGIC %md
# MAGIC ## Access dataframes using SWL
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cdells
# MAGIC 3. Access the view ftom Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration/"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# Output: AnalysisException: Temporary view 'v_race_results' already exists
# race_results_df.createTempView("v_race_results")

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select COUNT(1)
# MAGIC From v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_result_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_result_2019_df)

# COMMAND ----------

p_race_year = 2020

# COMMAND ----------

race_result_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")
display(race_result_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Global Temporary Views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL cdells
# MAGIC 3. Access the view ftom Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * \
    FROM global_temp.gv_race_results").show()

# COMMAND ----------


