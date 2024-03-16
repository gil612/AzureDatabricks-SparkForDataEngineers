# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Read dataframes : Constructors, Circuits, Races, Results, Drivers
# MAGIC ##### 2. race_circuits_df: Join on (race_df, circuit_df) circuit_id: race_circuits_df
# MAGIC ##### 3. race_results_df: Joins on (race_circuits_df, results_df): Race_id (race_circuits), driver_id (drivers_df), constructor_id (contructors_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental load changes
# MAGIC ##### 1. Add
# MAGIC %run "../includes/common_functions"
# MAGIC ##### 2. Add
# MAGIC dbutils.widgets.text("p_file_date", "2021-03-21")\
# MAGIC v_file_date = dbutils.widgets.get("p_file_date")
# MAGIC
# MAGIC
# MAGIC ##### 3. results_df 
# MAGIC ###### &nbsp;a. Add a filter of a file date
# MAGIC .filter(f"file_date = '{v_file_date}'")
# MAGIC ###### &nbsp;b. change column name 'race_id' to 'result_race_id'
# MAGIC .withColumnRenamed("race_id", "result_race_id")
# MAGIC
# MAGIC ##### 4. race_results_df
# MAGIC ###### change results_df.race_id to results_df.result_race_id
# MAGIC
# MAGIC ##### 5. final_df
# MAGIC ###### add 'race_id' column to the selected columns
# MAGIC
# MAGIC ##### 6. Overwrite:
# MAGIC overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')
# MAGIC
# MAGIC ##### 7. Run p_file_date 3 times (2021-03-21, 2021-03-28, 20021-04-18)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Amendments for Incremental load of driver standings
# MAGIC ##### 1. DROP TABLE f1_presentation.race_results;
# MAGIC ##### 2. results_df: Rename "file_date" to "result_file_date"
# MAGIC .withColumnRenamed("file_date", "result_file_date")
# MAGIC ##### 3. final_df
# MAGIC ###### &nbsp;a. Add 'result_file_date' to the selected columns<ul>
# MAGIC ###### &nbsp;b. change column name 'result_file_date' to 'file_date'
# MAGIC .withColumnRenamed("result_file_date", "file_date")
# MAGIC ##### 4. Run p_file_date 3 times (2021-03-21, 2021-03-28, 20021-04-18)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Constructors

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team")
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Circuits

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Races

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp", "race_date")
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Results

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drivers

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 
display(drivers_df)

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)    
display(race_circuits_df)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)
display(race_results_df)                            

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("result_file_date", "file_date")
display(final_df)

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_year')

# COMMAND ----------

# %sql
# SELECT  * FROM f1_presentation.race_results;

# COMMAND ----------

# %sql
# DROP TABLE f1_presentation.race_results;
