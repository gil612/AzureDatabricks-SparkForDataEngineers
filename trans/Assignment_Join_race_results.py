# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Read dataframes : Constructors, Circuits, Races, Results, Drivers
# MAGIC ##### 2. race_circuits_df: Join on (race_df, circuit_df) circuit_id: race_circuits_df
# MAGIC ##### 3. race_results_df: Joins on (race_circuits_df, results_df): Race_id (race_circuits), driver_id (drivers_df), constructor_id (contructors_df) 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

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
# races_df_fil= races_df.filter(races_df.race_year == 2020)
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Results

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time", "race_time") 
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

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)
display(race_results_df)                            

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position") \
                          .withColumn("created_date", current_timestamp())
display(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------



# COMMAND ----------

display(results_df.filter(results_df["race_id"] == 1044).select("points"))

# COMMAND ----------

results_df.filter(results_df["race_id"] == 1044).select("points")

# COMMAND ----------

df1 = results_df.filter(results_df["race_id"] == 1044)

# COMMAND ----------

your_max_value = df1.agg({"points": "max"}).collect()[0][0]

# COMMAND ----------

your_max_value

# COMMAND ----------

races_df_fil.count()

# COMMAND ----------

races_df_fil.select("race_id").collect()[9][0]

# COMMAND ----------

for i in range(17):
    id = races_df_fil.select("race_id").collect()[i][0]
    max_points = results_df.filter(results_df["race_id"] == id).agg({"fastest_lap_time": "max"}).collect()[0][0]
    print(id, max_points)

# COMMAND ----------


