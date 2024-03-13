# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# Two approaches
races_filtered_df = races_df.filter("race_year = 2019")
races_filtered_df = races_df.filter(races_df["race_year"] == 2019)
display(races_filtered_df)

# COMMAND ----------

races_filtered_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))
display(races_filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Demo

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name")
display(circuits_df)

# COMMAND ----------

races_df = races_df.filter("race_year = 2019").withColumnRenamed("name", "race_name")
display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select("circuit_name").show()

# COMMAND ----------

# Left Outer Join
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.filter("circuit_id < 70")\
.withColumnRenamed("name", "circuit_name")    
display(circuits_df)

# COMMAND ----------

# Left join.
# Output: 69 entries of circuits_df, 69 - -21 = 48 of them don't don't have number of rounds
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_id,circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
display(race_circuits_df)

# COMMAND ----------

# Right join.
# Output: All 21 entries of races_df. Austrian, Russian and Azerbaijan will be marked as null since we've cut the table
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
display(race_circuits_df)

# COMMAND ----------

# Semi.
# Output: We only get the entries from circuit_id
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Anti

# COMMAND ----------

# Anti.
# Output: We only get the entries from circuit_id, which don't have entries in races_id (opposite of semi join)
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")
display(race_circuits_df)

# COMMAND ----------

# Anti.
# Output: We only get the entries from races_id, which don't have entries in circuit_id.
# Azerbaijan, Russian, Austrian
race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")
display(race_circuits_df)

# COMMAND ----------

# crossJoin - Cartesian Product, 69*21 = 1449
race_circuits_df = races_df.crossJoin(circuits_df)
display(race_circuits_df)

# COMMAND ----------


