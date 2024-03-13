# Databricks notebook source
# MAGIC %md
# MAGIC ###### We are able to access the global temporary view from more the one notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM global_temp.gv_race_results;

# COMMAND ----------


