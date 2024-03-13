# Databricks notebook source
# MAGIC %md
# MAGIC * Access ADL using Access Keys

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl612.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl612.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl612.dfs.core.windows.net"))

# COMMAND ----------

display( spark.read.csv("abfss://demo@formula1dl612.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


