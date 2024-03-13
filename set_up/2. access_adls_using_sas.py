# Databricks notebook source
# MAGIC %md
# MAGIC * Access ADL using Access Keys

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl612.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl612.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl612.dfs.core.windows.net", formula1dl_account_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl612.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl612.dfs.core.windows.net"))

# COMMAND ----------

display( spark.read.csv("abfss://demo@formula1dl612.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


