# Databricks notebook source
# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/COVID')

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    print(files)

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    if files.name.endswith("/"):
        print(files.name)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('ls')

# COMMAND ----------


