# Databricks notebook source
# MAGIC %md
# MAGIC # Notebooks Introduction
# MAGIC ## UI Introduction
# MAGIC ## Magic Commands
# MAGIC * %python
# MAGIC * %sql
# MAGIC * %scala
# MAGIC * %md

# COMMAND ----------



# COMMAND ----------

message = "Welcome to the the DB Notebook Ex"

# COMMAND ----------

print(message)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello"

# COMMAND ----------

# MAGIC %scala
# MAGIC val msg = "Hello"
# MAGIC print(msg)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

# File Systems Utilities allows us to access databricks FS from a notebook and you can use various FS level
# Secret Utilities allow us to get secret values from secrets, which are stored in secret scopes backed by Databricks

# COMMAND ----------


