-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl612/processed";

-- COMMAND ----------

DROP DATABASE if EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dl612/presentation";

-- COMMAND ----------


