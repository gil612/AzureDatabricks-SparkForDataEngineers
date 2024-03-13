-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl612/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

SELECT 
    *
FROM
    f1_processed.circuits;


-- COMMAND ----------

drop table f1_processed.circuits;

-- COMMAND ----------

DROP DATABASE f1_processed;

-- COMMAND ----------


