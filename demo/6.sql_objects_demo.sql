-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### CREATE DATABASE
-- MAGIC https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-ddl-create-database.html

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df  = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
SHOW TABLES;

-- COMMAND ----------

DESC race_results_python;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
  WHere race_year = 2020;

-- COMMAND ----------

CREATE Table demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
  WHere race_year = 2020;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %python
-- MAGIC presentation_folder_path

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

Create Table demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula1dl612/presentation/race_results_ext_sql"

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

SELECT count(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC
-- MAGIC ###### Learning Objectives
-- MAGIC 1. Create Global Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

DROP TABLE v_race_results;

-- COMMAND ----------

CREATE TEMP VIEW v_race_results
AS
SELECT *
FROM demo.race_results_python
Where race_year = 2018;

-- COMMAND ----------

select * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
FROM demo.race_results_python
Where race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- Permanent = Even if you detach from the Databricks cluster or terminate the cluster and restart or from another, you will still be able to use it
CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
FROM demo.race_results_python
Where race_year = 2000;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

select * from demo.pv_race_results;

-- COMMAND ----------


