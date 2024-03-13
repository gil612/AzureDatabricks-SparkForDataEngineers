-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
circuitRef SRING,
name SRING,
location SRING,
country SRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url SRING
)
USING csv
-- Path is needed. header is optional and we have a heder in the table
OPTIONS (path "/mnt/formula1dl612/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name SRING,
date DATE,
time SRING,
url SRING
)
USING csv
OPTIONS (path "/mnt/formula1dl612/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constructors_schema = "constructorId INT\\n, constructorRef SRING, name SRING, nationality SRING, url SRING"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constructors_schema

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef SRING,
name SRING,
nationality SRING,
url SRING
)
USING json
OPTIONS (path "/mnt/formula1dl612/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename STRING, surname STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula1dl612/raw/drivers.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
USING json
OPTIONS (path "/mnt/formula1dl612/raw/results.json", header true)

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING json
OPTIONS (path "/mnt/formula1dl612/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1dl612/raw/lap_times")


-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING json
OPTIONS (path "/mnt/formula1dl612/raw/qualifying", multiLine true)


-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------


