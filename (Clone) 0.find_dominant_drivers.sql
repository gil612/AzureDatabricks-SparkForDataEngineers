-- Databricks notebook source
SELECT  driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING count(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

SELECT  driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
  GROUP BY driver_name
  HAVING count(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

SELECT  team_name, driver_name, COUNT(1) AS total_races FROM f1_presentation.calculated_race_results GROUP BY team_name, driver_name  ORDER BY driver_name, total_races DESC

-- COMMAND ----------


