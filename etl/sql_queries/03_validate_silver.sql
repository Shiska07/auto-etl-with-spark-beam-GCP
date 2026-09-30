-- 03_validate_silver.sql

-- This step validates the data cleaning and transformation process and whether the clean data is written
-- into the right location. the data validated here will be used by Spark fro feature engineering.

-- The external table must exist in the project (i.e. create a table using the cleaned .parquet file) before running these checks.
-- Run this once in your project using BigQuery (or again if the silver folder layout changes):
--
-- CREATE OR REPLACE EXTERNAL TABLE `chicago-taxi-tips-project.taxi_lake.silver_trips`
-- WITH PARTITION COLUMNS
-- OPTIONS (
--   format = 'PARQUET',
--   uris = ['gs://chicago-taxi-tips-project-lake/silver/trips/*'],
--   hive_partition_uri_prefix = 'gs://chicago-taxi-tips-project-lake/silver/trips'
-- );
--
-- "WITH PARTITION COLUMNS" turns the folder name run_id=... into a column,
-- so each run can be checked on its own. 
--
-- To list the available runs:
--   SELECT DISTINCT run_id FROM `chicago-taxi-tips-project.taxi_lake.silver_trips` ORDER BY run_id;


-- Set the run to validate (printed by the pipeline as ">> Silver: ...")
DECLARE target_run STRING DEFAULT '2022-01-01_2022-01-08_20260930-153000';

-- Row count vs. unique keys (should be equal: no duplicates)
SELECT COUNT(*) AS n_rows, COUNT(DISTINCT unique_key) AS n_keys
FROM `chicago-taxi-tips-project.taxi_lake.silver_trips`
WHERE run_id = target_run;

-- Nulls in columns you care about (should all be 0)
SELECT
  COUNTIF(trip_start_ts IS NULL) AS null_start,
  COUNTIF(fare IS NULL) AS null_fare,
  COUNTIF(tips IS NULL) AS null_tips
FROM `chicago-taxi-tips-project.taxi_lake.silver_trips`


