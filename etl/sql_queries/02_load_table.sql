
--- Query used to load raw data for intial model training
SELECT
  unique_key,
  taxi_id,
  trip_start_timestamp,
  trip_seconds,
  trip_miles,
  pickup_community_area,
  dropoff_community_area,
  fare,
  tips,
  tolls,
  extras,
  payment_type,
  company
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE trip_start_timestamp >= TIMESTAMP('{start_date}')
  AND trip_start_timestamp <  TIMESTAMP('{end_date}')
