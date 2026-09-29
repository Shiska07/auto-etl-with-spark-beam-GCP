--  What date range does the data cover? (Make sure 2022 Q1 exists)
SELECT MIN(trip_start_timestamp) AS first_trip,
       MAX(trip_start_timestamp) AS last_trip
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`;

--  How do people pay, and which payment types have tips recorded?
SELECT payment_type,
       COUNT(*) AS trips,
       ROUND(AVG(tips), 2) AS avg_tip,
       ROUND(COUNTIF(tips > 0) / COUNT(*), 3) AS share_with_tip
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE trip_start_timestamp >= '2022-01-01' AND trip_start_timestamp < '2022-04-01'
GROUP BY payment_type
ORDER BY trips DESC;

--  How dirty is the data?
SELECT
  COUNT(*)                                            AS total,
  COUNTIF(trip_seconds IS NULL OR trip_seconds < 60)  AS bad_duration,
  COUNTIF(trip_miles IS NULL OR trip_miles < 0.1)     AS bad_distance,
  COUNTIF(fare IS NULL OR fare <= 0)                  AS bad_fare,
  COUNTIF(pickup_community_area IS NULL)              AS no_pickup_area,
  COUNTIF(dropoff_community_area IS NULL)             AS no_dropoff_area,
  COUNT(*) - COUNT(DISTINCT unique_key)               AS duplicates
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE trip_start_timestamp >= '2022-01-01' AND trip_start_timestamp < '2022-04-01';

--  For card payments only: what does the tip percentage look like?
SELECT
  ROUND(SAFE_DIVIDE(tips, fare), 1) AS tip_pct_bucket,
  COUNT(*) AS trips
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE trip_start_timestamp >= '2022-01-01' AND trip_start_timestamp < '2022-04-01'
  AND payment_type = 'Credit Card' AND fare > 0
GROUP BY tip_pct_bucket
ORDER BY tip_pct_bucket
LIMIT 20;

