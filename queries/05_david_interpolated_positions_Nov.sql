# Copied from the original query by David and adapted to run for the new data

CREATE TEMP FUNCTION today() AS (timestamp('{.x}'));

CREATE TEMP FUNCTION weight_average_lons(lon float64, lon2 float64, timeto float64, timeto2 float64) AS 
( 
CASE 
WHEN lon - lon2 > 300 then ( (lon-360)*timeto2 + lon2*timeto)/(timeto+timeto2) 
WHEN lon - lon2 < -300 then ( (lon+360)*timeto2 + lon2*timeto)/(timeto+timeto2) 
ELSE (lon*timeto2 + lon2*timeto)/(timeto+timeto2) END );

CREATE TEMP FUNCTION reasonable_lon(lon float64) AS 
(CASE WHEN lon > 180 THEN lon - 360
WHEN lon < -180 THEN lon + 360
ELSE lon END
);

CREATE TEMP FUNCTION radians(x float64) AS (
  3.14159265359 * x / 180
);

WITH 
times_table AS (
SELECT * FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(today(), 
                                TIMESTAMP_ADD(today(), interval 24 * 60 - 1 minute),
                                INTERVAL 5 minute)) AS time
),


position_table AS 
(SELECT 
  study,
  species,
  BirdID,
  lat,
  lon,
  TIMESTAMP(datetime) AS timestamp
 FROM
`scratch_andrea_ttl100.orben_tracks_Nov_no_wkt`
WHERE DATE(datetime) = DATE(today())
),

leaded_positions AS (
SELECT
  study,
  species,
  BirdID,
  lat,
  LEAD(lat,1) OVER (PARTITION BY species,  BirdID ORDER BY timestamp) AS lat2,
  lon,
  LEAD(lon,1) OVER (PARTITION BY species, BirdID ORDER BY timestamp) AS lon2,
  timestamp,
  LEAD(timestamp,1) OVER (PARTITION BY species, BirdID ORDER BY timestamp) AS timestamp2,
FROM
position_table),


candidate_positions AS (
      SELECT
        study,
        species,
        BirdID,
        time,
        TIMESTAMP_DIFF(timestamp2,
          time,
          SECOND) + 1e-8 AS timeto2,
        TIMESTAMP_DIFF(time,
          timestamp,
          SECOND) + 1e-8 AS timeto,
        timestamp, timestamp2,
        lat, lat2,
        lon, lon2,

      FROM 
       leaded_positions
      CROSS JOIN
       times_table 
      WHERE
       timestamp <= time
       AND timestamp2 > time  
        ),
        interpolated_positions AS (
      SELECT
        (lat*timeto2 + lat2*timeto)/(timeto+timeto2) AS lat_center,
        reasonable_lon(weight_average_lons(lon, lon2, timeto, timeto2)) AS lon_center,
        *
      FROM candidate_positions)
SELECT 
* 
FROM
interpolated_positions
