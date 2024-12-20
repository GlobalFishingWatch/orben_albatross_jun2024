CREATE TEMP FUNCTION today() AS (timestamp('{.x}'));
WITH
encounters AS (
  SELECT DISTINCT 
  *
    FROM
  (SELECT ssvid, date FROM `scratch_andrea_ttl100.bird_encounters` 
    UNION ALL
    SELECT ssvid,
    DATE_ADD(date, interval 1 day) 
    FROM `scratch_andrea_ttl100.bird_encounters` 
    UNION ALL 
    SELECT ssvid, 
    DATE_SUB(date, interval 1 day) 
    FROM `scratch_andrea_ttl100.bird_encounters`)
  WHERE date = DATE(today())),

messages AS (
  SELECT
    ssvid, 
    lat,
    lon, 
    timestamp,
    speed_knots,
    heading,
    course,
    nnet_score,
    DATE(timestamp) AS date
  FROM
  `pipe_ais_v3_published.messages`
  WHERE DATE(timestamp) = DATE(today()) AND
  clean_segs),

messages_daily AS(
  SELECT *
    FROM encounters
  INNER JOIN messages
  USING (ssvid, date)
)

SELECT *
  FROM messages_daily
ORDER BY ssvid, date, timestamp