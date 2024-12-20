WITH boats AS

(SELECT
ssvid,
ST_GEOGPOINT(lon_center,lat_center) AS pos_ais,
time 
FROM `scratch_andrea_ttl100.orben_AIS_interpolate`),

birds AS 

(SELECT
species,
BirdID,
ST_GEOGPOINT(lon_center,lat_center) AS pos_bird,
time 
FROM `scratch_andrea_ttl100.orben_interpolate`),


positions_compared AS 
(SELECT
ssvid,
species,
BirdID,
ST_DISTANCE(pos_ais, pos_bird)/1000 AS distance_km,
time
FROM birds
JOIN
boats
USING(time)
WHERE ST_DISTANCE(pos_ais, pos_bird) < 30000)


SELECT DATE(time) AS date,
ssvid,
species,
BirdID,
AVG(distance_km) AS distance_km,
COUNT(*)*5 AS minutes
FROM positions_compared
GROUP BY date, ssvid, species, BirdID 