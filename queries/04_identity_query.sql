WITH
  res AS (
  SELECT
    ssvid,
    EXTRACT(year
    FROM
      date) AS year
  FROM
    `world-fishing-827.scratch_andrea_ttl100.orben_tracks_results`),
  year_ssvid AS(
  SELECT
    DISTINCT ssvid,
    year
  FROM
    res),
  identity AS (
  SELECT
  --vessel_id,
    ssvid,
    shipname,
    callsign,
    imo,
    year,
    gfw_best_flag,
    best_vessel_class,
    prod_shiptype,
    prod_geartype 
    #overlap_hours_multinames

  FROM
    `world-fishing-827.pipe_ais_v3_published.product_vessel_info_summary_v20240401` 
     WHERE prod_shiptype != "gear" #commenting this to generate no_filter, uncommenting for filtered
     AND noisy_vessel = FALSE #commenting this to generate no_filter, uncommenting for filtered
   
     )
SELECT
  DISTINCT *
FROM
  year_ssvid
JOIN
  identity
USING
  (
    ssvid,
   year
    )
ORDER BY
  ssvid,
  year