library(bigrquery)
library(dplyr)

bigrquery::bq_auth()
2
project_id <- "world-fishing-827"
dataset <- 'scratch_andrea_ttl100'


# david was combining dates 3 dates per ssvid: the day before, the day of the encounter and the day after
dates_query <- "
SELECT DISTINCT ssvid, date 
FROM (
SELECT ssvid, date 
FROM `scratch_david.bird_encounters` 
UNION ALL
SELECT ssvid,
DATE_ADD(date, interval 1 day) 
FROM
`scratch_david.bird_encounters` 
UNION ALL 
SELECT ssvid, 
DATE_SUB(date, interval 1 day) 
FROM `scratch_david.bird_encounters`)
"
#we have a single vector of 302 dates
dates_q <- bq_project_query(project_id, dates_query)
dates <- bq_table_download(dates_q)
dates <- dates %>% arrange(ssvid, date)

count(distinct(dates, date)) #302
count(distinct(track_proj, date))
count(distinct(encounters, date))
DATES <- unique(dates$date)
DATES <- sort(DATES)
head(DATES)

dates %>%
  count(ssvid) %>%
  arrange(desc(n))

# query for one day
#notice the {.x} to loop later
query_tracks <- "
CREATE TEMP FUNCTION today() AS (timestamp('{.x}'));
WITH
encounters AS (
  SELECT DISTINCT 
  *
    FROM
  (SELECT ssvid, date FROM `scratch_david.bird_encounters` 
    UNION ALL
    SELECT ssvid,
    DATE_ADD(date, interval 1 day) 
    FROM `scratch_david.bird_encounters` 
    UNION ALL 
    SELECT ssvid, 
    DATE_SUB(date, interval 1 day) 
    FROM `scratch_david.bird_encounters`)
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
"
# we loop through dates 
create_table <-  function(.x) {
  # create the query
  query <- glue::glue(query_tracks)
  # name the table to create a partitioned table by date
  table_name <- paste0(project_id, ".", dataset, ".orben_tracks_results$",format.Date(.x, "%Y%m%d"))
  # do the query
  bq_project_query(project_id,
                   query = query,
                   destination_table = table_name,
                   configuration = list(
                     query = list(
                       time_partitioning = 
                         list(field = "date",
                              type = "DAY"))
                     )
                   )
}
# execute the query in parallel 
library(furrr)
parallel::detectCores()
plan(multisession, workers = parallel::detectCores() - 2)
furrr::future_map(DATES, ~create_table(.x))
plan(sequential)

# download the large table. this has all positions but is not organized by **Encounter**
orben_tracks <- bq_table_download(paste(project_id, dataset, "orben_tracks_results", sep = "."))
readr::write_csv(orben_tracks, "orben_results.csv")
orben_tracks  <- readr::read_csv("orben_results.csv")
#8238304 lines
count(orben_tracks, ssvid)
count(orben_tracks, date)

