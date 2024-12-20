library(bigrquery)
library(dplyr)

bigrquery::bq_auth()
2
project_id <- "world-fishing-827"
dataset <- 'scratch_andrea_ttl100'


# combining dates 3 dates per ssvid: the day before, the day of the encounter and the day after
dates_query <- "
SELECT DISTINCT ssvid, date 
FROM (
SELECT ssvid, date 
FROM `scratch_andrea_ttl100.bird_encounters` 
UNION ALL
SELECT ssvid,
DATE_ADD(date, interval 1 day) 
FROM
`scratch_andrea_ttl100.bird_encounters` 
UNION ALL 
SELECT ssvid, 
DATE_SUB(date, interval 1 day) 
FROM `scratch_andrea_ttl100.bird_encounters`)
"
#we have a single vector of 162 dates
dates_q <- bq_project_query(project_id, dates_query)
dates <- bq_table_download(dates_q)
dates <- dates %>% arrange(ssvid, date)

count(distinct(dates, date)) #163 yay

DATES <- unique(dates$date)
DATES <- sort(DATES) %>% lubridate::date()
head(DATES)
all(DATES %in% dates_original)
all(dates_original %in% DATES)

dates
dates %>%
  count(ssvid) %>%
  arrange(desc(n))

# query for one day
#notice the {.x} to loop later
query_tracks <- readr::read_file("queries/08_query_tracks.sql")
# we loop through dates 
create_table <-  function(.x) {
  # create the query
  query <- glue::glue(query_tracks)
  # name the table to create a partitioned table by date
  table_name <- paste0(project_id, ".", dataset, ".orben_tracks_results_Nov$",format.Date(.x, "%Y%m%d"))
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
# execute for one (the missing one)
create_table("2024-06-09")
2,690,871 to 2694178

# execute the query in parallel -
-
library(furrr)
parallel::detectCores()
plan(multisession, workers = parallel::detectCores() - 2)
furrr::future_map(DATES, ~create_table(.x))
plan(sequential)

# download the large table. this has all positions but is not organized by **Encounter**
orben_tracks <- bq_table_download(paste(project_id, dataset, "orben_tracks_results_Nov", sep = "."))
readr::write_csv(orben_tracks, "results/orben_results_Nov.csv")
#orben_tracks  <- readr::read_csv("results/orben_results_Nov.csv")
#2694168 lines
count(orben_tracks, ssvid)
count(orben_tracks, date)

DATES
all(orben_tracks$date %in% DATES)
all(DATES %in% orben_tracks$date)
