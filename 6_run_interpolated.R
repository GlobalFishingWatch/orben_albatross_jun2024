library(bigrquery)
library(dplyr)

bigrquery::bq_auth()
2
project_id <- "world-fishing-827"
dataset <- 'scratch_andrea_ttl100'

#We have the new track file
tracks

# david was running interpolated for each date
# query for one day
#notice the {.x} to loop later
interpolated_albatross <- readr::read_file("queries/05_david_interpolated_positions_Nov.sql")
interpolated_AIS <- readr::read_file("queries/06_david_AIS_interpolated_Nov.sql")
interpolated <- interpolated_albatross

# a function to loop through dates 
create_table <-  function(.x) {
  # create the query
  query <- glue::glue(interpolated)
  # name the table to create a partitioned table by date
  table_name <- paste0(project_id, ".", dataset, ".orben_interpolate$",format.Date(.x, "%Y%m%d"))
  # do the query
  bq_project_query(project_id,
                   query = query,
                   destination_table = table_name,
                   configuration = list(
                     query = list(
                       time_partitioning = 
                         list(#field = "time",#error with field but partition ok
                              type = "DAY"))
                   )
  )
}
#orbencreate_table("2024-06-09")#test for one day creating a table interpolate_test #I ran this afterwards because we were missing that day
#two datasets in BQ, interpolate (larger) and albatross_intepolate, (smaller).
# will share with Rachael from the first and redo the loop in January

# execute the query in parallel across the list of dates
library(furrr)
library(dplyr)
parallel::detectCores()
plan(multisession, workers = parallel::detectCores() - 2)
DATES <- lubridate::date(tracks$datetime) %>% unique() %>% sort()
furrr::future_map(DATES, ~create_table(.x))
plan(sequential)

### AIS
interpolated <- interpolated_AIS
# a function to loop through dates 
create_table <-  function(.x) {
  # create the query
  query <- glue::glue(interpolated)
  # name the table to create a partitioned table by date
  table_name <- paste0(project_id, ".", dataset, ".orben_AIS_interpolate$",format.Date(.x, "%Y%m%d"))
  # do the query
  bq_project_query(project_id,
                   query = query,
                   destination_table = table_name,
                   configuration = list(
                     query = list(
                       time_partitioning = 
                         list(type = "DAY"))
                   )
  )
}
parallel::detectCores()
plan(multisession, workers = parallel::detectCores() - 2)
furrr::future_map(DATES, ~create_table(.x))
plan(sequential)
