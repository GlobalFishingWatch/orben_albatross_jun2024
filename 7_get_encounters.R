library(bigrquery)
library(dplyr)

bigrquery::bq_auth()
2
project_id <- "world-fishing-827"
dataset <- 'scratch_andrea_ttl100'
plan
table_name <- paste(project_id, dataset, "bird_encounters", sep = ".")
table_name
# get_encounters_query
encounters_query <- readr::read_file("queries/07_get_encounters_Nov.sql") 

bq_project_query(project_id,
                 query = encounters_query,
                 destination_table = table_name)

#creates "world-fishing-827.scratch_andrea_ttl100.bird_encounters"