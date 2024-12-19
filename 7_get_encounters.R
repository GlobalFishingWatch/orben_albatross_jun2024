aoi <- "SELECT 
   ST_BUFFER(ST_CONVEXHULL(ST_UNION_AGG(ST_GEOGPOINT(lon, lat))), 30000) AS aoi 
   FROM
`scratch_andrea_ttl100.orben_tracks_Nov_no_wkt`"
# that trial to make a buffer was indeed too slow

library(bigrquery)
library(dplyr)

bigrquery::bq_auth()
2
project_id <- "world-fishing-827"
dataset <- 'scratch_andrea_ttl100'
plan
table_name <- paste(project_id, dataset, "bird_encounters", sep = ".")
encounters_query <- readr::read_file("queries/07_get_encounters_Nov.sql")

bq_project_query(project_id,
                 query = encounters_query,
                 destination_table = table_name)

# get_encounters_query
