orben_tracks
vessel_positions_one_csv 
all(
  orben_tracks$ssvid 
  %in%
    vessel_positions_one_csv$ssvid
)
#use orben_results to goe identity data for the mmsis. 
#I ran identity_query.sql interactively from bigquery. 
identity_query <- readr::read_file("queries/09_Nov_identity_query.sql")

library(bigrquery)
project_id <-  "world-fishing-827"
dataset_id <- "scratch_andrea_ttl100"
table <- "orben_identity_results_no_noisy_Nov"
table_name = paste(project_id, dataset_id, table, sep = ".")
# do the query
bq_project_query(project_id,
                 query = identity_query,
                 destination_table = table_name
                 )


download <- bq_table_download(table_name)
readr::write_csv(download, "results/Nov_identity_results_nogear_nonoisy.csv")
download2 <- bq_table_download(table_name)
readr::write_csv(download2, "results/Nov_identity_results_yesgear_nonoisy.csv")
