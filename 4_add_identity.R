orben_tracks  <- readr::read_csv("orben_results.csv")
vessel_positions_one_csv <- readr::read_csv("all_vessel_positions.csv")
all(
  orben_tracks$ssvid 
  %in%
    vessel_positions_one_csv$ssvid
  )
#use orben_results to goe identity data for the mmsis. 
#I ran identity_query.sql interactively from bigquery. 

library(bigrquery)
project_id <-  "world-fishing-827"
dataset_id <- "scratch_andrea_ttl100"
table <- "orben_identity_results_no_filter"
download <- bq_table_download(paste(project_id, dataset_id, table, sep = "."))
2    
readr::write_csv(download, "results/identity_results_nofilter_noisy.csv")

table <- "orben_identity_nogear"
download <- bq_table_download(paste(project_id, dataset_id, table, sep = "."))
readr::write_csv(download, "results/identity_results_nogear.csv")
download
