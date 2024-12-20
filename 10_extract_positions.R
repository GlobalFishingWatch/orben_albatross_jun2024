# all positions
vessel_pos <- orben_tracks
encounters
#encounter info
encounters <- encounters %>% mutate(bird_id = paste(species, BirdID, sep = "_"))
#original tracks
bird_pos <- tracks %>% 
  mutate(date = lubridate::date(datetime)) %>% 
  mutate(bird_id = paste(species, BirdID, sep = "_"))

# join
bird_encounter <- encounters %>% 
  right_join(bird_pos, relationship = "many-to-many") 

# I decided to save in a single table
readr::write_csv(bird_encounter, "results/all_bird_positions_Nov.csv")

# Same function than in script 2, without the plots and reprojections and without the append! Saving in a separate file per encounter
parse_results <- function(i) {
  ssvid_oi <- encounters$ssvid[i]
  date_oi <- encounters$date[i]
  bird_oi <- encounters$BirdID[i]
  
  ## dates associated with that ssvid
  vessel_positions <- vessel_pos %>% 
    filter(ssvid %in% ssvid_oi) %>% 
    filter(date >= date_oi - 1  & date <= date_oi + 1) %>% 
    mutate(EncounterID = i) %>% 
    relocate(EncounterID)
  
readr::write_csv(vessel_positions, 
                   glue::glue("results/Nov_Encounter{i}_vessel_tracks_encounter.csv"))
}
###  Run the function

options(future.globals.maxSize = 90 * 1024 ^ 3) # for 50 Gb RAM
library(furrr)
plan(multisession, workers = 20)

all_vessel_positions <- furrr::future_map(seq_along(1:nrow(encounters)),
                  ~parse_results(.x))

#name the list to bind rows 
plan("default")
names(all_vessel_positions) <- paste0("Encounter_", seq_along(all_vessel_positions))
vessel_positions_one_csv <- bind_rows(all_vessel_positions)

# save directly in one csv
vessel_positions_one_csv %>% select(-nnet_score) %>% readr::write_csv("results/Nov_all_vessel_positions.csv")

#to upload to bigquery but not sure it's needed
project_id <-  "world-fishing-827"
dataset_id <- "scratch_andrea_ttl100"
bigrquery::bq_table_upload(x = bigrquery::bq_table(project = project_id,
                                                   dataset = dataset_id,
                                                   table = 'orben_vessel_per_encounter'),
                           values = vessel_positions_one_csv,
                          configuration = list(
                            query = list(
                              time_partitioning = 
                                list(field = "date",
                                     type = "DAY"))))