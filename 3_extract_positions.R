library(vroom)
library(dplyr)
vessel_pos <- vroom("orben_results.csv")
#encounters <- vroom("results/encounter_info.csv") 
encounters <- encounters %>% mutate(bird_id = paste(species, BirdID, sep = "_"))
bird_pos <- vroom("data/Orben_tracks_April5_2024.csv", col_select = -1) %>% 
  mutate(date = lubridate::date(datetime)) %>% 
  mutate(bird_id = paste(species, BirdID, sep = "_"))


bird_encounter <- encounters %>% 
  right_join(bird_pos, relationship = "many-to-many") 
f = bird_encounter$EncounterID
length(unique(f))
list_birdID <- split(bird_encounter, f)
bird_tracks_EncounterID <- split(bird_encounter, f)
readr::write_csv(bird_encounter, "results/all_bird_positions.csv")
#list bird puede ser esa pero 

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
                   glue::glue("results/Encounter{i}_vessel_tracks_encounter.csv"))
}
options(future.globals.maxSize)

options(future.globals.maxSize = 90 * 1024 ^ 3) # for 50 Gb RAM
library(furrr)
plan(multisession, workers = 20)
plan("default")

furrr::future_map(seq_along(1:nrow(encounters)),
                  ~parse_results(.x))
all_vessel_positions <- .Last.value 
names(all_vessel_positions) <- paste("Encounter",1:7399)
vessel_positions_one_csv <- bind_rows(all_vessel_positions)
readr::write_csv(vessel_positions_one_csv, "results/all_vessel_positions.csv")

#upload to bigquery
project_id <-  "world-fishing-827"
dataset_id <- "scratch_andrea_ttl100"
bigrquery::bq_table_upload(x = bigrquery::bq_table(project = project_id,
                                                   dataset = dataset_id,
                                                   table = 'orben_vessel_per_encounter'),
                           values = vessel_positions_one_csv,
                           configuration = list(
                             query = list(
                               integer_partitioning = 
                                 list(field = "EncounterID",
                                      type = "INTEGER")))) 



f = vessel_positions_one_csv$EncounterID
length(unique(f))

all_bird_positions
vessel_positions_one_csv %>% select(-nnet_score) %>% readr::write_csv("all_vessel_positions.csv")
