library(fishwatchr)
library(ggplot2)
library(sf)
library(dplyr)
library(bigrquery)

bigrquery::bq_auth()
2
project_id <- "world-fishing-827"
dataset <- 'scratch_andrea_ttl100'
encounters <- 'scratch_david.bird_encounters'

#download encounters by David
encounters <- bq_table_download(paste(project_id, encounters, sep = "."))
encounters <- encounters %>% 
  arrange(date, BirdID, ssvid) %>% 
#Encounter ID will be the common variable
    mutate(EncounterID = row_number()) %>% 
  relocate(EncounterID)

readr::write_csv(encounters, "results/encounter_info.csv") #I sent this

# Filter for all ssvid and dates 
## each ssvid

#changing for all
#orben_tracks <- readr::read_csv("results/orben_results.csv")
orben_sf <- orben_tracks %>%
  st_as_sf(coords = c("lon", "lat")) %>% 
  st_set_crs(4326)
vessel_proj <- orben_sf %>% 
  st_transform(3172) #to plot and to have a buffer in km!
tracks_sf <- tracks_sf %>% 
  mutate(date = lubridate::date(datetime))
track_proj <- tracks_sf %>% 
  st_transform(3172)


#all_encounters <- list()

# A function to parse the results and plot for each encounter

parse_results <- function(i) {
  ssvid_oi <- encounters$ssvid[i]
  date_oi <- encounters$date[i]
  bird_oi <- encounters$BirdID[i]

## dates associated with that ssvid
vessel_positions <- vessel_proj %>% 
  filter(ssvid %in% ssvid_oi) %>% 
  filter(date >= date_oi - 1  & date <= date_oi + 1) %>% 
  mutate(EncounterID = i) %>% 
  relocate(EncounterID)

encounter_bird_track <- track_proj %>% 
  filter(BirdID == bird_oi) %>% 
  filter(date == date_oi) %>% 
  mutate(EncounterID = i) %>% 
  relocate(EncounterID)

#I was writing each dataset and appending. 
# appending is slow, don't do it.
readr::write_csv(st_drop_geometry(encounter_bird_track), 
                  "results/bird_tracks_encounter.csv", append = T)
readr::write_csv(st_drop_geometry(vessel_positions), 
                  "results/vessel_tracks_encounter.csv", append = T)
# the buffer 30km 
buf <- st_buffer(encounter_bird_track, dist = 30000)

vessel_positions %>%
  ggplot(aes(col = date)) +
  geom_sf() + 
  theme_minimal() +
  geom_sf(data = buf, col = "grey90", fill = NA) +
  geom_sf(data = encounter_bird_track, col = "grey70") +
  labs(title = paste(date_oi, "SSVID =", ssvid_oi, "BirdID =", bird_oi))
ggsave(glue::glue("figs/encounter_{i}.png"), width = 8, height = 5)
  
}
#purrr::map(seq_along(1:nrow(encounters)),
#~parse_results(.x))


#options(future.globals.maxSize = 90 * 1024 ^ 3) # for 50 Gb RAM
#plan(multisession, workers = 20)
#furrr::future_map(seq_along(1:nrow(encounters)),
#~parse_results(.x))
#too slow