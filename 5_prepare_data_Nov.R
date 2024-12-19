library(vroom)
library(sf)
library(ggplot2)
library(dplyr)
library(lubridate)

tracks <- vroom("data/Orben_Albatrosstracks_Nov18_2024.csv",col_select = -1)
head(tracks$datetime, n = 100)
distinct(tracks)
tracks <- tracks %>% mutate(start = datetime -12*3600, end = datetime + 12*3600)


tracks %>% select(start, datetime, end)


#quick explore ----#quick explore -end---
#------  
#study_data <- tibble(
#  "study" = c("MIAT16", "MIAT19", "MIAT22", "MIAT23", "MIAT24"),
#  "start_date" = c("2016-01-21", "2019-01-31", "2021-12-23", "2022-12-5", "2024-01-12"),
#  "end_date" = c("2016-02-06", "2019-02-21", "2022-02-20", "2023-06-21", "2024-02-07"),
#  "nLocs" = c(19414, 24681, 123424, 264005, 109394),
#  "nBirds" = c(35, 50, 80, 137, 75)
#)
#study_data
#-----

#upload data ----
tracks_sf <- sf::st_as_sf(tracks, coords = c("lon", "lat"))
tracks_sf <- tracks_sf %>% st_set_crs(4326)
count(tracks, study)
count(tracks, species)
#-----save and upload to bigquery
shape_wkt <- tracks_sf %>%
  dplyr::mutate(wkt = st_as_text(geometry)) %>%
  sf::st_drop_geometry(.)
shape_wkt$wkt
distinct(shape_wkt, species, BirdID, start, end) %>% arrange(BirdID)
bigrquery::bq_auth()
2
project_id <-  "world-fishing-827"
dataset_id <- "scratch_andrea_ttl100"

# won´t upload the one with shape_wkt
bigrquery::bq_table_upload(x = bigrquery::bq_table(project = project_id,
                                                   dataset = dataset_id,
                                                   table = 'orben_tracks_Nov_no_wkt'),
                           values = tracks) 

# how many dates
unique(lubridate::date(tracks$datetime))
