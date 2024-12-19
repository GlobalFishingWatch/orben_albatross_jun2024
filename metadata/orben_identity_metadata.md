This table was created by Andrea Sánchez-Tapia from the `pipe_ais_v3_published.product_vessel_info_summary_v20240401` table and the list of ssvids in `world-fishing-827.scratch_andrea_ttl100.orben_tracks_results`

- ssvid: Source Specific Vessel Id (ssvid), synonymous with Maritime Mobile Service Identities (mmsi). Defined by the FCC as a “nine-digit numbers used by maritime digital selective calling (DSC), automatic identification systems (AIS) and certain other equipment to uniquely identify a ship or a coast radio station.
- year: year
- shipname: shipname most commonly broadcast on AIS
- callsign:callsign most commonly broadcast on AIS
- imo: imo most commonly broadcast on AIS
- gfw_best_flag: flag state (ISO3) assigned to the vessel by GFW after considering information from AIS, registries, and our vessel classification machine learning model.
- best_vessel_class: vessel class assigned to the vessel by GFW after considering information from registries and our vessel classification machine learning model. Best vessel class may display NULL value in certain cases: 1) no data: If the neural net (nnet) doesn’t have enough data to make a prediction and there’s no registry info. 2) nnet-registry disagreement: The nnet and registry disagree regarding if the vessel is fishing vs non-fishing. 3) registry disagreement: The vessel has multiple known gear types from registries
- prod_shiptype: the ship type displayed in products (referred to in products as Vessel Class). This is useful to understand which vessel class was picked by GFW after examination and analysis to display in the products, in case the vessel is noisy and best_vessel_class does not provide conclusive information. 
- prod_geartype: The gear type displayed in products
- noisy_vessel: TRUE if a vessel is offsetting (this vessel has been seen with an offset position at some point between 2012 and 2019) OR has more than 24 hours of overlapping segments between more than one identity

