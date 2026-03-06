# rqutils

## Overview 

Greetings traveller! Feel free to roam, but use at your own risk!

This repo holds a number of opinionated script fragments regularly used without having put them into a package "yet".
This avoids adding a huge number of (potential) dependencies (primarily on my side).

Scripts are useful as we can "simulate" a package in any R-project by creating a DESCRIPTION file and using devtools::load_all() to load the scripts (and keeping the environment list clean(er)).
The scripts also represent the latest versions ... as these fragments developed over time ... got some level of generalisation, etc.

Opinionated := they work for me and my workflow. Thus, they may inform you (dear non-RQ reader), but there is not guarantee that this works in your environment. Nonetheless, these fragments are not secret, etc. they represent things I have to do often ... and lacked a good package/function to help me in doing this.


## to-do

* [ ] get a halfway clean lookup table for PBWG ac types, also fill in for NM flight table misses: start from https://github.com/ColtJD45/icao-aircraft-designator-list/blob/main/icao_aircraft_data.csv
* [ ] ditto on location indicators for O(verflight) A(rrival) D(eparture) and I(internal) traffic counts; move via OurAirports -> iso code -> Member State and label ECTRL - the original lookup "got lost" & store for retrieval (and time-to-time maintenance / update); also include memberstates from https://github.com/eurocontrol/eurocontrol/blob/main/data-raw/member_state.csv
