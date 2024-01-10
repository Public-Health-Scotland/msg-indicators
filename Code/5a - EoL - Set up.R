# MSG Indicator 5 - End of Life 
# Code to set up packages / filepaths for the main analysis program:
# '5b. EoL - Last 6 months of life.R'

# Based on R code produced by the EoL team for their publication
# Written/run on - RStudio Server

# Last updated: Rachael Bainbridge 23/08/2022

### 1 - Load packages ----

library(odbc)          # For accessing SMRA
library(dplyr)         # For data manipulation in the "tidy" way
library(readr)         # For reading in csv files
library(janitor)       # For 'cleaning' variable names
library(magrittr)      # For %<>% operator
library(lubridate)     # For dates
library(tidylog)       # For printing results of some dplyr functions
library(tidyr)         # For data manipulation in the "tidy" way
library(stringr)       # For string manipulation and matching
library(here)          # For the here() function
library(glue)          # For working with strings
library(purrr)         # For functional programming
library(fst)           # For reading source linkage files
library(haven)         # For writing sav files
library(slfhelper)     # For anonimizing chi numbers
library(openxlsx)      # For writing to excel


### 2 - Extract dates ----

# Define the dates that the data are extracted from and to

# Start date Hospital Care Data
start_date_smr <- lubridate::ymd(20150401)

# End date
end_date_smr <- lubridate::ymd(20230331)

# Start date Death records
start_date_dths <- lubridate::ymd(20180401)

# End date Death records
end_date_dths <- ymd(20230331)


### 3 - Output file path

eol_folder <- "/conf/irf/03-Integration-Indicators/02-MSG/06-R-Project/msg-indicators/Annual Data/"


### 4 - Define list of external causes of death codes ----

external <- c(paste0("V", 0, 0:9), paste0("V", 10:99),
              paste0("W", 0, 0:9), paste0("W", 10:99),
              paste0("X", 0, 0:9), paste0("X", 10:99),
              paste0("Y", 0, 0:9), paste0("Y", 10:84))

falls <- c(paste0("W", 0, 0:9), paste0("W", 10:19))


### 5 - Define list of care homes to class as community and Hospices ----

care_homes <- c("A240V", "F821V", "G105V", "G518V", "G203V", "G315V", 
                "G424V", "G541V", "G557V", "H239V", "L112V", "L213V", 
                "L215V", "L330V", "L365V", "N124R", "N465R", "N498V", 
                "S312R", "S327V", "T315S", "T337V", "Y121V")


hospice <-  c("C413V", "C306V", "A227V", "G414V", "L102K", "S121K", 
              "C407V", "V103V", "S203K", "G583V", "W102V", "G501K", 
              "H220V", "G584V", "G604V", "T317V")


### 6 - Read in lookup files ----

#Community Hospital lookup
comm_hosp <- function(){
  
  read_rds(glue("/conf/irf/03-Integration-Indicators/02-MSG/01-Data/05-EoL - Community Hospital Lookup/community_hospital_lookup.rds")) %>%
    clean_names()
  
}

# Scottish Postcode lookup
postcode <- function(version =""){
  
  fs::dir_ls(glue("/conf/linkage/output/lookups/Unicode/Geography/",
                  "Scottish Postcode Directory/"),
             regexp = glue("{version}.parquet$"))  %>%
    
    #Read in the most up to date lookup version
    max() %>%
    
    arrow::read_parquet() %>%
    
    clean_names() %>%
    
    select(pc7, ca2019, ca2019name, ur6_2020_name, 
           datazone2011) %>%
    
    
    rename(ca = ca2019name,
           ca_code = ca2019,
           urban_rural = ur6_2020_name,
           datazone = datazone2011) %>%
    
    mutate(urban_rural = str_remove(urban_rural, "^\\d\\s"))
  
}
