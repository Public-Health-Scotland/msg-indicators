# MSG Indicators - Indicator 6: SMR activity for balance of care indicator
# Extracts data from SLFs to produce summary output for Excel

# Last Updated: RB 12/05/2022


#### 1. Set up ----

# packages
library(haven)
library(here)
library(dplyr)
library(tidylog)
library(tidyr)
library(janitor)
library(magrittr)
library(lubridate)
library(tidyverse)
library(openxlsx)
library(fs)
library(slfhelper)

# filepaths

#create output file_path
output_folder <- "/conf/irf/03-Integration-Indicators/02-MSG/06-R-Project/Annual Data/"

# create list of carehome codes 
ch_list <- c("A240V", "F821V", "G105V", "G518V", "G203V", "G315V", "G424V", "G541V", "G557V",
             "H239V", "L112V", "L213V", "L215V", "L330V", "L365V", 'N124R', "N465R", "N498V",
             "S312R", "S327V", "T315S", "T337V", "Y121V")
# create list of hospice codes
hospice_list <- c("C413V", "C306V", "A227V", "G414V", "G604V", "L102K", "S121K", "C407V", 
                  "V103V", "S203K", "G583V", "W102V", "G501K", "H220V", "G584V", "T317V")

# read in community hospital lookup
comm_hosp_lookup <- read_rds("Annual Data/community_hospital_lookup.rds")

# population lookup
pop_lookup <- read_rds("/conf/linkage/output/lookups/Unicode/Populations/Estimates/CA2019_pop_est_1981_2021.rds")  %>%
  clean_names() %>%
  filter(year >= 2013) 


#### 2. Extract Data ----

# extract episode level data from SLFs
slf_extract <- read_slf_episode(c("1314", "1415", "1516", "1617", "1718", "1819", "1920", "2021", "2122"),
                                recids = c("01B", "02B", "GLS", "50B", "04B"),
                                columns = c("anon_chi", "recid",  "ipdc", "location", 
                                            "year", "lca", "yearstay", "sigfac", "age")) %>%
  filter(ipdc == "I" & (anon_chi != "" | !is.na(anon_chi)) & (lca != "" | !is.na(lca)))

# match on comm hosp lookup
slf_extract %<>%
  left_join(comm_hosp_lookup, by = "location")

# flag location types - hospice/pcu, community hospital, care home
slf_location <- slf_extract %>%
  mutate(com_flag = if_else(type == "Community", 1, 0),
         pal_flag = if_else(location %in% hospice_list | sigfac == "1G", 1, 0),
         ch_flag  = if_else(location %in% ch_list, 1, 0)) %>%
  mutate(type = case_when(pal_flag == 1 | sigfac == "1G" ~ "hospice",
                          com_flag == 1 ~ "community_hospital",
                          ch_flag == 1 ~ "care_home",
                          ch_flag == 0 & pal_flag == 0 & (is.na(type)) ~ "acute"))
                                  
# aggregate data, sum beddays by fy, location, age and type
slf_all <- slf_location %>%
  dtplyr::lazy_dt() %>% 
  group_by(year, lca, type) %>%
  summarise(beddays_all = sum(yearstay, na.rm = T)) %>%
  ungroup() %>%
  tibble::as_tibble() %>% 
  mutate(beddays_year = (beddays_all/365),
         age_group = "All Ages")

# aged 65+
slf_65plus <- slf_location %>%
  filter(age >= 65) %>%
  dtplyr::lazy_dt() %>% 
  group_by(year, lca, type) %>%
  summarise(beddays_all = sum(yearstay)) %>%
  ungroup() %>%
  tibble::as_tibble() %>% 
  mutate(beddays_year = (beddays_all/365),
         age_group = "65+")

# aged 75+
slf_75plus <- slf_location %>%
  filter(age >= 75) %>%
  dtplyr::lazy_dt() %>% 
  group_by(year, lca, type) %>%
  summarise(beddays_all = sum(yearstay)) %>%
  ungroup() %>%
  tibble::as_tibble() %>% 
  mutate(beddays_year = (beddays_all/365),
         age_group = "75+")

# combine age groups to get output
slf_join <- bind_rows(slf_all, slf_65plus, slf_75plus) %>%
  # format lca
  mutate(lca = case_when(
    lca == '01' ~ "Aberdeen City",
    lca == '02' ~ "Aberdeenshire",
    lca == '03' ~ "Angus",
    lca == '04' ~ "Argyll and Bute",
    lca == '05' ~ "Scottish Borders",
    lca == '06' ~ "Clackmannanshire",
    lca == '07' ~ "West Dunbartonshire",
    lca == '08' ~ "Dumfries and Galloway",
    lca == '09' ~ "Dundee City",
    lca == '10' ~ "East Ayrshire",
    lca == '11' ~ "East Dunbartonshire",
    lca == '12' ~ "East Lothian",
    lca == '13' ~ "East Renfrewshire",
    lca == '14' ~ "City of Edinburgh",
    lca == '15' ~ "Falkirk",
    lca == '16' ~ "Fife",
    lca == '17' ~ "Glasgow City",
    lca == '18' ~ "Highland",
    lca == '19' ~ "Inverclyde",
    lca == '20' ~ "Midlothian",
    lca == '21' ~ "Moray",
    lca == '22' ~ "North Ayrshire",
    lca == '23' ~ "North Lanarkshire",
    lca == '24' ~ "Orkney Islands",
    lca == '25' ~ "Perth and Kinross",
    lca == '26' ~ "Renfrewshire",
    lca == '27' ~ "Shetland Islands",
    lca == '28' ~ "South Ayrshire",
    lca == '29' ~ "South Lanarkshire",
    lca == '30' ~ "Stirling",
    lca == '31' ~ "West Lothian",
    lca == '32' ~ "Na h-Eileanan Siar")) %>%
  rename(ca2019name = "lca")


#### 3. Populations ----

# all ages
pop_all <- pop_lookup %>%
  group_by(year, ca2019name) %>%
  summarise(population = sum(pop)) %>%
  mutate(age_group = "All Ages") %>%
  ungroup()

# aged 65+
pop_65plus <- pop_lookup %>%
  filter(age >= 65) %>%
  group_by(year, ca2019name) %>%
  summarise(population = sum(pop)) %>%
  mutate(age_group = "65+") %>%
  ungroup()

# aged 75+
pop_75plus <- pop_lookup %>%
  filter(age >= 75) %>%
  group_by(year, ca2019name) %>%
  summarise(population = sum(pop)) %>%
  mutate(age_group = "75+") %>%
  ungroup()

# final population data
pop_join <- 
  # combine ages
  rbind(pop_all, pop_65plus, pop_75plus) %>%
  # format year
  mutate(year = case_when(year == 2013 ~ "1314",
                          year == 2014 ~ "1415",
                          year == 2015 ~ "1516",
                          year == 2016 ~ "1617",
                          year == 2017 ~ "1718",
                          year == 2018 ~ "1819",
                          year == 2019 ~ "1920",
                          year == 2020 ~ "2021",
                          year == 2021 ~ "2122"))



#### 4. Format output ----
# we drop care home activity here, this will be counted by CHC figures

output_ca <- slf_join  %>%
  select(-beddays_all) %>%
  # rearrange output
  arrange(age_group, year, ca2019name) %>%
  group_by(year, age_group, ca2019name) %>%
  pivot_wider(names_from = "type", values_from = "beddays_year") %>%
  ungroup() %>%
  # match on populations
  left_join(pop_join, by = c("year", "age_group", "ca2019name")) %>%
  select(year, age_group, ca2019name, population, acute, community_hospital, hospice)

# Stirling & Clacks output
output_sc <- output_ca %>%
  filter(ca2019name == "Stirling" | ca2019name == "Clackmannanshire") %>%
  group_by(year, age_group) %>%
  summarise(population = sum(population),
            community_hospital = sum(community_hospital),
            acute = sum(acute),
            hospice = sum(hospice, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(ca2019name = "Stirling and Clackmannanshire") %>%
  select(year, age_group, ca2019name, population, acute, community_hospital, hospice)

# Scotland output
output_scot <- output_ca %>%
  group_by(year, age_group) %>%
  summarise(population = sum(population, na.rm = TRUE),
            acute = sum(acute, na.rm = TRUE),
            community_hospital = sum(community_hospital, na.rm = TRUE),
            hospice = sum(hospice, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(ca2019name = "Scotland") %>%
  select(year, age_group, ca2019name, population, acute, community_hospital, hospice)

# join ca all, stirling clack and all scotland
final_output <- bind_rows(output_ca, output_sc, output_scot) %>%
  mutate(acute = round(acute, digits = 2),
         community_hospital = round(community_hospital, digits = 2),
         hospice = round(hospice, digits = 2)) %>%
  arrange(year, age_group, ca2019name) %>%
  mutate(year = recode(year,
                       "1314" = "2013/14",
                       "1415" = "2014/15",
                       "1516" = "2015/16",
                       "1617" = "2016/17",
                       "1718" = "2017/18",
                       "1819" = "2018/19",
                       "1920" = "2019/20",
                       "2021" = "2020/21",
                       "2122" = "2021/22"))
  
# save output
write_rds(final_output, path(output_folder, "Final_Beddays.rds"), compress = "gz")
write.xlsx(final_output, path(output_folder, "Final_Beddays.xlsx"), overwrite = TRUE)


