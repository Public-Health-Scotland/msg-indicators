# MSG Indicator 5 - End of Life - Calander Year output
# exactly the same code but deaths split by CY

# Based on R code produced by the EoL team for their publication
# Written/run on - RStudio Server

# Original code by EoL team
# Last updated: Rachael Bainbridge 20/05/2022

### 1 - Set up ----

library(fs)
eol_folder <- "/conf/irf/03-Integration-Indicators/02-MSG/01-Data/05-EoL/R development/"
source(path(eol_folder, "0. Indicator 5 - Set up.R"))


### 2 - Open SMRA Connection ----

smra_connect <- dbConnect(odbc(), 
                          dsn  = "SMRA",
                          uid  = .rs.askForPassword("SMRA Username:"), 
                          pwd  = .rs.askForPassword("SMRA Password:"))

### 3 - Extract SMR01 / SMR01E / SMR04 data ----

smra_view <-m"SMR01_PI"

# Define SQL query
Query_SMR01 <- paste0(
  "select link_no, admission_date, discharge_date, sex, age_in_years,
  inpatient_daycase_identifier, specialty, length_of_stay, admission_type,
  location, council_area_2019, dr_postcode, admission, discharge, uri,
  hbres_currentdate, hbtreat_currentdate, main_condition, other_condition_1,
  other_condition_2, other_condition_3, other_condition_4,other_condition_5, significant_facility
  from ANALYSIS.", smra_view,"
  where discharge_date >= '1 OCT 2012'")


# Extract data from database using SQL query above
SMR01 <- odbc::dbGetQuery(smra_connect, Query_SMR01) %>%
  tibble::as_tibble() %>% 
  clean_names() %>% 
  mutate(recid = "01B")

smra_view <- "SMR01_1E_PI"

# Define SQL query
Query_SMR01_GLS <- paste0(
  "select link_no, admission_date, discharge_date, sex, age_in_years,
  inpatient_daycase_identifier, specialty, length_of_stay, admission_type,
  location, council_area_2019, dr_postcode, admission, discharge, uri,
  hbres_currentdate, hbtreat_currentdate, main_condition, other_condition_1,
  other_condition_2, other_condition_3, other_condition_4,other_condition_5, significant_facility
  from ANALYSIS.", smra_view,"
  where discharge_date >= '1 OCT 2012'")


# Extract data from database using SQL query above
SMR01_GLS <- odbc::dbGetQuery(smra_connect, Query_SMR01_GLS) %>%
  tibble::as_tibble() %>% 
  clean_names() %>% 
  mutate(recid = "50B")

# combine acute and gls extracts
smr01 <- bind_rows(SMR01, SMR01_GLS) %>%
  filter(inpatient_daycase_identifier == "I")

# remove original extracts
rm(SMR01, SMR01_GLS)


smra_view <- "SMR04_PI"

# Define SQL query
Query_SMR04 <- paste0(
  "select link_no, admission_date, discharge_date, sex, age_in_years,
  management_of_patient, specialty, length_of_stay, admission_type,
  location, council_area_2019, dr_postcode, admission, discharge, uri,
  hbres_currentdate, hbtreat_currentdate, main_condition, other_condition_1,
  other_condition_2, other_condition_3, other_condition_4,other_condition_5, significant_facility
  from ANALYSIS.", smra_view)

# Extract data from database using SQL query above
smr04 <- odbc::dbGetQuery(smra_connect, Query_SMR04) %>%
  tibble::as_tibble() %>% 
  clean_names() %>% 
  mutate(recid = "04B")

smr04 <- smr04 %>% mutate(
  include = if_else(discharge_date >= "2012-10-01", 1, 0, missing = 2)
)
table(smr04$include)

smr04 <- filter(smr04, include == 1 | include == 2)

smr04 <- smr04 %>% mutate(smr04,
                          Patient = case_when(
                            management_of_patient == "1" ~ "True",
                            management_of_patient == "3" ~ "True",
                            management_of_patient == "5" ~ "True",
                            management_of_patient == "7" ~ "True",
                            management_of_patient == "A" ~ "True",
                            management_of_patient == "" ~ "Unknown",
                            TRUE ~ "Unknown")
)
table(smr04$Patient)
smr04 <- filter(smr04, Patient == "True")


### 4 - Remove SMR01 and SMR04 duplicates using anon_chi, doa and dod ----

smr01 <- smr01 %>% arrange(link_no, admission_date, discharge_date, admission, discharge, uri)
#smr01 <- smr01 %>% distinct(link_no, admission_date, discharge_date)
smr01 <- smr01 %>% mutate(
  flag = if_else((link_no == lag(link_no))&(admission_date == lag(admission_date))&(discharge_date == lag(discharge_date)), 1, 0, missing = 0)
)
table(smr01$flag)
smr01 <- smr01 %>% filter(flag == 0)

# Flag records where doa=dod as the LOS=0
smr01 <- smr01 %>% mutate(zerostay = 0)
smr01 <- smr01 %>% mutate(
  zerostay = if_else(admission_date == discharge_date, 1, 0)
)
table(smr01$zerostay)

# Remove records which have the same doa but different dod
smr01 <- smr01 %>% arrange(admission_date, link_no, desc(discharge_date))
smr01 <- smr01 %>% mutate(
  flag1 = if_else((admission_date == lag(admission_date))&(link_no == lag(link_no))&(zerostay == 0), 1, 0, missing = 0)
)
table(smr01$flag1)
smr01 <- smr01 %>% filter(flag1 == 0)

# Remove records which have the same dod but different doa
smr01 <- smr01 %>% arrange(discharge_date, link_no, admission_date)
smr01 <- smr01 %>% mutate(
  flag2 = if_else((discharge_date == lag(discharge_date))&(link_no == lag(link_no))&(zerostay == 0), 1, 0, missing = 0)
)
table(smr01$flag2)
smr01 <- smr01 %>% filter(flag2 == 0)

smr01 <- select(smr01, -zerostay, -flag, -flag1, -flag2)



# Complete the same process of removing duplicates for SMR04 data

smr04 <- smr04 %>% arrange(link_no, admission_date, discharge_date, admission, discharge, uri)
#smr04 <- smr04 %>% distinct(link_no, admission_date, discharge_date)
smr04 <- smr04 %>% mutate(
  flag = if_else((link_no == lag(link_no))&(admission_date == lag(admission_date))&(discharge_date == lag(discharge_date)), 1, 0, missing = 0)
)
table(smr04$flag)
smr04 <- smr04 %>% filter(flag == 0)

# Flag records where doa=dod as the LOS=0
smr04 <- smr04 %>% mutate(zerostay = 0)
smr04 <- smr04 %>% mutate(
  zerostay = if_else(admission_date == discharge_date, 1, 0, missing = 0)
)
table(smr04$zerostay)

# Remove records which have the same doa but different dod
smr04 <- smr04 %>% arrange(admission_date, link_no, desc(discharge_date))
smr04 <- smr04 %>% mutate(
  flag1 = if_else((admission_date == lag(admission_date)) & (link_no == lag(link_no)) & (zerostay == 0), 1, 0, missing = 0)
)
table(smr04$flag1)
smr04 <- smr04 %>% filter(flag1 == 0)

# Remove records which have the same dod but different doa
smr04 <- smr04 %>% arrange(discharge_date, link_no, admission_date)
smr04 <- smr04 %>% mutate(
  flag2 = if_else((discharge_date == lag(discharge_date)) & (link_no == lag(link_no)) & (zerostay == 0), 1, 0, missing = 0)
)
table(smr04$flag2)
smr04 <- smr04 %>% filter(flag2 == 0)

smr04 <- select(smr04, -zerostay, -flag, -flag1, -flag2)


### 5 - Extract deaths data ----

smra_view <- "GRO_DEATHS_C"

# Define SQL query
Query_Deaths <- paste0(
  "select DATE_OF_DEATH, INSTITUTION, UNDERLYING_CAUSE_OF_DEATH,
  CAUSE_OF_DEATH_CODE_0, CAUSE_OF_DEATH_CODE_1, CAUSE_OF_DEATH_CODE_2,
  CAUSE_OF_DEATH_CODE_3, CAUSE_OF_DEATH_CODE_4, CAUSE_OF_DEATH_CODE_5,
  CAUSE_OF_DEATH_CODE_6, CAUSE_OF_DEATH_CODE_7, CAUSE_OF_DEATH_CODE_8,
  CAUSE_OF_DEATH_CODE_9, AGE, SEX, DATE_OF_BIRTH, POSTCODE, CHI, LINK_NO
  from ANALYSIS.", smra_view,"
  where date_of_death >= '1 APR 2013'")


# Extract data from database using SQL query above
deaths_cy <- odbc::dbGetQuery(smra_connect, Query_Deaths) %>%
  tibble::as_tibble() %>% 
  clean_names()

deaths_cy <- deaths_cy %>% arrange(link_no, date_of_death)
deaths_cy <- deaths_cy %>% mutate(
  flag1 = if_else((link_no == lag(link_no)), 1, 0, missing = 0)
)
# 14 duplicate CHI records, remove these
table(deaths_cy$flag1)
deaths_cy <- deaths_cy %>% filter(flag1 == 0)

deaths_cy <- select(deaths_cy, -flag1)

deaths_cy <- deaths_cy %>% mutate(
  externalcause = case_when(
    substr(underlying_cause_of_death,1,3) %in% external |
      substr(cause_of_death_code_0,1,3) %in% external |
      substr(cause_of_death_code_1,1,3) %in% external |
      substr(cause_of_death_code_2,1,3) %in% external |
      substr(cause_of_death_code_3,1,3) %in% external |
      substr(cause_of_death_code_4,1,3) %in% external |
      substr(cause_of_death_code_5,1,3) %in% external |
      substr(cause_of_death_code_6,1,3) %in% external |
      substr(cause_of_death_code_7,1,3) %in% external |
      substr(cause_of_death_code_8,1,3) %in% external |
      substr(cause_of_death_code_9,1,3) %in% external ~ 1, T ~0))

table(deaths_cy$externalcause)

deaths_cy <- deaths_cy %>% mutate(
  falls = case_when(
    substr(underlying_cause_of_death,1,3) %in% falls |
      substr(cause_of_death_code_0,1,3) %in% falls |
      substr(cause_of_death_code_1,1,3) %in% falls |
      substr(cause_of_death_code_2,1,3) %in% falls |
      substr(cause_of_death_code_3,1,3) %in% falls |
      substr(cause_of_death_code_4,1,3) %in% falls |
      substr(cause_of_death_code_5,1,3) %in% falls |
      substr(cause_of_death_code_6,1,3) %in% falls |
      substr(cause_of_death_code_7,1,3) %in% falls |
      substr(cause_of_death_code_8,1,3) %in% falls |
      substr(cause_of_death_code_9,1,3) %in% falls ~ 1, T ~0))

table(deaths_cy$falls)

deaths_cy <- filter(deaths_cy, externalcause == 0 & falls == 0 | falls == 1)

deaths_cy <- deaths_cy %>% mutate(
  cy = case_when(
    (date_of_death >= "2014-01-01" & date_of_death < "2015-01-01") ~ "2014",
    (date_of_death >= "2015-01-01" & date_of_death < "2016-01-01") ~ "2015",
    (date_of_death >= "2016-01-01" & date_of_death < "2017-01-01") ~ "2016",
    (date_of_death >= "2017-01-01" & date_of_death < "2018-01-01") ~ "2017",
    (date_of_death >= "2018-01-01" & date_of_death < "2019-01-01") ~ "2018",
    (date_of_death >= "2019-01-01" & date_of_death < "2020-01-01") ~ "2019",
    (date_of_death >= "2020-01-01" & date_of_death < "2021-01-01") ~ "2020",
    (date_of_death >= "2021-01-01" & date_of_death < "2022-01-01") ~ "2021",
    TRUE ~ "Unknown")
)

table(deaths_cy$cy)

deaths_cy <- deaths_cy %>% 
  filter(cy != "Unknown")

# add on postcode information
deaths_cy %<>% 
  #left_join(simd(), by = c("postcode" = "pc7")) %>%
  left_join(postcode(), by = c("postcode" = "pc7"))


# close connection to SMRA
dbDisconnect(smra_connect)



### 6 - Join SMR01/50 and SMR04 data, add Hospital categories ----

smr <- bind_rows(smr01, smr04) 

smr %<>%
  
  #convert adm_date,dis_date and death_of_death to Date format
  mutate(adm_date = as_date(admission_date)) %>% 
  mutate(dis_date = as_date(discharge_date)) %>%
  
  select(-admission_date, -discharge_date) %>%
  select(link_no, recid, adm_date, dis_date, location, significant_facility, include) %>%
  
  #Add Hospital Categories
  mutate(location_type =case_when(
    
    #Community Hospital
    location %in% comm_hosp()$location ~ "Community Hospital",
    
    #Add Hospice Category
    location %in% hospice ~ "Hospice / Palliative Care Unit",
    
    
    #Add Care Home Category
    location %in% care_homes ~ "Care Home"
  )) %>%
  
  #Additional category to flag where signifcant facility 1G is also Hospice Category
  mutate(location_type = case_when(significant_facility %in% "1G" ~ "Hospice / Palliative Care Unit",
                                   TRUE ~ location_type)) %>%
  
  #Add Large Hospital
  mutate(location_type = 
           case_when(is.na(location_type) | location_type == "" ~ "Large Hospital",
                     TRUE ~ location_type)) 

table(smr$location_type)


### 7 - Match on deaths information and select activity in last six months ----

# match on deaths
smr_deaths <- left_join(smr, deaths_cy, by = "link_no") %>% 
  mutate(date_of_death = as_date(date_of_death)) %>%
  # select activity for deaths
  filter(!is.na(date_of_death)) %>%
  # if MH records missing discharge date fill in with date of death
  mutate(dis_date = if_else(recid == "04B" & is.na(dis_date), date_of_death, dis_date))

# calculate activity in L6M
smr_deaths %<>%
  
  # Calculate date six months before death
  mutate(six_months = date_of_death - days(183)) %>%
  
  # For stays spanning this date, fix admission date to six months before death
  mutate(adm_date = if_else(adm_date < six_months & 
                              dis_date >= six_months,
                            six_months,
                            adm_date)) %>%
  
  # Select only stays within last six months of life
  filter(adm_date >= six_months) %>%
  
  # Remove records where admission date is after date of death
  filter(adm_date <= date_of_death) %>%
  
  # Where discharge date is after date of death, fix to date of death
  mutate(dis_date = if_else(dis_date > date_of_death,
                            date_of_death,
                            dis_date)) %>%
  
  # Calculate length of stay
  mutate(los = time_length(interval(adm_date, dis_date),
                           "days")) %>%
  
  # Recode 183 LOS to 182.5 (exact six months)
  mutate(los = if_else(los == 183, 182.5, los))


### 8 - Calculate large hosp / community hosp / hospice beddays ----

# Remove Care Home category at this point
# This is inpatient activity taking place within a care home, 
# don't want to count this under hospital categories
smr_deaths <- filter(smr_deaths, location_type != "Care Home")

# Calculation to remove overlapping stays between SMR01 and SMR04, 
# max los shouldn't be greater than 182.5 (6 months)
smr_deaths <- smr_deaths %>%
  arrange(link_no, los) %>%
  group_by(link_no) %>%
  mutate(flag = row_number()) %>%
  ungroup()

smr_deaths <- smr_deaths %>% 
  group_by(link_no) %>%
  mutate(maxcount = max(flag),
         totallos = sum(los)) %>%
  ungroup()

table(smr_deaths$totallos)

smr_deaths <- smr_deaths %>% 
  mutate(los = if_else(totallos > 182 & flag == maxcount, 
                       los-(totallos-182.5), los))

# Spread LOS values over three columns

hosp_los_wider <- smr_deaths %>%
  
  pivot_wider(names_from = location_type, values_from = los, values_fill = NULL) %>%
  
  clean_names() %>%
  
  replace_na(list(large_hospital = 0, community_hospital = 0, hospice_palliative_care_unit = 0))

# save temp file
write_rds(hosp_los_wider, path(eol_folder, "Final Activity CY.rds"), compress="gz")


# Aggregate total los for each category

agg_hosp_los <- hosp_los_wider %>% group_by(cy, ca, link_no) %>%
  
  summarise(large_hosp_los = sum(large_hospital, na.rm = TRUE),
            comm_hosp_los = sum(community_hospital, na.rm = TRUE),
            hospice_los = sum(hospice_palliative_care_unit, na.rm = TRUE)) %>%
  ungroup() 


#### 9 - Create output for MSG template ---- 

# get number of deaths
deaths_agg_cy <- deaths_cy %>% 
  group_by(cy, ca) %>%
  summarise(deaths = n()) %>%
  ungroup()

# create aggregate LCA output
output_lca_cy <- agg_hosp_los %>% 
  
  group_by(cy, ca) %>%
  
  # calculate aggregated beddays by location type
  summarise(large_beddays = sum(large_hosp_los, na.rm = TRUE),
            community_hosp_beddays = sum(comm_hosp_los, na.rm = TRUE),
            palliative_beddays = sum(hospice_los, na.rm = TRUE)) %>%
  
  ungroup() %>%
  
  # match on deaths
  left_join(deaths_agg_cy) %>%
  
  # rename vars
  rename(lca = ca,
         calyear = cy) %>%
  
  # remove blank lca
  filter(!is.na(lca)) 


# Group up stirling & clacks
output_sc_cy <- output_lca_cy %>%
  # select s+c activity
  filter(lca == "Stirling" | lca == "Clackmannanshire") %>%
  mutate(lca = "Stirling and Clackmannanshire") %>%
  # group up
  group_by(calyear, lca) %>%
  # calculate aggregated beddays by location type
  summarise(large_beddays = sum(large_beddays, na.rm = TRUE),
            community_hosp_beddays = sum(community_hosp_beddays, na.rm = TRUE),
            palliative_beddays = sum(palliative_beddays, na.rm = TRUE),
            deaths=sum(deaths))  %>%
  ungroup()

# repeat for scotland
output_scot_cy <- output_lca_cy %>%
  mutate(lca = "Scotland") %>%
  # group up
  group_by(calyear, lca) %>%
  
  # calculate aggregated beddays by location type
  summarise(large_beddays = sum(large_beddays, na.rm = TRUE),
            community_hosp_beddays = sum(community_hosp_beddays, na.rm = TRUE),
            palliative_beddays = sum(palliative_beddays, na.rm = TRUE),
            deaths=sum(deaths))  %>%
  ungroup()

# combine
output_all_cy <- rbind(output_lca_cy, output_sc_cy, output_scot_cy)


# create output for MSG
output_final_cy <- output_all_cy %>%
  
  # calculate total bedDays in all locations but Home 
  mutate(sum_bdays = (large_beddays + community_hosp_beddays + palliative_beddays)) %>%
  
  # calculate Home/Community BedDays 
  mutate(community_beddays = 182.5*deaths - sum_bdays) %>%
  
  select(calyear, lca, large_beddays, community_hosp_beddays, palliative_beddays,
         community_beddays, deaths) %>%
  
  # calculate all possible beddays
  mutate(possible_beddays = large_beddays + community_hosp_beddays + palliative_beddays +
           community_beddays) %>%
  
  # calculate percentages by location type
  mutate(perc_community = (community_beddays / possible_beddays),
         perc_large = (large_beddays / possible_beddays),
         perc_community_hosp = (community_hosp_beddays / possible_beddays),
         perc_palliative = (palliative_beddays / possible_beddays)) %>%
  
  # select final variables & sort
  select(calyear, lca, community_beddays, palliative_beddays, community_hosp_beddays, 
         large_beddays, possible_beddays, perc_community, perc_palliative, perc_community_hosp,
         perc_large, deaths) %>%
  arrange(calyear, lca) %>%
  
  # amend lca to match excel template
  mutate(
    lca = recode(lca, 
                 "Argyll and Bute" = "Argyll & Bute",
                 "Dumfries and Galloway" = "Dumfries & Galloway",
                 "Perth and Kinross" = "Perth & Kinross",
                 .default = lca)) %>%
  
  # set 2021/22 to provisional
  mutate(calyear = recode(calyear, "2021" = "2021p", .default = calyear))

# save output for MSG
write_rds(output_final_cy, path(eol_folder, "Final figures CY.rds"))
write.xlsx(output_final_cy, path(eol_folder, "Final figures CY.xlsx"), overwrite = TRUE)


