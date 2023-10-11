# MSG Indicators - Indicator 2c: MH beddays
# Uses data from SMR04 to produce LIST breakdown file


# Set dates

# start & end of extract (needs to be Jan for beddays function)
start_date_bd <- ymd("2017-01-01")
start_date <- earliest_date
end_date <- ymd(output_date <- strftime(Sys.Date(), format = "%Y%m%d"))
# end of reporting period (last day of preceding month - 1 month)
last_date <- reporting_month_date

#### 2. Extract Data ---- 

# Connect to SMRA tables using odbc connection
channel <- suppressWarnings(
  dbConnect(odbc(),
            dsn = "SMRA",
            uid = .rs.askForPassword("What is your user ID?"),
            pwd = .rs.askForPassword("What is your LDAP password?")))

# Extract SMR04 data
smr04_extract <- as_tibble(dbGetQuery(channel, statement = "SELECT LINK_NO, ADMISSION_DATE,
                                      DISCHARGE_DATE, SPECIALTY, LOCATION,
                                      SIGNIFICANT_FACILITY, ADMISSION_TYPE, DR_POSTCODE,
                                      AGE_IN_YEARS, HBTREAT_CURRENTDATE, ADMISSION,
                                      DISCHARGE, URI FROM ANALYSIS.SMR04_PI
                                      WHERE ADMISSION_DATE >= TO_DATE('1997-01-01','YYYY-MM-DD')")) %>%
  
  # 'Clean' variable names
  clean_names() %>%
  # set admission & discharge as dates
  mutate(admission_date=as_date(admission_date),
         discharge_date=as_date(discharge_date)) %>% 
  # Match on geographies
  left_join(postcode_lookup, by = "dr_postcode") %>%
  left_join(locality_lookup, by = "datazone2011")  %>%
  left_join(council_lookup, by = "ca2019")

# Close odbc connection
dbDisconnect(channel)

#### 3. Match geographies & select episodes ----

# Select required episodes
smr04_data <- smr04_extract %>%
  # select emergency stays (include transfers as many will have started as emergency in SMR01)
  filter(admission_type >= 20 & admission_type <= 22 | 
           admission_type >= 30 & admission_type <= 39 |
           admission_type == 18) %>%
  # select those resident in a CA
  drop_na(ca2019)

#### 4. Remove duplicates ----

# 4.1 Same link no, admission and discharge
dups_1 <- smr04_data %>%
  arrange(link_no, admission_date, discharge_date) %>%
  distinct(link_no, admission_date, discharge_date, .keep_all = TRUE)

# 4.2 Same link no, admission & discharge NA
dups_2 <- dups_1 %>%
  mutate(ad_flag = if_else(admission_date == lag(admission_date),1,0),
         dis_flag = if_else(is.na(discharge_date),1,0),
         link_flag = if_else(link_no == lag(link_no),1,0),
         duplicates = if_else(ad_flag == 1 & dis_flag == 1 & link_flag == 1,1,0)) %>%
  filter(duplicates == 0)

# 4.3 create flags for missing discharge & min / max
dups_2_flags <- dups_2 %>%
  #create missing dod flag
  mutate(dis_flag2 = if_else(is.na(discharge_date),1,0)) %>%
  group_by(link_no) %>%
  # calculate min/max admission & discharge dates per person
  mutate(min_admission = min(admission_date, na.rm = TRUE),
         max_admission = max(admission_date, na.rm = TRUE),
         min_discharge = min(discharge_date, na.rm = TRUE),
         max_discharge = max(discharge_date, na.rm = TRUE),
         dis_max = max(dis_flag2)) %>%
  ungroup()

# Remove records where discharge missing and there is a record with a later admission date
dups_3 <- dups_2_flags %>%
  arrange(link_no, admission_date, desc(discharge_date)) %>%
  group_by(link_no) %>%
  mutate(episodes = n(),
         dis_flag = if_else(is.na(discharge_date),1,0),
         ad_max = if_else(admission_date < max_admission,1,0),
         duplicates3 = if_else(dis_flag == 1 & ad_max == 1,1,0)) %>%
  filter(duplicates3 == 0)

# 4.4 Remove records which are embedded within another epsiode

# set up data
dups_4 <- dups_3 %>%
  arrange(link_no, admission_date, discharge_date) %>%
  ungroup()

# Loop which removes embedded episodes, all dups found within 30 runs
times <- 1:30

for (i in times)
{
  dups_4 %<>%
    mutate(link_flag = if_else(link_no == lag(link_no),1,0),
           ad_flag = if_else(admission_date >= lag(admission_date),1,0),
           dis_flag = if_else(discharge_date < lag(discharge_date),1,0),
           ep_flag = if_else(link_flag==1 & ad_flag == 1 & dis_flag == 1,1,0)) %>%
    filter(ep_flag == 0 | is.na(ep_flag)) 
  
  i <- i + 1 
  print(i)
}

# 4.5 Where there is an earlier closed record which overlaps next open record,
# remove the open record
dups_5 <- dups_4 %>%
  arrange(link_no, admission_date, discharge_date) %>%
  group_by(link_no) %>%
  mutate(open_flag = if_else((is.na(discharge_date)) & lag(!is.na(discharge_date)) 
                             & admission_date < lag(discharge_date),1,0)) %>%
  filter(open_flag == 0 | is.na(open_flag))

# 4.6	Records with same admission date, keep later discharge
dups_6 <- dups_5 %>%
  arrange(link_no, admission_date, desc(discharge_date)) %>%
  group_by(link_no) %>%
  mutate(duplicate6 = if_else((!is.na(discharge_date)) & admission_date == lag(admission_date) &
                                admission_date != discharge_date
                              & discharge_date < lag(discharge_date),1,0)) %>%
  filter(duplicate6 == 0 | is.na(duplicate6))

# 4.7 Records with same discharge date, keep earliest admission                      
dups_7 <- dups_6 %>%
  arrange(link_no, admission_date, discharge_date) %>%
  group_by(link_no) %>%
  mutate(duplicate7 = if_else((!is.na(discharge_date)) & admission_date != discharge_date &
                                discharge_date == lag(discharge_date)
                              & admission_date > lag(admission_date),1,0)) %>%
  filter(duplicate7 == 0 | (is.na(duplicate7)))

# All duplicates now removed, select those with discharge in reporting period or open records
smr04_no_dups <- dups_7 %>%
  filter(discharge_date >= start_date | is.na(discharge_date)) %>% 
  ungroup() %>%
  # remove variables not needed
  select(-(ad_flag : duplicate7))

# Save temp file
arrow::write_parquet(smr04_no_dups, path(data_folder, "SMR04_temp.parquet"))

#### 5. Calculate beddays ----

# Prepare data for beddays function
beddays <- smr04_no_dups %>%
  # create within / outwith HB variable
  mutate(area_treated = if_else(hbtreat_currentdate == hb2019,
                                'Within HBres', 'Outwith HBres', 'Outwith HBres')) %>%
  # remove admissions after reporting period
  filter(admission_date <= last_date) %>%
  # for open records set discharge date to end of reporting period
  mutate(discharge_date = if_else(is.na(discharge_date), last_date, discharge_date)) %>% 
  # Calculate monthly beddays from admissions data
  monthly_beddays(
    earliest_date = start_date_bd,
    latest_date = end_date
  )  %>%  
  # select required variables
  select(link_no, admission_date, discharge_date, month, year, council_area2019name,
         age_in_years, beddays, hscp_locality, area_treated, location, specialty, 
         significant_facility) %>%
  # select data in reporting period
  filter(discharge_date >= start_date) %>%
  # rename columns 
  rename(council = "council_area2019name", age = "age_in_years")

# Save temp file
arrow::write_parquet(beddays, path(data_folder, "SMR04_temp_beddays.parquet"))

#### 7. LIST breakdown file ----

# create 5 year age groups
age_labels <- c("<18", "18-24", "25-29", "30-34", "35-39", "40-44", "45-49",
               "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80-84",
               "85-89", "90-94", "95-99", "100+")

# add age group, select data in reporting period and format month
list_output <- beddays %>%
  mutate(age, age_group = cut(age, breaks = c(-1, 17, 24, 29, 34, 39, 44, 49, 54,
                                              59, 64, 69, 74, 79, 84, 89, 94, 99, 150),
                              labels = age_labels)) %>%
  # create month year variable and format as date
  mutate(month = dmy(glue("01-{month}-{year}"))) %>%
  # keep only months from April
  filter(month >= start_date & month <= last_date) %>% 
  # create list output for MH
  rename(locality=hscp_locality) %>%
  dtplyr::lazy_dt() %>% 
  group_by(council, locality, area_treated, location, specialty, 
           significant_facility, age_group, month) %>%
  summarise(unplanned_beddays = sum(beddays)) %>%
  filter(unplanned_beddays > 0) %>%
  ungroup() %>%
  tibble::as_tibble() %>% 
  mutate(age_group=as.character(age_group))

# save LIST output
# write_sav(list_output, path(data_folder, "2c-MH-Beddays-breakdown.sav"), compress = TRUE)
# write_rds(list_output, path(data_folder, "2c-MH-Beddays-breakdown.rds"), compress = "gz")
# arrow::write_parquet(list_output, path(data_folder, "2c-MH-Beddays-breakdown.parquet"))

