# MSG Indicators - Indicator 2b: GLS beddays
# Uses data from SMR01E to produce LIST breakdown file

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

# Extract SMR01E (GLS) data
smr01e_extract <- as_tibble(dbGetQuery(channel, statement = "SELECT LINK_NO, ADMISSION_DATE,
                                      DISCHARGE_DATE, SPECIALTY, LOCATION,
                                      SIGNIFICANT_FACILITY, ADMISSION_TYPE, DR_POSTCODE,
                                      AGE_IN_YEARS, HBTREAT_CURRENTDATE, ADMISSION,
                                      DISCHARGE, URI FROM ANALYSIS.SMR01_1E_PI
                                      WHERE DISCHARGE_DATE >= TO_DATE('2015-04-01','YYYY-MM-DD')")) %>%
  
  # 'Clean' variable names
  clean_names() %>% 
  # Match on geographies
  left_join(postcode_lookup, by = "dr_postcode")

# Close odbc connection
dbDisconnect(channel)

#### 3. Calculate beddays ----

# Select required episodes & match on remanining geographies
admissions <- smr01e_extract %>%
  # select episodes in reporting period
  filter(discharge_date >= start_date) %>%
  # select emergency admissions (include transfers as many will start as emergency in SMR01)
  filter(admission_type >= 20 & admission_type <= 22 | 
           admission_type >= 30 & admission_type <= 39 |
           admission_type == 18) %>%
  # select those resident in a CA
  drop_na(ca2019) %>%
  # remove identical duplicates
  distinct(link_no, admission_date, discharge_date, .keep_all = TRUE) %>%
  # create area treated variable
  mutate(area_treated = ifelse(hbtreat_currentdate == hb2019, 'Within HBres', 'Outwith HBres')) %>%
  # match on datazone & CA names
  left_join(locality_lookup, by = "datazone2011")  %>%
  left_join(council_lookup, by = "ca2019")

# Save temp file
arrow::write_parquet(admissions, path(data_folder, "SMR01E_temp.parquet"))

# Calculate beddays - run beddays function to count days based on month
beddays <- admissions %>%
  monthly_beddays(earliest_date = start_date_bd,
                  latest_date = end_date) %>%
  # select required variables
  select(link_no, admission_date, discharge_date, month, year, council_area2019name,
         age_in_years, beddays, hscp_locality, area_treated, location, specialty, significant_facility) %>%
  # rename columns 
  rename(council = "council_area2019name", age = "age_in_years")

# Save temp file
arrow::write_parquet(beddays, path(data_folder, "SMR01E_temp_beddays.parquet"))

#### 5. LIST breakdown file ----

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
  rename(locality=hscp_locality) %>%
  dtplyr::lazy_dt() %>% 
  group_by(council, locality, area_treated, location, specialty, 
           significant_facility, age_group, month) %>%
  summarise(unplanned_beddays = sum(beddays)) %>%
  filter(unplanned_beddays > 0) %>%
  ungroup() %>%
  tibble::as_tibble() %>% 
  mutate(age_group=as.character(age_group))

# Save LIST output
# write_sav(list_output, path(data_folder, "2b-GLS-Beddays-breakdown.sav"), compress = TRUE)
# write_rds(list_output, path(data_folder, "2b-GLS-Beddays-breakdown.rds"), compress = "gz")
# arrow::write_parquet(list_output, path(data_folder, "2b-GLS-Beddays-breakdown.parquet"))

