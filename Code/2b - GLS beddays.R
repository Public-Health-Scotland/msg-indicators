# MSG Indicators - Indicator 2b: GLS beddays
# Uses data from SMR01E to produce summary output for Excel & LIST breakdown file

# Updated: RB 02/06/2022
# Updated: BM 14/06/2022, changed format of date in breakdown output and removed finncial year 2016/17
# from all outputs

#### 1. Set up ----

# file paths
data_folder <- "Data/"
code_folder <- "Code/"

# load packages & lookups required
#library(fs)
#source(path(code_folder, "0. Packages & functions.R"))

# Set dates

# start & end of extract (needs to be Jan for beddays function)
start_date_bd <- ymd("2017-01-01")
start_date <- ymd("2017-04-01")
start_date_excel <- ymd("2019-04-01")
end_date <- ymd(output_date <- strftime(Sys.Date(), format = "%Y%m%d"))

# end of reporting period (last day of preceding month - 1 month)
last_date <- as.Date(format(Sys.Date() - months(2), '%Y-%m-01')) - 1

#### 2. Extract Data ----

# Connect to SMRA tables using odbc connection
channel <- suppressWarnings(
  dbConnect(odbc(),
            dsn = "SMRA",
            uid = "batemm01",
            pwd = .rs.askForPassword("What is your LDAP password?")))

# Extract SMR01E (GLS) data
smr01e_extract <- as_tibble(dbGetQuery(channel, statement = "SELECT LINK_NO, ADMISSION_DATE,
                                      DISCHARGE_DATE, SPECIALTY, LOCATION,
                                      SIGNIFICANT_FACILITY, ADMISSION_TYPE, DR_POSTCODE,
                                      AGE_IN_YEARS, HBTREAT_CURRENTDATE, ADMISSION,
                                      DISCHARGE, URI FROM ANALYSIS.SMR01_1E_PI
                                      WHERE DISCHARGE_DATE >= TO_DATE('2015-04-01','YYYY-MM-DD')")) %>%
  
  # 'Clean' variable names
  clean_names()

# Close odbc connection
dbDisconnect(channel)

#### 3. Match geographies & calculate beddays ----

# Match on geographies
smr01e_extract %<>%
  left_join(postcode_lookup, by = "dr_postcode")

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

#### 4. Excel output for GLS beddays ----

# create age labs
age_labs <- c("<18", "18-64", "65+")

# create age group for 18+
beddays_age <- beddays %>%
  mutate(age, age_group = cut(age, breaks = c(-1, 17, 64, 150), labels = age_labs))

# replicate for 18+
beddays_18 <- beddays_age %>%
  filter(age_group == "18-64" | age_group == "65+") %>%
  mutate(age_group = "18+")

# replicate for all ages
beddays_all <- beddays_age %>%
  mutate(age_group = "All Ages")

# combine age groups & add on quarter
beddays_quarter <-
  bind_rows(beddays_age, beddays_18, beddays_all) %>%
  mutate(date_quarter = case_when(month == "Jan" | month == "Feb" | month == "Mar" ~ "03",
                                  month == "Apr" | month == "May" | month == "Jun" ~ "06",
                                  month == "Jul" | month == "Aug" | month == "Sep" ~ "09",
                                  month == "Oct" | month == "Nov" | month == "Dec" ~ "12"),
         quarter_date = paste0("01-", date_quarter, "-", year)) %>%
  mutate(qtr_date = as.Date(quarter_date, format = "%d-%m-%Y")) %>%
  mutate(quarter_year = format(qtr_date, "%b-%y", abbr = TRUE))

# Data for excel output
gls_beddays <- beddays_quarter %>%
  dtplyr::lazy_dt() %>% 
  # group up and sum beddays
  group_by(council, age_group, qtr_date, quarter_year) %>%
  summarise(unplanned_beddays = sum(beddays)) %>%
  # remove data before and after the reporting period
  filter(qtr_date >= start_date_excel & qtr_date <= last_date) %>%
  # create service for when we combine with MH
  mutate(service = "GLS") %>%
  # select required variables
  select(council, age_group, quarter_year, qtr_date, service, unplanned_beddays) %>%
  arrange(age_group, council, qtr_date) %>%
  ungroup() %>%
  tibble::as_tibble() %>% 
  select(-qtr_date) %>%
  # remove remaining zeros
  filter(unplanned_beddays != 0)

# Save output
arrow::write_parquet(gls_beddays, path(data_folder, "2b_GLS_beddays.parquet"))

#### 5. LIST breakdown file ----

# create 5 year age groups
age_labs2 <- c("<18", "18-24", "25-29", "30-34", "35-39", "40-44", "45-49",
               "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80-84",
               "85-89", "90-94", "95-99", "100+")

# add age group, select data in reporting period and format month
list_beddays <- beddays %>%
  mutate(age, age_group = cut(age, breaks = c(-1, 17, 24, 29, 34, 39, 44, 49, 54,
                                              59, 64, 69, 74, 79, 84, 89, 94, 99, 150),
                              labels = age_labs2)) %>%
  # create month year variable and format as date
  mutate(month = dmy(glue("01-{month}-{year}"))) %>%
  # keep only months from April
  filter(month >= start_date & month <= last_date)

# create list output for GLS
list_output <- list_beddays %>%
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
arrow::write_parquet(list_output, path(data_folder, "2b-GLS-Beddays-breakdown.parquet"))

