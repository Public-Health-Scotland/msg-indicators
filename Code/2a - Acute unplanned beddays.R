# MSG Indicators - Indicator 2a: Acute unplanned beddays
# Uses data from SMR01 to produce summary output for Excel & LIST breakdown file

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

#### 2. Calculate monthly beddays ----

# Read in SMR01 admissions data (from script 1a)
admissions <- arrow::read_parquet(path(data_folder, "SMR01_temp.parquet")) %>%
  # select variables requried
  select(link_no, cis_marker, admission_date, discharge_date, month_year,
         council_area2019name, age)

# Use function to calculate monthly beddays from admissions data
smr01_beddays <- admissions %>%
  monthly_beddays(earliest_date = start_date_bd,
                  latest_date = end_date)

# Save temp output
arrow::write_parquet(smr01_beddays, path(data_folder, "SMR01_temp_beddays.parquet"))

#### 3. Create summary output for Excel ----

# create age labels 
age_labs <- c("<18", "18-64", "65+")

beddays_data <- smr01_beddays %>%
  # create age groups
  mutate(age, age_group = cut(age, breaks = c(-1, 17, 64, 150), labels = age_labs)) %>%
  rename(council = "council_area2019name") %>%
  # create month year variable and format as date
  mutate(date = dmy(paste0('01', sep ="-", month, sep ="-", year))) %>%
  # keep only months from April
  filter(date >= start_date_excel & date < last_date) %>%
  # format as mmm-yy
  mutate(month = format(date, "%b-%y", abbr=TRUE))


# Replicate data for ages 18+
beddays_18 <- beddays_data %>%
  filter(age_group == "18-64" | age_group == "65+") %>%
  mutate(age_group = "18+") 

# Replicate data for all ages
beddays_all <- beddays_data %>%
  mutate(age_group = "All Ages")

# Join outputs and sum up beddays
beddays_output <- bind_rows(beddays_data, beddays_18, beddays_all) %>%
  # group up and sum beddays
  group_by(month, council, age_group) %>%
  summarise(unplanned_beddays = sum(beddays)) %>%
  ungroup() %>%
  # create month and lookup
  mutate(lookup = paste0(age_group, month, council)) %>%
  # tidy up final file
  arrange(age_group, council) %>%
  select(lookup, month, council, age_group, unplanned_beddays)

# Save excel output
write.xlsx(beddays_output, path(data_folder, "2a_Acute_Beddays.xlsx"), overwrite=TRUE)


#### 4. LIST breakdown file ----

# We need to go back to episode level data for this extract

# Connect to SMRA tables using odbc connection
channel <- suppressWarnings(
  dbConnect(odbc(),
            dsn = "SMRA",
            uid = .rs.askForPassword("What is your user ID?"),
            pwd = .rs.askForPassword("What is your LDAP password?")))

# SMR01 Extract
smr01_extract <- as_tibble(dbGetQuery(channel, statement = "SELECT LINK_NO, ADMISSION_DATE,
                                      DISCHARGE_DATE, CIS_MARKER, SPECIALTY, LOCATION,
                                      SIGNIFICANT_FACILITY, ADMISSION_TYPE, DR_POSTCODE,
                                      AGE_IN_YEARS, HBTREAT_CURRENTDATE, ADMISSION,
                                      DISCHARGE, URI FROM ANALYSIS.SMR01_PI
                                      WHERE DISCHARGE_DATE >= TO_DATE('2015-04-01','YYYY-MM-DD')")) %>%
  # tidy up variable names
  clean_names()

# Close odbc connection
dbDisconnect(channel)

# Match on geographies
smr01_extract %<>%
  left_join(postcode_lookup, by = "dr_postcode") %>%
  arrange(link_no, cis_marker, admission_date, discharge_date, desc(admission_type)) 

# Convert to data table add on relevant cis information
smr_dt <-as.data.table(smr01_extract)
smr_cis = smr_dt[,.(cis_admission_date=min(admission_date),
                      cis_discharge_date=max(discharge_date),
                      cis_ca2019=first(ca2019),
                      cis_admission_type = first(admission_type),
                      cis_datazone2011 = first(datazone2011)), by = c('link_no', 'cis_marker')]  


smr_table = smr_dt[smr_cis, on=c("link_no", "cis_marker")]

# make same selections
smr01_emergency <- as_tibble(smr_table) %>%
  # select discharges within months reported
  filter(cis_discharge_date %within% interval(start_date, end_date)) %>%
  # select emergency admissions
  filter(cis_admission_type >= 20 & cis_admission_type <=22 |
           cis_admission_type >= 30 & cis_admission_type <= 39) %>%
  # select those resident in a CA
  drop_na(cis_ca2019) %>%
  # create within / outwith HB variable
  mutate(area_treated = if_else(hbtreat_currentdate == hb2019,
                               'Within HBres', 'Outwith HBres', 'Outwith HBres'))

# Match on locality and coucil area names
smr01_final <- smr01_emergency %>%
  # match on datazone & council area based on first episode in stay
  select(-datazone2011, -ca2019) %>%
  rename(datazone2011=cis_datazone2011,
         ca2019=cis_ca2019) %>%
  left_join(locality_lookup, by = "datazone2011")  %>%
  left_join(council_lookup, by = "ca2019")

# check file matches 1a when grouping up
# check <- smr01_final %>% group_by(link_no, cis_marker) %>% count()

# save temp file
arrow::write_parquet(smr01_final, path(data_folder, "SMR01_temp_ep.parquet"))

# remove working files
rm(smr01_extract)
rm(smr_dt)
rm(smr_table)
rm(smr_cis)
rm(smr01_emergency)

# Only keep required variables
admissions <- smr01_final %>%
  select(-cis_admission_date, -cis_discharge_date, -admission_type, -dr_postcode,
         -hbtreat_currentdate, -admission, -discharge, -uri, -cis_admission_type, -datazone2011)

# Use function to calculate monthly beddays from admissions data
smr01_beddays <- admissions %>%
  monthly_beddays(earliest_date = start_date_bd,
                  latest_date = end_date)

# create 5 year age groups
age_labs2 <- c("<18", "18-24", "25-29", "30-34", "35-39", "40-44", "45-49",
               "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80-84",
               "85-89", "90-94", "95-99", "100+")

# format for LIST output
list_output <- smr01_beddays %>%
  # create month year variable and format as date (first of the month)
  mutate(date = dmy(paste0('01', sep ="-", month, sep ="-", year))) %>%
  # keep only reported months from Apr-16 to last reporting month
  filter(date >= start_date & date <= last_date) %>%
  # add on 5 year age bands
  mutate(age_group = cut(age_in_years, breaks = c(-1, 17, 24, 29, 34, 39, 44, 49, 54,
                                              59, 64, 69, 74, 79, 84, 89, 94, 99, 150),
                              labels = age_labs2)) %>%
  # format as mmm-yy
  mutate(month_year = format(date, "01-%m-%Y"))
  
# group up and sum admissions
list_final <- list_output %>%
  dtplyr::lazy_dt() %>% 
  group_by(council_area2019name, hscp_locality, area_treated, location, specialty,
           significant_facility, age_group, month_year) %>%
  summarise(unplanned_beddays = sum(beddays)) %>%
  ungroup() %>%
  tibble::as_tibble() %>% 
  rename(council = "council_area2019name", locality = "hscp_locality", month="month_year") %>%
  filter(unplanned_beddays > 0) %>%
  mutate(age_group=as.character(age_group),
         month = dmy(month))

# Save LIST breakdown file
arrow::write_parquet(list_final, path(data_folder, "2a-Acute-Beddays-breakdown.parquet"))
