# MSG Indicators - Indicator 2a: Acute unplanned beddays
# Uses data from SMR01 to produce LIST breakdown file

#### 1. Set up ----

# Set dates

# start & end of extract (needs to be Jan for beddays function)
start_date_bd <- ymd("2017-01-01")
# Earliest date for data presentation
start_date <- earliest_date
# Date of extract
end_date <- ymd(output_date <- strftime(Sys.Date(), format = "%Y%m%d"))
# end of reporting period (last day of preceding month - 1 month)
last_date <- reporting_month_date

#### 2. LIST breakdown file ----

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
smr01_final <- as_tibble(smr_table) %>%
  # select discharges within months reported
  filter(cis_discharge_date %within% interval(start_date, end_date)) %>%
  # select emergency admissions
  filter(cis_admission_type >= 20 & cis_admission_type <=22 |
           cis_admission_type >= 30 & cis_admission_type <= 39) %>%
  # select those resident in a CA
  drop_na(cis_ca2019) %>%
  # create within / outwith HB variable
  mutate(area_treated = if_else(hbtreat_currentdate == hb2019,
                               'Within HBres', 'Outwith HBres', 'Outwith HBres')) %>% 
  # Match on locality and coucil area names
  # match on datazone & council area based on first episode in stay
  select(-datazone2011, -ca2019) %>%
  rename(datazone2011=cis_datazone2011,
         ca2019=cis_ca2019) %>%
  left_join(locality_lookup, by = "datazone2011")  %>%
  left_join(council_lookup, by = "ca2019")

# save temp file
arrow::write_parquet(smr01_final, path(data_folder, "SMR01_temp_ep.parquet"))

# remove working files
rm(smr01_extract, smr_cis, smr_dt, smr_table)

# Only keep required variables
smr01_beddays <- smr01_final %>%
  select(-cis_admission_date, -cis_discharge_date, -admission_type, -dr_postcode,
         -hbtreat_currentdate, -admission, -discharge, -uri, -cis_admission_type, -datazone2011) %>% 
  # Use function to calculate monthly beddays from admissions data
  monthly_beddays(earliest_date = start_date_bd,
                  latest_date = end_date)

# create 5 year age groups
age_labels <- c("<18", "18-24", "25-29", "30-34", "35-39", "40-44", "45-49",
               "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80-84",
               "85-89", "90-94", "95-99", "100+")

# format for LIST output
list_final <- smr01_beddays %>%
  # create month year variable and format as date (first of the month)
  mutate(date = dmy(paste0('01', sep ="-", month, sep ="-", year))) %>%
  # keep only reported months from Apr-16 to last reporting month
  filter(date >= start_date & date <= last_date) %>%
  # add on 5 year age bands
  mutate(age_group = cut(age_in_years, breaks = c(-1, 17, 24, 29, 34, 39, 44, 49, 54,
                                              59, 64, 69, 74, 79, 84, 89, 94, 99, 150),
                              labels = age_labels)) %>%
  # format as mmm-yy
  mutate(month_year = format(date, "01-%m-%Y")) %>% 
  # group up and sum admissions
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
# write_sav(list_final, path(data_folder, "2a-Acute-Beddays-breakdown.sav"), compress = TRUE)
# write_rds(list_final, path(data_folder, "2a-Acute-Beddays-breakdown.rds"), compress = "gz")
# arrow::write_parquet(list_final, path(data_folder, "2a-Acute-Beddays-breakdown.parquet"))
