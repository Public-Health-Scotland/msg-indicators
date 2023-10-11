# MSG Indicators - Indicator 1a: Emergency Admissions
# Extracts data from SMR01 (via SMRA) to produce LIST breakdown file

#### 1. Set up ----

# Set dates

# start & end of extract
start_date <- earliest_date
end_date <- ymd(output_date <- strftime(Sys.Date(), format = "%Y%m%d"))
# end of reporting period (last day of preceding month - 1 month)
last_date <- reporting_month_date

#### 2. Extract SMR01 Data ----

# Connect to SMRA tables using odbc connection
channel <- suppressWarnings(
  dbConnect(odbc(),
            dsn = "SMRA",
            uid = .rs.askForPassword("What is your user ID?"),
            pwd = .rs.askForPassword("What is your LDAP password?")
  )
)

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


#### 3. Group up to SMR01 CIS stays --------

# Match on geographies
smr01_extract %<>%
  left_join(postcode_lookup, by = "dr_postcode") %>%
  arrange(link_no, cis_marker, admission_date, discharge_date, desc(admission_type))

# Convert to data table, aggregate to stay level
# Take minimum & maximum dates, first of everything else
smr_dt <- as.data.table(smr01_extract)
smr_table <- smr_dt[, .(
  admission_date = min(admission_date),
  discharge_date = max(discharge_date),
  age = first(age_in_years), 
  dr_postcode = first(dr_postcode),
  specialty = first(specialty), 
  location = first(location),
  significant_facility = first(significant_facility),
  admission_type = first(admission_type),
  hbtreat_currentdate = first(hbtreat_currentdate),
  admission = first(admission), 
  discharge = first(discharge),
  uri = first(uri), 
  ca2019 = first(ca2019), 
  hb2019 = first(hb2019),
  datazone2011 = first(datazone2011)), 
  by = c("link_no", "cis_marker")]

# Convert back to data frame & sort
smr01_final <- as.data.frame(smr_table) %>%
  ungroup() %>%
  arrange(link_no, cis_marker, admission_date, discharge_date, desc(admission_type)) %>% 

#### 4. Select emergency admissions ----

  # select the required data
  # select discharges within months reported
  filter(discharge_date %within% interval(start_date, end_date)) %>%
  # select emergency admissions
  filter(admission_type >= 20 & admission_type <= 22 |
           admission_type >= 30 & admission_type <= 39) %>%
  # select those resident in a CA
  drop_na(ca2019) %>%
  # create within / outwith HB variable
  mutate(area_treated = ifelse(hbtreat_currentdate == hb2019,
                               "Within HBres", "Outwith HBres"
  )) %>% 
  # Match on locality and council area names
  left_join(locality_lookup, by = "datazone2011") %>%
  left_join(council_lookup, by = "ca2019") %>%
  # format month variable
  mutate(month_year = format(discharge_date, "%b-%y"))

# Save temp output
arrow::write_parquet(smr01_final, path(data_folder, "SMR01_temp.parquet"))

#### 6. LIST breakdown file ----

# create 5 year age groups
age_labels <- c(
  "<18", "18-24", "25-29", "30-34", "35-39", "40-44", "45-49",
  "50-54", "55-59", "60-64", "65-69", "70-74", "75-79", "80-84",
  "85-89", "90-94", "95-99", "100+"
)

# format for LIST output
list_output <- smr01_final %>%
  # select reporting period
  filter(discharge_date >= start_date & discharge_date <= last_date) %>%
  # add on 5 year age bands
  mutate(
    age_group = cut(age,
                    breaks = c(
                      -1, 17, 24, 29, 34, 39, 44, 49, 54,
                      59, 64, 69, 74, 79, 84, 89, 94, 99, max(age)
                    ),
                    labels = age_labels
    ),
    month_year = format(discharge_date, "01-%m-%Y"),
    admissions = 1
  ) %>% 
  # group up and sum admissions
  dtplyr::lazy_dt() %>% 
  group_by(
    council_area2019name, hscp_locality, area_treated, location, specialty,
    significant_facility, age_group, month_year
  ) %>%
  summarise(admissions = sum(admissions)) %>%
  rename(council = "council_area2019name", locality = "hscp_locality", month = "month_year") %>%
  ungroup() %>%
  tibble::as.tibble() %>% 
  mutate(
    age_group = as.character(age_group),
    month = dmy(month)
  )

# Save LIST breakdown file
# Take your pick of format
# write_sav(list_final, path(data_folder, "1a-Admissions-breakdown.sav"), compress = TRUE)
# write_rds(list_final, path(data_folder, "1a-Admissions-breakdown.rds"), compress = "gz")
# arrow::write_parquet(list_final, path(data_folder, "1a-Admissions-breakdown.parquet"))

