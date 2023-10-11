# MSG Indicators - Indicator 1a: Emergency Admissions
# Extracts data from SMR01 (via SMRA) to produce summary output for Excel & LIST breakdown file

# Updated: RB 02/06/2022
# Updated: BM 14/06/2022, changed format of date in breakdown output and removed financial year 2016/17
# from all outputs


#### 1. Set up ----

# file paths
data_folder <- "Data/"
code_folder <- "Code/"

# Set dates

# start & end of extract
start_date <- ymd("2017-04-01")
start_date_excel <- ymd("2019-04-01")
end_date <- ymd(output_date <- strftime(Sys.Date(), format = "%Y%m%d"))

# end of reporting period (last day of preceding month - 1 month)
last_date <- as.Date(format(Sys.Date() - months(2), "%Y-%m-01")) - 1

#### 2. Extract SMR01 Data ----

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
smr01_sort <- as.data.frame(smr_table) %>%
  ungroup() %>%
  arrange(link_no, cis_marker, admission_date, discharge_date, desc(admission_type))

#### 4. Select emergency admissions ----

# select the required data
smr01_emergency <- smr01_sort %>%
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
  ))

# Match on locality and council area names
smr01_final <- smr01_emergency %>%
  left_join(locality_lookup, by = "datazone2011") %>%
  left_join(council_lookup, by = "ca2019") %>%
  # format month variable
  mutate(month_year = format(discharge_date, "%b-%y"))

# Save temp output
arrow::write_parquet(smr01_final, path(data_folder, "SMR01_temp.parquet"))

#### 5. Excel output ----

# create age groups
age_labs <- c("<18", "18-64", "65+")

# Remove discharges after the reporting period
data_age <- smr01_final %>%
  filter(discharge_date >= start_date_excel & discharge_date <= last_date) %>%
  # create age groups
  mutate(age, age_group = cut(age, breaks = c(-1, 17, 64, max(age)), labels = age_labs))

# replicate data for ages 18+
data_18 <- data_age %>%
  filter(age_group == "18-64" | age_group == "65+") %>%
  mutate(age_group = "18+")

# replicate data for all ages
data_all <- data_age %>%
  mutate(age_group = "All Ages")

# Add all files together
data_final <- bind_rows(data_age, data_18, data_all) %>%
  mutate(admissions = 1)

# Calculate number of admissions and add lookup for excel
admissions <- data_final %>%
  group_by(month_year, council_area2019name, age_group) %>%
  summarise(admissions = sum(admissions)) %>%
  mutate(lookup = paste0(age_group, month_year, council_area2019name)) %>%
  rename(
    month = "month_year",
    council = "council_area2019name"
  ) %>%
  # reorder variables
  select(lookup, month, council, age_group, admissions) %>%
  arrange(month, lookup)

# Save excel output
write.xlsx(admissions, path(data_folder, "1a_Emergency_Admissions.xlsx"), overwrite = TRUE)


#### 6. LIST breakdown file ----

# create 5 year age groups
age_labs2 <- c(
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
      labels = age_labs2
    ),
    month_year = format(discharge_date, "01-%m-%Y"),
    admissions = 1
  )

# group up and sum admissions
list_final <- list_output %>%
  group_by(
    council_area2019name, hscp_locality, area_treated, location, specialty,
    significant_facility, age_group, month_year
  ) %>%
  summarise(admissions = sum(admissions)) %>%
  rename(council = "council_area2019name", locality = "hscp_locality", month = "month_year") %>%
  ungroup() %>%
  mutate(
    age_group = as.character(age_group),
    month = dmy(month)
  )

# Save LIST breakdown file
arrow::write_parquet(list_final, path(data_folder, "1a-Admissions-breakdown.parquet"))
