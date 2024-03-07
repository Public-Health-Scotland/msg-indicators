# Setup

question56 <- .rs.askForPassword("Are you updating Indicators 5 and 6?")
questionc <- .rs.askForPassword("Do you want to do completeness?")

# SECTION 1: Indicator 1 ----

# Get most recent breakdown file for indicator 1
ind_1 <- arrow::read_parquet("Data/1a-Admissions-breakdown.parquet") %>% 
  # Recode age groups into "<18", "18-64", and "65+"
  main_age_groups(age_group) %>% 
  lazy_dt() %>% 
  # Aggregate the admissions
  group_by(council, locality, area_treated, age_groups, month) %>% 
  summarise(one_admissions = sum(admissions), .groups = "keep") %>% 
  ungroup() %>% 
  as_tibble() 

# SECTION 2: Indicator 2 ----
# Firstly, read in and wrangle the indicator 2a data
# We use the SMR extract here because we want CIJ-level data

two_a_beddays <- function(data, 
                  earliest_date, 
                  latest_date, 
                  add_admissions = FALSE,
                  pivot_longer = TRUE)
{# Create a vector of years from the first to last
  years <- c(lubridate::year(earliest_date):lubridate::year(latest_date))
  
  # Create a vector of month names
  month_names <- lubridate::month(1:12, label = T)
  
  # Use purrr to create a list of intervals these will be
  # date1 -> date1 + 1 month
  # for every month in the time period we're looking at
  month_intervals <-
    purrr::map2(
      # The first parameter produces a list of the years
      # The second produces a list of months
      sort(rep(years, 12)), rep(0:11, length(years)),
      function(year, month) {
        # Initialise a date as start_date + x months * (12 * y years)
        dmy(glue("01-01-{year(earliest_date)}")) %m+% months(month + (12 * (year - min(years))))
      } 
    ) %>%
    map(function(interval_start) {
      # Take the list of months just produced and create a list of
      # one month intervals
      lubridate::interval(interval_start, interval_start %m+% months(1))
    }) %>%
    # Give them names these will be of the form MMM_YYYY
    setNames(str_c(
      rep(month_names, length(years)),
      "_", sort(rep(years, 12)), "_beddays"
    ))
  
  # Remove any months which are after the latest_date
  month_intervals <- month_intervals[map_lgl(
    month_intervals,
    ~ latest_date > lubridate::int_start(.) & earliest_date < lubridate::int_end(.)
  )]
  
  
  # Use the list of intervals to create new varaibles for each month
  # and work out the beddays
  data <- data %>%
    # map_dfc will return a single dataframe with all the others bound by column
    bind_cols(map_dfc(month_intervals, function(month_interval) {
      # Use intersect to find the overlap between the month of interest
      # and the stay, then use time_length to measure the length in days
      time_length(intersect(
        # use int_shift to move the interval forward by one day
        # This is so we count the last day (and not the first), which is
        # the correct methodology
        int_shift(interval(
          data %>%
            pull(admission_date),
          data %>%
            pull(discharge_date)
        ),
        by = days(1)
        ),
        month_interval
      ),
      unit = "days"
      )
    }))
  
  names(month_intervals) <- stringr::str_replace(
    names(month_intervals),
    "_beddays", "_admissions"
  )
  
  if (add_admissions)
  {data <- data %>%
    # map_dfc will return a single dataframe with all the others bound by column
    bind_cols(map_dfc(month_intervals, function(month_interval) {
      if_else(data %>%
                pull(discharge_date) %>%
                floor_date(unit = "month") == int_start(month_interval),
              1L,
              NA_integer_
      )
    }))}
  # Default behaviour
  # Turn all of the Mmm_YYYY (e.g. Jan_2019) into a Month and Year variable
  # This means many more rows so we drop any which aren't interesting
  # i.e. all NAs
  if (pivot_longer) {
    data <- data %>%
      # Use pivot longer to create a month, year and beddays column which
      # can be used to aggregate later
      pivot_longer(
        cols = contains("_20"),
        names_to = c("month", "year", ".value"),
        names_pattern = "^([A-Z][a-z]{2})_(\\d{4})_([a-z]+)$",
        names_ptypes = list(
          month = factor(
            levels = as.vector(lubridate::month(1:12,
                                                label = TRUE
            )),
            ordered = TRUE
          ),
          year = factor(
            levels = years,
            ordered = TRUE
          )
        ),
        values_drop_na = TRUE
      ) %>% 
      mutate(month_proper = dmy(glue("01-{month}-{year}"))) %>% 
      select(-month, -year, -month_year)
  }}

ind_2a <- arrow::read_parquet("Data/SMR01_temp.parquet") %>% 
  select(link_no, cis_marker, admission_date, discharge_date, month_year, area_treated,
         `council` = council_area2019name, `locality` = hscp_locality, age) %>% 
  # Group ages into 5-year band
  five_year_groups(age) %>% 
  # Re-calculate the beddays per month
  two_a_beddays(earliest_date = earliest_date, latest_date = reporting_month_date) %>% 
  # Aggregate and sum the beddays for each month
  lazy_dt() %>% 
  group_by(council, locality, area_treated, age_groups, `month` = month_proper) %>% 
  summarise(unplanned_beddays = sum(beddays, na.rm = TRUE)) %>% 
  # Replace the month columns with rows
  as_tibble() %>% 
  # Some formatting to deal with month names and numbers
  rename(age_group = age_groups)

ind_2b <- arrow::read_parquet("Data/2b-GLS-Beddays-breakdown.parquet") %>% 
  # Aggregate and sum the beddays for each month
  lazy_dt() %>% 
  group_by(council, locality, area_treated, age_group, month) %>% 
  summarise(across(unplanned_beddays, sum, na.rm = TRUE), .groups = "keep") %>% 
  # Replace the month columns with rows
  as_tibble()

ind_2c <- arrow::read_parquet("Data/2c-MH-Beddays-breakdown.parquet") %>% 
  # Aggregate and sum the beddays for each month
  lazy_dt() %>% 
  group_by(council, locality, area_treated, age_group, month) %>% 
  summarise(across(unplanned_beddays, sum, na.rm = TRUE), .groups = "keep") %>% 
  # Replace the month columns with rows
  as_tibble()

# Bring in data for indicators 2b and 2c, create an id based on input
ind_2 <- bind_rows(ind_2a, ind_2b, ind_2c, .id = "indicator") %>% 
  # Remove unnecessary variables
  main_age_groups(age_group) %>% 
  # Use pivot_wider to denote beddays for each sub-indicator
  pivot_wider(id_cols = c("council", "locality", "area_treated", "age_groups", "month"),
              names_from = indicator, 
              values_from = unplanned_beddays,
              values_fn = sum,
              values_fill = 0) %>% 
  rename(two_a = "1", two_b = "2", two_c = "3") %>% 
  standard_lca_names(council)

rm(ind_2a, ind_2b, ind_2c)

# SECTION 3: Indicator 3 ----

# Read in data from A&E breakdowns file
ind_3 <- arrow::read_parquet("Data/3-A&E-Breakdowns.parquet") %>% 
  # Drop unnecessary variables
  select(-ref_source, -location, -location_name) %>% 
  # Rename some variables to align with ind_1 and ind_2
  rename(age_groups = age_group) %>% 
  # Change age group formats to fit rest of script
  mutate(age_groups = if_else(age_groups == "0-17", "<18", age_groups)) %>% 
  mutate(age_groups = if_else(age_groups == "Over 100", "100+", age_groups)) %>% 
  main_age_groups(age_groups) %>% 
  # Recode area_treated 
  mutate(area_treated = case_when(
    area_treated == "IN" ~ "Within HBres",
    area_treated == "OUT" ~ "Outwith HBres"
  )) %>% 
  lazy_dt() %>% 
  # Aggregate attendances by break variables
  group_by(council, locality, area_treated, age_groups, month) %>% 
  summarise(three_attendances = sum(attendances),
            one_b_ae_admissions = sum(admissions),
            three_number_meeting_target = sum(number_meeting_target),
            .groups = "keep") %>% 
  as_tibble() %>% 
  mutate(council = str_replace(council," and ", " & ")) %>% 
  filter(month >= dmy("01-04-2017"))

# SECTION 4: Indicator 4 ----

ind_4_master <- arrow::read_parquet("Data/4-Delayed-Discharge-Breakdowns.parquet") %>% 
  # Get rid of any undefined council names
  filter(council != "Other") %>% 
  # We have to rename councils and derived partnerships so they match one another
  mutate(council = if_else(
    council == "Comhairle nan Eilean Siar", "Western Isles", council)) %>% 
  mutate(derived_partnership = case_when(
    derived_partnership == "Na h-Eileanan Siar" ~ "Western Isles",
    derived_partnership == "Edinburgh" ~ "City of Edinburgh",
    (derived_partnership == "Clackmannanshire & Stirling") & (locality == "Clackmannanshire") ~ "Clackmannanshire",
    (derived_partnership == "Clackmannanshire & Stirling") & (locality == "Rural Stirling") ~ "Stirling",
    (derived_partnership == "Clackmannanshire & Stirling") & (locality == "Stirling City with the Eastern Villages Bridge of Allan and Dunblane") ~ "Stirling",
    TRUE ~ derived_partnership)) %>%
  # Create an 'other localities' group for any places where the treatment was outside partnership
  mutate(locality = if_else(council != derived_partnership | is.na(derived_partnership), "Other Localities", locality)) %>% 
  rename(age_groups = age_group)

# We create two seperate tables here, one for "18+", which functions as an 'all' group for 
# indicator 4, and one for the other groups
ind_4_over18 <- ind_4_master %>% mutate(age_groups = "18+")
# This function differs from the other defined age groups 
ind_4_others <- ind_4_master %>% ind_4_ages(age_groups = age_groups)

# Add the datasets together and remove unnecessary ones
ind_4 <- bind_rows(ind_4_over18, ind_4_others)
rm(ind_4_master, ind_4_others, ind_4_over18)

# Use pivot_wider to create columns of bed days based on reason for delay
ind_4 <- ind_4 %>% 
  # These are the break variables
  pivot_wider(id_cols = c("council", "locality", "area_treated", "age_groups", "month"),
              # Get the names from reason_for_delay and sum the beddays
              names_from = reason_for_delay, values_from = delayed_bed_days, 
              values_fn = sum,
              # Fill any NAs with 0
              values_fill = 0) %>% 
  # Rename columns for ease
  rename(four_c9_beddays = `Code 9`, four_hsc_beddays = `Health and Social Care Reasons`, four_pcf_beddays = `Patient/Carer/Family-related Reasons`) %>% 
  # Get the total beddays across the split reasons
  rowwise() %>% 
  mutate(four_all_beddays = sum(c_across(four_hsc_beddays:four_pcf_beddays)))

# SECTION 5: Bringing indicators 1, 2 and 3 into the same data frame ----

ind_123 <- bind_rows(ind_1, ind_2, ind_3) %>% 
  # Use this opportunity to align Council names with those in ind_4
  standard_lca_names(council) %>% 
  select("council", "locality", "area_treated", "age_groups", "month", "one_admissions",
         "one_b_ae_admissions", "two_a", "two_b", "two_c", "three_attendances", "three_number_meeting_target") %>% 
  lazy_dt() %>% 
  group_by(council, locality, age_groups, area_treated, month) %>% 
  summarise(across(one_admissions:three_number_meeting_target, ~ sum(.x, na.rm = T))) %>% 
  ungroup() %>% 
  as_tibble()

# We need to add 'All Ages' groups here for population handling in Tableau
temp_18plus <- ind_123 %>% 
  # Select when age isn't under 18 and isn't missing
  filter(age_groups != "<18" & !is.na(age_groups)) %>% 
  # Create '18+' age group
  mutate(age_groups = "18+") %>% 
  # Aggregate
  lazy_dt() %>% 
  group_by(council, locality, age_groups, area_treated, month) %>% 
  summarise(across(one_admissions:three_number_meeting_target, 
                   ~ sum(.x, na.rm = TRUE))) %>% 
  ungroup() %>% 
  as_tibble()

temp_allages <- ind_123 %>% 
  #Similarly, crate an 'All Ages' group that includes every row
  mutate(age_groups = "All Ages") %>% 
  # Aggregate
  lazy_dt() %>% 
  group_by(council, locality, age_groups, area_treated, month) %>% 
  summarise(across(one_admissions:three_number_meeting_target, 
                   ~ sum(.x, na.rm = TRUE))) %>% 
  ungroup() %>% 
  as_tibble()

# Add the two temporary data frames to ind_123
ind_123 <- bind_rows(ind_123, temp_allages, temp_18plus)
  
# Remove the temp files
rm(temp_allages, temp_18plus)

# SECTION 6: Bringing indicators 1, 2, 3 and 4 together ----

# Join the data frame with 1-3 with the data frame for 4
ind_1234 <- full_join(ind_123, ind_4)

# Join dz_lookup to the main data frame to get la_codes. These are required for Tableau
# security filters
ind_1234 <- left_join(ind_1234, lca_lookup_tableau) 

# SECTION 7: Rolling 12-month totals ----

rolling_output <- data.frame()
rolling_input <- ind_1234 %>% 
  arrange(council, la_code, locality, area_treated, age_groups, month)

start_date_rolling <- earliest_date
end_date_rolling <- start_date_rolling + months(11)

while (end_date_rolling <= reporting_month_date) {
  temp <- rolling_input %>% 
    filter(month %within% interval(start_date_rolling, end_date_rolling)) %>% 
    mutate(rolldate = end_date_rolling) %>% 
    lazy_dt() %>% 
    group_by(council, la_code, locality, age_groups, area_treated) %>% 
    summarise(across(one_admissions:four_all_beddays, sum, na.rm = T), 
              rolldate = first(rolldate)) %>% 
    ungroup() %>% 
    as_tibble()
  rolling_output <- bind_rows(rolling_output, temp)
  start_date_rolling <- start_date_rolling + months(1)
  end_date_rolling <- end_date_rolling + months(1)
}

rolling_output <- rolling_output %>% 
  rename(r12_one_admissions = one_admissions,
         r12_one_b_ae_admissions = one_b_ae_admissions,
         r12_two_a = two_a,
         r12_two_b = two_b,
         r12_two_c = two_c,
         r12_three_attendances = three_attendances,
         r12_three_number_meeting_target = three_number_meeting_target,
         r12_four_all_beddays = four_all_beddays,
         r12_four_pcf_beddays = four_pcf_beddays,
         r12_four_hsc_beddays = four_hsc_beddays,
         r12_four_c9_beddays = four_c9_beddays,
         month = rolldate)

# Join the regular measure and the rolling 12-month measures
ind_all_and_r12 <- full_join(ind_1234, rolling_output)

# SECTION 8: Scotland totals and populations ----

# Create totals for the whole of Scotland
scotland_totals <- ind_all_and_r12 %>% 
  filter(!is.na(locality)) %>% 
  mutate(council = "Scotland", locality = "All", la_code = "S99999999") %>%
  relocate(la_code, .after = "month") %>% 
  lazy_dt() %>% 
  group_by(council, locality, age_groups, area_treated, month, la_code) %>% 
  summarise(across(one_admissions:r12_four_all_beddays, ~ sum(.x, na.rm = TRUE))) %>% 
  ungroup() %>% 
  as_tibble()
ind_all_and_r12 <- bind_rows(ind_all_and_r12, scotland_totals) %>% 
  mutate(year = as.character(year(month)))
rm(scotland_totals)

ind_final <- left_join(ind_all_and_r12, get_population(), 
                       by = c("locality" = "hscp_locality", "year", "age_groups")) %>% 
  select(-year) %>% 
  relocate(la_code, .after = "month")

# Tidy up 
rm(dz_lookup, ind_1, ind_2, ind_3, ind_4, ind_123, ind_1234, ind_all_and_r12)

# Add totals for Clackmannanshire & Stirling
cs <- ind_final %>%
  filter(council %in% c("Clackmannanshire", "Stirling")) %>%
  mutate(council = "Clackmannanshire & Stirling") %>% 
  relocate(la_code, .after = "month") %>% 
  lazy_dt() %>% 
  group_by(council, locality, age_groups, area_treated, month, la_code) %>% 
  summarise(across(one_admissions:last_col(), ~ sum(.x, na.rm = TRUE))) %>% 
  ungroup() %>% 
  as_tibble()
  
ind_final <- bind_rows(ind_final, cs) %>% 
  select("council", 
         "locality", 
         "age_groups", 
         "area_treated", 
         "month", 
         "la_code",
         "one_admissions", 
         "one_b_ae_admissions",
         "two_a", 
         "two_b", 
         "two_c", 
         "three_attendances", 
         "three_number_meeting_target",
         "four_hsc_beddays",
         "four_c9_beddays", 
         "four_pcf_beddays", 
         "four_all_beddays", 
         "r12_one_admissions", 
         "r12_one_b_ae_admissions",
         "r12_two_a", 
         "r12_two_b", 
         "r12_two_c", 
         "r12_three_attendances", 
         "r12_three_number_meeting_target",
         "r12_four_hsc_beddays", 
         "r12_four_c9_beddays", 
         "r12_four_pcf_beddays", 
         "r12_four_all_beddays", 
         "population")
rm(cs)

# SECTION 9: Rates for all measures ----

# Create an 'all localities' group for each partnership, but not Scotland
# as that is already set to 'all'.
temp_alllocalities <- ind_final %>% 
  filter(council != "Scotland") %>% 
  mutate(locality = "All") %>% 
  lazy_dt() %>% 
  group_by(across(council:la_code)) %>% 
  summarise(across(one_admissions:population, ~ sum(.x, na.rm = TRUE))) %>% 
  as_tibble()
ind_final <- bind_rows(ind_final, temp_alllocalities) %>% 
  rename(partnership = council)
rm(temp_alllocalities)

# Create rate variables
ind_final <- ind_final %>% mutate(
  one_rate = (one_admissions/population)*1000,
  one_b_rate = (one_b_ae_admissions/population)*1000,
  two_a_rate = (two_a/population)*1000,
  two_b_rate = (two_b/population)*1000,
  two_c_rate = (two_c/population)*1000,
  three_rate = (three_attendances/population)*1000,
  four_all_rate = (four_all_beddays/population)*1000,
  four_c9_rate = (four_c9_beddays/population)*1000,
  four_hsc_rate = (four_hsc_beddays/population)*1000,
  four_pcf_rate = (four_pcf_beddays/population)*1000) %>% 
  select(-population) %>% 
  # Rename for Tableau
  rename(Partnership = partnership,
         Locality = locality,
         AGE_GROUPS = age_groups,
         AreaTreated = area_treated,
         LA_Code = la_code,
         One_Admissions = one_admissions,
         Two_a = two_a,
         Two_b = two_b,
         Two_c = two_c,
         Three_Attendances = three_attendances,
         Four_all_beddays = four_all_beddays,
         Four_C9_beddays = four_c9_beddays,
         Four_HSC_beddays = four_hsc_beddays,
         Four_PCF_beddays = four_pcf_beddays,
         One_Admissions_R12 = r12_one_admissions,
         Two_a_R12 = r12_two_a,
         Two_b_R12 = r12_two_b,
         Two_c_R12 = r12_two_c,
         Three_Attendances_R12 = r12_three_attendances,
         Four_all_beddays_R12 = r12_four_all_beddays,
         Four_C9_beddays_R12 = r12_four_c9_beddays,
         Four_HSC_beddays_R12 = r12_four_hsc_beddays,
         Four_PCF_beddays_R12 = r12_four_pcf_beddays,
         One_rate = one_rate,
         Two_a_rate = two_a_rate,
         Two_b_rate = two_b_rate,
         Two_c_rate = two_c_rate,
         Three_rate = three_rate,
         Four_all_rate = four_all_rate,
         Four_C9_rate = four_c9_rate,
         Four_HSC_rate = four_hsc_rate,
         Four_PCF_rate = four_pcf_rate)

haven::write_sav(ind_final, "Data/MSG Tableau 1 to 4.sav")
write_csv(ind_final, "Data/MSG Tableau 1 to 4.csv", na="")

# SECTION 10: Indicators 5 and 6 ----

if (str_detect(question56, "[Yy]")){
  ind_5 <- arrow::read_parquet("Annual Data/Final figures.parquet") %>% 
    clean_names() %>% 
    filter(!(finyear %in% c("2013/2014", "2014/2015"))) %>% 
    mutate(year = str_c(str_sub(finyear, 1, 5), str_sub(finyear, 8, 9)),
           indicator = "5") %>% 
    mutate(year = if_else(year == "2022/23", "2022/23p", year)) %>% 
    rename(council = lca) %>% 
    rename_with(~ stringr::str_sub(.x, 6, -1), .cols = perc_community:perc_large) %>% 
    select(-finyear)
  ind_6 <- read_excel(path(latest_template), sheet = "Data6", range = "B3:S1200",
                      col_names = TRUE) %>% 
    clean_names() %>% 
    select(year, age_grp, partnership, acute:home_unsupported_18) %>% 
    filter(!(year %in% c("2013/14", "2014/15"))) %>% 
    mutate(year = as.character(year),
           indicator = "6") %>% 
    rename_with(~ gsub('.{3}$', '', .x), .cols = community_hospital_14:home_unsupported_18) %>% 
    rename(council = partnership)
  ind_5_6 <- bind_rows(ind_5, ind_6) %>% 
    standard_lca_names(council) %>% 
    mutate(council = if_else(council == "Stirling & Clackmannanshire", "Clackmannanshire & Stirling", council)) %>% 
    rename(
      `Age_Groups` = age_grp,
      `Indicator` = indicator,
      `Community_Bed_Days` = community_beddays,
      `Community_Hosp_Bed_Days` = community_hosp_beddays,
      `Community_Hosp_Percent` = community_hosp,
      `Community_Percent` = community,
      `CommunityHospitalRate6` = community_hospital,
      `HomeSupportedRate6` = home_supported,
      `HomeUnsupportedRate6` = home_unsupported,
      `HospiceRate6` = hospice,
      `Large_Bed_Days` = large_beddays,
      `Large_Percent` = large,
      `LHRate6` = acute,
      `Palliative_Bed_Days` = palliative_beddays,
      `Palliative_Percent` = palliative,
      `CareHomeRate6` = care_home,
      `possible_bed_days` = possible_beddays,
      `Partnership` = council
    )
  rm(ind_5, ind_6)
  haven::write_sav(ind_5_6, "Data/MSG Tableau 5 and 6.sav")
  write_csv(ind_5_6, "Data/MSG Tableau 5 and 6.csv", na="")
  # rm(ind_5_6)
}

# SECTION 11: Completeness ----]

if (str_detect(questionc, "[Yy]")) {
smr_completeness <-
  # Firstly, read in the SMR01 completeness from the latest template
  left_join(
    read_excel(latest_template, sheet = "Completeness", range = "A3:Y18") %>%
      # Turn the months into a variable and the completeness values under 'smr01'
      pivot_longer(cols = c(2:last_col()), names_to = "Month", values_to = "smr01") %>%
      # Format months
      mutate(Month = str_to_title(str_replace(Month, "-", "")),
             smr01 = smr01 * 100),
    # This is the second argument to the left_join(), covering SMR01E
    read_excel(latest_template, sheet = "Completeness", range = "A23:E38") %>%
      pivot_longer(cols = c(2:last_col()), names_to = "Month", values_to = "smr01e") %>%
      mutate(
        Month = str_to_title(str_replace(Month, "-", "")),
        smr01e = as.numeric(smr01e) * 100
      )) %>%
  # Second left join, which joins the above df to the SMR04 one
  left_join(., read_excel(latest_template, sheet = "Completeness", range = "A43:E58") %>% 
              pivot_longer(cols = c(2:last_col()), names_to = "Month", values_to = "smr04") %>%
              mutate(
                Month = str_to_title(str_replace(Month, "-", "")),
                smr04 = as.numeric(smr04) * 100
              )) %>% 
  clean_names()
haven::write_sav(smr_completeness, "Data/Completeness.sav")
write_csv(smr_completeness, "Data/Completeness.csv", na="")
rm(smr_completeness)
}
