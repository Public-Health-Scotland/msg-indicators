# 0. Set up for MSG Indicators Code

# Packages required for MSG scripts ----

library(odbc) # connecting to SMRA
library(here)
library(dplyr) # data manipulation etc.
library(janitor) # tidy up names
library(magrittr) # for double pipe operator
library(data.table)
library(dtplyr) # Use lazy data tables for quick aggregation
library(lubridate) # working with dates
library(openxlsx) # reading & writing excel files
library(tidylog) # detailed output
library(glue) # combining text
library(fst) # fast data frame storage
library(fs)
library(purrr) # mapping functions
library(readxl)
library(writexl)
library(readr) # Reading of *.rds files
library(stringr) # String manipulation
library(phsmethods)

find_latest_file <- function(directory, regexp) {
  latest_file_path <- fs::dir_info(path = directory,
                                   type = "file",
                                   regexp = regexp,
                                   recurse = TRUE) %>%
    dplyr::arrange(dplyr::desc(.data$birth_time),
                   dplyr::desc(.data$modification_time)) %>%
    dplyr::pull(.data$path) %>%
    magrittr::extract(1)
  
  if (!is.na(latest_file_path)) {
    return(latest_file_path)
  } else {
    cli::cli_abort("There was no file in {.path {directory}} that matched the
                   regular expression {.arg {regexp}}")
  }
}

# Exterior file paths ----

# Indicator 4 breakdowns
ind_4_breakdowns <- 
  fs::path("/conf/LIST_analytics/MSG/2023-06 June/Breakdowns/4-Delayed-Discharge-Breakdowns.rds")
# Latest MSG template
latest_template <- 
  find_latest_file("/conf/irf/03-Integration-Indicators/02-MSG/02-Templates/",
  regexp = "Integration-performance-indicators-v\\d\\.\\d+?\\.xlsx")
# Locality lookup (searches for most recent)
locality_lookup_path <- 
  find_latest_file("/conf/linkage/output/lookups/Unicode/Geography/HSCP Locality/",
  regexp = "HSCP Localities_DZ11_Lookup_\\d+?\\.rds")
# Postcode lookup (searches for most recent)
postcode_lookup_path <- 
  find_latest_file("/conf/linkage/output/lookups/Unicode/Geography/Scottish Postcode Directory/",
  regexp = "Scottish_Postcode_Directory_.+?\\.rds")
# Indicator 5 location (for dashboard)
ind_5_path <- 
  fs::path("/conf/irf/03-Integration-Indicators/02-MSG/01-Data/05-EoL/Final figures.sav")

# Constants ----

reporting_month_string <- "jun23"

lookup_date <- "20220630"

earliest_date <- lubridate::ymd("2017-04-01")

reporting_month_date <- as.Date(format(Sys.Date() - months(2), "%Y-%m-01")) - 1

# Lookups for template/breakdown data ----
postcode_lookup <- readRDS(postcode_lookup_path) %>%
  clean_names() %>%
  dplyr::select(ca2019, hb2019, datazone2011, pc7) %>%
  rename(dr_postcode = "pc7")

locality_lookup <- readRDS(locality_lookup_path) %>%
  clean_names() %>%
  dplyr::select(datazone2011, hscp_locality)

dz_lookup <- readRDS(locality_lookup_path) %>%
  clean_names() %>%
  dplyr::select(ca2019name, ca2011)

council_lookup <- read_csv("/conf/linkage/output/lookups/Unicode/Geography/Scottish Postcode Directory/Codes and Names/Council Area 2019 Lookup.csv") %>%
  clean_names() %>%
  rename(ca2019 = "council_area2019code")

# Functions for the Source dashboard ----

# This function ensures that datasets from different sources have matching Local Authority names
# to the ones we expect for the Source Platform
standard_lca_names <- function(df, council) {
  newdf <- df %>% mutate(council1 = str_replace({{ council }}, " and ", " & "))
  newdf <- newdf %>%
    mutate(council = if_else({{ council }} == "Na h-Eileanan Siar", "Western Isles", council1)) %>%
    select(-council1)
  return(newdf)
}

lca_lookup_tableau <- read_rds(locality_lookup_path) %>%
  select(ca2019name, ca2011) %>%
  standard_lca_names(ca2019name) %>%
  group_by(council) %>%
  summarise(la_code = first(ca2011))

# This function is hard-coded with the standard age groups we use in the MSG template.
main_age_groups <- function(dataset, age_variable) {
  agerecoded <-
    dplyr::mutate(dataset,
      age_groups = dplyr::case_when(
        {{ age_variable }} == "<18" ~ "<18",
        {{ age_variable }} == "65-69" | {{ age_variable }} == "70-74" | {{ age_variable }} == "75-79" |
          {{ age_variable }} == "80-84" | {{ age_variable }} == "85-89" | {{ age_variable }} == "90-94" |
          {{ age_variable }} == "95-99" | {{ age_variable }} == "100+"
        ~ "65+",
        {{ age_variable }} == "18-24" | {{ age_variable }} == "25-29" | {{ age_variable }} == "30-34" |
          {{ age_variable }} == "35-39" | {{ age_variable }} == "40-44" | {{ age_variable }} == "45-49" |
          {{ age_variable }} == "50-54" | {{ age_variable }} == "55-59" | {{ age_variable }} == "60-64"
        ~ "18-64",
        {{ age_variable }} == (999 | NA) ~ "Unknown"
      )
    )
  return(agerecoded)
}

# Function to get the age groups for Indicator 4
ind_4_ages <- function(dataset, age_groups) {
  ind_4_others <- ind_4_master %>% dplyr::mutate(age_groups = dplyr::case_when(
    age_groups == "18-24" | age_groups == "25-29" | age_groups == "30-34" | age_groups == "35-39" |
      age_groups == "40-44" | age_groups == "45-49" | age_groups == "50-54" | age_groups == "55-59" |
      age_groups == "60-64" | age_groups == "65-69" | age_groups == "70-74"
    ~ "18-74",
    age_groups == "75-79" | age_groups == "80-84" | age_groups == "85-89" | age_groups == "90-94" |
      age_groups == "95-99" | age_groups == "100+"
    ~ "75+"
  ))
}

# Five-year age group recoding
# This function is hard-coded with the standard age groups we use in the Source platform.
# Do not use if you need different age groups.
five_year_groups <- function(dataset, age_variable) {
  agerecoded <- dplyr::mutate(dataset,
    age_groups = dplyr::case_when(
      {{ age_variable }} < 18 ~ "<18",
      dplyr::between({{ age_variable }}, 18, 24) ~ "18-24",
      dplyr::between({{ age_variable }}, 25, 29) ~ "25-29",
      dplyr::between({{ age_variable }}, 30, 34) ~ "30-34",
      dplyr::between({{ age_variable }}, 35, 39) ~ "35-39",
      dplyr::between({{ age_variable }}, 40, 44) ~ "40-44",
      dplyr::between({{ age_variable }}, 45, 49) ~ "45-49",
      dplyr::between({{ age_variable }}, 50, 54) ~ "50-54",
      dplyr::between({{ age_variable }}, 55, 59) ~ "55-59",
      dplyr::between({{ age_variable }}, 60, 64) ~ "60-64",
      dplyr::between({{ age_variable }}, 65, 69) ~ "65-69",
      dplyr::between({{ age_variable }}, 70, 74) ~ "70-74",
      dplyr::between({{ age_variable }}, 75, 79) ~ "75-79",
      dplyr::between({{ age_variable }}, 80, 84) ~ "80-84",
      dplyr::between({{ age_variable }}, 85, 89) ~ "85-89",
      dplyr::between({{ age_variable }}, 90, 94) ~ "90-94",
      dplyr::between({{ age_variable }}, 95, 99) ~ "95-99",
      {{ age_variable }} >= 100 ~ "100+",
      {{ age_variable }} == (999 | NA) ~ "Unknown"))
  return(agerecoded)
}

# This function ensures that datasets from different sources have matching Local Authority names
# to the ones we expect for the Source Platform
standard_lca_names <- function(df, council) {
  newdf <- df %>% mutate(council1 = str_replace({{ council }}, " and ", " & "))
  newdf <- newdf %>%
    mutate(council = if_else({{ council }} == "Na h-Eileanan Siar", "Western Isles", council1)) %>%
    select(-council1)
  return(newdf)
}

# Population functions ----

population_fin_year <- function(dataset, yearvariable) {
  dataset <- dataset %>%
    dplyr::mutate(year2 = as.integer({{ yearvariable }}) + 1) %>%
    mutate(year2 = as.character(year2)) %>%
    mutate(year = str_c({{ yearvariable }}, "/", str_sub(year2, start = 3, end = 4))) %>%
    select(-year2)
}

get_population <- function() {
  raw_pops <- read_rds("/conf/linkage/output/lookups/Unicode/Populations/Estimates/DataZone2011_pop_est_2011_2021.rds") %>%
    filter(year > 2015) %>%
    left_join(locality_lookup) %>%
    group_by(year, hscp_locality) %>%
    summarise(across(age0:age90plus, sum, na.rm = TRUE), allages = sum(total_pop)) %>%
    ungroup()
  
  raw_pops_2022 <- raw_pops %>%
    filter(year == 2021) %>%
    mutate(year = 2022)
  
  raw_pops_2023 <- raw_pops %>%
    filter(year == 2021) %>%
    mutate(year = 2023)
  
  pops <- bind_rows(raw_pops, raw_pops_2022, raw_pops_2023) %>%
    rowwise() %>%
    mutate(under18 = sum(c_across(age0:age17))) %>%
    mutate(eighteen_to_64 = sum(c_across(age18:age64))) %>%
    mutate(over65 = sum(c_across(age65:age90plus))) %>%
    mutate(over18 = sum(c_across(age18:age90plus))) %>%
    mutate(eighteen_to_74 = sum(c_across(age18:age74))) %>%
    mutate(over75 = sum(c_across(age75:age90plus))) %>%
    select(-(age0:age90plus))
  
  pops <- pops %>%
    pivot_longer(cols = allages:over75, names_to = "age_groups", values_to = "population") %>%
    mutate(age_groups = case_when(
      age_groups == "under18" ~ "<18",
      age_groups == "eighteen_to_64" ~ "18-64",
      age_groups == "over65" ~ "65+",
      age_groups == "over18" ~ "18+",
      age_groups == "eighteen_to_74" ~ "18-74",
      age_groups == "over75" ~ "75+",
      age_groups == "allages" ~ "All Ages"))
  
  scot_pops <- pops %>%
    mutate(hscp_locality = "All") %>%
    group_by(year, hscp_locality, age_groups) %>%
    summarise(population = sum(population))
  pops <- bind_rows(pops, scot_pops) %>%
    mutate(year = as.character(year))
  return(pops)
}

# Monthly beddays function ----

# use beddays function to count days based on month
monthly_beddays <- function(data,
                            earliest_date = NA,
                            latest_date = NA,
                            pivot_longer = TRUE) {

  # Create a vector of years from the first to last
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
        earliest_date %m+% months(month + (12 * (year - min(years))))
      }
    ) %>%
    map(function(interval_start) {
      # Take the list of months just produced and create a list of
      # one month intervals
      lubridate::interval(interval_start, interval_start %m+% months(1))
    }) %>%
    # Give them names these will be of the form MMM_YYYY
    setNames(str_c(rep(month_names, length(years)), "_", sort(rep(years, 12)), "_beddays"))

  # Remove any months which are after the latest_date
  month_intervals <- month_intervals[map_lgl(month_intervals,    ~ latest_date > lubridate::int_start(.))]


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
        int_shift(interval(data %>%
                             pull(admission_date),
                           data %>%
                             pull(discharge_date)),
                  by = days(1)),
        month_interval),
        unit = "days")
    }))

  names(month_intervals) <- stringr::str_replace(
    names(month_intervals),
    "_beddays", "_admissions")

  data <- data %>%
    # map_dfc will return a single dataframe with all the others bound by column
    bind_cols(map_dfc(month_intervals, function(month_interval) {
      if_else(data %>%
        pull(discharge_date) %>%
        floor_date(unit = "month") == int_start(month_interval),
      1L,
      NA_integer_)
    }))
  # Default behaviour
  # Turn all of the Mmm_YYYY (e.g. Jan_2019) into a Month and Year variable
  # This means many more rows so we drop any which aren't interesting
  # i.e. all NAs
  if (pivot_longer) {
    data <- data %>%
      # Use pivot longer to create a month, year and beddays column which
      # can be used to aggregate later
      pivot_longer(cols = contains("_20"),
                   names_to = c("month", "year", ".value"),
                   names_pattern = "^([A-Z][a-z]{2})_(\\d{4})_([a-z]+)$",
                   names_ptypes = list(month = factor(levels = as.vector(lubridate::month(1:12,
                                                                                          label = TRUE)),
                                                      ordered = TRUE),
                                       year = factor(levels = years,
                                                     ordered = TRUE)),
                   values_drop_na = TRUE)
  }
  
  return(data)
}
