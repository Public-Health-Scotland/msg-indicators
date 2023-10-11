# Routine checks on MSG Update
# RB 25/07/2022


#### Set up ----

# file paths
data_folder <- "/conf/irf/03-Integration-Indicators/02-MSG/06-R-Project/Data/"
code_folder <- "/conf/irf/03-Integration-Indicators/02-MSG/06-R-Project/Code/"
previous_template <- "v1.60"
reporting_month <- "Dec-22"
previous_month <- "Nov-22"
reporting_quarter <- "Dec-22"
last_quarter <- "Sep-22"

# load packages & lookups required
library(fs)
# source(path(code_folder, "0. Packages & functions.R"))

# Quick function to check for missing values in vectors
is_missing <- function(x) {
  if (typeof(x) != "character") {
    rlang::abort(
      message = glue::glue("You must supply a character vector, but {class(x)} was supplied.")
    )
  }
  return(is.na(x) | x == "")
}

# Function to bring several reports together
collate_reports <- function(new_data, old_data, measure, indicator) {
  report_one <- left_join(new_data, old_data,
    by = c("month", "council", "age_group"),
    suffix = c("_new", "_old")
  ) %>%
    filter(age_group == "All Ages") %>%
    mutate(
      diff = round(.[[glue(measure, "_new")]] - .[[glue(measure, "_old")]], digits = 2),
      pct_change = scales::percent(diff / .[[glue(measure, "_old")]]),
      issue = !dplyr::between(diff / .[[glue(measure, "_old")]], -.05, .05)
    ) %>%
    filter(month != reporting_month) %>%
    filter(issue == TRUE)

  if (indicator %in% c("1a", "3_adm", "3_att")) {
    report_two <- left_join(
      new_data %>% filter(month == reporting_month),
      old_data %>% filter(month == previous_month),
      by = c("council", "age_group"),
      suffix = c("_new", "_old")
    ) %>%
      filter(age_group == "All Ages") %>%
      mutate(
        diff = round(.[[glue(measure, "_new")]] - .[[glue(measure, "_old")]], digits = 2),
        pct_change = scales::percent(diff / .[[glue(measure, "_old")]])
      ) %>%
      arrange(pct_change) %>%
      select(council, age_group, contains(c("new", "old")), diff, pct_change)
  } else {
    report_two <- left_join(
      new_data %>% filter(month == reporting_quarter),
      old_data %>% filter(month == last_quarter),
      by = c("council", "age_group"),
      suffix = c("_new", "_old")
    ) %>%
      filter(age_group == "All Ages") %>%
      mutate(
        diff = round(.[[glue(measure, "_new")]] - .[[glue(measure, "_old")]], digits = 2),
        pct_change = scales::percent(diff / .[[glue(measure, "_old")]])
      ) %>%
      arrange(pct_change) %>%
      select(council, age_group, contains(c("new", "old")), diff, pct_change)
  }

  list_names <- c(glue("{indicator}_diffs"), glue("{indicator}_comp"))

  report_list <- list(report_one, report_two) %>% setNames(list_names)

  return(report_list)
}

#### Read in all relevant files ----

# 1a
output_1a <- read_xlsx("Data/1a_Emergency_Admissions.xlsx") %>%
  select(-lookup)

previous_output_1a <- read_xlsx(
  glue("/conf/irf/03-Integration-Indicators/02-MSG/02-Templates/Integration-performance-indicators-{previous_template}.xlsx"),
  sheet = "Data1",
  range = cell_cols("B:E"),
  col_names = TRUE
)

# 2a
output_2a <- read_xlsx(path(data_folder, "2a_Acute_Beddays.xlsx")) %>%
  select(-lookup)

previous_output_2a <- read_xlsx(
  glue("/conf/irf/03-Integration-Indicators/02-MSG/02-Templates/Integration-performance-indicators-{previous_template}.xlsx"),
  sheet = "Data2",
  range = cell_cols("B:E"),
  col_names = TRUE
)

# 2b
output_2b <- read_rds(path(data_folder, "2b_GLS_beddays.rds")) %>%
  rename(month = quarter_year)

previous_output_2b <- read_xlsx(
  glue("/conf/irf/03-Integration-Indicators/02-MSG/02-Templates/Integration-performance-indicators-{previous_template}.xlsx"),
  sheet = "Data2",
  range = cell_cols("H:L"),
  col_names = TRUE
) %>%
  rename(month = quarter_year) %>%
  filter(service == "GLS")

# 2c
output_2c <- read_xlsx(path(data_folder, "2bc_GLS_MH_beddays.xlsx")) %>%
  filter(service == "MH") %>%
  select(-lookup) %>%
  rename(month = quarter_year)

previous_output_2c <- read_xlsx(
  glue("/conf/irf/03-Integration-Indicators/02-MSG/02-Templates/Integration-performance-indicators-{previous_template}.xlsx"),
  sheet = "Data2",
  range = cell_cols("H:L"),
  col_names = TRUE
) %>%
  rename(month = quarter_year) %>%
  filter(service == "MH")

# 3
output_3 <- read_xlsx(path(data_folder, "3-A&E-For-Template.xlsx")) %>%
  rename(
    month = data_month,
    council = council_area
  )

previous_output_3 <- read_xlsx(
  glue("/conf/irf/03-Integration-Indicators/02-MSG/02-Templates/Integration-performance-indicators-{previous_template}.xlsx"),
  sheet = "A&EData",
  range = cell_cols("C:L"),
  col_names = TRUE
) %>%
  clean_names() %>%
  rename(
    month = data_month,
    council = council_area_desc,
    age_group = age_grp_18
  )

# Quality check function
quality_check <- function(data, indicator) {
  return <- data %>%
    mutate(
      missing_age = is_missing(age_group),
      missing_month = is_missing(month),
      missing_council = is_missing(council)
    ) %>%
    group_by(council) %>%
    summarise(across(c(missing_age:missing_council), sum)) %>%
    mutate(indicator = {{ indicator }})

  return(return)
}

# Run quality checks and comparisons
quality_checks <- list("quality_checks" = bind_rows(
  quality_check(output_1a, "1a"),
  quality_check(output_2a, "2a"),
  quality_check(output_2b, "2b"),
  quality_check(output_2c, "2c"),
  quality_check(output_3, "3")
))

final_checks <- c(
  collate_reports(output_1a, previous_output_1a, "admissions", "1a"),
  collate_reports(output_2a, previous_output_2a, "unplanned_beddays", "2a"),
  collate_reports(output_2b, previous_output_2b, "unplanned_beddays", "2b"),
  collate_reports(output_2c, previous_output_2c, "unplanned_beddays", "2c"),
  collate_reports(output_3, previous_output_3, "admissions", "3_adm"),
  collate_reports(output_3, previous_output_3, "number_of_attendances", "3_att"),
  quality_checks
)

write_xlsx(final_checks, glue("Checks/Raw-Data-Checks-{reporting_month}.xlsx"))
