# MSG Indicators - Indicator 4: Delayed Discharges
# Uses data from DD publication to produce data for excel output
# Formats and saves DD breakdown file produced by DD team

# Last Updated: RB 06/09/2022


#### 1. Set up ----

# # file paths
# data_folder <- "Data/"
# code_folder <- "Code/"
# 
# # load packages & lookups required
# library(fs)
# source(path(code_folder, "0 - Packages & functions.R"))

# pick up file from DD team - date needs changed monthly
dd_list <- read_rds("Data/2023-07_dd-msg.rds")

#### 2. Excel output ----

# Create reporting month in a format that the open data expects
# last_date <- str_sub(as.character(as.Date(format(Sys.Date() - months(1), '%Y-%m-01')) - 1), 1, 7)
last_date <- "2023-06"
# Create reporting month in a format that our template expects
extra_column <- str_to_lower(str_c(month.abb[as.integer(str_sub(last_date, 6))], "_", str_sub(last_date, 3, 4)))

# Read in open data
open_data <- read_csv(glue("https://www.opendata.nhs.scot/dataset/52591cba-fd71-48b2-bac3-e71ac108dfee/resource/513d2d71-cf73-458e-8b44-4fa9bccbf50a/download/{last_date}_delayed-discharge-beddays-council-area.csv")) %>% 
  # Select specific columns
  select(c(1,2,4,6,8)) %>% 
  # R-standard names
  clean_names() %>% 
  # We only want the beddays for the reporting month
  filter(month_of_delay == as.integer(str_replace(last_date, "-", ""))) %>% 
  # Change the age group names to have a '+' instead of 'plus'
  mutate(age_group = str_replace(age_group, "plus", "+"),
         # Get LCA names
         la = phsmethods::match_area(ca),
         # Recode delay reasons to match our template
         reason_for_delay = case_when(
           reason_for_delay == "All Delay Reasons" ~ "All reasons",
           str_detect(reason_for_delay, "Code 9") == TRUE ~ "Code 9",
           reason_for_delay == "Patient and Family Related Reasons" ~ "Patient/Carer/Family-related reasons",
           TRUE ~ reason_for_delay
         )) %>% 
  # Change some LCA names and recode NA as 'Other'
  mutate(la = case_when(
    str_detect(la, " and ") == TRUE ~ str_replace(la, " and ", " & "),
    is.na(la) == TRUE ~ "Other",
    TRUE ~ la
  )) %>% 
  # We do the sum for Scotland in our own template
  filter(la != "Scotland") %>% 
  # Rename the number of beddays column to 'mmm_yy' format
  rename(!!extra_column := number_of_delayed_bed_days) %>% 
  # 'Standard' is the coding for the sum of PCF reasons and HSC reasons, so we pivot longer and 
  # then aggregate to get the sums
  mutate(temp_reason = if_else(
    reason_for_delay == "Patient/Carer/Family-related reasons" | reason_for_delay == "Health and Social Care Reasons",
    "Standard", NA_character_)) %>% 
  pivot_longer(cols = c(reason_for_delay, temp_reason), values_to = "reason_for_delay", values_drop_na = TRUE) %>%
  select(-name) %>% 
  group_by(la, age_group, reason_for_delay) %>% 
  summarise(across(!!extra_column, sum, na.rm = TRUE), .groups = "keep")

# Bring in existing data from template
existing_data <- read_excel(path = "Data/4-Delayed-Discharges.xlsx")

# Join the one extra column to our existing base data
output <- left_join(existing_data, open_data, by = c("la", "age_group", "complex_needs_flag" = "reason_for_delay"))

# Archive old data
# write_xlsx(existing_data, glue("Archive/4-Delayed-Discharges-pre-{Sys.Date()}"))
# Save new data
write_xlsx(output, "Data/4-Delayed-Discharges.xlsx")

#### 3. LIST output ----

dd_list %<>% 
  mutate(month = dmy(month)) %>% 
  filter(month <= dmy("01-06-2023"))

# save R & SPSS files
arrow::write_parquet(dd_list, "Data/4-Delayed-Discharge-Breakdowns.parquet")
