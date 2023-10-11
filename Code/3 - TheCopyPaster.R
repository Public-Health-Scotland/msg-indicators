## All of the extracts from BOXI start with this
file_front <- "Data/MSG-Monthly-Update-Data"

## Set up a function that can copy and paste all the rows from the
## Excel files and put them into one table
pastyboy <- function(pathend) {
  # Get the file in question, denoted by pathend
  path <- path(glue(file_front, pathend, ".xlsx"))
  set <- path %>%
    # Command for reading all sheets
    excel_sheets() %>%
    # Map function reads each sheet and puts them into one data frame
    map_df(~ read_excel(path = path, sheet = .x), .id = "sheet") %>%
    # Clean variable names to R standard
    clean_names() %>%
    # Change any missing values to empty string
    mutate(across(c("age_grp_18", "age_grp_65_18_64", "age_grp_all_ages"), ~ replace_na(.x, ""))) %>%
    # The age variables are named differently in each sheet so this combines them
    unite(age_group, c(age_grp_18, age_grp_65_18_64, age_grp_all_ages), sep = "")
  return(set)
}

## Call function for all three extracts. Need to specify file name in each call
aefinal <- bind_rows(pastyboy(""), pastyboy("-Scotland"), pastyboy("-S&C"))
## Tidy up LCA names, make sure they're in one column
aefinal %<>% mutate(across(c("council_area", "council_area_desc"), ~ replace_na(.x, ""))) %>%
  unite(council_area, c(council_area_desc, council_area), sep = "") %>%
  # Drop unnecessary variables
  select(-sheet) %>%
  # Ensure numbers are coded as such
  mutate(across(
    c("data_month_calendar_year", "data_month_calendar_month_number",
    "number_of_attendances", "admissions", "percent_admitted", "percent_seen_within_4h",
    "percent_admitted_2"), as.numeric))


## Write out an Excel worksheet with all the data
write_xlsx(aefinal, path("Data/3-A&E-For-Template.xlsx"))

# Remove the composite files - only if you're serious about it
# file_names <- list.files("Data/", full.names = TRUE) %>%
#   subset(str_detect(., "MSG") & !str_detect(., "Break"))
# do.call(file.remove, list(file_names))
