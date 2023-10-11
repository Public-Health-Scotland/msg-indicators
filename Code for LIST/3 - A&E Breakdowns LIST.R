# MSG Indicators - Indicator 3: A&E Attendances
# Uses queries from A&E datamart to produce LIST breakdown file

# set current year
current_year <- "2223"


#### 2. LIST breakdown file ----

# Use this section only when you have updated A&E data from a previous year
# i <- 1718
# old_temp <- list()
# while (i < as.numeric(current_year)){
#   finyear <- as.character(i)
#   old_temp[[finyear]] <- read_rds(glue("Data/3-A&E-{finyear}.rds")) %>% clean_names()
#   i <- i + 101}
# old_temp <- bind_rows(old_temp) %>%
#   clean_names() %>%
#   filter(!(month %in% c("Jan 2017", "Feb 2017", "Mar 2017")))
# write_rds(old_temp, "Data/3-Complete-Years.rds", compress = "gz")

# Note that you will have to have a copy of the A&E data from all previous years for this to work
old_admissions <- read_rds("Data/3-Complete-Years.rds")

new_admissions <- read_csv(
  # Read directly from zip file from BOXI
  glue("Data/MSG-MonthlyUpdate-Breakdowns-{current_year}.zip"),
  # Data starts on row 4
  skip = 4,
  # Apply old column names to the new data
  col_names = colnames(old_admissions),
  # Define column types (n = numeric, c = character)
  col_types = "nnccccccccnnn"
) %>% 
  # Remove empty councils
 filter(council != "" | !is.na(council))

# Bind rows of old data and new data together
temp_ae <- bind_rows(old_admissions, new_admissions) %>%
  # Get locality names
  left_join(., locality_lookup, by = c("datazone" = "datazone2011")) %>%
  # Define empty localities as unknown
  mutate(hscp_locality = replace_na(hscp_locality, "Unknown")) %>%
  relocate(hscp_locality, .before = datazone) %>%
  # Turn month into a date
  mutate(month = dmy(glue("01-{month_num}-{cal_year}"))) %>% 
  select(-cal_year, -month_num)

# save out temp file
write_rds(temp_ae, "Data/A&E_temp.rds", compress = "gz")

# aggregate to get breakdown file
final_output <-  temp_ae %>%
  # Convert to lazy data table for quick aggregate
  lazy_dt() %>% 
  # Aggregate
  group_by(across(month:hscp_locality)) %>% 
  summarise(across(attendances:number_meeting_target, sum, na.rm = TRUE)) %>% 
  # Change output to tibble
  as_tibble() %>% 
  # Quick rename for ease
  rename(area_treated = treated_within_hb,
         age_group = age_grp,
         locality = hscp_locality) %>%
  # Get rid of missing age groups
  mutate(age_group = if_else(is.na(age_group), "", age_group))

# save out R breakdown file
write_rds(final_output, "Data/3-A&E-Breakdowns.rds", compress = "gz")

# Adding value labels for SPSS output
val_labels(final_output) <- list(
  area_treated = c("Within HBres" = "IN", "Outwith HBres" = "OUT"),
  ref_source = c(
    "Self Referral" = "01",
    "Healthcare professional/ service/ organisation" = "02",
    "Local Authority" = "03",
    "Private professional /agency /organisation" = "04",
    "Other agency" = "05",
    "Other" = "98",
    "Not Known" = "99"
  )
)
write_sav(final_output, "Data/3-A&E-Breakdowns.sav", compress = TRUE)
