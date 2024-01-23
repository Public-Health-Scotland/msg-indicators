ind_3_raw <- arrow::read_parquet("Data/3-A&E-Breakdowns.parquet") %>% 
  mutate(four_hour_target = number_meeting_target/attendances,
         percent_admitted = admissions/attendances)

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

x <- get_population()

test <- ind_final %>% filter(council %in% c("Clackmannanshire", "Stirling"))

test <- ind_3 %>% 
  mutate(age_groups = if_else(age_groups %in% c("18-64", "65+"), "18+", age_groups)) %>% 
  group_by(council, month, age_groups) %>% 
  summarise(across(attendances:number_meeting_target, ~ sum(.x, na.rm = t))) %>% 
  ungroup()
