ind_3_raw <- arrow::read_parquet("Data/3-A&E-Breakdowns.parquet") %>% 
  mutate(four_hour_target = number_meeting_target/attendances,
         percent_admitted = admissions/attendances)
