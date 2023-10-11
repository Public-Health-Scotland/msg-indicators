# Syntax for checking breakdowns for MSG 
# Purpose?
# 2022 edit by Megan McNicol 9th November 2022
# 
rm(list = ls())
gc()

################################### Set up - libraries and directories ###################################
library(tidyverse)

################################### Set up - read in files ###################################
Admissions_breakdown <- readRDS("Data/1a-Admissions-breakdown.rds")

Acute_Beddays_breakdown <- readRDS("Data/2a-Acute-Beddays-breakdown.rds")

GLS_Beddays_breakdown <- readRDS("Data/2b-GLS-Beddays-breakdown.rds")

MH_Beddays_breakdown <- readRDS("Data/2c-MH-Beddays-breakdown.rds")

AE_Breakdowns <- readRDS("Data/3-A&E-Breakdowns.rds")

Delayed_Discharge_Breakdowns <- readRDS("Data/4-Delayed-Discharge-Breakdowns.rds")


################################### 1a Admissions_breakdown ###################################
CHECK_1a_council_admissions <- Admissions_breakdown %>% 
  group_by(council, month) %>% 
  summarise(admissions = sum(admissions)) %>%
  group_by(council) %>% 
  mutate(mean = mean(admissions)) %>% 
  mutate(diff = ((admissions-mean) / admissions) *100)

CHECK_1a_council_age_admissions <- Admissions_breakdown %>% 
  group_by(council, month, age_group) %>% 
  summarise(admissions = sum(admissions)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = "age_group", values_from = "admissions")



################################### 2a Acute_Beddays_breakdown ###################################


CHECK_2a_council_beddays  <- Acute_Beddays_breakdown %>%  
  group_by(council, month) %>% 
  summarise(unplanned_beddays = sum(unplanned_beddays))%>% 
  ungroup() %>% 
  pivot_wider(names_from = "council", values_from = "unplanned_beddays")


CHECK_2a_council_agegroup_beddays  <- Acute_Beddays_breakdown %>%  
  group_by(council, month, age_group) %>% 
  summarise(unplanned_beddays = sum(unplanned_beddays)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = "age_group", values_from = "unplanned_beddays")


CHECK_2a_counts  <- Acute_Beddays_breakdown %>%  
  group_by(council, locality, area_treated, age_group, month) %>% 
  summarise(count = n()) %>% 
  ungroup() %>% 
  pivot_wider(names_from = "area_treated", values_from = "count")


################################### 2b GLS_Beddays_breakdown ###################################

CHECK_2b_council_beddays  <- GLS_Beddays_breakdown %>%  
  group_by(council, month) %>% 
  summarise(unplanned_beddays = sum(unplanned_beddays))%>% 
  ungroup() %>% 
  pivot_wider(names_from = "council", values_from = "unplanned_beddays")


CHECK_2b_council_age_group_beddays  <- GLS_Beddays_breakdown %>%  
  arrange(age_group) %>% 
  group_by(council, month, age_group) %>% 
  summarise(unplanned_beddays = sum(unplanned_beddays))%>% 
  ungroup() %>% 
  pivot_wider(names_from = "council", values_from = "unplanned_beddays") 



################################### 2c MH_Beddays_breakdown ###################################
CHECK_2c_council_beddays  <- MH_Beddays_breakdown %>%  
  group_by(council, month) %>% 
  summarise(unplanned_beddays = sum(unplanned_beddays))%>% 
  ungroup() %>% 
  pivot_wider(names_from = "council", values_from = "unplanned_beddays")


CHECK_2c_council_age_group_beddays  <- MH_Beddays_breakdown %>%  
  arrange(age_group) %>% 
  group_by(council, month, age_group) %>% 
  summarise(unplanned_beddays = sum(unplanned_beddays))%>% 
  ungroup() %>% 
  pivot_wider(names_from = "council", values_from = "unplanned_beddays") 

  

################################### 3 AE_Breakdowns ###################################
CHECK_3_rate  <- AE_Breakdowns %>%  
  group_by(month) %>% 
  summarise(attendances = sum(attendances),
            number_meeting_target = sum(number_meeting_target))%>% 
  ungroup() %>%
  mutate(rate = (number_meeting_target/attendances) *100)


CHECK_3_council_rate  <- AE_Breakdowns %>%  
  group_by(council, month) %>% 
  summarise(attendances = sum(attendances),
            number_meeting_target = sum(number_meeting_target))%>% 
  ungroup() %>%
  mutate(rate = (number_meeting_target/attendances) *100)


CHECK_3_council_age_group_rate  <- AE_Breakdowns %>%  
  group_by(council, month, age_group) %>% 
  summarise(attendances = sum(attendances),
            number_meeting_target = sum(number_meeting_target))%>% 
  ungroup() %>%
  mutate(rate = (number_meeting_target/attendances) *100)



################################### 4 Delayed_Discharge_Breakdowns ###################################
CHECK_4_council_beddays  <- Delayed_Discharge_Breakdowns %>%  
  group_by(council, month) %>% 
  summarise(delayed_bed_days = sum(delayed_bed_days))%>% 
  ungroup() %>% 
  pivot_wider(names_from = "council", values_from = "delayed_bed_days")


CHECK_4_council_delay_group_beddays  <- Delayed_Discharge_Breakdowns %>%  
  arrange(reason_for_delay) %>% 
  group_by(council, month, reason_for_delay) %>% 
  summarise(delayed_bed_days = sum(delayed_bed_days))%>% 
  ungroup() %>% 
  pivot_wider(names_from = "reason_for_delay", values_from = "delayed_bed_days") 

