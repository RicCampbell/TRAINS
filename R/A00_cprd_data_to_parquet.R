## Script for reading in each table from CPRD in turn and saving it as a parquet file for access
## Doing dates separate to schema as there was parsing issues

library(data.table)
library(readxl)
library(arrow)
library(dplyr)
library(googlesheets4)
source("R/Z01_schema_creation.R")

  source_data_file_location <- "D:/source_data/Deliveryv2/Data/Primary care/"

  
# Patient table -----------------------------------------------------------

## Need a schema in case of null cols (column headings)

  patient_schema <- getSchema("patient")
  
  patient_data <- arrow::open_dataset(paste0(source_data_file_location,"TRAINS_Extract_Patient_001.txt"),
                                      format = "text",
                                      delimiter = "\t",
                                      schema = patient_schema,
                                      skip = 1)

  patient_data %>%
    mutate(regstartdate = case_when(as.character(regstartdate) %in% "regstartdate" ~ NA_character_,
                                    is.na(regstartdate) ~ NA_character_,
                                    regstartdate == "" ~ NA_character_,
                                    TRUE ~ regstartdate)) %>%
    mutate(emis_date = case_when(as.character(emis_date) %in% "emis_date" ~ NA_character_,
                                 is.na(emis_date) ~ NA_character_,
                                 emis_date == "" ~ NA_character_,
                                 TRUE ~ emis_date)) %>%
    mutate(regendate = case_when(as.character(regendate) %in% "regendate" ~ NA_character_,
                                 is.na(regendate) ~ NA_character_,
                                 regendate == "" ~ NA_character_,
                                 TRUE ~ regendate)) %>%
    mutate(cprd_ddate = case_when(as.character(cprd_ddate) %in% "cprd_ddate" ~ NA_character_,
                                  is.na(cprd_ddate) ~ NA_character_,
                                  cprd_ddate == "" ~ NA_character_,
                                  TRUE ~ cprd_ddate)) %>%
    mutate(regstartdate = as.Date(regstartdate, format = "%d/%m/%Y")) %>%
    mutate(emis_date = as.Date(emis_date, format = "%d/%m/%Y")) %>%
    mutate(regendate = as.Date(regendate, format = "%d/%m/%Y")) %>%
    mutate(cprd_ddate = as.Date(cprd_ddate, format = "%d/%m/%Y")) %>%
    group_by(acceptable) %>%
    arrow::write_dataset("D:/TRAINS/data/parquet/patient/",
                         format = "parquet")
  
  rm(patient_data, patient_schema)

  
# Practice ----------------------------------------------------------------

## Need a schema in case of null cols (column headings)

  practice_schema <- getSchema("practice")
  
  practice_data <- arrow::open_dataset(paste0(source_data_file_location,"TRAINS_Extract_Practice_001.txt"),
                                       format = "text",
                                       delimiter = "\t",
                                       schema = practice_schema,
                                       skip = 1)

  practice_data %>%
    mutate(lcd = case_when(as.character(lcd) %in% "lcd" ~ NA_character_,
                           is.na(lcd) ~ NA_character_,
                           lcd == "" ~ NA_character_,
                           TRUE ~ lcd)) %>%
    mutate(lcd_year = substr(lcd, 7, 10)) %>%
    mutate(lcd = as.Date(lcd, format = "%d/%m/%Y")) %>%
    group_by(lcd_year) %>%
    arrow::write_dataset("D:/TRAINS/data/parquet/practice/",
                         format = "parquet")
  
  rm(practice_data, practice_schema)
  
  
# Observation -------------------------------------------------------------

## Need a schema in case of null cols (column headings)
  
  observation_schema <- getSchema("observation")

  
## Create list of file names
  
  file_names_observation <- paste0(source_data_file_location,"TRAINS_Extract_Observation_00", 1:9, ".txt")
  
  observation_data <- arrow::open_dataset(file_names_observation,
                                          format = "text",
                                          delimiter = "\t",
                                          schema = observation_schema,
                                          skip = 1)
  
  
## Read in diagnosis code list that has been created  
  
  diag_terms <- fread("D:/reference_data/Asthma diagnosis codes 4 April.txt",
                      colClasses = "character",
                      na.strings = "")
  
## First partitioning is on, 'is the observation is related to asthma or not',
## Second partitioning is on observation date
  ## One reason being that linkage with problem and referral (and consultation and drug), is not certain would be similar dates
  
## First partitioning, asthma observation or not
## Not a very good way of doing this - see drug for much more simple approach (and Term can be added at a later time)
  observation_data %>%
    select(patid, consid, pracid, obsid, obsdate, parentobsid, medcodeid, value, obstypeid, probobsid) %>%
    map_batches(function(join) {
      join %>%
        left_join(x = .,
                  y = diag_terms[, .(MedCodeId, Term)],
                  by = c("medcodeid" = "MedCodeId"))
    }) %>%
    mutate(asthma_diag = !is.na(Term)) %>%
    mutate(obsdate = case_when(as.character(obsdate) %in% "" ~ NA_character_,
                               is.na(obsdate) ~ NA_character_,
                               obsdate == "" ~ NA_character_,
                               TRUE ~ obsdate)) %>%
    mutate(obsdate_year = substr(obsdate, 7, 10)) %>%
    mutate(obsdate = as.Date(obsdate, format = "%d/%m/%Y")) %>%
    group_by(asthma_diag) %>%
    arrow::write_dataset("D:/TRAINS/data/parquet/observation/asthma_diag/",
                         format = "parquet")

## Second partition, observation year
  observation_data %>%
    mutate(obsdate = case_when(as.character(obsdate) %in% "" ~ NA_character_,
                               is.na(obsdate) ~ NA_character_,
                               obsdate == "" ~ NA_character_,
                               TRUE ~ obsdate)) %>%
    mutate(enterdate = case_when(as.character(enterdate) %in% "" ~ NA_character_,
                                 is.na(enterdate) ~ NA_character_,
                                 enterdate == "" ~ NA_character_,
                                 TRUE ~ enterdate)) %>%
    mutate(obsdate_year = substr(obsdate, 7, 10)) %>%
    mutate(obsdate = as.Date(obsdate, format = "%d/%m/%Y")) %>%
    mutate(enterdate = as.Date(enterdate, format = "%d/%m/%Y")) %>%
    group_by(obsdate_year) %>%
    arrow::write_dataset("D:/TRAINS/data/parquet/observation/observation_date/",
                         format = "parquet")
  
  
  rm(observation_data, observation_schema, file_names_observation, diag_terms)
  gc()

  
# Consultation ------------------------------------------------------------

## Need a schema in case of null cols (column headings)
  
  consultation_schema <- getSchema("consultation")
  
  
## Create list of file names
  
  file_names_consultation <- paste0(source_data_file_location, "TRAINS_Extract_Consultation_00", 1:4, ".txt")
  
  consultation_data <- arrow::open_dataset(file_names_consultation,
                                           format = "text",
                                           delimiter = "\t",
                                           schema = consultation_schema,
                                           skip = 1)

## Partitioning is on observation date
## consmedcodeid appears to be about type of consultation 'medication requested', hospital outpatient report'
  ## Be aware that date may not be the best as a consultation could link with problem and referral (and consultation and drug)
  ## but it would not certain would be similar dates (or empty dates)
  ## Also if a consultation with 'real' date is linked to observation with 'unrealistic' date, can we trust the real one (and vice-versa)?
  
## None Hive-style partitioning does not appear to work for some reason, loses a lot of records
    
  consultation_data %>%
    select(patid, consid, pracid, consdate, consdourceid, consmedcodeid) %>%
    mutate(consdate = case_when(as.character(consdate) %in% "" ~ NA_character_,
                                is.na(consdate) ~ NA_character_,
                                consdate == "" ~ NA_character_,
                                TRUE ~ consdate)) %>%
    mutate(consdate_month = as.integer(substr(consdate, 4, 5))) %>%
    mutate(consdate_year = as.integer(substr(consdate, 7, 10))) %>%
    mutate(consdate = as.Date(consdate, format = "%d/%m/%Y")) %>%
    group_by(consdate_year, consdate_month) %>%
    arrow::write_dataset("D:/TRAINS/data/parquet/consultation/consultation_date/",
                         format = "parquet")


  rm(consultation_data, consultation_schema, file_names_consultation)
  gc()

  
# Drug Issue --------------------------------------------------------------

## Need a schema in case of null cols (column headings)
  
  drug_schema <- getSchema("drug")
  
  
## Create list of file names

  file_names_drug <- paste0(source_data_file_location, "TRAINS_Extract_DrugIssue_00", 1:6, ".txt")
  
  drug_data <- arrow::open_dataset(file_names_drug,
                                   format = "text",
                                   delimiter = "\t",
                                   schema =drug_schema,
                                   skip = 1)
  
## Read in diagnosis code list that has been created, now has preventer_reliver col (might be updated)
  
  drug_meta_location <- as.character(as_sheets_id("https://https://docs.google.com/spreadsheets/d/1LfbKdBY4ygA6hWIMZbFCVlTm3Ecnf2z2ktl8FEZtleE/edit#gid=1523310571")) 
  
  drug_meta <- data.table(range_read(drug_meta_location,
                                     sheet = "asthma_drugs",
                                     col_types = "c"))

## Good to partition on if the record involves one of the asthma medications used to create cohort
## Updated to split between preventer and reliever (or non-asthma related) (not run yet)
## Waiting on code list to be split by preventer and reliever to be finalised

  drug_data %>%
    mutate(asthma_drug = prodcodeid %in% drug_meta$ProdCodeId) %>%
    mutate(preventer = prodcodeid %in% drug_meta[preventer_reliver == "preventer", ProdCodeId]) %>%
    mutate(issuedate = case_when(as.character(issuedate) %in% "" ~ NA_character_,
                                is.na(issuedate) ~ NA_character_,
                                issuedate == "" ~ NA_character_,
                                TRUE ~ issuedate)) %>%
    mutate(enterdate = case_when(as.character(enterdate) %in% "" ~ NA_character_,
                                is.na(enterdate) ~ NA_character_,
                                enterdate == "" ~ NA_character_,
                                TRUE ~ enterdate)) %>%
    mutate(issuedate_year = as.integer(substr(issuedate, 7, 10))) %>%
    mutate(issuedate = as.Date(issuedate, format = "%d/%m/%Y")) %>%
    mutate(enterdate = as.Date(enterdate, format = "%d/%m/%Y")) %>%
    group_by(asthma_drug, preventer) %>%
    arrow::write_dataset("D:/TRAINS/data/parquet/drug",
                       format = "parquet")

  rm(drug_data, drug_schema, drug_meta, file_names_drug)
  gc()

  
# Problem -----------------------------------------------------------------

## Need a schema in case of null cols (column headings)
  
  problem_schema <- getSchema("problem")
  
  problem_data <- arrow::open_dataset(paste0(source_data_file_location, "TRAINS_Extract_Problem_001.txt"),
                                       format = "text",
                                       delimiter = "\t",
                                       schema = problem_schema,
                                       skip = 1)
  
## There are 826,139 records with a non-date for probenddate
## There is no good way to partition problems, as both date fields are not suitable
  
  problem_data %>%
    select(patid, obsid, pracid, parentprobobsid, probenddate, parentprobrelid, probstatusid) %>%
    mutate(probenddate = case_when(as.character(probenddate) %in% "" ~ NA_character_,
                                   is.na(probenddate) ~ NA_character_,
                                   probenddate == "" ~ NA_character_,
                                   TRUE ~ probenddate)) %>%
    mutate(probenddate = as.Date(probenddate, format = "%d/%m/%Y")) %>%
    write_dataset("D:/TRAINS/data/parquet/problem/",
                  format = "parquet")
  
  rm(problem_data, problem_schema)

  
# Referral ----------------------------------------------------------------

## Need a schema in case of null cols (column headings)

  referral_schema <- getSchema("referral")
  
  referral_data <- arrow::open_dataset(paste0(source_data_file_location, "TRAINS_Extract_Referral_001.txt"),
                                       format = "text",
                                       delimiter = "\t",
                                       schema = referral_schema,
                                       skip = 1)
## No date fields in referral table
  
  referral_data %>%
    select(patid, obsid, pracid, refurgencyid, refservicetypeid, refmodeid) %>%
    group_by(refurgencyid) %>%
    arrow::write_dataset("D:/TRAINS/data/parquet/referral",
                       format = "parquet")
  
  rm(referral_data, referral_schema)
  

# Staff -------------------------------------------------------------------

  staff_schema <- getSchema("staff")
  
  staff_data <- arrow::open_dataset(paste0(source_data_file_location, "TRAINS_Extract_Staff_001.txt"),
                                    format = "text",
                                    delimiter = "\t",
                                    schema = staff_schema,
                                    skip = 1)

## No date fields in staff table  
    
  arrow::write_dataset(staff_data,
                       "D:/TRAINS/data/parquet/staff",
                       format = "parquet")
  
  rm(staff_data, staff_schema)

  