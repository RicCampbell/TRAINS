## Health Foundation Aurum pipeline usage
## Ends in an error

library(aurumpipeline)
library(arrow)
library(dplyr)
source("R/Z01_schema_creation.R")

## Trying out Health Foundation Analytics Lab Aurum Pipeline package

###~~~ DO NOT USE THIS AS THROWS ERROR WHEN USING AURUM_PIPELINE FUNCTION, SO CAN NOT TRUST OUTPUT PARQUET FILES ~~~~####

## Warning message: One or more parsing issues, see `problems()` for details
## Works in check mode (first 100 records)
patient <- aurum_pipeline(type = "Patient",
                          dataloc = "D:/source_data/Deliveryv2/Data/Primary care",
                          saveloc = "D:/TRAINS/data/Aurum_pipeline/patient/")

check_vars(table = "Patient",
           data = patient)


## Seems to run
practice <- aurum_pipeline(type = "Practice",
                           dataloc = "D:/source_data/Deliveryv2/Data/Primary care",
                           saveloc = "D:/TRAINS/data/Aurum_pipeline/practice/")

check_vars(table = "Practice",
           data = practice)

## Seems to run
observation <- aurum_pipeline(type = "Observation",
                       dataloc = "D:/source_data/Deliveryv2/Data/Primary care",
                       saveloc = "D:/TRAINS/data/Aurum_pipeline/observation/")


## Seems to run
consultation <- aurum_pipeline(type = "Consultation",
                             dataloc = "D:/source_data/Deliveryv2/Data/Primary care",
                             saveloc = "D:/TRAINS/data/Aurum_pipeline/consultation/")


## Seems to run
drug_issue <- aurum_pipeline(type = "DrugIssue",
                             dataloc = "D:/source_data/Deliveryv2/Data/Primary care",
                             saveloc = "D:/TRAINS/data/Aurum_pipeline/drug_issue/")


## Seems to run
problem <- aurum_pipeline(type = "Problem",
                             dataloc = "D:/source_data/Deliveryv2/Data/Primary care",
                             saveloc = "D:/TRAINS/data/Aurum_pipeline/problem/")


## Seems to run
referral <- aurum_pipeline(type = "Referral",
                             dataloc = "D:/source_data/Deliveryv2/Data/Primary care",
                             saveloc = "D:/TRAINS/data/Aurum_pipeline/referral/")


## Seems to run
staff <- aurum_pipeline(type = "Staff",
                             dataloc = "D:/source_data/Deliveryv2/Data/Primary care",
                             saveloc = "D:/TRAINS/data/Aurum_pipeline/staff/")


## Read in patient table direct from raw data (is small enought to hold)

  patients_raw <- fread("D:/source_data/Deliveryv2/Data/Primary care/TRAINS_Extract_Patient_001.txt",
                        colClasses = "character",
                        na.strings = "")
  patients_raw[,.N]
  


## Read in patient parquet using arrow and the aurum pipeline

  patient_schema <- getSchema("patient")
  
  patients <- arrow::open_dataset("D:/TRAINS/data/Aurum_pipeline/patient/Data/Patient",
                                  format = "parquet",
                                  schema = patient_schema)
  
  patients %>%
    summarise(N = n()) %>%
    collect() %>%
    print()
  
  
  patient_table <- opendt(data_in = "D:/TRAINS/data/Aurum_pipeline/patient/Data/Patient")
  patient_table[,.N]
  
  
  practice_table <- opendt(data_in = "D:/TRAINS/data/Aurum_pipeline/practice/Data/Practice")


## Only goes on year (so will use 2022-01-01 effectively in this example)
  patient_plus_age <- add_age(patient_table,
                              measurefrom = "2022-09-01")

  
  patient_plus_age[, .N, by = age]
  patient_plus_age[is.na(mob)]
  

## Needs codelist of ethnicity_medcodeid - ethnicity double    
  add_ethnicity (patient_table)

check_patid_links(dts = c("Patient", "Observation", "DrugIssue"))

