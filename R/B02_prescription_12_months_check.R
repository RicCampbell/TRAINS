## Script for checking that all patients had an asthma prescription in the last 12 months as stated in the data specification form

library(arrow)
library(dplyr)
library(data.table)

## CPRD should have identified all patients of interest
## However, good to check that every patient that has been provided has a asthma prescription in the past 12 months in data provided


  ## Point to drugs table
  
    drugs <- arrow::open_dataset("D:/TRAINS/data/parquet/drug",
                                 format = "parquet")
    
    
    asthma_drug <- data.table(drugs %>%
                                filter(asthma_drug == "true") %>%
                                collect())
    
    
  ## Reduce to the '12 months' as stated in data spec form
    
    asthma_drug_12_months <- asthma_drug[issuedate >= as.Date("2020-06-01") & issuedate <= as.Date("2021-05-31")]
    

  ## Summarise as amount of times each PRESCRIPTION for each patient (maybe interesting as to type of asthma later) 
    
    patient_asthma_drug <- asthma_drug_12_months[, drug_count := .N, by = .(patid, prodcodeid)]
    
  
  ## Order by patient and most prevalent drug and, and take most recent observation
    
    patient_asthma_drug <- setorder(patient_asthma_drug, -issuedate, -drug_count)
    
    patient_asthma_drug[, order := 1:.N, by = patid]
    
    latest_drug_patient <- patient_asthma_drug[order == 1]
    
  
  ## Check that no patient appears more than once
    
    stopifnot(latest_drug_patient[, .N, by = patid][N > 1] == 0)
    
    
  ## Read in cleaned patient table with all patient in - ~~Upate if patient table changed~~
    
    patients <- readRDS("D:/TRAINS/data/datasets/patient_table-2022-10-17-151141.rds")

  
  ## Check if all patients have an asthma drug in past '12 months'
    
    stopifnot(patients[!(patid %in% latest_drug_patient$patid), .N] == 0)    
    
  
  ### ~~~All patients have had a drug presciption in the past 12 months as defined in the data spec form ~~~###
    
    
  ## There are patients with drug prescription that are no longer in patient table as these were removed as being flagged 'unacceptable'
    #latest_drug_patient[!(patid %in% patients$patid)]
    
    
    
    
    