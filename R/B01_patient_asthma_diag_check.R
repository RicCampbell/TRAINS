## Script for checking all patients have an asthma diagnosis

library(arrow)
library(dplyr)
library(data.table)

## CPRD should have identified all patients of interest
## However, good to check that every patient that has been provided has a asthma diagnosis at some point in data provided


  ## Point to observation table with only asthma diags in (medcodeid is the diagnosis field)
    
    observation_asthma <- arrow::open_dataset("D:/TRAINS/data/parquet/observation/asthma_diag",
                                              format = "parquet")
    
    asthma_diags <- data.table(observation_asthma %>%
                                 filter(asthma_diag == "true") %>%
                                 collect())
    
  ## Summarise as amount of times each diagnosis for each patient (maybe interesting as to type of asthma later)
    ## Although there are possible multiple diags per consultation, and some are annual review or 'Asthma not disturbing sleep'
    ## Could also think about most recent diagnosis
    
    patient_asthma_diag <- asthma_diags[, diag_count := .N, by = .(patid, medcodeid)]
    
    
  ## Order by patient and most prevalent diag and, and take most recent observation
    
    patient_asthma_diag <- setorder(patient_asthma_diag, -diag_count, -obsdate)
    
    patient_asthma_diag[, order := 1:.N, by = patid]
    
    latest_most_prevalent_diag_patient <- patient_asthma_diag[order == 1]
    
  
  ## Check that no patient appears more than once
    
    stopifnot(latest_most_prevalent_diag_patient[, .N, by = patid][N > 1] == 0)

  
  ## Read in cleaned patient table with all patient in - ~~Upate if patient table changed~~
    
    patients <- readRDS("D:/TRAINS/data/datasets/patient_table-2022-10-17-151141.rds")
    
    
  ## Check if all patients have an asthma diag
    
    stopifnot(patients[!(patid %in% latest_most_prevalent_diag_patient$patid), .N] == 0)
    
    
  ###~~~ All patients had an asthma diagnosis ~~~###
  
    
  ## There are patients with diagnosis that are no longer in patient table as these were removed as being flagged 'unacceptable'
    #latest_most_prevalent_diag_patient[!(patid %in% patients$patid)]
    
  
    
    
    