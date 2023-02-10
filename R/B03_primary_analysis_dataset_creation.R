## Script for creating primary outcome analysis table

library(arrow)
library(dplyr)
library(data.table)
source("R/cleaning_fns_etl.r")


## Primary outcome is
##    'The proportion of children with asthma preventer medication who have a prescription for an asthma preventer medication 
##    from 1st August 2021 to 30th September 2021'

  ## Point to drugs table
  ## Remove any drug records for practice that has been removed due to duplication

    drugs <- arrow::open_dataset("D:/TRAINS/data/parquet/drug",
                                              format = "parquet")
    
    asthma_drug <- data.table(drugs %>%
                                filter(asthma_drug == "true" & preventer == "true" & pracid != "20155") %>%
                                collect())

    
  ## Read in cleaned practice table
    
    practice <- readRDS("D:/TRAINS/data/datasets/practice_table-2022-10-28-125510.rds")
    
    
  ## Read in cleaned patient table
    
    patients <- readRDS("D:/TRAINS/data/datasets/patient_table-2022-10-17-151141.rds")
    
    
  ## Merge practice and patient (excluding duplicated practice) tables - this order of merging is better for patient with no drug data
    
    patient_practice <- merge(patients[pracid != "20155", .(patid, pracid, gender, yob, mob, regstartdate, regendate, emis_date, created_dob, age, census_code, ethnicity, ethnicity_group_2021)],
                              practice[, .(pracid, lcd, lcd_year, deciles, randomisation, randomisation_code)],
                              by = "pracid",
                              all.x = TRUE)
    
    
  ## Check have all practices and patients in table  
    
    stopifnot(practice[, .N] == uniqueN(patient_practice$pracid))
    stopifnot(patients[ ,.N] == patient_practice[ ,.N] + patients[pracid == "20155", .N])
    
    
  ## Link drug table patients-practice, removing unwanted fields
  ## Need to link practice to drug to know if the issue date is past the last contributed date
  ## Keeping drug record id to be decider field when set order later
    
    patient_practice_drug <- merge(patient_practice,
                                   asthma_drug[(issuedate >= as.Date("2019-07-01") & issuedate <= as.Date("2021-12-31")),
                                               .(patid, drugrecid, issuedate, enterdate, prodcodeid, asthma_drug, preventer)],
                                   by = "patid",
                                   all.x = TRUE)
    
  
  ## Check same number of practices
    
    stopifnot(patient_practice_drug[!(pracid %in% practice$pracid)][, .N] == 0)
    stopifnot(practice[!(pracid %in% patient_practice_drug$pracid)][, .N] == 0)

  
  ## Check all patients still in table
    
    stopifnot(patients[pracid != "20155", .N] == uniqueN(patient_practice_drug$patid))
  
    
  ## Create field highlighting when the last contribution date was
  
    patient_practice_drug[lcd < as.Date("2021-09-01"), last_data_group := "before_august_2021"]
    patient_practice_drug[lcd >= as.Date("2021-09-01") & lcd < as.Date("2021-10-01"), last_data_group := "all_august_2021"]
    patient_practice_drug[lcd >= as.Date("2021-10-01") & lcd < as.Date("2021-11-01"), last_data_group := "all_september_2021"]
    patient_practice_drug[lcd >= as.Date("2021-11-01") & lcd < as.Date("2021-12-01"), last_data_group := "all_october_2021"]
    patient_practice_drug[lcd >= as.Date("2021-12-01") & lcd < as.Date("2022-01-01"), last_data_group := "all_november_2021"]
    patient_practice_drug[lcd >= as.Date("2022-01-01"), last_data_group := "all_december_2021"]
    
  
  ## Add a flag for where registration data is before the start of the analysis period
    
    patient_practice_drug[, registered_pre_august_2021 := regstartdate < as.Date("2021-08-01")]
    
  
  ## Create field highlighting when patient de_registered
    
    patient_practice_drug[regendate < as.Date("2021-09-01"), de_registered := "before_august_2021"]
    patient_practice_drug[regendate >= as.Date("2021-09-01") & regendate < as.Date("2021-10-01"), de_registered := "in_august_2021"]
    patient_practice_drug[regendate >= as.Date("2021-10-01") & regendate < as.Date("2021-11-01"), de_registered := "in_september_2021"]
    patient_practice_drug[regendate >= as.Date("2021-11-01") & regendate < as.Date("2021-12-01"), de_registered := "in_october_2021"]
    patient_practice_drug[regendate >= as.Date("2021-12-01") & regendate < as.Date("2022-01-01"), de_registered := "in_november_2021"]
    patient_practice_drug[regendate >= as.Date("2022-01-01"), de_registered := "after_december_2021"]
  
      
  ## Check all drug records have a issue and an enter date - not all rows in main table will as not all patients will have a drug record
  ## Do not mess with issuedate and enterdate, just if they are out of sequence (e.g. after last contribution date)
    
      stopifnot(asthma_drug[is.na(issuedate), .N] == 0)
      stopifnot(asthma_drug[is.na(enterdate), .N] == 0)
      
      patient_practice_drug[, issue_after_lcd := issuedate > lcd]
      
    
    ## Field to highlight those records where issuedate is after de_registration date
      
      patient_practice_drug[, issue_after_de_reg := issuedate > regendate]
      

  ## Create field of number of prescriptions per patient in analysis timeframe
      
    setorder(patient_practice_drug, patid, issuedate, drugrecid, na.last = TRUE)
      
    patient_practice_drug[issuedate >= as.Date("2021-08-01") & issuedate < as.Date("2021-10-01"),
                          prescription_count := 1:.N, by = patid]
      
  
  ## Add in field to make it clear that this is for primary analysis question only, and put this first
      
      patient_practice_drug[, analysis_dataset := "Primary"]
      
      setcolorder(patient_practice_drug, c("analysis_dataset", setdiff(names(patient_practice_drug), "analysis_dataset")))
      
      
  ## Remove '3' in gender - hack for now, move to practice cleaning A01
      
      patient_practice_drug[gender == 3L, gender := NA]
      
  
## Read in the read-receipts file
      
      read_receipts <- fread("D:/source_data/read receipt/TRAINS Read Receipts.csv",
                             colClasses = "character",
                             sep = ",",
                             na.strings = ",,")
      
  ## Change col name
      
      setnames(read_receipts, make.names(colnames(read_receipts), unique = TRUE))
      
  ## Change empty to NA, as not reading in that way for some reason
      
      read_receipts[Read == "", Read := NA]
      read_receipts[Not.Read == "", Not.Read := NA]
      
  ## Remove duplicated practice
      
      read_receipts <- read_receipts[CPRDPracticeId != "20155"]
      
  ## Check only one row per practice, and have all practices of interest
      
      stopifnot(read_receipts[, .N, by = CPRDPracticeId][N > 1, .N] == 0)
      stopifnot(length(setdiff(practice$pracid, read_receipts$CPRDPracticeId)) == 0)
      
      
  ## Check if there are any rows where it has been read and not read - yes there are
  ## There are read receipts that state both read and not read, create field to highlight these
      
      read_receipts[!is.na(Read) & !is.na(Not.Read), read_receipt_conflict := TRUE]
      read_receipts[is.na(read_receipt_conflict), read_receipt_conflict := FALSE]
      

## Merge in read receipts with the drugs table
      
      patient_practice_drug_receipt <- merge(patient_practice_drug,
                                             read_receipts[, .(CPRDPracticeId, Read, Not.Read, read_receipt_conflict)],
                                             by.x = "pracid",
                                             by.y = "CPRDPracticeId")
      
## Check has same number of rows
      
      stopifnot(patient_practice_drug_receipt[, .N] == patient_practice_drug[, .N])
      
    
  ## Write out dataset to folder for analysis
      
    write.csv(patient_practice_drug_receipt,
              file = paste0("D:/TRAINS/data/data_for_analysis/primary_analysis_dataset_", getDateTimeForFilename(), ".csv"))
    

    test <- fread("D:/TRAINS/data/data_for_analysis/primary_analysis_dataset_2023-02-09-151015.csv",
                  colClasses = "character")
    
    
  
    
    