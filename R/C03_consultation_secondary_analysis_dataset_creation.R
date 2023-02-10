### Script for working on the consultation table (and creating analysis dataset) (outcomes 10 & 11)

library(arrow)
library(dplyr)
library(lubridate)
library(data.table)
library(googlesheets4)
source("R/cleaning_fns_etl.r")


## Point to consultation table

  consultation <- arrow::open_dataset("D:/TRAINS/data/parquet/consultation",
                                      format = "parquet")
  
  
  ## Get all consultations in 2021 and 2022 before April, not in duplicated practice
  ## Need to increase this to have baseline period in as well
  
  consultation_interest <- data.table(consultation %>%
                                        filter ((consdate_year == 2019
                                                | consdate_year == 2020
                                                | consdate_year == 2021
                                                | consdate_year == 2022) & (pracid != "20155")) %>%
                                        collect())
  
  consultation_interest <- consultation_interest[consdate >= "2019-07-01" & consdate < "2022-04-01"]
  
  
## Read in consultation source code that have been coded as a true 'medical contact'
  
  consultation_reason_ref_data <- data.table(read_sheet(ss = "https://docs.google.com/spreadsheets/d/1uZemsA6tzggJn7OukM2ITwxy0eJaDBgmMaw7HnnuxEM/edit#gid=0",
                                                        sheet = "consultation_codes"))
  
  medical_contact_codes <- consultation_reason_ref_data[, .(consmedcodeid, Term, true_consultation)]
  
  
## Merge all consultations of interest with list of consultations that are wanted, so keep the readable term
  
  consultation_medical_contact <- merge(consultation_interest,
                                        medical_contact_codes,
                                        by = "consmedcodeid",
                                        all.x = TRUE)
  
## There are two rows that are NA for true_consultation, ones with blank consmedcodeid's and one code that appears to not to be in the CPRD medical dictionary
## Set these to FALSE consultation
  
  consultation_medical_contact[is.na(true_consultation), true_consultation := FALSE]
  

## Want to add staffing here as would like the single record chosen to be, if exists:
  ## a true_consultation
  ## an unscheduled consultation
  
  staffing_ref_data <- data.table(read_sheet(ss = "https://docs.google.com/spreadsheets/d/1Ro-ouWkkv5NGBAEwhxYjUEBKhkmW54vXzA_yNZ9uK8k/edit#gid=0",
                                             sheet = "job_category_descriptions",
                                             col_types = "iclc"))
  
## Make some fake stuff for testing
  # staffing_ref_data[jobcatid > 0 & jobcatid <= 100, unscheduled_visit := TRUE]
  # staffing_ref_data[jobcatid > 100, unscheduled_visit := FALSE]
  

## Point to staff table
## Remove any staff records for practice that has been removed due to duplication
  
  staff <- arrow::open_dataset("D:/TRAINS/data/parquet/staff",
                               format = "parquet")
  
  
## There are no years in staff table, so can only remove duplicate pracid
  
  staff_interest <- data.table(staff %>%
                                 filter(pracid != "20155") %>%
                                 collect())
  
  
## Merge in category descriptions to staff table
  
  staff_interest <- merge(staff_interest,
                          staffing_ref_data,
                          by = "jobcatid",
                          all.x = TRUE)
  

## Merge into consultation table
## Do not merge pracid from staff table as end up with duplicate col and have checked that none do not match up
  
  consultation_medical_contact <- merge(consultation_medical_contact,
                                        staff_interest[, .(jobcatid, staffid, Description, unscheduled_visit)],
                                        by = "staffid",
                                        all.x = TRUE)
  

## Order the data, so it is ready to be limited
  
  consultation_medical_contact <- consultation_medical_contact[order(patid, consdate, -true_consultation, -unscheduled_visit, consid)]

  consultation_medical_contact[, order := 1:.N, by = .(patid, consdate)]
  

## Reduce to only one consultation per person per day, with precedence taken for true consultations and unscheduled
  
  reduced_consultation_medical_contact <- consultation_medical_contact[order == 1]
  

## Read in cleaned patient table for dates and demographics
  
  patients <- readRDS("D:/TRAINS/data/datasets/patient_table-2022-10-17-151141.rds")
  
  
## Merge in patient information, ensuring have all patients at the end, all.x as want all patients, but there are consultations for non-acceptable patients
  
  reduced_consultation_medical_contact_patients <- merge(patients[pracid != "20155", .(patid, pracid, gender, yob, mob, regstartdate, regendate, emis_date, created_dob, age, census_code, ethnicity, ethnicity_group_2021)],
                                                         reduced_consultation_medical_contact,
                                                         by = c("patid", "pracid"),
                                                         all.x = TRUE)
  

## Check have all patients, and all consultations that we wanted in this table
  
  stopifnot(patients[pracid != "20155", .N] == uniqueN(reduced_consultation_medical_contact_patients$patid))


## Read in cleaned practice table for lcd date, randomisation, deciles
  
  practice <- readRDS("D:/TRAINS/data/datasets/practice_table-2022-10-28-125510.rds")


## Merge in the practice information that we want
  
  reduced_consultation_medical_contact_patient_practice <- merge(reduced_consultation_medical_contact_patients,
                                                                 practice[pracid != "20115", .(pracid, lcd, region, lcd_year, deciles, randomisation, randomisation_code)],
                                                                 by = "pracid",
                                                                 all.x = TRUE)
  
  
## Check again that all patients and now all practices are in table
  
  stopifnot(patients[pracid != "20155", .N] == uniqueN(reduced_consultation_medical_contact_patient_practice$patid))
  stopifnot(practice[, .N] == uniqueN(reduced_consultation_medical_contact_patient_practice$pracid))
  
  
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
  
  
# Merge the read receipts into the consultation table
  
  reduced_consultation_medical_contact_patient_practice_read <- merge(reduced_consultation_medical_contact_patient_practice,
                                                                      read_receipts[, .(CPRDPracticeId, Read, Not.Read, read_receipt_conflict)],
                                                                      by.x = "pracid",
                                                                      by.y = "CPRDPracticeId",
                                                                      all.x = TRUE)
  
## Check that has the same number of rows
  
  stopifnot(reduced_consultation_medical_contact_patient_practice_read[, .N] == reduced_consultation_medical_contact_patient_practice[, .N])


## Create field highlighting when the last contribution date was
  
  reduced_consultation_medical_contact_patient_practice_read[lcd < as.Date("2021-09-01"), last_data_group := "before_august_2021"]
  reduced_consultation_medical_contact_patient_practice_read[lcd >= as.Date("2021-09-01") & lcd < as.Date("2021-10-01"), last_data_group := "all_august_2021"]
  reduced_consultation_medical_contact_patient_practice_read[lcd >= as.Date("2021-10-01") & lcd < as.Date("2021-11-01"), last_data_group := "all_september_2021"]
  reduced_consultation_medical_contact_patient_practice_read[lcd >= as.Date("2021-11-01") & lcd < as.Date("2021-12-01"), last_data_group := "all_october_2021"]
  reduced_consultation_medical_contact_patient_practice_read[lcd >= as.Date("2021-12-01") & lcd < as.Date("2022-01-01"), last_data_group := "all_november_2021"]
  reduced_consultation_medical_contact_patient_practice_read[lcd >= as.Date("2022-01-01"), last_data_group := "all_december_2021"]
  
## Add a flag for where registration data is before the start of the analysis period
  
  reduced_consultation_medical_contact_patient_practice_read[, registered_pre_august_2021 := regstartdate < as.Date("2021-08-01")]
  

## Create field highlighting when patient de_registered
  
  reduced_consultation_medical_contact_patient_practice_read[regendate < as.Date("2021-09-01"), de_registered := "before_august_2021"]
  reduced_consultation_medical_contact_patient_practice_read[regendate >= as.Date("2021-09-01") & regendate < as.Date("2021-10-01"), de_registered := "in_august_2021"]
  reduced_consultation_medical_contact_patient_practice_read[regendate >= as.Date("2021-10-01") & regendate < as.Date("2021-11-01"), de_registered := "in_september_2021"]
  reduced_consultation_medical_contact_patient_practice_read[regendate >= as.Date("2021-11-01") & regendate < as.Date("2021-12-01"), de_registered := "in_october_2021"]
  reduced_consultation_medical_contact_patient_practice_read[regendate >= as.Date("2021-12-01") & regendate < as.Date("2022-01-01"), de_registered := "in_november_2021"]
  reduced_consultation_medical_contact_patient_practice_read[regendate >= as.Date("2022-01-01"), de_registered := "after_december_2021"]

  
## Check all consultation records have a consdate and an enter date - not all rows in main table will as not all patients will have a consultation record
## Do not mess with consdate and enterdate, just if they are out of sequence (e.g. after last contribution date)
  
  stopifnot(reduced_consultation_medical_contact_patient_practice_read[is.na(consdate) & !is.na(consid), .N] == 0)
  stopifnot(reduced_consultation_medical_contact_patient_practice_read[is.na(enterdate) & !is.na(consid), .N] == 0)
  
  reduced_consultation_medical_contact_patient_practice_read[, consultation_after_lcd := consdate > lcd]
  
  
## Field to highlight those records where consdate is after de_registration date
  
  reduced_consultation_medical_contact_patient_practice_read[, consultation_after_de_reg := consdate > regendate]


## Field to indicate which analysis dataset this is
  
  reduced_consultation_medical_contact_patient_practice_read[, analysis_dataset := "Secondary"]
  

## Remove '3' in gender - hack for now, move to practice cleaning A01
  
  reduced_consultation_medical_contact_patient_practice_read[gender == 3L, gender := NA]

  
## Write out dataset to folder for analysis
  
  write.csv(reduced_consultation_medical_contact_patient_practice_read,
            file = paste0("D:/TRAINS/data/data_for_analysis/secondary_analysis_dataset_", getDateTimeForFilename(), ".csv"))  
  
