## Script for doing some checks on the data received from CPRD
## Updated for second version

library(data.table)
library(readxl)
library(aurumpipeline)
library(arrow)
library(dplyr)
source("R/cleaning_fns_etl.r")

## Read in tables relating to patient or practice

  patients <- fread("D:/source_data/Deliveryv2/Data/Primary care/TRAINS_Extract_Patient_001.txt",
                    colClasses = "character")
  
  practices <- fread("D:/source_data/Deliveryv2/Data/Primary care/TRAINS_Extract_Practice_001.txt",
                    colClasses = "character")

  
## Linkage table has, amongst other things, patient-practice link (although this is already in patient)
## patient_imd is patient-practice-imd 20ths

  
  linkage_availability <- fread("D:/source_data/Deliveryv2/Data/Linked data/linkage_eligibility_aurumv2.txt",
                                colClasses = "character")
  
  patient_imd <- fread("D:/source_data/Deliveryv2/Data/Linked data/patient_imd2019_set22v2.txt",
                       colClasses = "character")
  
  randomisation <- fread("D:/reference_data/TRAINS Randomisation for CPRD.txt",
                         colClasses = "character")
  
  setnames(randomisation, make.names(colnames(randomisation), unique = TRUE))
  
  
## Merge patient and practice by link file to check are the same
  
  patient_prac <- merge(patients,
                        linkage_availability[, .(patid, pracid)],
                        by = "patid",
                        all = TRUE)
  
  stopifnot(patient_prac[pracid.x != pracid.y, .N] == 0)

## Check for duplicates

  patients[, duplicated_record := duplicated(patients)]
  practices[, duplicated_record := duplicated(practices)]
  linkage_availability[, duplicated_record := duplicated(linkage_availability)]
    
  stopifnot(patients[duplicated_record == TRUE, .N] == 0)
  stopifnot(practices[duplicated_record == TRUE, .N] == 0)
  stopifnot(linkage_availability[duplicated_record == TRUE, .N] == 0)
  
## Check same number of patients and practices in all files
  stopifnot(patients[, .N] == patients[, uniqueN(patid)])
  stopifnot(practices[, .N] == patients[, uniqueN(pracid)])
  stopifnot(linkage_availability[, .N] == linkage_availability[, uniqueN(patid)])

## Check patient/practice ids are throughout all relevant files  
  ## Check patient ids in patient and link files are the same
    stopifnot(length(setdiff(patients[, patid], linkage_availability[, patid])) == 0)
    stopifnot(length(setdiff(linkage_availability[, patid], patients[, patid])) == 0)
  
  ## Check patient ids in patient and IMD files are the same
  ### ~~~~ See below ~~~~~
    stopifnot(length(setdiff(patient_imd[, patid], patients[, patid])) == 0)
  
  ## Check practice ids in practice and patient files are the same
    stopifnot(length(setdiff(patients[, unique(pracid)], practices[, pracid])) == 0)
    
  ## Check practice ids in practice and link files are the same
    stopifnot(length(setdiff(practices[, pracid], linkage_availability[, pracid])) == 0)
    stopifnot(length(setdiff(linkage_availability[, pracid], practices[, pracid])) == 0)
    
  ## Check practice ids in practice and randomisation are the same
  ### ~~~~ See below ~~~~~
    stopifnot(length(setdiff(practices[, pracid], randomisation[, CPRDPracticeId])) == 0)


## These numbers below are also in the RMarkdown report
    
## Not all patient IDS that are in the patient file have been supplied in the IMD file
  ##stopifnot(length(setdiff(patients[, patid], patient_imd[, patid])) == 0)
  
  # patients_no_imd <- patients[patid %chin% setdiff(patients[, patid], patient_imd[, patid])]
  
## Not all practice IDs that are in the randomistaion file, have been supplied in the practice file
  ##stopifnot(length(setdiff(randomisation[, CPRDPracticeId], practices[, pracid])) == 0)
  
  # practices_randomised_no_data <- randomisation[CPRDPracticeId %chin% setdiff(randomisation[, CPRDPracticeId], practices[, pracid])]
  
    
## Tidy up environment before 'opening' parquet files
    
  patients[, duplicated_record := NULL]
  practices[, duplicated_record := NULL]
      
  rm(list = setdiff(ls(), c("patients", "practices")))


# Check have received all fields ------------------------------------------

## All fields in all parquet files should be string at this point
  
  staff <- arrow::open_dataset("D:/source_data/parquet_v2/staff",
                                      format = "parquet")
  
  consultation <- arrow::open_dataset("D:/source_data/parquet_v2/consultation",
                                       format = "parquet")
  
  observation <- arrow::open_dataset("D:/source_data/parquet_v2/observation",
                                      format = "parquet")
  
  referral <- arrow::open_dataset("D:/source_data/parquet_v2/referral",
                               format = "parquet")
  
  problem <- arrow::open_dataset("D:/source_data/parquet_v2/problem",
                               format = "parquet")
  
  drug <- arrow::open_dataset("D:/source_data/parquet_v2/drug",
                               format = "parquet")


## Read in each sheet of CPRD meta data file created from AURUM data spec
  
  patient_meta <- data.table(read_excel("D:/reference_data/cprd_metadata_2022-07-05.xlsx",
                                        sheet = "Patient_table_meta",
                                        col_names = TRUE,
                                        col_types = "text",
                                        trim_ws = TRUE))
  
  practice_meta <- data.table(read_excel("D:/reference_data/cprd_metadata_2022-07-05.xlsx",
                                         sheet = "Practice_table_meta",
                                         col_names = TRUE,
                                         col_types = "text",
                                         trim_ws = TRUE))
  
  staff_meta <- data.table(read_excel("D:/reference_data/cprd_metadata_2022-07-05.xlsx",
                                      sheet = "Staff_table_meta",
                                      col_names = TRUE,
                                      col_types = "text",
                                      trim_ws = TRUE))
  
  consultation_meta <- data.table(read_excel("D:/reference_data/cprd_metadata_2022-07-05.xlsx",
                                             sheet = "Consultation_table_meta",
                                             col_names = TRUE,
                                             col_types = "text",
                                             trim_ws = TRUE))
  
  observation_meta <- data.table(read_excel("D:/reference_data/cprd_metadata_2022-07-05.xlsx",
                                            sheet = "Observation_table_meta",
                                            col_names = TRUE,
                                            col_types = "text",
                                            trim_ws = TRUE))
  
  referral_meta <- data.table(read_excel("D:/reference_data/cprd_metadata_2022-07-05.xlsx",
                                         sheet = "Referral_table_meta",
                                         col_names = TRUE,
                                         col_types = "text",
                                         trim_ws = TRUE))
  
  problem_meta <- data.table(read_excel("D:/reference_data/cprd_metadata_2022-07-05.xlsx",
                                        sheet = "Problem_table_meta",
                                        col_names = TRUE,
                                        col_types = "text",
                                        trim_ws = TRUE))
  
  drug_meta <- data.table(read_excel("D:/reference_data/cprd_metadata_2022-07-05.xlsx",
                                     sheet = "Drug_issue_table_meta",
                                     col_names = TRUE,
                                     col_types = "text",
                                     trim_ws = TRUE))

## Check meta data against files received
  
  missing_patient_cols <- patient_meta[!patient_meta[, field_name] %in% colnames(patients)][, field_name]
  extra_patient_cols <- colnames(patients)[!colnames(patients) %in% patient_meta[, field_name]]
  
  missing_practice_cols <- practice_meta[!practice_meta[, field_name] %in% colnames(practices)][, field_name]
  extra_practice_cols <- colnames(practices)[!colnames(practices) %in% practice_meta[, field_name]]
  
  missing_staff_cols <- staff_meta[!staff_meta[, field_name] %in% staff$schema$names][, field_name]
  extra_staff_cols <-staff$schema$names[!staff$schema$names %in% staff_meta[, field_name]]
  
  missing_consultation_cols <- consultation_meta[!consultation_meta[, field_name] %in% consultation$schema$names][, field_name]
  extra_consultation_cols <- consultation$schema$names[!consultation$schema$names %in% consultation_meta[, field_name]]
  
  missing_observation_cols <- observation_meta[!observation_meta[, field_name] %in% observation$schema$names][, field_name]
  extra_observation_cols <- observation$schema$names[!observation$schema$names %in% observation_meta[, field_name]]
  
  missing_referral_cols <- referral_meta[!referral_meta[, field_name] %in% referral$schema$names][, field_name]
  extra_referral_cols <- referral$schema$names[!referral$schema$names %in% referral_meta[, field_name]]
  
  missing_problem_cols <- problem_meta[!problem_meta[, field_name] %in% problem$schema$names][, field_name]
  extra_problem_cols <- problem$schema$names[!problem$schema$names %in% problem_meta[, field_name]]
  
  missing_drug_cols <- drug_meta[!drug_meta[, field_name] %in% drug$schema$names][, field_name]
  extra_drug_cols <- drug$schema$names[!drug$schema$names %in% drug_meta[, field_name]]


## Check there is nothing in all these

  stopifnot(lengths(c(missing_patient_cols, extra_patient_cols,
                      missing_practice_cols, extra_practice_cols,
                      missing_staff_cols, extra_staff_cols,
                      missing_consultation_cols, extra_consultation_cols,
                      missing_observation_cols, extra_observation_cols,
                      missing_referral_cols, extra_referral_cols,
                      missing_problem_cols, extra_problem_cols,
                      missing_drug_cols, extra_drug_cols)) == 0)
  

  
  
  
## Quick tests for graphs want for obs and drugs, but using small amount of data
## Prep before using parquent file
library(ggplot2)
library(lubridate)
library(dplyr)
## read in data in dpylr, then in using arrow
  
  obs1 <- fread("D:/source_data/Delivery/Data/Primary care/TRAINS_Extract_Observation_001.txt",
                    sep = "\t",
                    colClasses = "character",
                    nrows = 10000L)
    
  obs2 <- read.csv2("D:/source_data/Delivery/Data/Primary care/TRAINS_Extract_Observation_001.txt",
                    sep = "\t",
                    colClasses = "character",
                    nrows = 10000L)
  
  drug1 <- read.csv2("D:/source_data/Delivery/Data/Primary care/TRAINS_Extract_DrugIssue_001.txt",
                    sep = "\t",
                    colClasses = "character",
                    nrows = 10000L)
  
  pat1 <- arrow::open_dataset("D:/source_data/parquet/patient",
                              format = "parquet")
  
  observation <- arrow::open_dataset("D:/source_data/parquet/observation",
                                     format = "parquet")


## Print out selected cols using arrow  
  pat1 %>%
    select(patid, regstartdate) %>%
    collect %>%
    print()
  
## This works and pulls in required data (because of floor_date), but have to remember, base parquet file is not changed 
  pat1 %>%
    select(regstartdate) %>%
    collect() %>%
    mutate(regdate_month_floor = floor_date(as.Date(regstartdate, format = "%d/%m/%Y"), unit = "month"))
  
  obs2 %>%
    select(patid, obsdate) %>%
    mutate(obsdate_floor = paste0("01/", substr(obsdate, 4, 10))) %>%
    mutate(obsdate_month = as.Date(obsdate_floor, format = "%d/%m/%Y"))
  
  drug_table <- data.table(drug %>%
                             filter(issuedate != "issuedate") %>%
                             select(issuedate) %>%
                             collect())
    
    
    
    
## Graph has to be done in one go, as no changes to parquet file, could store pat1 with changes but would be in memory  
  ggplot(pat1 %>%
           select(regstartdate) %>%
           collect() %>%
           mutate(regdate_month_floor = floor_date(as.Date(regstartdate, format = "%d/%m/%Y"), unit = "month")) %>%
           group_by(regdate_month_floor) %>%
           summarise(n = n()),
         aes(x = regdate_month_floor, y = n)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("Number of observations over time by month for all patients")
    
  
## Plot of number of drugs over time by month

  ggplot(obs2 %>%
           select(obsdate) %>%
           filter(obsdate != "obsdate" & !is.na(obsdate) & obsdate != "") %>%
           map_batches(function(batch) {
             batch %>%
               mutate(obsdate_month = paste0("01/", substr(obsdate, 4, 10))) %>%
               mutate(obsdate_month = as.Date(obsdate_month, format = "%d/%m/%Y")) %>%
               group_by(obsdate_month) %>%
               summarise(N = n())    
           }) %>%
           filter(obsdate_month >= "2004-01-01") %>%
           group_by(obsdate_month) %>%
           summarise(N = sum(N)),
         aes(x = obsdate_month, y = N)) +
    geom_point() + 
    geom_smooth() + 
    ggtitle("Number of observations over time by month for all patients")

  
## Get the count of each code in observations
  
  diag_counts <- data.table(obs2 %>%
                              group_by(medcodeid) %>%
                              collect() %>%
                              summarise(N = n()))

    
  
  
  
## Trying out Health Foundation Analytics Lab Aurum Pipeline package
  
###~~~ DO NOT USE THIS AS THROWS ERROR WHEN USING AURUM_PIPELINE FUNCTION, SO CAN NOT TRUST OUTPUT PARQUET FILES ~~~~####

# observation <- aurum_pipeline(type = "Observation",
#                        dataloc = "D:/source_data/Delivery/Data/Primary care")
# 
# drug_issue <- aurum_pipeline(type = "DrugIssue",
#                              dataloc = "D:/source_data/Delivery/Data/Primary care")
# 
# patient <- aurum_pipeline(type = "Patient",
#                        dataloc = "D:/source_data/Delivery/Data/Primary care")
# 
# practice <- aurum_pipeline(type = "Practice",
#                           dataloc = "D:/source_data/Delivery/Data/Primary care")
# 
# patient_table <- opendt("Patient")
# check_vars("Patient")
# check_patid_links(dts = c("Patient", "Observation", "DrugIssue"))
