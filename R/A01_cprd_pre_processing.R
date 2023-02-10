library(data.table)
library(readxl)
library(arrow)
library(dplyr)
library(lubridate)
library(aurumpipeline)
library(googlesheets4)
source("R/cleaning_fns_etl.r")

## DO NOT CHANGE medcode TO NUMBER

## Patient table
## Read in patient table, convert to data table for ease (small table)

  patients <- arrow::open_dataset("D:/TRAINS/data/parquet/patient/",
                                  format = "parquet")

  
## Bring in only acceptable patients
  
  patients_table <- data.table(patients %>%
                                 filter(acceptable == 1) %>%
                                 collect())
  
  patient_count <- patients_table[,.N]


# Add age to each patient -------------------------------------------------

  ## Might be able to work out which 16 year olds are viable, but will look into late
  
  
  ## For those with no month of birth:
  ## 2006 year of birth, could be in either year 10 or 11, and should be included - create mob to create inaccurate age to be grouped later
  ## 2005 year of birth, could be in either year 11 or 12, and should be excluded
  
    patients_table[is.na(mob) & yob == 2006, mob := 1L]
    
    patients_table[!is.na(mob), created_dob := paste0(yob, "-", formatC(mob, width = 2, flag = "0"), "-01")]
    
    patients_table[, created_dob := as.Date(make_date(year = yob, month = mob, day = 1L))]
  
  
  ## There are two dates of interest:
  ## 1) School year that patient is in, and therefore age at start of school year 2021, 1st September
  ## 2) Age at end of primary outcome
  ## However, as we only have a month of birth, these will be the same for everything, as have set dob to first of month.
  
    patients_table[, age := trunc(interval(created_dob, "2021-09-01")/years(1))]
  

    
# Add ethnicity to each patient -------------------------------------------

  ## Read in patient id to ethnicity term lookup
  ## MedcodeID or SNOMED code was lost as used the AurumPipeline function that returns only patientid and term
    
    patient_ethnicity_lookup <- readRDS("D:/TRAINS/data/lookup_tables/patient_ethnicity_lookup-2022-10-17-143452.rds")
    
    
  ## Read in mapping file for group to readable term
    
    ethnicity_mapping_census_terms <- data.table(read_sheet(ss = "https://docs.google.com/spreadsheets/d/1q6xfRWPgcse2qOXWNIEVaJjPjs0kmZ2cgbNXwDO0Jo8/edit#gid=0",
                                                            sheet = "ethnicity_census_2021_lookup"))
    
  ## Check no missing final census codes
    
    stopifnot(patient_ethnicity_lookup[is.na(census_code), .N] == 0)
  
  
  ## Merge to add census grouping to patient table
    
    patient_ethnicity_census_lookup <- merge(patient_ethnicity_lookup,
                                             ethnicity_mapping_census_terms,
                                             by.x = "census_code",
                                             by.y = "ethnicity_code_2021",
                                             all.x = TRUE)
    
    
  ## Merge patient table with mapping to add census category to patient table
    
    patient_count <- patients_table[, .N]
    
    patients_table <- merge(patients_table,
                            patient_ethnicity_census_lookup,
                            by = "patid",
                            all.x = TRUE)
    
    stopifnot(patients_table[, .N] == patient_count)
    
    
  ## Save patient table with added age and ethnicity fields

    saveRDS(patients_table,
            file = paste0("D:/TRAINS/data/datasets/patient_table-", getDateTimeForFilename(), ".rds"))
    

# Practice table ----------------------------------------------------------

  ## Point to parquet file, read into a data table for ease and remove duplicate practice
  ## using != on a filter seems to have odd consequences, just remove practice after for now
  
    practices <- arrow::open_dataset("D:/TRAINS/data/parquet/practice/",
                                     format = "parquet")
    
    
    practices_table <- data.table(practices %>%
                                    filter(pracid != "20155") %>%
                                    collect())
    

  ## Merge in randomisation to practice table and save
    
    randomisation <- fread("D:/reference_data/TRAINS Randomisation for CPRD.txt",
                           colClasses = "character")
    
    practices_rand <- merge(practices_table[ ,.(pracid, lcd, region, lcd_year)],
                            randomisation[, .(CPRDPracticeId, deciles, Randomisation, `Randomisation Code`)],
                            by.x = "pracid",
                            by.y = "CPRDPracticeId",
                            all.x = TRUE)
  
  ## Change col names
  
    setnames(practices_rand, colnames(practices_rand), c("pracid", "lcd", "region", "lcd_year", "deciles", "randomisation", "randomisation_code"))
  
  
  ## Check all practices remain
  
    stopifnot(practices_table[, .N] == practices_rand[, .N])
    stopifnot(practices_rand[is.na(randomisation_code), .N] == 0)
  
    saveRDS(practices_rand,
            file = paste0("D:/TRAINS/data/datasets/practice_table-", getDateTimeForFilename(), ".rds"))

    
