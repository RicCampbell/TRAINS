### Script for working on the consultation table (outcomes 10 & 11)

library(arrow)
library(dplyr)
library(lubridate)
library(data.table)
library(googlesheets4)


## Point to consultation table
## Remove any consultation records for practice that has been removed due to duplication

  consultation <- arrow::open_dataset("D:/TRAINS/data/parquet/consultation",
                               format = "parquet")
  

## Get all consultations in 2021 and 2022 before April, not in duplicated practice
  
  consultation_interest <- data.table(consultation %>%
                                        filter ((consdate_year == 2021 | consdate_year == 2022) & (pracid != "20155")) %>%
                                        collect())
  
  consultation_interest <- consultation_interest[consdate < "2022-04-01"]

  
## Group by consultation source code identifier
  
  consulation_interest_reason <- consultation_interest[, .N, by = consmedcodeid]
  

## Read in the Medical dictionary to attach a readable term (tidy this up as not in good order/code chunks)
  
  cprd_medical_dictionary <- fread("D:/source_data/Deliveryv2/Dictionary Browsers/CPRD_CodeBrowser_202205_Aurum/CPRDAurumMedical.txt",
                                   colClasses = "character")
  
  consultation_interest_reason_term <- merge(consulation_interest_reason,
                                             cprd_medical_dictionary,
                                             by.x = "consmedcodeid",
                                             by.y = "MedCodeId",
                                             all.x = TRUE)[, .(consmedcodeid, N, Term)]
  
  stopifnot(consultation_interest[, .N] == consultation_interest_reason_term[, sum(N)])
  
  consultation_interest_reason_term[, N := round(N/5)*5]
  
  consultation_interest_reason_term[ N == 0 | N == 5, N := NA]
  
  setorder(consultation_interest_reason_term, consmedcodeid, Term, N)
  
  consultation_interest_reason_term <- consultation_interest_reason_term[order(-N, na.last = TRUE)]
  
  
## Export this to the shared Google Drive
## Commented out so do not overwrite sheet as has been saved
  
  # sheet_write(data = consultation_interest_reason_term,
  #             ss = "https://docs.google.com/spreadsheets/d/1uZemsA6tzggJn7OukM2ITwxy0eJaDBgmMaw7HnnuxEM/edit#gid=0",
  #             sheet = "consultation_codes")
  
  
  
  
  
    