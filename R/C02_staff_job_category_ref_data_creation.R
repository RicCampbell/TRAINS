## Script for looking at the staff table

library(arrow)
library(dplyr)
library(lubridate)
library(data.table)
library(googlesheets4)

## Point to staff table
## Remove any staff records for practice that has been removed due to duplication

  staff <- arrow::open_dataset("D:/TRAINS/data/parquet/staff",
                                    format = "parquet")


## There are no years in staff table, so can only remove duplicate pracid
  
  staff_interest <- data.table(staff %>%
                                 filter(pracid != "20155") %>%
                                 collect())
  
## Sum by jbcatid as only interested in those that are used
  
  staff_interest <- staff_interest[, .N, by = jobcatid]
  
## Read in look up table and merge to add readable terms to jobcatid
  
  job_category <- fread("D:/source_data/Deliveryv2/Look-up/JobCat.txt")
  
  staff_interest_terms <- merge(staff_interest,
                                job_category,
                                by = "jobcatid",
                                all.x = TRUE)
  
  staff_interest_terms <- setorder(staff_interest_terms, -N, na.last = TRUE)[, .(jobcatid, Description)]
  
  
## Export this to the shared Google Drive
## Commented out so do not overwrite sheet as has been saved
  
  # sheet_write(data = staff_interest_terms,
  #             ss = "https://docs.google.com/spreadsheets/d/1Ro-ouWkkv5NGBAEwhxYjUEBKhkmW54vXzA_yNZ9uK8k/edit#gid=0",
  #             sheet = "job_category_descriptions")
  
  
  