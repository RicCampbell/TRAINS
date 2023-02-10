## Script for making document for CPRD of practices to investigate

library(data.table)
library(lubridate)
library(openxlsx)

## Read in practice and randomisation files

  practices <- fread("D:/source_data/Deliveryv2/Data/Primary care/TRAINS_Extract_Practice_001.txt",
                     colClasses = "character")
  
  randomisation <- fread("D:/reference_data/TRAINS Randomisation for CPRD.txt",
                         colClasses = "character")

  
## Convert last contribution field to a date
  
  practices[, lcd := as.Date(lcd, format = "%d/%m/%Y")]
  
  
## Find one practice that should not have been included as has not supplied data in 12 month period required
## Patients need to have a drug prescription in 12 months 2020-06-01 - 2021-05-31
  
  no_recent_contribution_practice <- practices[lcd < as.Date("2020-06-01")][, notes := "Last contribution date before date of interest"][, .(pracid, notes)]
  

## Find all practices that have last contributed in a time period that would be problematic if merged at this point
## Could have merged into a contributing practice
  
  no_upto_date_contribution <- practices[lcd < as.Date("2022-01-01") & lcd >= as.Date("2020-06-01")][, notes := "Last contribution date before end of period of interest"][, .(pracid, notes)]
  
  
## Find practice that was duplicated in randomisation file
  
  dup_rand <- randomisation[,.N, by = CPRDPracticeId][N > 1]
  
  duplicate_rand <- randomisation[CPRDPracticeId %chin% dup_rand$CPRDPracticeId][, notes := "Practice occured twice in randomisation file"][, .(CPRDPracticeId, deciles, notes)]
  
  
## Write all these to one excel file
  
  workbook <- createWorkbook()
  
  addWorksheet(workbook, "no_recent_contribution_practice")
  addWorksheet(workbook, "no_upto_date_contribution")
  addWorksheet(workbook, "duplicate_rand")
  
  writeDataTable(workbook, sheet = 1, x = no_recent_contribution_practice, colNames = TRUE)
  writeDataTable(workbook, sheet = 2, x = no_upto_date_contribution, colNames = TRUE)
  writeDataTable(workbook, sheet = 3, x = duplicate_rand, colNames = TRUE)
  
  
  file_path <- paste0("D:/TRAINS/data/datasets_outputs/CPRD_practices_of_interest_", format(Sys.time(), "%Y-%m-%d"), ".xlsx")
  
  saveWorkbook(workbook, file = file_path, overwrite = TRUE)
  
  
  
  