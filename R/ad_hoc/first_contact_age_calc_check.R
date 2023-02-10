### Looking at if can get age of 16 year olds from first contact

library(data.table)
library(readxl)
library(arrow)
library(dplyr)


consultation %>%
  head(., 10) %>%
  collect %>%
  print()

drug %>%
  filter(asthma_drug == "true") %>%
  head(., 10) %>%
  collect %>%
  print()

## Read in patients, and only keep 16 year olds (currently not many as incorrect data sent through)

  patients <- fread("D:/source_data/Deliveryv2/Data/Primary care/TRAINS_Extract_Patient_001.txt",
                    colClasses = "character",
                    na.strings = "")
  
  sixteen_no_month <- patients[(yob == "2005" | yob == "2006") & is.na(mob)]
  
  
## Get connection to the consultation table
  
  consultation <- arrow::open_dataset("D:/source_data/parquet/consultation",
                                      format = "parquet")
  
## Only interested in the patient id and date of consultation
## And need to be a consultation when patient was very young
  
  consultation %>%
    select(patid, consdate) %>%
    filter(consdate)
  
  