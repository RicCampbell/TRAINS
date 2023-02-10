library(data.table)
library(usethis)
library(readxl)
source("R/cleaning_fns_etl.r")



# Download OpenCodelists ethnicity code list ------------------------------

  download.file(url = "https://www.opencodelists.org/codelist/opensafely/ethnicity-snomed/2020-04-27/download.csv",
                destfile = paste0("D:/reference_data/opensafely-ethnicity-snomed-", getDateTimeForFilename(), ".csv"),
                mode = "wb")

