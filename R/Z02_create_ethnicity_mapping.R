## Script that uses found SNOMED codelist for ethnicity, finds all that are present in data
## Then uses a mapping from NHSD to map SNOMED codes to 2011 census ethnicity groups 
## Saves this in project Google drive, but there are 9 codes and terms that are not mapped
## These will need completing manually by the study team.

library(data.table)
library(googlesheets4)
library(dplyr)
library(arrow)
source("R/cleaning_fns_etl.r")


# Find the SNOMED codes that are actually in use in data to limit  --------


  ## Point to observation tables
    
    observations <- arrow::open_dataset("D:/TRAINS/data/parquet/observation/asthma_diag",
                                        format = "parquet")
  
  ## Read in CPRD Medical dictionary
    
    medical_dictionary <- fread("D:/source_data/Deliveryv2/Dictionary Browsers/CPRD_CodeBrowser_202205_Aurum/CPRDAurumMedical.txt",
                                colClasses = "character")[, .(MedCodeId, SnomedCTConceptId)]

    
  ## Read in OpenCodelists SNOMED ethnicity code list, and rename cols

    ethnicity_codelist <- fread("D:/reference_data/opensafely-ethnicity-snomed-2022-09-20-144946.csv",
                                colClasses = "character")[, .(id, name)]
    
    setnames(ethnicity_codelist, colnames(ethnicity_codelist), c("snomedcode", "ethnicity"))


  ## Read in NHS Digital SNOMED-census mapping
    
    nhsd_ethnicity_mapping <- fread("D:/reference_data/nhsd_snomed_census_ethnic_mapping.csv",
                                    colClasses = "character")
    
  ## Merge opendCodelist and NSH Digital, to get a full list of all ethnicity SNOMED codes (some missing terms, some missing census category group)
    
    ethnicity_snomed_census <- merge(ethnicity_codelist,
                                     nhsd_ethnicity_mapping,
                                     by.x = "snomedcode",
                                     by.y = "ConceptId",
                                     all = TRUE)
    
  ## Check have everything
    
    stopifnot(ethnicity_codelist[!(snomedcode %in% ethnicity_snomed_census$snomedcode)][, .N] == 0)
    stopifnot(nhsd_ethnicity_mapping[!(ConceptId %in% ethnicity_snomed_census$snomedcode)][, .N] == 0)
    

  ## Merge together with the medical dictionary to get a list using medcodeid's

    medcode_ethnicity <- merge(ethnicity_snomed_census,
                               medical_dictionary,
                               by.x = "snomedcode",
                               by.y = "SnomedCTConceptId",
                               all = FALSE)

    setnames(medcode_ethnicity, colnames(medcode_ethnicity), c("snomedcode", "ethnicity", "census_code", "medcodeid"))
    

  ## From all observations limit to those not asthma related, limit the cols, and select only those in the ethnicity code list

    observations_ethnicity <- data.table(observations %>%
                                           filter(asthma_diag == "false") %>%
                                           select(obsdate, medcodeid, patid) %>%
                                           filter(medcodeid %in% medcode_ethnicity$medcodeid) %>%
                                           collect)
    
  ## Use this to isolate the codes we are interested in providing in the standardisation spreadsheet
    
    standisation_data <- medcode_ethnicity[medcodeid %in% observations_ethnicity$medcodeid]
    
  ## Export this table to shared drive to manually complete the missing data
    
    sheet_write(data = standisation_data,
                ss = "https://docs.google.com/spreadsheets/d/1q6xfRWPgcse2qOXWNIEVaJjPjs0kmZ2cgbNXwDO0Jo8/edit#gid=0",
                sheet = "ethnicity_standardisation")
    
    
    
##~~~~~ Manual processing of any codes that do not have a readable term or census category grouping are added here
    
  ## Read in standardisation data from shared Google drive
    
    ethnicity_standardisation <- data.table(read_sheet(ss = "https://docs.google.com/spreadsheets/d/1q6xfRWPgcse2qOXWNIEVaJjPjs0kmZ2cgbNXwDO0Jo8/edit#gid=0",
                                                       sheet = "ethnicity_standardisation"))
    
    
    
  ## Use Aurum pipeline function to add the terms and cesus category group from the list created above
    
    patient_ethnicity_lookup <- aurumpipeline::add_ethnicity(observations_ethnicity, ethnicity_standardisation, c("ethnicity", "census_code"))
    
    
  ## Check that each patient only appears once
    
    stopifnot(patient_ethnicity_lookup[, .N, by = patid][N > 1, .N] == 0)
    
    
  ## Save patid-ethnicity-census term table so can add census code later and use to add this to analysis dataset
    
    saveRDS(patient_ethnicity_lookup,
            file = paste0("D:/TRAINS/data/lookup_tables/patient_ethnicity_lookup-", getDateTimeForFilename(), ".rds"))
    

  