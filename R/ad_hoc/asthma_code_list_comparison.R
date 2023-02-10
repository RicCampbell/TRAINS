## Ad_hoc script to check the differences between two asthma code lists

library(data.table)
library(readxl)


## Read in both sets of lists

  website_list <- data.table(read_xls("D:/reference_data/PLEASANT_read_codes_website.xlsx",
                                       sheet = "asthma_codes",
                                       col_names = TRUE))
  
  asthma_inclusions_list <- fread("D:/reference_data/asthma_inclusions.csv",
                                  colClasses = "character")
  
  
  website_extra_term <- setdiff(website_list$read_term, asthma_inclusions_list$read.term)
  
  inclusion_extra_term <- setdiff(asthma_inclusions_list$read.term, website_list$read_term)
  
  website_extra_code <- setdiff(website_list$clinical_events, asthma_inclusions_list$medical.event)
  
  inclusion_extra_code <- setdiff(asthma_inclusions_list$medical.event, website_list$clinical_events)
  
  full_terms <- asthma_inclusions_list[medical.event %in% inclusion_extra_code]
  
  fwrite(full_terms, "D:/reference_data/difference_in_asthma_lists.csv")
  