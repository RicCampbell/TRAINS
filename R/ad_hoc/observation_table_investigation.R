## Script for looking into observations table and the linkage between tables


library(arrow)
library(dplyr)
library(data.table)

  ## Point to observation table with only asthma diags in (medcodeid is the diagnosis field)
  
    observation_asthma <- arrow::open_dataset("D:/TRAINS/data/parquet/observation/asthma_diag",
                                              format = "parquet")
    
    asthma_obs <- data.table(observation_asthma %>%
                               filter(asthma_diag == "true") %>%
                               collect())

  ## Remove fields of no interest
    
    asthma_obs <- asthma_obs[, .(patid, pracid, obsdate, medcodeid, Term, obsdate_year, asthma_diag)]
    
    
  ## testing something - someone has an middle ear infection (otitis media), which was linked to suspected asthma
  ## this means not all diags linked to asthma fall under 'asthma_diag == "true"'
    observation_asthma %>%
      filter(obsid == "12614624752") %>%
      collect()
    