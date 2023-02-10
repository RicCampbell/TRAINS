## Ad-hoc reading in of synthetic CPRD data

library(data.table)


consultation <- fread("D:/TRAINSDM/data/synthetic_data/version3/consultation.csv",
                      colClasses = "character")

observation <- fread("D:/TRAINSDM/data/synthetic_data/version3/observation.csv",
                      colClasses = "character")

practice <- fread("D:/TRAINSDM/data/synthetic_data/version3/practice.csv",
                     colClasses = "character")

patient <- fread("D:/TRAINSDM/data/synthetic_data/version3/Patient.csv",
                 colClasses = "character")


## Text version is a lot smaller! - but original one reads in fine.

consultation_text <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/Consultation.txt",
                      colClasses = "character")

observation_text <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/Observation.txt",
                     colClasses = "character")

practice_text <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/Practice.txt",
                  colClasses = "character")

patient_text <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/patient.txt",
                 colClasses = "character")




## This one doesn't work as there is a comma in a line (so looks like 7 values instead of 6)
## "Druid, follower of religion

ClinicalCode <- fread("D:/TRAINSDM/data/synthetic_data/version3/ClinicalCode.csv",
                      colClasses = "character")


## Second set of files sent

ClinicalCode <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/ClinicalCode.txt",
                      colClasses = "character")


## Only has 3 field names on top row, but 5 fields

DrugCode <- fread("D:/TRAINSDM/data/synthetic_data/version3/DrugCode.csv",
                  colClasses = "character")


## Second set of files sent

DrugCode <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/DrugCode.txt",
                  colClasses = "character")


DrugIssue <- fread("D:/TRAINSDM/data/synthetic_data/version3/DrugIssue.csv",
                  colClasses = "character")


## csv with descriptions with commas in!!

lkpConsultationSourceTerm <- fread("D:/TRAINSDM/data/synthetic_data/version3/lkpConsultationSourceTerm.csv",
                                   colClasses = "character")

## Second set of files sent

lkpConsultationSourceTerm_text <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/lkpConsultationSourceTerm.txt",
                                   colClasses = "character")



lkpGender <- fread("D:/TRAINSDM/data/synthetic_data/version3/lkpGender.csv",
                                   colClasses = "character")



## Second set of files sent

MedicalDictionary <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/MedicalDictionary.txt",
                      colClasses = "character")

region_text <- fread("D:/TRAINSDM/data/synthetic_data/SynAurumRelease_textfiles_v3/textfiles/lkpRegion.txt",
                     colClasses = "character")

