## File of functions for creating schemas of CPRD tables
## Schema's will not involved dates as (currently) parsing errors occur

library(arrow)

## Create patient table schema

getSchema <- function(table_name) {
  
  if(table_name == "patient") {
    
    schema <- schema(patid = string(),
                     pracid = string(),
                     usualgpstaffid = string(),
                     gender = int64(),
                     yob = int64(),
                     mob = int64(),
                     emis_date = string(),
                     regstartdate = string(),
                     patienttypeid = int64(),
                     regendate = string(),
                     acceptable = int64(),
                     cprd_ddate = string())
    
  }
  
  if(table_name == "practice")  {
    
    schema <- schema(pracid = string(),
                     lcd = string(),
                     urs = string(),
                     region = int64())
  }
  
  if(table_name == "staff") {
    
    schema <- schema(staffid = string(),
                     pracid = string(),
                     jobcatid = int64())
  }
  
  if(table_name == "consultation")  {
    
    schema <- schema(patid = string(),
                     consid = string(),
                     pracid = string(),
                     consdate = string(),
                     enterdate = string(),
                     staffid = string(),
                     conssourceid = string(),
                     cprdcpnstype = string(),
                     consmedcodeid = string())
  }
  
  if(table_name == "observation") {
    
    schema <- schema(patid = string(),
                     consid = string(),
                     pracid = string(),
                     obsid = string(),
                     obsdate = string(),
                     enterdate = string(),
                     staffid = string(),
                     parentobsid = string(),
                     medcodeid = string(),
                     value = float64(),
                     numunitid = int64(),
                     obstypeid = int64(),
                     numrangelow = float64(),
                     numrangehigh = float64(),
                     probobsid = string())
  }
  
  if(table_name == "referral") {
    
    schema <- schema(patid = string(),
                     obsid = string(),
                     pracid = string(),
                     refsourceorgid = int64(),
                     reftargetorgid = int64(),
                     refurgencyid = int64(),
                     refservicetypeid = int64(),
                     refmodeid = int64())
  }
  
  if(table_name == "problem")  {
    
    schema <- schema(patid = string(),
                     obsid = string(),
                     pracid = string(),
                     parentprobobsid = string(),
                     probenddate = string(),
                     expduration = int64(),
                     lastrevdate = string(),
                     lastrevstaffid = string(),
                     parentprobrelid = int64(),
                     probstatusid = int64(),
                     signid = int64())
  }
  
  if(table_name == "drug") {
    
    schema <- schema(patid = string(),
                     issueid = string(),
                     pracid = string(),
                     probobsid = string(),
                     drugrecid =  string(),
                     issuedate = string(),
                     enterdate = string(),
                     staffid = string(),
                     prodcodeid = string(),
                     dosageid = string(),
                     quantity = float64(),
                     quantunitid = int64(),
                     duration = int64(),
                     estnhscost = float64())
  }
  
  return(schema)
}