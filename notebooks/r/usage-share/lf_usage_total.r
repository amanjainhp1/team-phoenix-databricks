# Databricks notebook source
# ---
# #Version 2022.08.29.1#
# title: "Large Format Usage"
# output: html_notebook
# ---
# 
# UPM from Cumulus

# COMMAND ----------

# MAGIC %run ../../scala/common/Constants

# COMMAND ----------

# MAGIC %run ../../python/common/secrets_manager_utils

# COMMAND ----------

notebook_start_time <- Sys.time()

# COMMAND ----------

dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.text("usage_data","")
dbutils.widgets.dropdown("stack", "dev", list("dev", "itg", "prd"))

# COMMAND ----------

# MAGIC %python
# MAGIC # retrieve secrets based on incoming/inputted secrets name - variables will be accessible across languages
# MAGIC 
# MAGIC sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")
# MAGIC spark.conf.set("sfai_username", sqlserver_secrets["username"])
# MAGIC spark.conf.set("sfai_password", sqlserver_secrets["password"])
# MAGIC 
# MAGIC   
# MAGIC sfai_username = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")["username"]
# MAGIC sfai_password = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")["password"]

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /usr/bin/java
# MAGIC ls -l /etc/alternatives/java
# MAGIC ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/default-java
# MAGIC R CMD javareconf

# COMMAND ----------

packages <- c("rJava", "RJDBC", "DBI", "sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR", "spatstat","readxl")

# COMMAND ----------

# MAGIC %run ../common/package_check

# COMMAND ----------

options(java.parameters = "-Xmx30g" )

# setwd("~/work") #Set Work Directory ("~/NPI_Model") ~ for root directory
options(scipen=999) #remove scientific notation
tempdir(check=TRUE)

# COMMAND ----------

aws_bucket_name <- "insights-environment-sandbox/BrentT/"
mount_name <- "insights-environment-sandbox"

tryCatch(dbutils.fs.mount(paste0("s3a://", aws_bucket_name), paste0("/mnt/", mount_name)),
 error = function(e)
print("Mount does not exist or is already mounted to cluster"))

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC cd /dbfs/mnt
# MAGIC ls -l

# COMMAND ----------

# MAGIC %scala
# MAGIC var configs: Map[String, String] = Map()
# MAGIC configs += ("stack" -> dbutils.widgets.get("stack"),
# MAGIC "sfaiUsername" -> spark.conf.get("sfai_username"),
# MAGIC "sfaiPassword" -> spark.conf.get("sfai_password"))

# COMMAND ----------

sqlserver_driver <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/dbfs/FileStore/jars/801b0636_e136_471a_8bb4_498dc1f9c99b-mssql_jdbc_9_4_0_jre8-13bd8.jar")

sfai <- dbConnect(sqlserver_driver, paste0("jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database=IE2_Prod;user=", sparkR.conf("sfai_username"), ";password=", sparkR.conf("sfai_password")))

# COMMAND ----------

todaysDate <- Sys.Date()
nyear <- 2  #number of years
start1 <- as.numeric(ifelse(lubridate::month(todaysDate)<10, (paste((lubridate::year(todaysDate)-nyear), 0,lubridate::month(todaysDate), sep="")), (paste((lubridate::year(todaysDate)-nyear), lubridate::month(todaysDate), sep=""))))

end1 <- as.numeric(ifelse(lubridate::month(todaysDate)<10, (paste(lubridate::year(todaysDate), 0,lubridate::month(todaysDate), sep="")), (paste(lubridate::year(todaysDate), lubridate::month(todaysDate), sep=""))))
# start1 <- 201307  #to set a specific start date instead of n years ago
# end1 <- 201808     #to set a specific end date instead of todays date

# ---- Specify the minimum sample size required while calculating MUT (Step 2) -------------------#

MUTminSize <- 200

# ---- Specify the minimum sample size required while calculating MUT (Step 5) -------------------#

minSize <- 20

# ---- Specify the time window that will be considered while calculating MUT (Step 7) ------------#

startMUT <- 201101 #or set to start1
endMUT  <- end1

# ---- Specify the last year avaialble in FIJI IB ------------------------------------------------#

#lastAvaiableYear <- 2057

# ---- Specify whether to replace the data of PRO by ENT after the end of MUT creation (Step 7) --#

replace <- 0 # Value 1 will replace the values of PRO by the respective values from ENT set

# ---- Specify time (YYYYMM) that works as a demarcation b/n Old and New Platforms (Step 50) -----#

oldNewDemarcation <- end1

options(stringsAsFactors= FALSE)
options(scipen=999)

# COMMAND ----------

# Step 1 - query for Normalized extract specific to PE and RM
zero_init_nm = dbutils.widgets.get("usage_data")

#zero_init = s3read_using(FUN = read.csv,object=zero_init, header=TRUE, na="", sep = ",")
#UPM <- SparkR::read.parquet(path=paste0("s3://", aws_bucket_name, "UPM_Ink_Ctry(", UPMDate, ").parquet"))

path1=paste0("s3://", aws_bucket_name,zero_init_nm)

zero_init <- read.csv2(file=paste0("/dbfs/mnt/insights-environment-sandbox/",zero_init_nm),header=TRUE, na="", sep=',')


# COMMAND ----------

#RDMA info
zero_rdma <- dbGetQuery(sfai,"SELECT Base_Prod_Number
                ,PL
                ,Base_Prod_Name
                ,Base_Prod_Desc
                ,Product_Lab_Name
                ,Prev_Base_Prod_Number
                ,Product_Class
                ,Platform
                ,Product_Platform_Desc
                ,Product_Platform_Status_Cd
                ,Product_Platform_Group
                ,Product_Family
                ,Product_Family_Desc
                ,Product_Family_Status_CD
                ,FMC
                ,Platform_Subset
                ,Platform_Subset_ID
                ,Platform_Subset_Desc
                ,Platform_Subset_Status_CD
                ,Product_Technology_Name
                ,Product_Technology_ID
                ,Media_Size
                ,Product_Status
                ,Product_Status_Date
                ,Product_Type
                ,Sheets_Per_Pack_Quantity
                ,After_Market_Flag
                ,CoBranded_Product_Flag
                ,Forecast_Assumption_Flag
                ,Base_Product_Known_Flag
                ,OEM_Flag
                ,Selectability_Code
                ,Supply_Color
                ,Ink_Available_CCs_Quantity
                ,SPS_Type
                ,Warranty_Reporting_Flag
                ,Base_Prod_AFR_Goal_Percent
                ,Refurb_Product_Flag
                ,Base_Prod_Acpt_Goal_Range_Pc
                ,Base_Prod_TFR_Goal_Percent
                ,Engine_Type
                ,Engine_Type_ID
                ,Market_Category_Name
                ,Finl_Market_Category
                ,Category
                ,Tone
                ,Platform_Subset_Group_Name
                ,ID
                ,Attribute_Extension1
                ,Leveraged_Product_Platform_Name
                ,Platform_Subset_Group_Status_Cd
                ,Platform_Subset_Group_Desc
                ,Product_Segment_Name
      FROM IE2_Prod.dbo.rdma
        WHERE PL IN ('30','GE','ID','IW','TW') 
  ")

# COMMAND ----------

#Get Market10 Information
country_info <- dbGetQuery(sfai,"
                      SELECT country_alpha2, region_5, market10, developed_emerging, country
                             FROM IE2_Prod.dbo.iso_country_code_xref with (NOLOCK)
                           ")

# COMMAND ----------

  ibtable <- dbGetQuery(sfai,paste0("
                     select  upper(a.platform_subset) as platform_subset
                            ,a.month_begin as cal_date
                            ,YEAR(a.month_begin)*100+MONTH(a.month_begin) AS month_begin
                            ,d.technology AS hw_type
                            ,a.hps_ops
                            ,a.region_5
                            ,b.market10
                            ,b.developed_emerging
                            ,a.country_alpha2
                            ,sum(a.ib) as ib
                      from IE2_Staging.test.ib_18_ce_splits_post_scrub a with (NOLOCK)
                      left join IE2_Prod.dbo.iso_country_code_xref b with (NOLOCK)
                        on (a.country_alpha2=b.country_alpha2)
                      left join IE2_Prod.dbo.hardware_xref d with (NOLOCK)
                         on (a.platform_subset=d.platform_subset)
                      where 1=1
                          and ib>0
                          and d.technology='LF'
                          and b.developed_emerging != 'NOT APPLICABLE'
                      group by a.platform_subset, a.month_begin, d.technology, a.hps_ops
                              ,a.region_5, a.country_alpha2, b.market10,b.developed_emerging
                     "))

# COMMAND ----------

#hw info

   #prod_in2 <- s3read_using(FUN = read_xlsx, object = paste0(path = "s3://insights-environment-sandbox/BrentT/RDMA Categories and Subsets - From Fiji to IE v7 25.04.2022.xlsx"), sheet='LF',col_names=TRUE, na="")
      #prod_inp <- subset(prod_in2,`HW/Supp`=='HW' & Segment != '#N/A' & `SKU Yes/No` == 'Yes')
prod_in2 <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/RDMA Categories and Subsets - From Fiji to IE v7 25.04.2022.xlsx", sheet='LF',col_names=TRUE, na="")

#str(prod_in2)
#table(prod_in2$Segment)

# COMMAND ----------

prod_inp <- subset(prod_in2,`HW/Supp`=='HW' & Segment != 'N/A' & `SKU Yes/No` == 'Yes')
#str(prod_inp)

# COMMAND ----------

penmap <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/PensxPrinter Mapping.xlsx", sheet='Map',col_names=TRUE, na="")

  
hw_info <- dbGetQuery(sfai,"
                      SELECT platform_subset, business_feature as biz, business_segment as segment, hw_product_family                       
                      FROM IE2_Prod.dbo.hardware_xref with (NOLOCK)
                      WHERE (upper(technology)='LF') 
                      ")

# COMMAND ----------

    zero_init2 <- sqldf("select distinct a.[Base Product Number] as SKUp
      , a.[Product Status]
      , a.[End of life]
      , CASE WHEN b.biz is not null then b.biz ELSE a.[Biz] end as Biz
      , CASE WHEN (b.segment is not null and b.segment != '#N/A') then b.segment else a.[Segment] end as Segment
      , a.[Final Product Family Name] 
      , upper(a.[New RDMA Subset Name]) as platform_subset
      , a.[Predecessor], a.[Financial Market Category Name]
      , a.[Product Class Code]
      , a.[Product Type]
      FROM hw_info b
      LEFT JOIN prod_inp a
        ON a.[New RDMA Subset Name]=b.[platform_subset]
      ")

# COMMAND ----------

  ibintrodt <- sqldf("
              SELECT  platform_subset
                      ,min(month_begin) AS intro_date
                      FROM ibtable
                      GROUP BY platform_subset
                     ")
  lastAvaiableYear <- as.numeric(sqldf("select max(substr(month_begin,1,4)) from ibtable"))
  firstAvaiableYear <- as.numeric(sqldf("select min(substr(month_begin,1,4)) from ibtable"))

# COMMAND ----------

zero <- sqldf("SELECT distinct --a.SKU, 
              a.[Reporting.Level] as geography, a.YearMonth, a.FY, a.FQ, a.year, a.month
              --, a.MonthlyUsage, a.MonthlyUsageSD, a.MonthlyMedia, a.MonthlyMediaSD
              --,a.MonthlyCount, a.IB, a.QuarterlyUsage, a.QuarterlyUsageSD
              --,a.QuarterlyMedia, a.QuarterlyMediaSD, a.QuarterlyCount, a.QuarterlyIB, a.YearlyUsage, a.YearlyUsageSD, a.YearlyMedia, a.YearlyMediaSD
              --,a.YearlyCount, a.YearlyIB, a.MonthlyTotalUsage, a.QuarterlyTotalUsage  
              --,a.YearlyTotalUsage, a.MonthlyTotalUsageSD, a.QuarterlyTotalUsageSD, a.YearlyTotalUsageSD, a.MonthlyTotalMedia
             -- ,a.QuarterlyTotalMedia, a.YearlyTotalMedia, a.MonthlyTotalMediaSD, a.QuarterlyTotalMediaSD
             -- ,a.YearlyTotalMediaSD, a.MonthlyReportingIB, a.QuarterlyReportingIB , a.YearlyReportingIB, a.MonthlyUsageIB, a.QuarterlyUsageIB
              --,a.YearlyUsageIB, a.[HP..], a.MonthlyTotalLiters, a.QuarterlyTotalLiters, a.YearlyTotalLiters 
              --,b.[SKUp]
              --,b.[Product Status]
              , b.[End of life], b.[Biz], b.[Segment], b.[Final Product Family Name] 
              ,upper(b.platform_subset) as platform_subset, b.[Predecessor], b.[Financial Market Category Name], b.[Product Class Code], b.[Product Type]
              --,c.Platform_Subset 
              , c.Product_Technology_Name
              
              , SUM(MonthlyUsage*1000) AS SumMPVwtN
              , SUM(IB) AS SumibWt
              , SUM(MonthlyUsage*1000*IB) AS meansummpvwtn
              , SUM(MonthlyCount) AS SumN
              , SUM(IB) AS IB
                      
              FROM zero_init a
              LEFT JOIN zero_init2 b
              ON a.SKU=b.SKUp
              LEFT JOIN zero_rdma c
              ON b.SKUp=c.Base_Prod_Number
              GROUP BY 
              --a.SKU, 
              a.[Reporting.Level], a.YearMonth, a.FY, a.FQ, a.year, a.month 
              --,b.[SKUp]
              --,b.[Product Status]
              , b.[End of life], b.[Biz], b.[Segment], b.[Final Product Family Name] 
              ,b.platform_subset, b.[Predecessor], b.[Financial Market Category Name], b.[Product Class Code], b.[Product Type]
              --,c.Platform_Subset 
              , c.Product_Technology_Name
              ")

# COMMAND ----------

#need geography to market10 and region5 mapping
  geomap <-  read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/GeoMap.xlsx",sheet='Sheet1', col_names=TRUE, na="")
  zero <- sqldf("SELECT z.*, g.market10, g.region_5, g.developed_emerging, g.country_alpha2
                FROM zero z 
                LEFT JOIN geomap g
                ON z.geography=g.LF_geography
                where z.platform_subset is not null
                ")
 
zero$pMPV <- ifelse(((zero$YearMonth) >= start1 & (zero$YearMonth <=end1)), as.numeric(zero$meansummpvwtn)/as.numeric(zero$IB),NA)
zero$pN <- ifelse(((zero$YearMonth) >= start1 & (zero$YearMonth <=end1)), zero$SumN, NA)
zero$pMPVN <- zero$pMPV*zero$pN
#zero$financial_prod_category <- ifelse(zero$financial_prod_category=="Graphics Production","GPA",zero$financial_prod_category)

# COMMAND ----------

introdatetbl <- sqldf("
                      WITH mkt10 as (SELECT platform_subset, market10, region_5, developed_emerging
                        ,min(month_begin) as intro_date
                        FROM ibtable
                        WHERE region_5 != 'JP'
                        GROUP BY platform_subset, market10, region_5, developed_emerging)
                      , rgn5 as (SELECT platform_subset, region_5, developed_emerging
                        ,min(month_begin) as intro_date
                      FROM ibtable
                      WHERE region_5 != 'JP'
                      GROUP BY platform_subset, region_5, developed_emerging)
                      , ww as (SELECT platform_subset, developed_emerging
                        ,min(month_begin) as intro_date
                      FROM ibtable
                      WHERE region_5 != 'JP'
                      GROUP BY platform_subset, developed_emerging)
                      SELECT a.platform_subset, a.market10, a.region_5, a.developed_emerging
                          ,CASE WHEN a.intro_date is not null then a.intro_date
                               WHEN b.intro_date is not null then b.intro_date
                               ELSE c.intro_date
                               END as intro_date
                      FROM mkt10 a
                      LEFT JOIN rgn5 b
                        ON a.platform_subset=b.platform_subset and a.region_5=b.region_5 
                          and a.developed_emerging=b.developed_emerging
                      LEFT JOIN ww c
                        ON a.platform_subset=c.platform_subset 
                          and a.developed_emerging=c.developed_emerging
                      
                      ")

introdatetbl$mkt10 <- ifelse(introdatetbl$market10=="Central Europe" & introdatetbl$developed_emerging=="Developed",'INTRODATE_CED', ifelse(introdatetbl$market10=="Central Europe" & introdatetbl$developed_emerging=="Emerging", 'INTRODATE_CEE',ifelse(introdatetbl$market10=="Greater Asia" & introdatetbl$developed_emerging=="Developed" , 'INTRODATE_GAD',ifelse(introdatetbl$market10=="Greater Asia" & introdatetbl$developed_emerging=="Emerging" , 'INTRODATE_GAE' ,ifelse(introdatetbl$market10=="Greater China" & introdatetbl$developed_emerging=="Developed" , 'INTRODATE_GCD',ifelse(introdatetbl$market10=="Greater China" & introdatetbl$developed_emerging=="Emerging" , 'INTRODATE_GCE'                     ,ifelse(introdatetbl$market10=="India SL & BL", 'INTRODATE_IN', ifelse(introdatetbl$market10=="ISE", 'INTRODATE_IS', ifelse(introdatetbl$market10=="Latin America", 'INTRODATE_LA' 
,ifelse(introdatetbl$market10=="North America",'INTRODATE_NA', ifelse(introdatetbl$market10=="Northern Europe",'INTRODATE_NE',ifelse(introdatetbl$market10=="Southern Europe",'INTRODATE_SE' 
                       ,ifelse(introdatetbl$market10=="UK&I",'INTRODATE_UK' , NA
                              )))))))))))))

introdatetbl2 <- reshape2::dcast(introdatetbl, platform_subset ~ mkt10	, value.var="intro_date")

# COMMAND ----------

# Note: problem with PARIS 8A record (805,441,001 pages) for 201509 in Malaysia, piid != 'dde34e2695b24785d710edf6137e53d74e254cba4ee4537057948a9f22efd838' #piid has now changed, put in other checks

# Step 2 - extract Normalized value for RM and PE based on Plat_Nm, Region_Cd and Src_Cd

# Based on the extract from Step 1, for a given group defined by Plat_Nm, Region_Cd and Src_Cd, create the 
# following variables; Sum of pMPVN as SUMpMPVN, Sum of pN as SUMpN	and SUMpMPVN/SUMpN as NormMPV

 one <- sqldf(paste("								
                     SELECT *, SUMpMPVN/SUMpN AS NormMPV	
                     , 'avaiable' as dummy
                     FROM										
                     (										
                     SELECT platform_subset, market10,developed_emerging
                     , SUM(pMPVN) AS SUMpMPVN										
                     , SUM(pN) AS SUMpN										
                     FROM										
                     zero										
                     GROUP BY platform_subset, market10, developed_emerging
                     )AA0	
                     where SUMpN >=", MUTminSize, "
                     and SUMpMPVN is not null
                     ORDER BY platform_subset, market10, developed_emerging
                     ", sep = " "))
  
  head(one)
  colnames(one)
  dim(one)
  str(one)
  
  oneT <- reshape2::dcast(one, platform_subset ~ market10 + developed_emerging	, value.var="dummy")

# COMMAND ----------

# Step 3 - combine zero and one using left outer join

# Combine outcome of Step 2 to the outcome of Step 1 using left outer join and 
  # further create the following variables; MPV_Pct = meanSumMPVWtN/ NormMPV and MPVPN =  MPV_Pct* SumN
 
two <- sqldf('select aa1.*, aa2.NormMPV 
               from 
               zero aa1 
               left outer join 
               one aa2
               on 
               aa1.platform_subset = aa2.platform_subset
               and 
               aa1.market10 = aa2.market10
               and 
               aa1.developed_emerging=aa2.developed_emerging
               where 
               aa2.NormMPV is NOT NULL 
               or 
               aa2.NormMPV != 0
               order by 
               platform_subset ASC, 								
               market10 ASC, 								
               yearmonth ASC 								
               ')
  
  two$MPV_Pct <- two$meansummpvwtn/two$NormMPV
  two$MPVPN <- two$MPV_Pct*two$SumN
  str(two)

# COMMAND ----------

# Step 4 - summary stat for a group  defined by EP, CM, printer_region_code and FYearMo

# Using the extract from Step 3, create the following summary stat for a group defined by EP,CM, printer_region_code 
# and FYearMo (notice, Plat_Nm is no more a part of a group); Sum of MPVPN as SumMPVPN and Sum of SumN as SumN

 three <- sqldf('select segment, biz, market10, developed_emerging, YearMonth
                 , sum(MPVPN) as SumMPVPN
                 --, sum(SumN) as SumN
                 , count(*) as SumN
                 FROM two
                 --WHERE PL is not null 
                 GROUP by segment, biz, market10, developed_emerging, YearMonth
                 ORDER by segment, biz, market10, developed_emerging, YearMonth
                 ')

# COMMAND ----------

# Step 5 - drop groups if respective number < 200 and create MUT

four <- sqldf(paste("select Segment, Biz, market10, YearMonth ,developed_emerging
                      , SumMPVPN
                      , SumN
                      , SumMPVPN/SumN as MUT
                      from three
                      WHERE SumMPVPN/SumN>0
                      order by segment, biz, market10, developed_emerging, YearMonth", sep = " ")
  )

# COMMAND ----------

#Step 6 - Create variable STRATA by concatenating 3 variables

# columns to paste together
  cols <- c( 'Segment', 'Biz', 'market10' , 'developed_emerging')
  
  # create a new column `x` with the four columns collapsed together
  four$strata <- apply( four[ , cols ] , 1 , paste , collapse = "_" )
  
  str(four)

# COMMAND ----------

# Step 7 - creating extended MUT dataset for each Strata

# Each extended MUT dataset contains the following variables;

# CM: Color Code: M, C
# EP: Enterprise Code: ENT, PRO
# Region_Cd: AP, EU, JP, LA, NA
# FYearMo: Year and month MUT was calculated for, YYYYMM formatted
# SumMPVPN: metric used for MUT calculation
# SumN: metric used for MUT calculation
# MUT: Monthly Usage Trend; the primary variable of interest
# strata: distinct combination of CM, EP and Region_Cd
# J90Mo: no of months lapsed since Jan, 1990
# x: independent variable of Linear fit; year(FYearMo) + (month(FYearMo)-1)/12
# y: dependent variable of Linear fit; natural log of MUT
# a0:	estimated intercept of Linear fit; y = a0+b1*x+ error
# b1: estimated slope of Linear fit; y = a0+b1*x+ error
# yhat:  a0+b1*x
# Detrend:	y - yhat (also known as residual)
# month:	numerical representation of month(FYearMo)
# seasonality	Monthly seasonal value for Detrend
# DTDS: Detrend  - seasonality
# mo_Smooth:	Rolling Window mean of DTDS (length =5)
# Eyhat: exp(yhat)
# EDetrend:	exp(Detrend)
# Eseasonality:	exp(seasonality)
# Emo_Smooth:	exp(mo_Smooth)
 
  data <- list()
  
  for (cat in unique(four$strata))
  {
    
    #print(cat)
    
    d <- subset(four, strata==cat)
    d <- d[d$YearMonth >= startMUT & d$YearMonth < endMUT & d$MUT != 0,]
    d <- d[d$YearMonth < 202003 | d$YearMonth > 202005,]  #remove covid months
    
    if (nrow(d) == 0) next
    
    yyyy <- as.numeric(substr(d$YearMonth, 1,4))
    mm <- as.numeric(substr(d$YearMonth, 5,6))
    
    d$J90Mo <- (yyyy - 1990)*12 + (mm-1)
    
    d$x <- yyyy + (mm-1)/12
    
    #d$y <- (d$MUT)
    d$y <- log(d$MUT)
    
    #dmod <- tail(d, 12)
    dmod <- d
    
    fit <- lm(y ~ x, data=dmod)
    
    
    
    #abline(fit)
    #summary(fit)
    d$a0 <- fit$coefficients[[1]]
    d$b1 <- fit$coefficients[[2]]
    d$yhat <- d$a0 + d$b1*d$x
    #d$yhat2 <- d$y[1] -0.01*(d$x-d$x[1])
    
    #fit2 <- drm(y ~ x, fct = LL.4(), data = dmod)
    #fit2 <- drm(y ~ x, fct = W2.4(), data = dmod)
    #d$bt <- fit2$coefficients[[1]]
    #d$ct <- fit2$coefficients[[2]]
    #d$dt <- fit2$coefficients[[3]]
    #d$et <- fit2$coefficients[[4]]
    #d$yhat2 <- d$ct + (d$dt - d$ct)/(1+exp(-exp(d$bt*(log(d$x)-log(d$et)))))
    #d$yhat2 <- d$ct + (d$dt - d$ct)*(exp(-exp(d$bt*log(d$x)-log(d$et))))
    #d$yhat2 <- d$bt + d$ct*log(d$x)
    
    
    d$Detrend <- d$y - d$yhat
    d$month <- mm
    
    temp <- sqldf('select month, avg(Detrend) as avgtr from d group by month order by month')
    sumtemp <- sum(temp$avgtr)
    temp$avgtr2 <- temp$avgtr-(sumtemp/12)
    
    d2 <- sqldf('select aa1.*, aa2.avgtr2 as seasonality from d aa1 left join temp aa2 on aa1.month=aa2.month
                order by YearMonth')
    
    d2$DTDS <- d2$Detrend - d2$seasonality
    d2$mo_Smooth <-rollmean(d2$DTDS, 5,fill = NA)
    d2$Irregular <- d2$y-d2$yhat-d2$seasonality-d2$mo_Smooth
    
    # d2$Eyhat <- (d2$yhat)
    # d2$Eyhat2 <- (d2$yhat2)
    # d2$EDetrend <- d2$Detrend
    # d2$Eseasonality <- (d2$seasonality)
    # d2$Emo_Smooth <-(d2$mo_Smooth)
    # d2$EIrregular <- (d2$Irregular)
    d2$Eyhat <- exp(d2$yhat)
    #d2$Eyhat2 <- exp(d2$yhat2)
    d2$EDetrend <- exp(d2$Detrend)
    d2$Eseasonality <- exp(d2$seasonality)
    d2$Emo_Smooth <- exp(d2$mo_Smooth)
    d2$EIrregular <- exp(d2$Irregular)
    
    #ymin <- min(d2$MUT,d2$Eyhat, d2$EIrregular, d2$Eseasonality, d2$Emo_Smooth, na.rm=TRUE )
    #ymax <- max(d2$MUT,d2$Eyhat, d2$EIrregular, d2$Eseasonality, d2$Emo_Smooth, na.rm=TRUE )
    #xmin <- min(d2$x)
    #xmax <- max(d2$x)
    

    #plot(d2$x, d2$MUT, typ='l', col = "#0096d6", main=paste("Decomposition for",cat),
         #xlab="Year", ylab="MUT and Other Series", xlim=c(2015, 2021), ylim=c(ymin, ymax)
         #, lwd = 1, frame.plot=FALSE, las=1, xaxt='n'
    #)Blue, las=0: parallel to the axis, 1: always horizontal, 2: perpendicular to the axis, 3: always vertical
    #axis(side=1,seq(2015,2021, by=1)) #increase number of years on x axis
    #lines(d2$x, d2$Eyhat,col="#822980",lwd=1) #purple
    #lines(d2$x, d2$EIrregular,col="#838B8B",lwd = 1)
    #lines(d2$x, d2$Eseasonality,col="#fdc643",lwd = 1) #yellow
    #lines(d2$x, d2$Emo_Smooth,col="#de2e43",lwd=1) #red
    #lines(d2$x, d2$Eyhat2,col="#822780",lwd=1, lty=3) #purple
    #box(bty="l") #puts x and y axis lines back in
    #grid(nx=NULL, ny=NULL, col="cornsilk2", lty="dotted", lwd=par("lwd"))
    #grid(nx=96, ny=NULL, col="cornsilk2", lty="dotted", lwd=par("lwd"))

    #legend("bottom", bty="n", # places a legend at the appropriate place
           #c("Original", "Trend", "Irregular", "Seasonality", "Cyclical","minslope"), # puts text in the legend
           #xpd = TRUE, horiz = TRUE,
           #lty=c(1,1,1,1,1,3), # gives the legend appropriate symbols (lines)
           #lwd=c(1,1,1,1,1,1),col=c("#0096d6", "#822980", "#838B8B", "#fdc643", "#de2e43","#822780"))
    #plot(fit2, add=T, col="#822780",lwd=1, lty=3)

    data[[cat]] <- d2
    
    #print(cat)
    
    }

# COMMAND ----------

# Step 8 - Combining extended MUT dataset for each Strata

# ---- Also implementing the Source Replacement ---------------------------- #

outcome <- as.data.frame(do.call("rbind", data))
  
  outcome$b1 <- ifelse(outcome$b1 >0, -.01,outcome$b1)
  
  outcome <- sqldf("select o.*, g.region_5
                   from outcome o
                   left join (select distinct region_5, market10, developed_emerging from geomap) g
                   on o.market10=g.market10 and o.developed_emerging=g.developed_emerging")
  
  stratps <- sqldf("SELECT distinct segment from outcome where segment is not null")
  stratfc <- sqldf("SELECT distinct biz from outcome where biz is not null")

  #stratge <- sqldf("SELECT distinct geography from outcome where geography is not null")
  stratmn <- sqldf("SELECT distinct month from outcome where month is not null")
  stratmk <- sqldf("SELECT distinct region_5, market10, developed_emerging from outcome where region_5 is not null")
  stratjn <- sqldf("SELECT a.*, b.*, d.*, e.*
                   from stratps a , stratfc b , stratmn d, stratmk e")
 
  
  #Add region, get regional estimates...
  outcome_r <- sqldf("SELECT segment, biz, developed_emerging, month, region_5, avg(b1) as b1, avg(seasonality) as seasonality
                     from outcome
                     group by segment, biz, developed_emerging, month, region_5")
  
  outcome_a <- sqldf("SELECT segment, biz, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality
                     from outcome
                     group by segment, biz, developed_emerging, month")
  
  outcome_b <- sqldf("SELECT segment, market10, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality
                     from outcome
                     group by segment, market10, developed_emerging, month")
  outcome_c <- sqldf("SELECT biz, market10, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality
                     from outcome
                     group by biz, market10, developed_emerging, month")
  outcome_d <- sqldf("SELECT biz, market10, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality
                     from outcome
                     group by biz, market10, developed_emerging, month")
  outcome_e <- sqldf("SELECT segment, biz, market10, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality
                     from outcome
                     group by segment, biz, market10, developed_emerging, month")
  outcome_f <- sqldf("SELECT biz, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality
                     from outcome
                     group by biz, developed_emerging, month")
  
  outcome2 <- sqldf(" SELECT distinct s.biz, s.segment,  s.market10, s.developed_emerging, s.month --,o.geography
                  , CASE WHEN o.b1 is not null then o.b1
                         WHEN r.b1 is not null then r.b1
                         WHEN a.b1 is not null then a.b1
                         WHEN b.b1 is not null then b.b1
                         WHEN c.b1 is not null then c.b1
                         WHEN d.b1 is not null then d.b1
                         --WHEN e.b1 is not null then e.b1
                         WHEN f.b1 is not null then f.b1
                         ELSE NULL
                    END AS b1
                  , CASE WHEN o.seasonality is not null then o.seasonality
                         WHEN r.seasonality is not null then r.seasonality
                         WHEN a.seasonality is not null then a.seasonality
                         WHEN b.seasonality is not null then b.seasonality
                         WHEN c.seasonality is not null then c.seasonality
                         WHEN d.seasonality is not null then d.seasonality
                         --WHEN e.seasonality is not null then e.seasonality
                         WHEN f.seasonality is not null then f.seasonality
                         ELSE NULL
                         END as seasonality
                  , CASE WHEN o.b1 is not null then 'self'
                         WHEN r.b1 is not null then 'reg5'
                         WHEN a.b1 is not null then 'nomkt'
                         WHEN b.b1 is not null then 'nocs'
                         WHEN c.b1 is not null then 'noprinter_size'
                         WHEN d.b1 is not null then 'nomc'
                         --WHEN e.b1 is not null then 'nopfc'
                         WHEN f.b1 is not null then 'noprinter_sizemkt'
                         ELSE NULL
                    END AS src
                
                  from stratjn s
                  
                  left join outcome o
                  on s.segment=o.segment and s.biz=o.biz  
                    and s.market10=o.market10 and s.developed_emerging=o.developed_emerging and s.month=o.month
                  left join outcome_r r
                  on s.segment=r.segment and s.biz=r.biz  
                    and s.developed_emerging=r.developed_emerging and s.region_5=r.region_5 and s.month=r.month
                  left join outcome_a a
                  on s.segment=a.segment and s.biz=a.biz 
                    and s.developed_emerging=a.developed_emerging and s.month=a.month
                  left join outcome_b b
                  on s.segment=b.segment
                    and s.market10=b.market10 and s.developed_emerging=b.developed_emerging and s.month=b.month
                  left join outcome_c c
                  on s.biz=c.biz 
                    and s.market10=c.market10 and s.developed_emerging=c.developed_emerging and s.month=c.month
                  left join outcome_d d
                  on s.biz=d.biz 
                    and s.market10=d.market10 and s.developed_emerging=d.developed_emerging and s.month=d.month
                  left join outcome_f f
                  on s.biz=f.biz 
                    and s.developed_emerging=f.developed_emerging and s.month=f.month
                   ")
  
  #outcome3 <- outcome2 %>% 
  #group_by(Biz, market10, developed_emerging, month) %>%
  #summarise(across(everything(), mean))

  outcome3 <- sqldf("SELECT biz, market10, developed_emerging, month
              , avg(b1) as b1
              , avg(seasonality) as seasonality
              FROM outcome2
              GROUP BY biz, market10, developed_emerging, month
")

  outcome3$Segment <- ifelse(outcome3$Biz=='Pro','Echo',ifelse(outcome3$Biz=='Design','NULL',NA))
  outcome3$src <- 'noprinter_size'
 
outcome2 <- dplyr::bind_rows(outcome2,outcome3)
  
  outcome2$strata <- apply( outcome2[ , cols ] , 1 , paste , collapse = "_" )
  ## What to do when decay is not negative ###


# COMMAND ----------

# Step 9 - creating decay matrix

# Create a decay matrix by extracting "b1", specific to each strata (i.e. the combination 
# of EP, CM, and Region_Cd), from the extended MUT dataset.

 decay <- sqldf('
                 select distinct biz, segment, market10, developed_emerging, strata, b1 from outcome2 
                 order by biz, segment, market10, developed_emerging
                 ')
  str(decay)

# COMMAND ----------

# Step 10 - Query to create Usage output in de-normalized form

  #since changing to Cumulus, and sources are no longer separate, zero is not limited to time (just the values zero$pMPV and zero$pN are), so is not different anymore
usage <- sqldf("with sub0 as (select a.platform_subset as printer_platform_name,c.region_5 as printer_region_code, c.country_alpha2, c.market10, c.developed_emerging, a.YearMonth as FYearMo, a.biz
                  , a.segment
                  , avg(meansummpvwtn/IB) AS SumMPV, sum(a.SumN) as SumN, sum(a.SumibWt) as SUMib
              from zero a
              left join geomap c
              on a.geography=c.LF_geography
              group by  a.platform_subset, a.geography, c.country_alpha2, c.market10, c.developed_emerging, c.region_5, a.YearMonth, a.biz, a.segment
                )
              , sub_1 AS (
              SELECT printer_platform_name
                      , printer_region_code
                      , country_alpha2
                      , market10
                      , developed_emerging
                      , FYearMo
                      , segment
                      , biz
                      , SUM(SumMPV) AS SumMPVwtN
                      , SUM(SUMib) AS SumibWt
                      , AVG(SumMPV) AS MPVa
                      , SUM(SumN) AS Na
                     FROM sub0
                      GROUP BY
                        printer_platform_name
                      , printer_region_code
                      , country_alpha2
                      , market10
                      , developed_emerging
                      , FYearMo
                      , segment
                      , biz
                      ORDER BY
                        printer_platform_name
                      , printer_region_code
                      , country_alpha2
                      , market10
                      , developed_emerging
                      , FYearMo
                      , segment
                      , biz)
                      select * from sub_1
                      where SumibWt is not null
              ")

head(usage)
colnames(usage)
dim(usage)
str(usage)

usage_platform <- sqldf('select distinct printer_platform_name, printer_region_code from usage order by printer_platform_name, printer_region_code')
usage_platform$source2 <- "tri_printer_usage_sn"
usage_platformList <- reshape2::dcast(usage_platform, printer_platform_name~printer_region_code, value.var="source2")


# ----- no. of Plat_Nm found 209 in dataset "usage" -------------------#

# COMMAND ----------

# Step 11 - Calculating sum of N for each of the 5 Sources

# Based on Usage Matrix (outcome of Step 10), Calculate sum of N for each of 
# the 5 Sources for all groups defined by Plat_Nm and Region_Cd.

u2 <- sqldf('with prc as (select printer_platform_name, printer_region_code, developed_emerging,FYearMo
            , sum(Na) as SNA
            from usage
            group by printer_platform_name, printer_region_code, FYearMo
            order by printer_platform_name, printer_region_code, FYearMo
            )
            ,prc2 as (select printer_platform_name, printer_region_code
            , max(SNA) as SNA
            from prc
            group by printer_platform_name, printer_region_code
            order by printer_platform_name, printer_region_code
            )
            ,dem as (select printer_platform_name, printer_region_code, market10, developed_emerging, FYearMo
            , sum(Na) as SNA
            from usage
            group by printer_platform_name, printer_region_code, market10, developed_emerging, FYearMo
            order by printer_platform_name, printer_region_code, market10, developed_emerging, FYearMo
            )
            ,dem2 as (select printer_platform_name, printer_region_code, market10, developed_emerging
            , MAX(SNA) as SNA
            from dem
            group by printer_platform_name, printer_region_code, market10, developed_emerging
            order by printer_platform_name, printer_region_code, market10, developed_emerging
            )
            ,mkt10 as (
            select printer_platform_name, market10, FYearMo
            , sum(na) as SNA
            from usage
            group by printer_platform_name, market10, FYearMo
            order by printer_platform_name, market10, FYearMo
            )
            ,mkt102 as (
            select printer_platform_name, market10
            , max(SNA) as SNA
            from mkt10
            group by printer_platform_name, market10
            order by printer_platform_name, market10
            )
            ,ctry as (select printer_platform_name, country_alpha2, FYearMo
            , sum(na) as SNA
            from usage
            group by printer_platform_name, country_alpha2, FYearMo
            order by printer_platform_name, country_alpha2, FYearMo
            )
            ,ctry2 as (select printer_platform_name, country_alpha2
            , max(SNA) as SNA
            from ctry
            group by printer_platform_name, country_alpha2
            order by printer_platform_name, country_alpha2
            )
            select distinct a.printer_platform_name, a.printer_region_code, a.developed_emerging, a.market10, a.country_alpha2
            , b.SNA as prcN, d.SNA as mktN, c.SNA as demN, e.SNA as ctyN
            from usage a
            left join prc2 b
            on a.printer_platform_name=b.printer_platform_name and a.printer_region_code=b.printer_region_code
            left join dem2 c
            on a.printer_platform_name=c.printer_platform_name and a.printer_region_code=c.printer_region_code and a.developed_emerging=c.developed_emerging
              and a.market10=c.market10
            left join mkt102 d
            on a.printer_platform_name=d.printer_platform_name and a.market10=d.market10
            left join ctry2 e
            on a.printer_platform_name=e.printer_platform_name and a.country_alpha2=e.country_alpha2
            ')


str(u2)

# COMMAND ----------

# Step 12 - Finding the reference Source

# Based on the values of SNIS, SNRM, SNPE, SNWJ and SNJA, find the Reference Source 
# variable created "Source_vlook". The following logic was implemented to create Source_vlook 
# = "PE" when SNPE >= 200, = "IS", when (SNPE is NULL and SNIS >=200) or (SNPE < 200 and SNIS >=200), else = "RM"
iblist <- sqldf("select distinct ib.platform_subset as printer_platform_name, ci.country_alpha2, ci.region_5 as printer_region_code, ci.market10, substr(ci.developed_emerging,1,1) as developed_emerging
                from ibtable ib left join country_info ci
                on ib.country_alpha2=ci.country_alpha2")


sourceR <- sqldf("
                 select ib.printer_platform_name, ib.printer_region_code, ib.developed_emerging, ib.market10, ib.country_alpha2,
                 COALESCE(u2.prcN,0) as prcN, COALESCE(u2.mktN,0) as mktN, COALESCE(u2.demN,0) as demN,COALESCE(u2.ctyN,0) as ctyN
                 , case 
                 when ctyN >= 30 then 'country'  ---should be 200--
                 when demN >= 30 then 'dev/em'
                 when mktN >= 30 then 'market10'
                 when prcN >= 30 then 'region5'
                 else 'None'
                 end as Source_vlook
                 from iblist ib
                 left join u2
                 on ib.printer_platform_name=u2.printer_platform_name and ib.printer_region_code=u2.printer_region_code and ib.market10=u2.market10 and ib.country_alpha2=u2.country_alpha2
                 order by ib.printer_platform_name, ib.printer_region_code
                 ")


str(sourceR)

# COMMAND ----------

# Step 13 - Append Source_vlook to the Usage Matrix

# Append SNIS, SNRM, SNPE, SNWJ, SNJA and Source_vlook to the Usage Matrix (Step 10 outcome) 
# for each of the Plat_Nm and Region_Cd combinations.

usage2 <- sqldf('select aa1.*
                , aa2.Source_vlook 
                from usage aa1 left join sourceR aa2 on 
                aa1.printer_platform_name = aa2.printer_platform_name
                and
                aa1.printer_region_code = aa2.printer_region_code
                and 
                aa1.developed_emerging = aa2.developed_emerging
                and
                aa1.market10 = aa2.market10
                and
                aa1.country_alpha2 = aa2.country_alpha2
                ORDER BY
                printer_platform_name, printer_region_code, country_alpha2, fyearmo, segment, biz')

# COMMAND ----------

# Step 14 - Create MPV and N that would be finally worked upon

# Using the outcome of Step 13, Create MPV and N that would be finally worked upon. 

usage3 <- sqldf("with reg as (
                select printer_platform_name, printer_region_code, fyearmo, segment, biz, SUM(SumMPVwtN*SumibWt) as SMPV, SUM(SumibWt) as SIB
                , SUM(Na) as N
                from usage2
                GROUP BY printer_platform_name, printer_region_code, fyearmo, segment, biz
                ORDER BY
                printer_platform_name, printer_region_code, fyearmo, segment, biz
                )
                ,mkt as (
                select printer_platform_name, market10, fyearmo, segment, biz, SUM(SumMPVwtN) as SMPV, SUM(SumibWt) as SIB
                , SUM(Na) as N
                from usage2
                GROUP BY printer_platform_name, market10, fyearmo, segment, biz
                ORDER BY
                printer_platform_name, market10, fyearmo, segment, biz
                )
                ,de as (
                select printer_platform_name, market10, developed_emerging, fyearmo, segment, biz, SUM(SumMPVwtN) as SMPV, SUM(SumibWt) as SIB
                , SUM(Na) as N
                from usage2
                GROUP BY printer_platform_name, market10, developed_emerging, fyearmo, segment, biz
                ORDER BY
                printer_platform_name, market10, developed_emerging, fyearmo, segment, biz
                )

                select a.* , 
                  CASE 
                    WHEN Source_vlook = 'region5' THEN b.SMPV/b.SIB
                    WHEN Source_vlook = 'market10' THEN c.SMPV/c.SIB
                    WHEN Source_vlook = 'dev/em' THEN d.SMPV/c.SIB
                    WHEN Source_vlook = 'country' THEN a.MPVa 
                    ELSE 0
                    END as MPV
                  ,CASE 
                    WHEN Source_vlook = 'region5' THEN b.N
                    WHEN Source_vlook = 'market10' THEN c.N
                    WHEN Source_vlook = 'dev/em' THEN d.N
                    WHEN Source_vlook = 'country' THEN a.Na 
                    ELSE 0
                    END as N
                from usage2 a
                LEFT JOIN reg b on a.printer_platform_name=b.printer_platform_name and a.printer_region_code=b.printer_region_code and a.fyearmo=b.fyearmo
                LEFT JOIN mkt c on a.printer_platform_name=c.printer_platform_name and a.market10=c.market10 and a.fyearmo=c.fyearmo
                LEFT JOIN de d on a.printer_platform_name=d.printer_platform_name and a.market10=d.market10 and a.developed_emerging=d.developed_emerging 
                    and a.fyearmo=d.fyearmo
                ORDER BY
                printer_platform_name, printer_region_code, fyearmo, segment, biz
                ")

# COMMAND ----------

# Step 15 - Combining Usage and Decay matrices

# Combine Usage (outcome of Step 14) and Decay (outcome of Step 9) matrices by using Region_Cd, VV, CM and SM.

usage4<-sqldf('select aa1.printer_platform_name, aa1.printer_region_code, aa1.country_alpha2, aa1.market10, aa1.developed_emerging, aa1.fyearmo, aa1.MPV, aa1.N, aa2.b1
              from 
              usage3 aa1 
              inner join
              decay aa2
              on
              aa1.market10 = aa2.market10
              and aa1.segment = aa2.segment
              and aa1.biz=aa2.biz
              and aa1.developed_emerging=aa2.developed_emerging
              order by aa1.printer_platform_name, aa1.printer_region_code, aa1.fyearmo
              ' )

# COMMAND ----------

# Step 16 - extracting Intro year for a given Platform n region

    introYear <- sqldf(" SELECT a.platform_subset as printer_platform_name
                            , a.Region_5 AS printer_region_code
                            , b.country_alpha2
                            , b.market10
                            , SUBSTR(b.developed_emerging,1,1) as developed_emerging
                            , min(a.month_begin) as Intro_FYearMo
                      FROM ibtable a
                      left join country_info b
                        on a.country_alpha2=b.country_alpha2
                      GROUP BY a.platform_subset
                            , a.Region_5
                            , b.country_alpha2
                            , b.market10
                            , b.developed_emerging
                     ")
    
  introYear$Source <- "FL_Installed_Base"
  head(introYear)
  colnames(introYear)
  dim(introYear)
  
  introYear2a <- sqldf('select printer_platform_name, min(intro_fyearmo) as minYear
                      FROM introYear 
                      GROUP BY printer_platform_name')
  introYear2b <- sqldf('select printer_platform_name, printer_region_code, min(intro_fyearmo) as minYear
                      FROM introYear 
                      GROUP BY printer_platform_name, printer_region_code')
  introYear2c <- sqldf('select printer_platform_name, market10, min(intro_fyearmo) as minYear
                      FROM introYear 
                      GROUP BY printer_platform_name, market10')
  introYear2d <- sqldf('select printer_platform_name, market10, developed_emerging, min(intro_fyearmo) as minYear
                      FROM introYear 
                      GROUP BY printer_platform_name, market10, developed_emerging')
  
  introYear3 <- sqldf('select aa1.printer_platform_name 
                      ,aa1.printer_region_code 
                      ,aa1.market10
                      ,aa1.developed_emerging
                      ,aa1.country_alpha2
                      ,aa1.intro_fyearmo as Intro_FYearMoCtry 
                      ,aa5.minYear as Intro_FYearMoDevEm
                      ,aa4.minYear as Intro_FYearMoMkt10
                      ,aa3.minYear as Intro_FYearMoRg5
                      ,aa2.minYear as Intro_FYearMo

                      from
                      introYear aa1
                      left join introYear2a aa2
                        on aa1.printer_platform_name = aa2.printer_platform_name
                      left join introYear2b aa3
                        on aa1.printer_platform_name = aa3.printer_platform_name and aa1.printer_region_code=aa3.printer_region_code
                      left join introYear2c aa4
                        on aa1.printer_platform_name = aa4.printer_platform_name and aa1.market10=aa4.market10 
                      left join introYear2d aa5
                        on aa1.printer_platform_name = aa5.printer_platform_name and aa1.market10=aa5.market10 and aa1.developed_emerging=aa5.developed_emerging
                      ')

#write.csv(paste(mainDir,subDir1,"/","introYear",".csv", sep=''), x=introYear3,row.names=FALSE, na="")

# introYearNA <- introYear[ which(introYear$Region_Cd =='NA'), ]

# COMMAND ----------

# Step 17 -  Creation of Extended Usage Matrix

usage5 <- sqldf('select aa1.*, aa2.Intro_FYearMo 
                  from usage4 aa1 
                  inner join introYear3 aa2
                  on 
                  aa1.printer_platform_name=aa2.printer_platform_name
                  and
                  aa1.country_alpha2 = aa2.country_alpha2
                  order by aa1.printer_platform_name, aa1.printer_region_code, aa1.FYearMo
                  ')
  
  usage5 <- subset(usage5,MPV>0)
  
  str(usage5)
  usage5$printer_platform_name <- factor(usage5$printer_platform_name)
  str(usage5)
  
  usage5$RFYearMo <- (as.numeric(substr(usage5$FYearMo, 1,4)))*1 + (as.numeric(substr(usage5$FYearMo, 5,6))-1)/12
  usage5$RFIntroYear <- (as.numeric(substr(usage5$Intro_FYearMo, 1,4)))*1 + (as.numeric(substr(usage5$Intro_FYearMo, 5,6))-1)/12
  usage5$MoSI <- (usage5$RFYearMo-usage5$RFIntroYear)*12
  usage5$NMoSI	<- usage5$N*usage5$MoSI
  usage5$LnMPV	<- log(usage5$MPV)
  usage5$NLnMPV <- usage5$N*usage5$LnMPV

# COMMAND ----------

# Step 18 -  Creation of Usage Summary Matrix

# Using extended Usage Matrix (outcome of Step 17), for every combinations of Plat_Nm and Region_Cd, 
# the following variables will be created;

usageSummary <- sqldf('select printer_platform_name, country_alpha2
                        , sum(NMoSI) as SumNMOSI
                        , sum(N) as SumN
                        , sum(NLnMPV) as SumNLNMPV
                        from usage5
                        group by printer_platform_name, country_alpha2
                        order by printer_platform_name, country_alpha2
                        ')
  
  usageSummary$x <- usageSummary$SumNMOSI/usageSummary$SumN
  usageSummary$y <- usageSummary$SumNLNMPV/usageSummary$SumN
  
  
  usageSummaryMkt <- sqldf('select printer_platform_name, market10
                        , sum(NMoSI) as SumNMOSI
                        , sum(N) as SumN
                        , sum(NLnMPV) as SumNLNMPV
                        from usage5
                        group by printer_platform_name, market10
                        order by printer_platform_name, market10
                        ')
  
  usageSummaryMkt$x <- usageSummaryMkt$SumNMOSI/usageSummaryMkt$SumN
  usageSummaryMkt$y <- usageSummaryMkt$SumNLNMPV/usageSummaryMkt$SumN
  
  usageSummaryDE <- sqldf('select printer_platform_name, market10, developed_emerging
                        , sum(NMoSI) as SumNMOSI
                        , sum(N) as SumN
                        , sum(NLnMPV) as SumNLNMPV
                        from usage5
                        group by printer_platform_name, market10, developed_emerging
                        order by printer_platform_name, market10, developed_emerging
                        ')
  
  usageSummaryDE$x <- usageSummaryDE$SumNMOSI/usageSummaryDE$SumN
  usageSummaryDE$y <- usageSummaryDE$SumNLNMPV/usageSummaryDE$SumN
    
  usageSummaryR5 <- sqldf('select printer_platform_name, printer_region_code
                        , sum(NMoSI) as SumNMOSI
                        , sum(N) as SumN
                        , sum(NLnMPV) as SumNLNMPV
                        from usage5
                        group by printer_platform_name, printer_region_code
                        order by printer_platform_name, printer_region_code
                        ')
  
  usageSummaryR5$x <- usageSummaryR5$SumNMOSI/usageSummaryR5$SumN
  usageSummaryR5$y <- usageSummaryR5$SumNLNMPV/usageSummaryR5$SumN

# COMMAND ----------

# Step 19 -  decay matrix for every combinations of Plat_Nm and Region_Cd

# Using extended Usage Matrix (outcome of Step 17), for every combinations of Plat_Nm and Region_Cd, 
# extract respective "b1" or the decay rate.

decay2 <- sqldf('select distinct printer_platform_name, country_alpha2, b1 from usage5 order by printer_platform_name, country_alpha2')
  decay2Mkt <- sqldf('select distinct printer_platform_name, market10, sum(b1*N)/sum(N) as b1 from usage5 group by printer_platform_name, market10')
  decay2DE <- sqldf('select distinct printer_platform_name, market10,developed_emerging,sum(b1*N)/sum(N) as b1 from usage5 group by printer_platform_name, market10, developed_emerging')
  decay2R5 <- sqldf('select distinct printer_platform_name, printer_region_code, sum(b1*N)/sum(N) as b1 from usage5 group by printer_platform_name, printer_region_code')


# COMMAND ----------

# Step 20 -  Initial MPV matrix creation

# Append b1 (from the outcome of Step 19) to Usage Summary Matrix (outcome of Step 18).
# Create initial MPV (iMPV) = exp(y -(x*b1/12))

usageSummary2 <- sqldf('select aa1.*, aa2.b1 from 
                         usageSummary aa1 
                         inner join 
                         decay2 aa2
                         on
                         aa1.printer_platform_name = aa2.printer_platform_name
                         and
                         aa1.country_alpha2 = aa2.country_alpha2
                         order by printer_platform_name, country_alpha2
                         ')
  usageSummary2Mkt <- sqldf('select aa1.*, aa2.b1 from 
                         usageSummaryMkt aa1 
                         inner join 
                         decay2Mkt aa2
                         on
                         aa1.printer_platform_name = aa2.printer_platform_name
                         and
                         aa1.market10 = aa2.market10
                         order by printer_platform_name, market10
                         ')
    usageSummary2DE <- sqldf('select aa1.*, aa2.b1 from 
                         usageSummaryDE aa1 
                         inner join 
                         decay2DE aa2
                         on
                         aa1.printer_platform_name = aa2.printer_platform_name
                         and
                         aa1.market10 = aa2.market10
                         and 
                         aa1.developed_emerging=aa2.developed_emerging
                         order by printer_platform_name, market10, developed_emerging
                         ')
      usageSummary2R5 <- sqldf('select aa1.*, aa2.b1 from 
                         usageSummaryR5 aa1 
                         inner join 
                         decay2R5 aa2
                         on
                         aa1.printer_platform_name = aa2.printer_platform_name
                         and
                         aa1.printer_region_code = aa2.printer_region_code
                         order by printer_platform_name, printer_region_code
                         ')
  
  usageSummary2$iMPV <- exp(usageSummary2$y -(usageSummary2$x*usageSummary2$b1/12))
  usageSummary2Mkt$iMPV <- exp(usageSummary2Mkt$y -(usageSummary2Mkt$x*usageSummary2Mkt$b1/12))
  usageSummary2DE$iMPV <- exp(usageSummary2DE$y -(usageSummary2DE$x*usageSummary2DE$b1/12))
  usageSummary2R5$iMPV <- exp(usageSummary2R5$y -(usageSummary2R5$x*usageSummary2R5$b1/12))
  
  #write.csv(paste(mainDir,subDir1,"/","usageSummary2_iMPV_all available Platforms",".csv", sep=''), x=usageSummary2,row.names=FALSE, na="")
  
  usageSummary2Drop <- usageSummary2[which(usageSummary2$SumN <50),]
  usageSummary2MktDrop <- usageSummary2Mkt[which(usageSummary2Mkt$SumN <50),]
  usageSummary2DEDrop <- usageSummary2DE[which(usageSummary2DE$SumN <50),]
  usageSummary2R5Drop <- usageSummary2R5[which(usageSummary2R5$SumN <50),]
  #usageSummary2Drop$printer_platform_name <- factor(usageSummary2Drop$printer_platform_name)
  
  usageSummary2Keep <- sqldf("select * from usageSummary2 where printer_platform_name not in (select distinct printer_platform_name from usageSummary2Drop)")
  usageSummary2MktKeep <- sqldf("select * from usageSummary2Mkt where printer_platform_name not in (select distinct printer_platform_name from usageSummary2MktDrop)")
  usageSummary2DEKeep <- sqldf("select * from usageSummary2DE where printer_platform_name not in (select distinct printer_platform_name from usageSummary2DEDrop)")
  usageSummary2R5Keep <- sqldf("select * from usageSummary2R5 where printer_platform_name not in (select distinct printer_platform_name from usageSummary2R5Drop)")

# COMMAND ----------

# Step 21 -  De-normalizing Initial MPV matrix

#usageSummary2Keep <- sqldf("select a.*, b.market10, b.developed_emerging, b.region_5 
#                           from usageSummary2Keep a left join country_info b on a.country_alpha2=b.country_alpha2")

usageSummary2Tctry <- reshape2::dcast(usageSummary2Keep, printer_platform_name ~country_alpha2, value.var="iMPV",fun.aggregate = mean)
usageSummary2Tmkt10 <- reshape2::dcast(usageSummary2MktKeep, printer_platform_name ~market10, value.var="iMPV",fun.aggregate = mean)
usageSummary2Tde <- reshape2::dcast(usageSummary2DEKeep, printer_platform_name ~market10+developed_emerging, value.var="iMPV",fun.aggregate = mean)
usageSummary2Tr5 <- reshape2::dcast(usageSummary2R5Keep, printer_platform_name ~printer_region_code, value.var="iMPV",fun.aggregate = mean)

# COMMAND ----------

# Step 22 - Extracting Platforms which have iMPVs calculated for NA and for at least for one more Region

# Extract Platforms which have iMPVs calculated for North America (NA) and for at least one more Region 
  # and having a sample size of at least 50. Initially, iMPVs could be calculated for 201 platforms, however, 
  # the sample size restriction reduces the platform counts to 187. Further, the restriction on platform with 
  # iMPV populated for NA and for at least one other region, reduces the platform counts to 148. 
  # These selected 148 platforms will be used to calculate regional coefficients.

 # For each of the 148 platforms, create weights by dividing the iMPVs of the 4 regions 
  # (i.e. EU, AP, LA and JP) by the iMPV of NA.
  strata <- unique(usage[c("biz", "segment", "printer_platform_name")])

  usageSummary2TEr5 <- sqldf('select aa1.printer_platform_name, aa2.segment, aa2.biz, aa1.NA, aa1.EU, aa1.AP, aa1.LA
                           , COALESCE(aa1.NA/aa1.NA,0) as NA_wt
                           , COALESCE(aa1.EU/aa1.NA,0) as EU_wt
                           , COALESCE(aa1.AP/aa1.NA,0) as AP_wt
                           , COALESCE(aa1.LA/aa1.NA,0) as LA_wt
                           , COALESCE(aa1.NA/aa1.EU,0) as NA_wt2
                           , COALESCE(aa1.EU/aa1.EU,0) as EU_wt2
                           , COALESCE(aa1.AP/aa1.EU,0) as AP_wt2
                           , COALESCE(aa1.LA/aa1.EU,0) as LA_wt2
                           , COALESCE(aa1.NA/aa1.AP,0) as NA_wt3
                           , COALESCE(aa1.EU/aa1.AP,0) as EU_wt3
                           , COALESCE(aa1.AP/aa1.AP,0) as AP_wt3
                           , COALESCE(aa1.LA/aa1.AP,0) as LA_wt3
                           from  usageSummary2Tr5 aa1
                           left join strata aa2
                             on 
                             aa1.printer_platform_name = aa2.printer_platform_name
                           order by NA desc, EU desc, AP desc, LA desc
                           ')
    usageSummary2TEr5[usageSummary2TEr5 == 0] <- NA
    
  usageSummary2TEmkt10 <- sqldf('select aa1.printer_platform_name, aa2.segment, aa2.biz, aa1.[Central Europe], aa1.[Greater Asia], aa1.[Greater China], aa1.[India SL & BL], aa1.[ISE], aa1.[Latin America], aa1.[North America], aa1.[Northern Europe], aa1.[Southern Europe], aa1.[UK&I]
                           --North America
                           , COALESCE(aa1.[North America]/aa1.[North America],0) as [North America_wt]
                           , COALESCE(aa1.[Central Europe]/aa1.[North America],0) as [Central Europe_wt]
                           , COALESCE(aa1.[Greater Asia]/aa1.[North America],0) as [Greater Asia_wt]
                           , COALESCE(aa1.[Greater China]/aa1.[North America],0) as [Greater China_wt]
                           , COALESCE(aa1.[India SL & BL]/aa1.[North America],0) as [India_wt]
                           , COALESCE(aa1.[ISE]/aa1.[North America],0) as [ISE_wt]
                           , COALESCE(aa1.[Latin America]/aa1.[North America],0) as [Latin America_wt]
                           , COALESCE(aa1.[Northern Europe]/aa1.[North America],0) as [Northern Europe_wt]
                           , COALESCE(aa1.[Southern Europe]/aa1.[North America],0) as [Southern Europe_wt]
                           , COALESCE(aa1.[UK&I]/aa1.[North America],0) as [UK&I_wt]
                           --Central Europe
                           , COALESCE(aa1.[North America]/aa1.[Central Europe],0) as [North America_wt2]
                           , COALESCE(aa1.[Central Europe]/aa1.[Central Europe],0) as [Central Europe_wt2]
                           , COALESCE(aa1.[Greater Asia]/aa1.[Central Europe],0) as [Greater Asia_wt2]
                           , COALESCE(aa1.[Greater China]/aa1.[Central Europe],0) as [Greater China_wt2]
                           , COALESCE(aa1.[India SL & BL]/aa1.[Central Europe],0) as [India_wt2]
                           , COALESCE(aa1.[ISE]/aa1.[Central Europe],0) as [ISE_wt2]
                           , COALESCE(aa1.[Latin America]/aa1.[Central Europe],0) as [Latin America_wt2]
                           , COALESCE(aa1.[Northern Europe]/aa1.[Central Europe],0) as [Northern Europe_wt2]
                           , COALESCE(aa1.[Southern Europe]/aa1.[Central Europe],0) as [Southern Europe_wt2]
                           , COALESCE(aa1.[UK&I]/aa1.[Central Europe],0) as [UK&I_wt2]
                           --Greater Asia
                           , COALESCE(aa1.[North America]/aa1.[Greater Asia],0) as [North America_wt3]
                           , COALESCE(aa1.[Central Europe]/aa1.[Greater Asia],0) as [Central Europe_wt3]
                           , COALESCE(aa1.[Greater Asia]/aa1.[Greater Asia],0) as [Greater Asia_wt3]
                           , COALESCE(aa1.[Greater China]/aa1.[Greater Asia],0) as [Greater China_wt3]
                           , COALESCE(aa1.[India SL & BL]/aa1.[Greater Asia],0) as [India_wt3]
                           , COALESCE(aa1.[ISE]/aa1.[Greater Asia],0) as [ISE_wt3]
                           , COALESCE(aa1.[Latin America]/aa1.[Greater Asia],0) as [Latin America_wt3]
                           , COALESCE(aa1.[Northern Europe]/aa1.[Greater Asia],0) as [Northern Europe_wt3]
                           , COALESCE(aa1.[Southern Europe]/aa1.[Greater Asia],0) as [Southern Europe_wt3]
                           , COALESCE(aa1.[UK&I]/aa1.[Greater Asia],0) as [UK&I_wt3]
                           --Greater China
                           , COALESCE(aa1.[North America]/aa1.[Greater China],0) as [North America_wt4]
                           , COALESCE(aa1.[Central Europe]/aa1.[Greater China],0) as [Central Europe_wt4]
                           , COALESCE(aa1.[Greater Asia]/aa1.[Greater China],0) as [Greater Asia_wt4]
                           , COALESCE(aa1.[Greater China]/aa1.[Greater China],0) as [Greater China_wt4]
                           , COALESCE(aa1.[India SL & BL]/aa1.[Greater China],0) as [India_wt4]
                           , COALESCE(aa1.[ISE]/aa1.[Greater China],0) as [ISE_wt4]
                           , COALESCE(aa1.[Latin America]/aa1.[Greater China],0) as [Latin America_wt4]
                           , COALESCE(aa1.[Northern Europe]/aa1.[Greater China],0) as [Northern Europe_wt4]
                           , COALESCE(aa1.[Southern Europe]/aa1.[Greater China],0) as [Southern Europe_wt4]
                           , COALESCE(aa1.[UK&I]/aa1.[Greater China],0) as [UK&I_wt4]
                           --India SL & BL
                           , COALESCE(aa1.[North America]/aa1.[India SL & BL],0) as [North America_wt5]
                           , COALESCE(aa1.[Central Europe]/aa1.[India SL & BL],0) as [Central Europe_wt5]
                           , COALESCE(aa1.[Greater Asia]/aa1.[India SL & BL],0) as [Greater Asia_wt5]
                           , COALESCE(aa1.[Greater China]/aa1.[India SL & BL],0) as [Greater China_wt5]
                           , COALESCE(aa1.[India SL & BL]/aa1.[India SL & BL],0) as [India_wt5]
                           , COALESCE(aa1.[ISE]/aa1.[India SL & BL],0) as [ISE_wt5]
                           , COALESCE(aa1.[Latin America]/aa1.[India SL & BL],0) as [Latin America_wt5]
                           , COALESCE(aa1.[Northern Europe]/aa1.[India SL & BL],0) as [Northern Europe_wt5]
                           , COALESCE(aa1.[Southern Europe]/aa1.[India SL & BL],0) as [Southern Europe_wt5]
                           , COALESCE(aa1.[UK&I]/aa1.[India SL & BL],0) as [UK&I_wt5]
                           --ISE
                           , COALESCE(aa1.[North America]/aa1.[ISE],0) as [North America_wt6]
                           , COALESCE(aa1.[Central Europe]/aa1.[ISE],0) as [Central Europe_wt6]
                           , COALESCE(aa1.[Greater Asia]/aa1.[ISE],0) as [Greater Asia_wt6]
                           , COALESCE(aa1.[Greater China]/aa1.[ISE],0) as [Greater China_wt6]
                           , COALESCE(aa1.[India SL & BL]/aa1.[ISE],0) as [India_wt6]
                           , COALESCE(aa1.[ISE]/aa1.[ISE],0) as [ISE_wt6]
                           , COALESCE(aa1.[Latin America]/aa1.[ISE],0) as [Latin America_wt6]
                           , COALESCE(aa1.[Northern Europe]/aa1.[ISE],0) as [Northern Europe_wt6]
                           , COALESCE(aa1.[Southern Europe]/aa1.[ISE],0) as [Southern Europe_wt6]
                           , COALESCE(aa1.[UK&I]/aa1.[ISE],0) as [UK&I_wt6]
                           --Latin America
                           , COALESCE(aa1.[North America]/aa1.[Latin America],0) as [North America_wt7]
                           , COALESCE(aa1.[Central Europe]/aa1.[Latin America],0) as [Central Europe_wt7]
                           , COALESCE(aa1.[Greater Asia]/aa1.[Latin America],0) as [Greater Asia_wt7]
                           , COALESCE(aa1.[Greater China]/aa1.[Latin America],0) as [Greater China_wt7]
                           , COALESCE(aa1.[India SL & BL]/aa1.[Latin America],0) as [India_wt7]
                           , COALESCE(aa1.[ISE]/aa1.[Latin America],0) as [ISE_wt7]
                           , COALESCE(aa1.[Latin America]/aa1.[Latin America],0) as [Latin America_wt7]
                           , COALESCE(aa1.[Northern Europe]/aa1.[Latin America],0) as [Northern Europe_wt7]
                           , COALESCE(aa1.[Southern Europe]/aa1.[Latin America],0) as [Southern Europe_wt7]
                           , COALESCE(aa1.[UK&I]/aa1.[Latin America],0) as [UK&I_wt7]
                           --Northern Europe
                           , COALESCE(aa1.[North America]/aa1.[Northern Europe],0) as [North America_wt8]
                           , COALESCE(aa1.[Central Europe]/aa1.[Northern Europe],0) as [Central Europe_wt8]
                           , COALESCE(aa1.[Greater Asia]/aa1.[Northern Europe],0) as [Greater Asia_wt8]
                           , COALESCE(aa1.[Greater China]/aa1.[Northern Europe],0) as [Greater China_wt8]
                           , COALESCE(aa1.[India SL & BL]/aa1.[Northern Europe],0) as [India_wt8]
                           , COALESCE(aa1.[ISE]/aa1.[Northern Europe],0) as [ISE_wt8]
                           , COALESCE(aa1.[Latin America]/aa1.[Northern Europe],0) as [Latin America_wt8]
                           , COALESCE(aa1.[Northern Europe]/aa1.[Northern Europe],0) as [Northern Europe_wt8]
                           , COALESCE(aa1.[Southern Europe]/aa1.[Northern Europe],0) as [Southern Europe_wt8]
                           , COALESCE(aa1.[UK&I]/aa1.[Northern Europe],0) as [UK&I_wt8]
                           --Southern Europe
                           , COALESCE(aa1.[North America]/aa1.[Southern Europe],0) as [North America_wt9]
                           , COALESCE(aa1.[Central Europe]/aa1.[Southern Europe],0) as [Central Europe_wt9]
                           , COALESCE(aa1.[Greater Asia]/aa1.[Southern Europe],0) as [Greater Asia_wt9]
                           , COALESCE(aa1.[Greater China]/aa1.[Southern Europe],0) as [Greater China_wt9]
                           , COALESCE(aa1.[India SL & BL]/aa1.[Southern Europe],0) as [India_wt9]
                           , COALESCE(aa1.[ISE]/aa1.[Southern Europe],0) as [ISE_wt9]
                           , COALESCE(aa1.[Latin America]/aa1.[Southern Europe],0) as [Latin America_wt9]
                           , COALESCE(aa1.[Northern Europe]/aa1.[Southern Europe],0) as [Northern Europe_wt9]
                           , COALESCE(aa1.[Southern Europe]/aa1.[Southern Europe],0) as [Southern Europe_wt9]
                           , COALESCE(aa1.[UK&I]/aa1.[Southern Europe],0) as [UK&I_wt9]
                           --UK&I
                           , COALESCE(aa1.[North America]/aa1.[UK&I],0) as [North America_wt10]
                           , COALESCE(aa1.[Central Europe]/aa1.[UK&I],0) as [Central Europe_wt10]
                           , COALESCE(aa1.[Greater Asia]/aa1.[UK&I],0) as [Greater Asia_wt10]
                           , COALESCE(aa1.[Greater China]/aa1.[UK&I],0) as [Greater China_wt10]
                           , COALESCE(aa1.[India SL & BL]/aa1.[UK&I],0) as [India_wt10]
                           , COALESCE(aa1.[ISE]/aa1.[UK&I],0) as [ISE_wt10]
                           , COALESCE(aa1.[Latin America]/aa1.[UK&I],0) as [Latin America_wt10]
                           , COALESCE(aa1.[Northern Europe]/aa1.[UK&I],0) as [Northern Europe_wt10]
                           , COALESCE(aa1.[Southern Europe]/aa1.[UK&I],0) as [Southern Europe_wt10]
                           , COALESCE(aa1.[UK&I]/aa1.[UK&I],0) as [UK&I_wt10]
                           from  usageSummary2Tmkt10 aa1
                           left join strata aa2
                             on 
                             aa1.printer_platform_name = aa2.printer_platform_name
                           ')
usageSummary2TEmkt10[usageSummary2TEmkt10 == 0] <- NA

usageSummary2TEde <- sqldf("select aa1.printer_platform_name, aa2.segment, aa2.biz, aa1.[Central Europe_D],aa1.[Central Europe_E]
                  ,aa1.[Greater Asia_D],aa1.[Greater Asia_E],aa1.[Greater China_D],aa1.[Greater China_E]
                  ,aa1.[India SL & BL_E],aa1.[ISE_E],aa1.[Latin America_E],aa1.[North America_D]
                  ,aa1.[Northern Europe_D],aa1.[Southern Europe_D],aa1.[UK&I_D]
                           --North America_D
                           , COALESCE(aa1.[North America_D]/aa1.[North America_D],0) as [North America_D_wt]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[North America_D],0) as [Central Europe_D_wt]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[North America_D],0) as [Central Europe_E_wt]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[North America_D],0) as [Greater Asia_D_wt]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[North America_D],0) as [Greater Asia_E_wt]
                           , COALESCE(aa1.[Greater China_D]/aa1.[North America_D],0) as [Greater China_D_wt]
                           , COALESCE(aa1.[Greater China_E]/aa1.[North America_D],0) as [Greater China_E_wt]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[North America_D],0) as [India SL & BL_E_wt]
                           , COALESCE(aa1.[ISE_E]/aa1.[North America_D],0) as [ISE_E_wt]
                           , COALESCE(aa1.[Latin America_E]/aa1.[North America_D],0) as [Latin America_E_wt]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[North America_D],0) as [Northern Europe_D_wt]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[North America_D],0) as [Southern Europe_D_wt]
                           , COALESCE(aa1.[UK&I_D]/aa1.[North America_D],0) as [UK&I_D_wt]
                           --Central Europe_D
                           , COALESCE(aa1.[North America_D]/aa1.[Central Europe_D],0) as [North America_D_wt2]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Central Europe_D],0) as [Central Europe_D_wt2]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Central Europe_D],0) as [Central Europe_E_wt2]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Central Europe_D],0) as [Greater Asia_D_wt2]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Central Europe_D],0) as [Greater Asia_E_wt2]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Central Europe_D],0) as [Greater China_D_wt2]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Central Europe_D],0) as [Greater China_E_wt2]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Central Europe_D],0) as [India SL & BL_E_wt2]
                           , COALESCE(aa1.[ISE_E]/aa1.[Central Europe_D],0) as [ISE_E_wt2]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Central Europe_D],0) as [Latin America_E_wt2]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Central Europe_D],0) as [Northern Europe_D_wt2]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Central Europe_D],0) as [Southern Europe_D_wt2]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Central Europe_D],0) as [UK&I_D_wt2]
                           --Central Europe_E
                           , COALESCE(aa1.[North America_D]/aa1.[Central Europe_E],0) as [North America_D_wt3]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Central Europe_E],0) as [Central Europe_D_wt3]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Central Europe_E],0) as [Central Europe_E_wt3]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Central Europe_E],0) as [Greater Asia_D_wt3]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Central Europe_E],0) as [Greater Asia_E_wt3]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Central Europe_E],0) as [Greater China_D_wt3]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Central Europe_E],0) as [Greater China_E_wt3]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Central Europe_E],0) as [India SL & BL_E_wt3]
                           , COALESCE(aa1.[ISE_E]/aa1.[Central Europe_E],0) as [ISE_E_wt3]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Central Europe_E],0) as [Latin America_E_wt3]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Central Europe_E],0) as [Northern Europe_D_wt3]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Central Europe_E],0) as [Southern Europe_D_wt3]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Central Europe_E],0) as [UK&I_D_wt3]
                           --Greater Asia_D
                           , COALESCE(aa1.[North America_D]/aa1.[Greater Asia_D],0) as [North America_D_wt4]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Greater Asia_D],0) as [Central Europe_D_wt4]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Greater Asia_D],0) as [Central Europe_E_wt4]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Greater Asia_D],0) as [Greater Asia_D_wt4]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Greater Asia_D],0) as [Greater Asia_E_wt4]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Greater Asia_D],0) as [Greater China_D_wt4]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Greater Asia_D],0) as [Greater China_E_wt4]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Greater Asia_D],0) as [India SL & BL_E_wt4]
                           , COALESCE(aa1.[ISE_E]/aa1.[Greater Asia_D],0) as [ISE_E_wt4]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Greater Asia_D],0) as [Latin America_E_wt4]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Greater Asia_D],0) as [Northern Europe_D_wt4]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Greater Asia_D],0) as [Southern Europe_D_wt4]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Greater Asia_D],0) as [UK&I_D_wt4]
                           --Greater Asia_E
                           , COALESCE(aa1.[North America_D]/aa1.[Greater Asia_E],0) as [North America_D_wt5]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Greater Asia_E],0) as [Central Europe_D_wt5]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Greater Asia_E],0) as [Central Europe_E_wt5]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Greater Asia_E],0) as [Greater Asia_D_wt5]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Greater Asia_E],0) as [Greater Asia_E_wt5]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Greater Asia_E],0) as [Greater China_D_wt5]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Greater Asia_E],0) as [Greater China_E_wt5]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Greater Asia_E],0) as [India SL & BL_E_wt5]
                           , COALESCE(aa1.[ISE_E]/aa1.[Greater Asia_E],0) as [ISE_E_wt5]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Greater Asia_E],0) as [Latin America_E_wt5]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Greater Asia_E],0) as [Northern Europe_D_wt5]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Greater Asia_E],0) as [Southern Europe_D_wt5]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Greater Asia_E],0) as [UK&I_D_wt5]
                           --Greater China_D
                           , COALESCE(aa1.[North America_D]/aa1.[Greater China_D],0) as [North America_D_wt6]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Greater China_D],0) as [Central Europe_D_wt6]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Greater China_D],0) as [Central Europe_E_wt6]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Greater China_D],0) as [Greater Asia_D_wt6]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Greater China_D],0) as [Greater Asia_E_wt6]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Greater China_D],0) as [Greater China_D_wt6]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Greater China_D],0) as [Greater China_E_wt6]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Greater China_D],0) as [India SL & BL_E_wt6]
                           , COALESCE(aa1.[ISE_E]/aa1.[Greater China_D],0) as [ISE_E_wt6]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Greater China_D],0) as [Latin America_E_wt6]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Greater China_D],0) as [Northern Europe_D_wt6]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Greater China_D],0) as [Southern Europe_D_wt6]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Greater China_D],0) as [UK&I_D_wt6]
                           --Greater China_E
                           , COALESCE(aa1.[North America_D]/aa1.[Greater China_E],0) as [North America_D_wt7]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Greater China_E],0) as [Central Europe_D_wt7]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Greater China_E],0) as [Central Europe_E_wt7]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Greater China_E],0) as [Greater Asia_D_wt7]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Greater China_E],0) as [Greater Asia_E_wt7]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Greater China_E],0) as [Greater China_D_wt7]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Greater China_E],0) as [Greater China_E_wt7]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Greater China_E],0) as [India SL & BL_E_wt7]
                           , COALESCE(aa1.[ISE_E]/aa1.[Greater China_E],0) as [ISE_E_wt7]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Greater China_E],0) as [Latin America_E_wt7]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Greater China_E],0) as [Northern Europe_D_wt7]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Greater China_E],0) as [Southern Europe_D_wt7]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Greater China_E],0) as [UK&I_D_wt7]
                           --India SL & BL_E
                           , COALESCE(aa1.[North America_D]/aa1.[India SL & BL_E],0) as [North America_D_wt8]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[India SL & BL_E],0) as [Central Europe_D_wt8]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[India SL & BL_E],0) as [Central Europe_E_wt8]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[India SL & BL_E],0) as [Greater Asia_D_wt8]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[India SL & BL_E],0) as [Greater Asia_E_wt8]
                           , COALESCE(aa1.[Greater China_D]/aa1.[India SL & BL_E],0) as [Greater China_D_wt8]
                           , COALESCE(aa1.[Greater China_E]/aa1.[India SL & BL_E],0) as [Greater China_E_wt8]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[India SL & BL_E],0) as [India SL & BL_E_wt8]
                           , COALESCE(aa1.[ISE_E]/aa1.[India SL & BL_E],0) as [ISE_E_wt8]
                           , COALESCE(aa1.[Latin America_E]/aa1.[India SL & BL_E],0) as [Latin America_E_wt8]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[India SL & BL_E],0) as [Northern Europe_D_wt8]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[India SL & BL_E],0) as [Southern Europe_D_wt8]
                           , COALESCE(aa1.[UK&I_D]/aa1.[India SL & BL_E],0) as [UK&I_D_wt8]
                           --ISE_E
                           , COALESCE(aa1.[North America_D]/aa1.[ISE_E],0) as [North America_D_wt9]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[ISE_E],0) as [Central Europe_D_wt9]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[ISE_E],0) as [Central Europe_E_wt9]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[ISE_E],0) as [Greater Asia_D_wt9]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[ISE_E],0) as [Greater Asia_E_wt9]
                           , COALESCE(aa1.[Greater China_D]/aa1.[ISE_E],0) as [Greater China_D_wt9]
                           , COALESCE(aa1.[Greater China_E]/aa1.[ISE_E],0) as [Greater China_E_wt9]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[ISE_E],0) as [India SL & BL_E_wt9]
                           , COALESCE(aa1.[ISE_E]/aa1.[ISE_E],0) as [ISE_E_wt9]
                           , COALESCE(aa1.[Latin America_E]/aa1.[ISE_E],0) as [Latin America_E_wt9]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[ISE_E],0) as [Northern Europe_D_wt9]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[ISE_E],0) as [Southern Europe_D_wt9]
                           , COALESCE(aa1.[UK&I_D]/aa1.[ISE_E],0) as [UK&I_D_wt9]
                           --Latin America_E
                           , COALESCE(aa1.[North America_D]/aa1.[Latin America_E],0) as [North America_D_wt10]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Latin America_E],0) as [Central Europe_D_wt10]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Latin America_E],0) as [Central Europe_E_wt10]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Latin America_E],0) as [Greater Asia_D_wt10]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Latin America_E],0) as [Greater Asia_E_wt10]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Latin America_E],0) as [Greater China_D_wt10]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Latin America_E],0) as [Greater China_E_wt10]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Latin America_E],0) as [India SL & BL_E_wt10]
                           , COALESCE(aa1.[ISE_E]/aa1.[Latin America_E],0) as [ISE_E_wt10]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Latin America_E],0) as [Latin America_E_wt10]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Latin America_E],0) as [Northern Europe_D_wt10]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Latin America_E],0) as [Southern Europe_D_wt10]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Latin America_E],0) as [UK&I_D_wt10]
                           --Northern Europe_D
                           , COALESCE(aa1.[North America_D]/aa1.[Northern Europe_D],0) as [North America_D_wt11]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Northern Europe_D],0) as [Central Europe_D_wt11]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Northern Europe_D],0) as [Central Europe_E_wt11]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Northern Europe_D],0) as [Greater Asia_D_wt11]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Northern Europe_D],0) as [Greater Asia_E_wt11]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Northern Europe_D],0) as [Greater China_D_wt11]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Northern Europe_D],0) as [Greater China_E_wt11]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Northern Europe_D],0) as [India SL & BL_E_wt11]
                           , COALESCE(aa1.[ISE_E]/aa1.[Northern Europe_D],0) as [ISE_E_wt11]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Northern Europe_D],0) as [Latin America_E_wt11]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Northern Europe_D],0) as [Northern Europe_D_wt11]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Northern Europe_D],0) as [Southern Europe_D_wt11]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Northern Europe_D],0) as [UK&I_D_wt11]
                           --Southern Europe_D
                           , COALESCE(aa1.[North America_D]/aa1.[Southern Europe_D],0) as [North America_D_wt12]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[Southern Europe_D],0) as [Central Europe_D_wt12]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[Southern Europe_D],0) as [Central Europe_E_wt12]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[Southern Europe_D],0) as [Greater Asia_D_wt12]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[Southern Europe_D],0) as [Greater Asia_E_wt12]
                           , COALESCE(aa1.[Greater China_D]/aa1.[Southern Europe_D],0) as [Greater China_D_wt12]
                           , COALESCE(aa1.[Greater China_E]/aa1.[Southern Europe_D],0) as [Greater China_E_wt12]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[Southern Europe_D],0) as [India SL & BL_E_wt12]
                           , COALESCE(aa1.[ISE_E]/aa1.[Southern Europe_D],0) as [ISE_E_wt12]
                           , COALESCE(aa1.[Latin America_E]/aa1.[Southern Europe_D],0) as [Latin America_E_wt12]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[Southern Europe_D],0) as [Northern Europe_D_wt12]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[Southern Europe_D],0) as [Southern Europe_D_wt12]
                           , COALESCE(aa1.[UK&I_D]/aa1.[Southern Europe_D],0) as [UK&I_D_wt12]
                           --UK&I_D
                           , COALESCE(aa1.[North America_D]/aa1.[UK&I_D],0) as [North America_D_wt13]
                           , COALESCE(aa1.[Central Europe_D]/aa1.[UK&I_D],0) as [Central Europe_D_wt13]
                           , COALESCE(aa1.[Central Europe_E]/aa1.[UK&I_D],0) as [Central Europe_E_wt13]
                           , COALESCE(aa1.[Greater Asia_D]/aa1.[UK&I_D],0) as [Greater Asia_D_wt13]
                           , COALESCE(aa1.[Greater Asia_E]/aa1.[UK&I_D],0) as [Greater Asia_E_wt13]
                           , COALESCE(aa1.[Greater China_D]/aa1.[UK&I_D],0) as [Greater China_D_wt13]
                           , COALESCE(aa1.[Greater China_E]/aa1.[UK&I_D],0) as [Greater China_E_wt13]
                           , COALESCE(aa1.[India SL & BL_E]/aa1.[UK&I_D],0) as [India SL & BL_E_wt13]
                           , COALESCE(aa1.[ISE_E]/aa1.[UK&I_D],0) as [ISE_E_wt13]
                           , COALESCE(aa1.[Latin America_E]/aa1.[UK&I_D],0) as [Latin America_E_wt13]
                           , COALESCE(aa1.[Northern Europe_D]/aa1.[UK&I_D],0) as [Northern Europe_D_wt13]
                           , COALESCE(aa1.[Southern Europe_D]/aa1.[UK&I_D],0) as [Southern Europe_D_wt13]
                           , COALESCE(aa1.[UK&I_D]/aa1.[UK&I_D],0) as [UK&I_D_wt13]
                           from  usageSummary2Tde aa1
                           left join strata aa2
                             on 
                             aa1.printer_platform_name = aa2.printer_platform_name
                           ")
usageSummary2TEde[usageSummary2TEde == 0] <- NA

usageSummary2TE <- sqldf("select a.[printer_platform_name],a.[segment],a.[biz]
,a.[NA],a.[EU],a.[AP],a.[LA] ,b.[Central Europe],b.[Greater Asia],b.[Greater China],b.[India SL & BL],b.[ISE],b.[Latin America],b.[North America]
,b.[Northern Europe],b.[Southern Europe],b.[UK&I]
,a.[NA_wt],a.[EU_wt],a.[AP_wt],a.[LA_wt],a.[NA_wt2],a.[EU_wt2],a.[AP_wt2],a.[LA_wt2],a.[NA_wt3] ,a.[EU_wt3],a.[AP_wt3],a.[LA_wt3] ,b.[North America_wt],b.[Central Europe_wt],b.[Greater Asia_wt],b.[Greater China_wt],b.[India_wt],b.[ISE_wt],b.[Latin America_wt],b.[Northern Europe_wt],b.[Southern Europe_wt],b.[UK&I_wt],b.[North America_wt2],b.[Central Europe_wt2],b.[Greater Asia_wt2],b.[Greater China_wt2] ,b.[India_wt2],b.[ISE_wt2],b.[Latin America_wt2],b.[Northern Europe_wt2],b.[Southern Europe_wt2],b.[UK&I_wt2],b.[North America_wt3],b.[Central Europe_wt3]
,b.[Greater Asia_wt3],b.[Greater China_wt3],b.[India_wt3],b.[ISE_wt3],b.[Latin America_wt3],b.[Northern Europe_wt3],b.[Southern Europe_wt3],b.[UK&I_wt3]            ,b.[North America_wt4],b.[Central Europe_wt4],b.[Greater Asia_wt4],b.[Greater China_wt4],b.[India_wt4],b.[ISE_wt4],b.[Latin America_wt4],b.[Northern Europe_wt4] ,b.[Southern Europe_wt4],b.[UK&I_wt4],b.[North America_wt5],b.[Central Europe_wt5],b.[Greater Asia_wt5],b.[Greater China_wt5],b.[India_wt5],b.[ISE_wt5],b.[Latin America_wt5],b.[Northern Europe_wt5],b.[Southern Europe_wt5],b.[UK&I_wt5],b.[North America_wt6],b.[Central Europe_wt6],b.[Greater Asia_wt6]
,b.[Greater China_wt6],b.[India_wt6],b.[ISE_wt6],b.[Latin America_wt6],b.[Northern Europe_wt6],b.[Southern Europe_wt6],b.[UK&I_wt6],b.[North America_wt7]
,b.[Central Europe_wt7],b.[Greater Asia_wt7],b.[Greater China_wt7],b.[India_wt7],b.[ISE_wt7],b.[Latin America_wt7],b.[Northern Europe_wt7],b.[Southern Europe_wt7] ,b.[UK&I_wt7],b.[North America_wt8],b.[Central Europe_wt8],b.[Greater Asia_wt8],b.[Greater China_wt8],b.[India_wt8],b.[ISE_wt8],b.[Latin America_wt8]
,b.[Northern Europe_wt8],b.[Southern Europe_wt8],b.[UK&I_wt8],b.[North America_wt9],b.[Central Europe_wt9],b.[Greater Asia_wt9],b.[Greater China_wt9],b.[India_wt9]      ,b.[ISE_wt9],b.[Latin America_wt9],b.[Northern Europe_wt9],b.[Southern Europe_wt9],b.[UK&I_wt9],b.[North America_wt10],b.[Central Europe_wt10],b.[Greater Asia_wt10]   ,b.[Greater China_wt10],b.[India_wt10],b.[ISE_wt10],b.[Latin America_wt10],b.[Northern Europe_wt10],b.[Southern Europe_wt10],b.[UK&I_wt10] 
,c.[Central Europe_D],c.[Central Europe_E],c.[Greater Asia_D],c.[Greater Asia_E],c.[Greater China_D],c.[Greater China_E]
,c.[India SL & BL_E],c.[ISE_E],c.[Latin America_E],c.[North America_D],c.[Northern Europe_D]
,c.[Southern Europe_D],c.[UK&I_D],c.[North America_D_wt],c.[Central Europe_D_wt],c.[Central Europe_E_wt]
,c.[Greater Asia_D_wt],c.[Greater Asia_E_wt],c.[Greater China_D_wt],c.[Greater China_E_wt],c.[India SL & BL_E_wt]
,c.[ISE_E_wt],c.[Latin America_E_wt],c.[Northern Europe_D_wt],c.[Southern Europe_D_wt],c.[UK&I_D_wt]
,c.[North America_D_wt2],c.[Central Europe_D_wt2],c.[Central Europe_E_wt2],c.[Greater Asia_D_wt2],c.[Greater Asia_E_wt2]
,c.[Greater China_D_wt2],c.[Greater China_E_wt2],c.[India SL & BL_E_wt2],c.[ISE_E_wt2],c.[Latin America_E_wt2]
,c.[Northern Europe_D_wt2],c.[Southern Europe_D_wt2],c.[UK&I_D_wt2],c.[North America_D_wt3],c.[Central Europe_D_wt3]
,c.[Central Europe_E_wt3],c.[Greater Asia_D_wt3],c.[Greater Asia_E_wt3],c.[Greater China_D_wt3],c.[Greater China_E_wt3]
,c.[India SL & BL_E_wt3],c.[ISE_E_wt3],c.[Latin America_E_wt3],c.[Northern Europe_D_wt3],c.[Southern Europe_D_wt3]
,c.[UK&I_D_wt3],c.[North America_D_wt4],c.[Central Europe_D_wt4],c.[Central Europe_E_wt4],c.[Greater Asia_D_wt4]
,c.[Greater Asia_E_wt4],c.[Greater China_D_wt4],c.[Greater China_E_wt4],c.[India SL & BL_E_wt4],c.[ISE_E_wt4]
,c.[Latin America_E_wt4],c.[Northern Europe_D_wt4],c.[Southern Europe_D_wt4],c.[UK&I_D_wt4],c.[North America_D_wt5]
,c.[Central Europe_D_wt5],c.[Central Europe_E_wt5],c.[Greater Asia_D_wt5],c.[Greater Asia_E_wt5],c.[Greater China_D_wt5]
,c.[Greater China_E_wt5],c.[India SL & BL_E_wt5],c.[ISE_E_wt5],c.[Latin America_E_wt5],c.[Northern Europe_D_wt5]
,c.[Southern Europe_D_wt5],c.[UK&I_D_wt5],c.[North America_D_wt6],c.[Central Europe_D_wt6],c.[Central Europe_E_wt6]
,c.[Greater Asia_D_wt6],c.[Greater Asia_E_wt6],c.[Greater China_D_wt6],c.[Greater China_E_wt6],c.[India SL & BL_E_wt6]
,c.[ISE_E_wt6],c.[Latin America_E_wt6],c.[Northern Europe_D_wt6],c.[Southern Europe_D_wt6],c.[UK&I_D_wt6]
,c.[North America_D_wt7],c.[Central Europe_D_wt7],c.[Central Europe_E_wt7],c.[Greater Asia_D_wt7],c.[Greater Asia_E_wt7]
,c.[Greater China_D_wt7],c.[Greater China_E_wt7],c.[India SL & BL_E_wt7],c.[ISE_E_wt7],c.[Latin America_E_wt7]
,c.[Northern Europe_D_wt7],c.[Southern Europe_D_wt7],c.[UK&I_D_wt7],c.[North America_D_wt8],c.[Central Europe_D_wt8]
,c.[Central Europe_E_wt8],c.[Greater Asia_D_wt8],c.[Greater Asia_E_wt8],c.[Greater China_D_wt8],c.[Greater China_E_wt8]
,c.[India SL & BL_E_wt8],c.[ISE_E_wt8],c.[Latin America_E_wt8],c.[Northern Europe_D_wt8],c.[Southern Europe_D_wt8]
,c.[UK&I_D_wt8],c.[North America_D_wt9],c.[Central Europe_D_wt9],c.[Central Europe_E_wt9],c.[Greater Asia_D_wt9]
,c.[Greater Asia_E_wt9],c.[Greater China_D_wt9],c.[Greater China_E_wt9],c.[India SL & BL_E_wt9],c.[ISE_E_wt9]
,c.[Latin America_E_wt9],c.[Northern Europe_D_wt9],c.[Southern Europe_D_wt9],c.[UK&I_D_wt9],c.[North America_D_wt10]
,c.[Central Europe_D_wt10],c.[Central Europe_E_wt10],c.[Greater Asia_D_wt10],c.[Greater Asia_E_wt10],c.[Greater China_D_wt10]
,c.[Greater China_E_wt10],c.[India SL & BL_E_wt10],c.[ISE_E_wt10],c.[Latin America_E_wt10],c.[Northern Europe_D_wt10]
,c.[Southern Europe_D_wt10],c.[UK&I_D_wt10],c.[North America_D_wt11],c.[Central Europe_D_wt11],c.[Central Europe_E_wt11]
,c.[Greater Asia_D_wt11],c.[Greater Asia_E_wt11],c.[Greater China_D_wt11],c.[Greater China_E_wt11],c.[India SL & BL_E_wt11]
,c.[ISE_E_wt11],c.[Latin America_E_wt11],c.[Northern Europe_D_wt11],c.[Southern Europe_D_wt11],c.[UK&I_D_wt11]
,c.[North America_D_wt12],c.[Central Europe_D_wt12],c.[Central Europe_E_wt12],c.[Greater Asia_D_wt12],c.[Greater Asia_E_wt12]
,c.[Greater China_D_wt12],c.[Greater China_E_wt12],c.[India SL & BL_E_wt12],c.[ISE_E_wt12],c.[Latin America_E_wt12]
,c.[Northern Europe_D_wt12],c.[Southern Europe_D_wt12],c.[UK&I_D_wt12],c.[North America_D_wt13],c.[Central Europe_D_wt13]
,c.[Central Europe_E_wt13],c.[Greater Asia_D_wt13],c.[Greater Asia_E_wt13],c.[Greater China_D_wt13],c.[Greater China_E_wt13]
,c.[India SL & BL_E_wt13],c.[ISE_E_wt13],c.[Latin America_E_wt13],c.[Northern Europe_D_wt13],c.[Southern Europe_D_wt13]
,c.[UK&I_D_wt13]
                         from usageSummary2TEr5 a 
                         left join usageSummary2TEmkt10 b
                          on a.printer_platform_name=b.printer_platform_name and a.segment=b.segment and a.biz=b.biz
                         left join usageSummary2TEde c
                          on a.printer_platform_name=c.printer_platform_name and a.segment=c.segment and a.biz=c.biz
")

# COMMAND ----------

wtaverage <- sqldf('select segment,biz
                     , min(avg(EU_wt),3) as EUa
                     , min(avg([Central Europe_wt]),3) as Central_Europena
                     , min(avg([Central Europe_D_wt]),3) as Central_Europe_Dna
                     , min(avg([Central Europe_E_wt]),3) as Central_Europe_Ena
                     , min(avg([Northern Europe_wt]),3) as Northern_Europena   --dont need to split as only developed
                     , min(avg([Southern Europe_wt]),3) as Southern_Europena
                     , min(avg([ISE_wt]),3) as ISEna
                     , min(avg([UK&I_wt]),3) as UKIna
                     , min(avg([Greater Asia_wt]),3) as Greater_Asiana
                     , min(avg([Greater Asia_D_wt]),3) as Greater_Asia_Dna
                     , min(avg([Greater Asia_E_wt]),3) as Greater_Asia_Ena
                     , min(avg([Greater China_wt]),3) as Greater_Chinana
                     , min(avg([Greater China_D_wt]),3) as Greater_China_Dna
                     , min(avg([Greater China_E_wt]),3) as Greater_China_Ena
                     , min(avg([India_wt]),3) as Indiana
                     , min(avg([Latin America_wt]),3) as Latin_Americana
                     --
                     , min(avg([North America_wt2]),3) as North_Americace
                     , min(avg([Central Europe_E_wt2]),3) as Central_Europe_Eced
                     , min(avg([Central Europe_D_wt3]),3) as Central_Europe_Dcee
                     , min(avg([Northern Europe_wt2]),3) as Northern_Europece
                     , min(avg([Southern Europe_wt2]),3) as Southern_Europece
                     , min(avg([ISE_wt2]),3) as ISEce
                     , min(avg([UK&I_wt2]),3) as UKIce
                     , min(avg([Greater Asia_wt2]),3) as Greater_Asiace
                     , min(avg([Greater Asia_D_wt2]),3) as Greater_Asia_Dced
                     , min(avg([Greater Asia_E_wt2]),3) as Greater_Asia_Eced
                     , min(avg([Greater Asia_D_wt3]),3) as Greater_Asia_Dcee
                     , min(avg([Greater Asia_E_wt3]),3) as Greater_Asia_Ecee
                     , min(avg([Greater China_wt2]),3) as Greater_Chinace
                     , min(avg([Greater China_D_wt2]),3) as Greater_China_Dced
                     , min(avg([Greater China_E_wt2]),3) as Greater_China_Eced
                     , min(avg([Greater China_D_wt3]),3) as Greater_China_Dcee
                     , min(avg([Greater China_E_wt3]),3) as Greater_China_Ecee
                     , min(avg([India_wt2]),3) as Indiace
                     , min(avg([Latin America_wt2]),3) as Latin_Americace
                     --
                     , min(avg([Central Europe_wt3]),3) as Central_Europega
                     , min(avg([Central Europe_E_wt4]),3) as Central_Europe_Egad
                     , min(avg([Central Europe_D_wt4]),3) as Central_Europe_Dgad
                     , min(avg([Central Europe_E_wt5]),3) as Central_Europe_Egae
                     , min(avg([Central Europe_D_wt5]),3) as Central_Europe_Dgae
                     , min(avg([Northern Europe_wt3]),3) as Northern_Europega
                     , min(avg([Southern Europe_wt3]),3) as Southern_Europega
                     , min(avg([ISE_wt3]),3) as ISEga
                     , min(avg([UK&I_wt3]),3) as UKIga
                     , min(avg([North America_wt3]),3) as North_Americaga
                     , min(avg([Greater Asia_D_wt5]),3) as Greater_Asia_Dgae
                     , min(avg([Greater Asia_E_wt4]),3) as Greater_Asia_Egad
                     , min(avg([Greater China_wt3]),3) as Greater_Chinaga
                     , min(avg([Greater China_D_wt4]),3) as Greater_China_Dgad
                     , min(avg([Greater China_E_wt4]),3) as Greater_China_Egad
                     , min(avg([Greater China_D_wt5]),3) as Greater_China_Dgae
                     , min(avg([Greater China_E_wt5]),3) as Greater_China_Egae
                     , min(avg([India_wt3]),3) as Indiaga
                     , min(avg([Latin America_wt3]),3) as Latin_Americaga
                     --
                     , min(avg([Central Europe_wt4]),3) as Central_Europegc
                     , min(avg([Central Europe_E_wt6]),3) as Central_Europe_Egcd
                     , min(avg([Central Europe_D_wt6]),3) as Central_Europe_Dgcd
                     , min(avg([Central Europe_E_wt7]),3) as Central_Europe_Egce
                     , min(avg([Central Europe_D_wt7]),3) as Central_Europe_Dgce
                     , min(avg([Northern Europe_wt4]),3) as Northern_Europegc
                     , min(avg([Southern Europe_wt4]),3) as Southern_Europegc
                     , min(avg([ISE_wt4]),3) as ISEgc
                     , min(avg([UK&I_wt4]),3) as UKIgc
                     , min(avg([Greater Asia_wt4]),3) as Greater_Asiagc
                     , min(avg([Greater Asia_D_wt6]),3) as Greater_Asia_Dgcd
                     , min(avg([Greater Asia_E_wt6]),3) as Greater_Asia_Egcd
                     , min(avg([Greater Asia_D_wt7]),3) as Greater_Asia_Dgce
                     , min(avg([Greater Asia_E_wt7]),3) as Greater_Asia_Egce
                     , min(avg([Greater China_D_wt7]),3) as Greater_China_Dgce
                     , min(avg([Greater China_E_wt6]),3) as Greater_China_Egcd
                     , min(avg([North America_wt4]),3) as North_Americagc
                     , min(avg([India_wt4]),3) as Indiagc
                     , min(avg([Latin America_wt4]),3) as Latin_Americagc
                     --
                     , min(avg([Central Europe_wt5]),3) as Central_Europeia
                     , min(avg([Central Europe_E_wt8]),3) as Central_Europe_Eia
                     , min(avg([Central Europe_D_wt8]),3) as Central_Europe_Dia
                     , min(avg([Northern Europe_wt5]),3) as Northern_Europeia
                     , min(avg([Southern Europe_wt5]),3) as Southern_Europeia
                     , min(avg([ISE_wt5]),3) as ISEia
                     , min(avg([UK&I_wt5]),3) as UKIia
                     , min(avg([Greater Asia_wt5]),3) as Greater_Asiaia
                     , min(avg([Greater Asia_D_wt8]),3) as Greater_Asia_Dia
                     , min(avg([Greater Asia_E_wt8]),3) as Greater_Asia_Eia
                     , min(avg([Greater China_wt5]),3) as Greater_Chinaia
                     , min(avg([Greater China_D_wt8]),3) as Greater_China_Dia
                     , min(avg([Greater China_E_wt8]),3) as Greater_China_Eia
                     , min(avg([North America_wt5]),3) as North_Americaia
                     , min(avg([Latin America_wt5]),3) as Latin_Americaia
                     --
                     , min(avg([Central Europe_wt6]),3) as Central_Europeis
                     , min(avg([Central Europe_E_wt9]),3) as Central_Europe_Eis
                     , min(avg([Central Europe_D_wt9]),3) as Central_Europe_Dis
                     , min(avg([Northern Europe_wt6]),3) as Northern_Europeis
                     , min(avg([Southern Europe_wt6]),3) as Southern_Europeis
                     , min(avg([North America_wt6]),3) as North_Americais
                     , min(avg([UK&I_wt6]),3) as UKIis
                     , min(avg([Greater Asia_wt6]),3) as Greater_Asiais
                     , min(avg([Greater Asia_D_wt9]),3) as Greater_Asia_Dis
                     , min(avg([Greater Asia_E_wt9]),3) as Greater_Asia_Eis
                     , min(avg([Greater China_wt6]),3) as Greater_Chinais
                     , min(avg([Greater China_D_wt9]),3) as Greater_China_Dis
                     , min(avg([Greater China_E_wt9]),3) as Greater_China_Eis
                     , min(avg([India_wt6]),3) as Indiais
                     , min(avg([Latin America_wt6]),3) as Latin_Americais
                     --
                     , min(avg([Central Europe_wt7]),3) as Central_Europela
                     , min(avg([Central Europe_E_wt10]),3) as Central_Europe_Ela
                     , min(avg([Central Europe_D_wt10]),3) as Central_Europe_Dla
                     , min(avg([Northern Europe_wt7]),3) as Northern_Europela
                     , min(avg([Southern Europe_wt7]),3) as Southern_Europela
                     , min(avg([ISE_wt7]),3) as ISEla
                     , min(avg([UK&I_wt7]),3) as UKIla
                     , min(avg([Greater Asia_wt7]),3) as Greater_Asiala
                     , min(avg([Greater Asia_D_wt10]),3) as Greater_Asia_Dla
                     , min(avg([Greater Asia_E_wt10]),3) as Greater_Asia_Ela
                     , min(avg([Greater China_wt7]),3) as Greater_Chinala
                     , min(avg([Greater China_D_wt10]),3) as Greater_China_Dla
                     , min(avg([Greater China_E_wt10]),3) as Greater_China_Ela
                     , min(avg([India_wt7]),3) as Indiala
                     , min(avg([North America_wt7]),3) as North_Americala
                     --
                     , min(avg([Central Europe_wt8]),3) as Central_Europene
                     , min(avg([Central Europe_E_wt11]),3) as Central_Europe_Ene
                     , min(avg([Central Europe_D_wt11]),3) as Central_Europe_Dne
                     , min(avg([North America_wt8]),3) as North_Americane
                     , min(avg([Southern Europe_wt8]),3) as Southern_Europene
                     , min(avg([ISE_wt8]),3) as ISEne
                     , min(avg([UK&I_wt8]),3) as UKIne
                     , min(avg([Greater Asia_wt8]),3) as Greater_Asiane
                     , min(avg([Greater Asia_D_wt11]),3) as Greater_Asia_Dne
                     , min(avg([Greater Asia_E_wt11]),3) as Greater_Asia_Ene
                     , min(avg([Greater China_wt8]),3) as Greater_Chinane
                     , min(avg([Greater China_D_wt11]),3) as Greater_China_Dne
                     , min(avg([Greater China_E_wt11]),3) as Greater_China_Ene
                     , min(avg([India_wt8]),3) as Indiane
                     , min(avg([Latin America_wt8]),3) as Latin_Americane
                     --
                     , min(avg([Central Europe_wt9]),3) as Central_Europese
                     , min(avg([Central Europe_E_wt12]),3) as Central_Europe_Ese
                     , min(avg([Central Europe_D_wt12]),3) as Central_Europe_Dse
                     , min(avg([Northern Europe_wt9]),3) as Northern_Europese
                     , min(avg([North America_wt9]),3) as North_Americase
                     , min(avg([ISE_wt9]),3) as ISEse
                     , min(avg([UK&I_wt9]),3) as UKIse
                     , min(avg([Greater Asia_wt9]),3) as Greater_Asiase
                     , min(avg([Greater Asia_D_wt12]),3) as Greater_Asia_Dse
                     , min(avg([Greater Asia_E_wt12]),3) as Greater_Asia_Ese
                     , min(avg([Greater China_wt9]),3) as Greater_Chinase
                     , min(avg([Greater China_D_wt12]),3) as Greater_China_Dse
                     , min(avg([Greater China_E_wt12]),3) as Greater_China_Ese
                     , min(avg([India_wt9]),3) as Indiase
                     , min(avg([Latin America_wt9]),3) as Latin_Americase
                     --
                     , min(avg([Central Europe_wt10]),3) as Central_Europeuk
                     , min(avg([Central Europe_E_wt13]),3) as Central_Europe_Euk
                     , min(avg([Central Europe_D_wt13]),3) as Central_Europe_Duk
                     , min(avg([Northern Europe_wt10]),3) as Northern_Europeuk
                     , min(avg([Southern Europe_wt10]),3) as Southern_Europeuk
                     , min(avg([ISE_wt10]),3) as ISEuk
                     , min(avg([North America_wt10]),3) as North_Americauk
                     , min(avg([Greater Asia_wt10]),3) as Greater_Asiauk
                     , min(avg([Greater Asia_D_wt13]),3) as Greater_Asia_Duk
                     , min(avg([Greater Asia_E_wt13]),3) as Greater_Asia_Euk
                     , min(avg([Greater China_wt10]),3) as Greater_Chinauk
                     , min(avg([Greater China_D_wt13]),3) as Greater_China_Duk
                     , min(avg([Greater China_E_wt13]),3) as Greater_China_Euk
                     , min(avg([India_wt10]),3) as Indiauk
                     , min(avg([Latin America_wt10]),3) as Latin_Americauk
                     --
                     , min(avg(LA_wt),3) as LAa 
                     , min(avg(AP_wt),3) as APa 
                     , min(avg(AP_wt2),3) as APa2 
                     , min(avg(LA_wt2),3) as LAa2
                     , min(avg(LA_wt3),3) as LAa3
                     from usageSummary2TE 
                     where 
                     segment is not null 
                     group by segment, biz')
 wtaverage[is.na(wtaverage)] <- 1

# add in some missing data 
  #wtavg2 <- wtaverage %>% 
  #group_by(biz) %>%
  #summarise(across(everything(), mean))

# COMMAND ----------

wtavg2 <- sqldf("SELECT biz
                  , AVG(EUa) as EUa, AVG(Central_Europena) as Central_Europena, Avg(Central_Europe_Dna) as Central_Europe_Dna, Avg(Central_Europe_Ena) as Central_Europe_Ena
                  , AVG(Northern_Europena) as Northern_Europena, AVG(Southern_Europena) as  Southern_Europena, AVG(ISEna) as ISEna, AVG(UKIna) as UKIna, AVG(Greater_Asiana) as Greater_Asiana
                  , AVG(Greater_Asia_Dna) as Greater_Asia_Dna, AVG(Greater_Asia_Ena) as Greater_Asia_Ena, AVG(Greater_Chinana) as Greater_Chinana, AVG(Greater_China_Dna) as Greater_China_Dna
                  , AVG(Greater_China_Ena) as Greater_China_Ena, AVG(Indiana) as Indiana, AVG(Latin_Americana) as Latin_Americana, AVG(North_Americace) as North_Americace
                  , AVG(Central_Europe_Eced) as Central_Europe_Eced, AVG(Central_Europe_Dcee) as Central_Europe_Dcee, AVG(Northern_Europece) as Northern_Europece, AVG(Southern_Europece) as Southern_Europece
                  , AVG(ISEce) as  ISEce, AVG(UKIce) as UKIce, AVG(Greater_Asiace) as Greater_Asiace, AVG(Greater_Asia_Dced) as Greater_Asia_Dced, AVG(Greater_Asia_Eced) as Greater_Asia_Eced
                  , AVG(Greater_Asia_Dcee) as Greater_Asia_Dcee, AVG(Greater_Asia_Ecee) as Greater_Asia_Ecee, AVG(Greater_Chinace) as Greater_Chinace, AVG(Greater_China_Dced) as  Greater_China_Dced
                  , AVG(Greater_China_Eced) as Greater_China_Eced, AVG(Greater_China_Dcee) as Greater_China_Dcee, AVG(Greater_China_Ecee) as Greater_China_Ecee, AVG(Indiace) as Indiace
                  , AVG(Latin_Americace) as Latin_Americace, AVG(Central_Europega) as Central_Europega, AVG(Central_Europe_Egad) as Central_Europe_Egad, AVG(Central_Europe_Dgad) as Central_Europe_Dgad
                  , AVG(Central_Europe_Egae) as Central_Europe_Egae, AVG(Central_Europe_Dgae) as Central_Europe_Dgae, AVG(Northern_Europega) as Northern_Europega, AVG(Southern_Europega) as Southern_Europega
                  , AVG(ISEga) as  ISEga, AVG(UKIga) as UKIga, AVG(North_Americaga) as North_Americaga, AVG(Greater_Asia_Dgae) as Greater_Asia_Dgae, AVG(Greater_Asia_Egad) as Greater_Asia_Egad
                  , AVG(Greater_Chinaga) as Greater_Chinaga, AVG(Greater_China_Dgad) as Greater_China_Dgad, AVG(Greater_China_Egad) as Greater_China_Egad, AVG(Greater_China_Dgae) as Greater_China_Dgae
                  , AVG(Greater_China_Egae) as Greater_China_Egae, AVG(Indiaga) as Indiaga, AVG(Latin_Americaga) as  Latin_Americaga, AVG(Central_Europegc) as Central_Europegc 
                  , AVG(Central_Europe_Egcd) as Central_Europe_Egcd, AVG(Central_Europe_Dgcd) as Central_Europe_Dgcd, AVG(Central_Europe_Egce) as Central_Europe_Egce, AVG(Central_Europe_Dgce) as Central_Europe_Dgce
                  , AVG(Northern_Europegc) as Northern_Europegc, AVG(Southern_Europegc) as Southern_Europegc, AVG(ISEgc) as ISEgc, AVG(UKIgc) as UKIgc, AVG(Greater_Asiagc) as Greater_Asiagc
                  , AVG(Greater_Asia_Dgcd) as Greater_Asia_Dgcd, AVG(Greater_Asia_Egcd) as Greater_Asia_Egcd, AVG(Greater_Asia_Dgce) as Greater_Asia_Dgce, AVG(Greater_Asia_Egce) as Greater_Asia_Egce   
                  , AVG(Greater_China_Dgce) as Greater_China_Dgce, AVG(Greater_China_Egcd) as Greater_China_Egcd, AVG(North_Americagc) as North_Americagc, AVG(Indiagc) as  Indiagc
                  , AVG(Latin_Americagc) as Latin_Americagc, AVG(Central_Europeia) as Central_Europeia, AVG(Central_Europe_Eia) as Central_Europe_Eia, AVG(Central_Europe_Dia) as Central_Europe_Dia
                  , AVG(Northern_Europeia) as Northern_Europeia, AVG(Southern_Europeia) as Southern_Europeia, AVG(ISEia) as ISEia, AVG(UKIia) as UKIia, AVG(Greater_Asiaia) as Greater_Asiaia
                  , AVG(Greater_Asia_Dia) as Greater_Asia_Dia, AVG(Greater_Asia_Eia) as Greater_Asia_Eia, AVG(Greater_Chinaia) as Greater_Chinaia, AVG(Greater_China_Dia) as Greater_China_Dia
                  , AVG(Greater_China_Eia) as Greater_China_Eia, AVG(North_Americaia) as North_Americaia, AVG(Latin_Americaia) as Latin_Americaia, AVG(Central_Europeis) as Central_Europeis
                  , AVG(Central_Europe_Eis) as Central_Europe_Eis, AVG(Central_Europe_Dis) as Central_Europe_Dis, AVG(Northern_Europeis) as Northern_Europeis, AVG(Southern_Europeis) as Southern_Europeis
                  , AVG(North_Americais) as North_Americais, AVG(UKIis) as UKIis, AVG(Greater_Asiais) as Greater_Asiais, AVG(Greater_Asia_Dis) as Greater_Asia_Dis, AVG(Greater_Asia_Eis) as Greater_Asia_Eis
                  , AVG(Greater_Chinais) as Greater_Chinais, AVG(Greater_China_Dis) as Greater_China_Dis, AVG(Greater_China_Eis) as Greater_China_Eis, AVG(Indiais) as  Indiais          
                  , AVG(Latin_Americais) as Latin_Americais, AVG(Central_Europela) as Central_Europela, AVG(Central_Europe_Ela) as Central_Europe_Ela, AVG(Central_Europe_Dla) as Central_Europe_Dla
                  , AVG(Northern_Europela) as Northern_Europela, AVG(Southern_Europela) as Southern_Europela, AVG(ISEla) as ISEla, AVG(UKIla) as UKIla, AVG(Greater_Asiala) as Greater_Asiala
                  , AVG(Greater_Asia_Dla) as Greater_Asia_Dla, AVG(Greater_Asia_Ela) as Greater_Asia_Ela, AVG(Greater_Chinala) as Greater_Chinala, AVG(Greater_China_Dla) as Greater_China_Dla
                  , AVG(Greater_China_Ela) as Greater_China_Ela, AVG(Indiala) as Indiala, AVG(North_Americala) as North_Americala, AVG(Central_Europene) as Central_Europene, AVG(Central_Europe_Ene) as Central_Europe_Ene
                  , AVG(Central_Europe_Dne) as Central_Europe_Dne, AVG(North_Americane) as North_Americane, AVG(Southern_Europene) as Southern_Europene, AVG(ISEne) as ISEne, AVG(UKIne) as UKIne
                  , AVG(Greater_Asiane) as Greater_Asiane, AVG(Greater_Asia_Dne) as Greater_Asia_Dne, AVG(Greater_Asia_Ene) as Greater_Asia_Ene, AVG(Greater_Chinane) as Greater_Chinane
                  , AVG(Greater_China_Dne) as Greater_China_Dne, AVG(Greater_China_Ene) as Greater_China_Ene, AVG(Indiane) as Indiane, AVG(Latin_Americane) as Latin_Americane, AVG(Central_Europese) as Central_Europese  
                  , AVG(Central_Europe_Ese) as Central_Europe_Ese, AVG(Central_Europe_Dse) as Central_Europe_Dse, AVG(Northern_Europese) as Northern_Europese, AVG(North_Americase) as North_Americase
                  , AVG(ISEse) as ISEse, AVG(UKIse) as UKIse, AVG(Greater_Asiase) as Greater_Asiase, AVG(Greater_Asia_Dse) as Greater_Asia_Dse, AVG(Greater_Asia_Ese) as Greater_Asia_Ese
                  , AVG(Greater_Chinase) as Greater_Chinase, AVG(Greater_China_Dse) as Greater_China_Dse, AVG(Greater_China_Ese) as Greater_China_Ese, AVG(Indiase) as Indiase, AVG(Latin_Americase) as Latin_Americase    
                  , AVG(Central_Europeuk) as Central_Europeuk, AVG(Central_Europe_Euk) as Central_Europe_Euk, AVG(Central_Europe_Duk) as Central_Europe_Duk, AVG(Northern_Europeuk) as Northern_Europeuk   
                  , AVG(Southern_Europeuk) as Southern_Europeuk, AVG(ISEuk) as ISEuk, AVG(North_Americauk) as North_Americauk, AVG(Greater_Asiauk) as Greater_Asiauk, AVG(Greater_Asia_Duk) as Greater_Asia_Duk    
                  , AVG(Greater_Asia_Euk) as Greater_Asia_Euk, AVG(Greater_Chinauk) as Greater_Chinauk, AVG(Greater_China_Duk) as Greater_China_Duk, AVG(Greater_China_Euk) as Greater_China_Euk
                  , AVG(Indiauk) as Indiauk, AVG(Latin_Americauk) as Latin_Americauk, AVG(LAa) as LAa, AVG(APa) as APa, AVG(APa2) as APa2, AVG(LAa2) as LAa2, AVG(LAa3) as LAa3 
                FROM wtaverage 
                GROUP BY biz
")

  wtavg2$segment <- ifelse(wtavg2$biz=='Pro','Echo',ifelse(wtavg2$biz=='Design','NULL',NA))
 
wtaverage <- rbind(wtaverage,wtavg2)

# COMMAND ----------

str(wtaverage)

# COMMAND ----------

usageSummary2TE_D2<-sqldf('select aa1.*,aa2.[EUa],aa2.[Central_Europena],aa2.[Central_Europe_Dna],aa2.[Central_Europe_Ena],aa2.[Northern_Europena]
,aa2.[Southern_Europena],aa2.[ISEna],aa2.[UKIna],aa2.[Greater_Asiana],aa2.[Greater_Asia_Dna],aa2.[Greater_Asia_Ena],aa2.[Greater_Chinana]
,aa2.[Greater_China_Dna],aa2.[Greater_China_Ena],aa2.[Indiana],aa2.[Latin_Americana],aa2.[North_Americace],aa2.[Central_Europe_Eced],aa2.[Central_Europe_Dcee]
,aa2.[Northern_Europece],aa2.[Southern_Europece],aa2.[ISEce],aa2.[UKIce],aa2.[Greater_Asiace],aa2.[Greater_Asia_Dced],aa2.[Greater_Asia_Eced]
,aa2.[Greater_Asia_Dcee],aa2.[Greater_Asia_Ecee],aa2.[Greater_Chinace],aa2.[Greater_China_Dced],aa2.[Greater_China_Eced],aa2.[Greater_China_Dcee]
,aa2.[Greater_China_Ecee],aa2.[Indiace],aa2.[Latin_Americace],aa2.[Central_Europega],aa2.[Central_Europe_Egad],aa2.[Central_Europe_Dgad],aa2.[Central_Europe_Egae]
,aa2.[Central_Europe_Dgae],aa2.[Northern_Europega],aa2.[Southern_Europega],aa2.[ISEga],aa2.[UKIga],aa2.[North_Americaga],aa2.[Greater_Asia_Dgae],aa2.[Greater_Asia_Egad]
,aa2.[Greater_Chinaga],aa2.[Greater_China_Dgad],aa2.[Greater_China_Egad],aa2.[Greater_China_Dgae],aa2.[Greater_China_Egae],aa2.[Indiaga],aa2.[Latin_Americaga]
,aa2.[Central_Europegc],aa2.[Central_Europe_Egcd],aa2.[Central_Europe_Dgcd],aa2.[Central_Europe_Egce],aa2.[Central_Europe_Dgce],aa2.[Northern_Europegc]
,aa2.[Southern_Europegc],aa2.[ISEgc],aa2.[UKIgc],aa2.[Greater_Asiagc],aa2.[Greater_Asia_Dgcd],aa2.[Greater_Asia_Egcd],aa2.[Greater_Asia_Dgce],aa2.[Greater_Asia_Egce]
,aa2.[Greater_China_Dgce],aa2.[Greater_China_Egcd],aa2.[North_Americagc],aa2.[Indiagc],aa2.[Latin_Americagc],aa2.[Central_Europeia],aa2.[Central_Europe_Eia]
,aa2.[Central_Europe_Dia],aa2.[Northern_Europeia],aa2.[Southern_Europeia],aa2.[ISEia],aa2.[UKIia],aa2.[Greater_Asiaia],aa2.[Greater_Asia_Dia]
,aa2.[Greater_Asia_Eia],aa2.[Greater_Chinaia],aa2.[Greater_China_Dia],aa2.[Greater_China_Eia],aa2.[North_Americaia],aa2.[Latin_Americaia],aa2.[Central_Europeis]
,aa2.[Central_Europe_Eis],aa2.[Central_Europe_Dis],aa2.[Northern_Europeis],aa2.[Southern_Europeis],aa2.[North_Americais],aa2.[UKIis],aa2.[Greater_Asiais]
,aa2.[Greater_Asia_Dis],aa2.[Greater_Asia_Eis],aa2.[Greater_Chinais],aa2.[Greater_China_Dis],aa2.[Greater_China_Eis],aa2.[Indiais],aa2.[Latin_Americais]
,aa2.[Central_Europela],aa2.[Central_Europe_Ela],aa2.[Central_Europe_Dla],aa2.[Northern_Europela],aa2.[Southern_Europela],aa2.[ISEla],aa2.[UKIla]
,aa2.[Greater_Asiala],aa2.[Greater_Asia_Dla],aa2.[Greater_Asia_Ela],aa2.[Greater_Chinala],aa2.[Greater_China_Dla],aa2.[Greater_China_Ela],aa2.[Indiala]
,aa2.[North_Americala],aa2.[Central_Europene],aa2.[Central_Europe_Ene],aa2.[Central_Europe_Dne],aa2.[North_Americane],aa2.[Southern_Europene],aa2.[ISEne]
,aa2.[UKIne],aa2.[Greater_Asiane],aa2.[Greater_Asia_Dne],aa2.[Greater_Asia_Ene],aa2.[Greater_Chinane],aa2.[Greater_China_Dne],aa2.[Greater_China_Ene]
,aa2.[Indiane],aa2.[Latin_Americane],aa2.[Central_Europese],aa2.[Central_Europe_Ese],aa2.[Central_Europe_Dse],aa2.[Northern_Europese],aa2.[North_Americase]
,aa2.[ISEse],aa2.[UKIse],aa2.[Greater_Asiase],aa2.[Greater_Asia_Dse],aa2.[Greater_Asia_Ese],aa2.[Greater_Chinase],aa2.[Greater_China_Dse]
,aa2.[Greater_China_Ese],aa2.[Indiase],aa2.[Latin_Americase],aa2.[Central_Europeuk],aa2.[Central_Europe_Euk],aa2.[Central_Europe_Duk],aa2.[Northern_Europeuk]
,aa2.[Southern_Europeuk],aa2.[ISEuk],aa2.[North_Americauk],aa2.[Greater_Asiauk],aa2.[Greater_Asia_Duk],aa2.[Greater_Asia_Euk],aa2.[Greater_Chinauk]
,aa2.[Greater_China_Duk],aa2.[Greater_China_Euk],aa2.[Indiauk],aa2.[Latin_Americauk],aa2.[LAa],aa2.[APa],aa2.[APa2],aa2.[LAa2], aa2.[LAa3]
                              from usageSummary2TE aa1
                              inner join 
                              wtaverage aa2
                              on 
                              (aa1.segment=aa2.segment and aa1.biz=aa2.biz)
                              order by segment, biz')

# COMMAND ----------

usageSummary2TE_D3 <- sqldf("select *
                                , CASE 
                                WHEN NA is not null THEN NA*1
                                WHEN NA is null and EU is not null THEN EU*1/EUa
                                WHEN NA is null and EU is null and AP is not null then AP*1/APa
                                WHEN NA is null and EU is null and AP is null and LA is not null then LA*1/LAa
                                ELSE null 
                                END as NA2
                                , CASE
                                WHEN [North America] is not null then [North America]*1
                                WHEN [North America] is null and [UK&I] is not null then [UK&I]*1/UKIna
                                WHEN [North America] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europena
                                WHEN [North America] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europena
                                WHEN [North America] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiana
                                WHEN [North America] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europena
                                WHEN [North America] is null and [ISE] is not null then [ISE]*1/ISEna
                                WHEN [North America] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiana
                                WHEN [North America] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinana
                                WHEN [North America] is null and [Latin America] is not null then [Latin America]*1/Latin_Americana
                                ELSE null
                                END as North_America2
                                , CASE
                                WHEN [North America] is not null then [North America]*1
                                WHEN [North America] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europena
                                WHEN [North America] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europena
                                WHEN [North America] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dna
                                WHEN [North America] is null and [UK&I] is not null then [UK&I]*1/UKIna
                                WHEN [North America] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiana
                                WHEN [North America] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Ena
                                WHEN [North America] is null and [ISE] is not null then [ISE]*1/ISEna
                                WHEN [North America] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dna
                                WHEN [North America] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Ena
                                WHEN [North America] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dna
                                WHEN [North America] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Ena
                                WHEN [North America] is null and [Latin America] is not null then [Latin America]*1/Latin_Americana
                                ELSE null
                                END as North_America_D2
                                , case
                                WHEN EU is not null then EU*1
                                WHEN EU is null and NA is not null then NA*EUa
                                WHEN EU is null and NA is null and AP is not null then AP*1/APa2
                                WHEN EU is null and NA is null and AP is null and LA is not null then LA*1/LAa2
                                ELSE null
                                END as EU2
                                , CASE
                                WHEN [UK&I] is not null then [UK&I]*1
                                WHEN [UK&I] is null and [North America] is not null then [North America]*UKIna
                                WHEN [UK&I] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europeuk
                                WHEN [UK&I] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europeuk
                                WHEN [UK&I] is null and [ISE] is not null then [ISE]*1/ISEuk
                                WHEN [UK&I] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiauk
                                WHEN [UK&I] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europeuk
                                WHEN [UK&I] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiauk
                                WHEN [UK&I] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinauk
                                WHEN [UK&I] is null and [Latin America] is not null then [Latin America]*1/Latin_Americauk
                                ELSE null
                                END as UKI2
                                , CASE
                                WHEN [UK&I] is not null then [UK&I]*1
                                WHEN [UK&I] is null and [North America] is not null then [North America]*UKIna
                                WHEN [UK&I] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europeuk
                                WHEN [UK&I] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europeuk
                                WHEN [UK&I] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Duk
                                WHEN [UK&I] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Duk
                                WHEN [UK&I] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Duk
                                WHEN [UK&I] is null and [ISE] is not null then [ISE]*1/ISEuk
                                WHEN [UK&I] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiauk
                                WHEN [UK&I] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Euk
                                WHEN [UK&I] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Euk
                                WHEN [UK&I] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Euk
                                WHEN [UK&I] is null and [Latin America] is not null then [Latin America]*1/Latin_Americauk
                                ELSE null
                                END as UKI_D2
                                , CASE
                                WHEN [Northern Europe] is not null then [Northern Europe]*1
                                WHEN [Northern Europe] is null and [North America] is not null then [North America]*Northern_Europena
                                WHEN [Northern Europe] is null and [UK&I] is not null then [UK&I]*1/UKIne
                                WHEN [Northern Europe] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europene
                                WHEN [Northern Europe] is null and [ISE] is not null then [ISE]*1/ISEne
                                WHEN [Northern Europe] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiane
                                WHEN [Northern Europe] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europene
                                WHEN [Northern Europe] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiane
                                WHEN [Northern Europe] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinane
                                WHEN [Northern Europe] is null and [Latin America] is not null then [Latin America]*1/Latin_Americane
                                ELSE null
                                END as Northern_Europe2
                                , CASE
                                WHEN [Northern Europe] is not null then [Northern Europe]*1
                                WHEN [Northern Europe] is null and [North America] is not null then [North America]*Northern_Europena
                                WHEN [Northern Europe] is null and [UK&I] is not null then [UK&I]*1/UKIne
                                WHEN [Northern Europe] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europene
                                WHEN [Northern Europe] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dne
                                WHEN [Northern Europe] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dne
                                WHEN [Northern Europe] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dne
                                WHEN [Northern Europe] is null and [ISE] is not null then [ISE]*1/ISEne
                                WHEN [Northern Europe] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiane
                                WHEN [Northern Europe] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Ene
                                WHEN [Northern Europe] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Ene
                                WHEN [Northern Europe] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Ene
                                WHEN [Northern Europe] is null and [Latin America] is not null then [Latin America]*1/Latin_Americane
                                ELSE null
                                END as Northern_Europe_D2
                                , CASE
                                WHEN [Southern Europe] is not null then [Southern Europe]*1
                                WHEN [Southern Europe] is null and [North America] is not null then [North America]*Southern_Europena
                                WHEN [Southern Europe] is null and [UK&I] is not null then [UK&I]*1/UKIse
                                WHEN [Southern Europe] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europese
                                WHEN [Southern Europe] is null and [ISE] is not null then [ISE]*1/ISEse
                                WHEN [Southern Europe] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiase
                                WHEN [Southern Europe] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europese
                                WHEN [Southern Europe] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiase
                                WHEN [Southern Europe] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinase
                                WHEN [Southern Europe] is null and [Latin America] is not null then [Latin America]*1/Latin_Americase
                                ELSE null
                                END as Southern_Europe2
                                , CASE
                                WHEN [Southern Europe] is not null then [Southern Europe]*1
                                WHEN [Southern Europe] is null and [North America] is not null then [North America]*Southern_Europena
                                WHEN [Southern Europe] is null and [UK&I] is not null then [UK&I]*1/UKIse
                                WHEN [Southern Europe] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europese
                                WHEN [Southern Europe] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dse
                                WHEN [Southern Europe] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dse
                                WHEN [Southern Europe] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dse
                                WHEN [Southern Europe] is null and [ISE] is not null then [ISE]*1/ISEse
                                WHEN [Southern Europe] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiase
                                WHEN [Southern Europe] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Ese
                                WHEN [Southern Europe] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Ese
                                WHEN [Southern Europe] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Ese
                                WHEN [Southern Europe] is null and [Latin America] is not null then [Latin America]*1/Latin_Americase
                                ELSE null
                                END as Southern_Europe_D2
                                , CASE
                                WHEN [ISE] is not null then [ISE]*1
                                WHEN [ISE] is null and [North America] is not null then [North America]*ISEna
                                WHEN [ISE] is null and [UK&I] is not null then [UK&I]*1/UKIis
                                WHEN [ISE] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europeis
                                WHEN [ISE] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europeis
                                WHEN [ISE] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiais
                                WHEN [ISE] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europeis
                                WHEN [ISE] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiais
                                WHEN [ISE] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinais
                                WHEN [ISE] is null and [Latin America] is not null then [Latin America]*1/Latin_Americais
                                ELSE null
                                END as ISE2
                                , CASE
                                WHEN [ISE] is not null then [ISE]*1
                                WHEN [ISE] is null and [North America] is not null then [North America]*ISEna
                                WHEN [ISE] is null and [UK&I] is not null then [UK&I]*1/UKIis
                                WHEN [ISE] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europeis
                                WHEN [ISE] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europeis
                                WHEN [ISE] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dis
                                WHEN [ISE] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dis
                                WHEN [ISE] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dis
                                WHEN [ISE] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiais
                                WHEN [ISE] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Eis
                                WHEN [ISE] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Eis
                                WHEN [ISE] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Eis
                                WHEN [ISE] is null and [Latin America] is not null then [Latin America]*1/Latin_Americais
                                ELSE null
                                END as ISE_E2
                                , CASE
                                WHEN [Central Europe] is not null then [Central Europe]*1
                                WHEN [Central Europe] is null and [North America] is not null then [North America]*Central_Europena
                                WHEN [Central Europe] is null and [UK&I] is not null then [UK&I]*1/UKIce
                                WHEN [Central Europe] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europece
                                WHEN [Central Europe] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europece
                                WHEN [Central Europe] is null and [ISE] is not null then [ISE]*1/ISEce
                                WHEN [Central Europe] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiace
                                WHEN [Central Europe] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiace
                                WHEN [Central Europe] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinace
                                WHEN [Central Europe] is null and [Latin America] is not null then [Latin America]*1/Latin_Americace
                                ELSE null
                                END as Central_Europe2
                                , CASE
                                WHEN [Central Europe_D] is not null then [Central Europe_D]*1
                                WHEN [Central Europe_D] is null and [North America] is not null then [North America]*Central_Europena
                                WHEN [Central Europe_D] is null and [UK&I] is not null then [UK&I]*1/UKIce
                                WHEN [Central Europe_D] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europece
                                WHEN [Central Europe_D] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europece
                                WHEN [Central Europe_D] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dcee
                                WHEN [Central Europe_D] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dced
                                WHEN [Central Europe_D] is null and [ISE] is not null then [ISE]*1/ISEce
                                WHEN [Central Europe_D] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiace
                                WHEN [Central Europe_D] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Dcee
                                WHEN [Central Europe_D] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Dcee
                                WHEN [Central Europe_D] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Dcee
                                WHEN [Central Europe_D] is null and [Latin America] is not null then [Latin America]*1/Latin_Americace
                                ELSE null
                                END as Central_Europe_D2
                                , CASE
                                WHEN [Central Europe_E] is not null then [Central Europe_E]*1
                                WHEN [Central Europe_E] is null and [North America] is not null then [North America]*Central_Europena
                                WHEN [Central Europe_E] is null and [UK&I] is not null then [UK&I]*1/UKIce
                                WHEN [Central Europe_E] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europece
                                WHEN [Central Europe_E] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europece
                                WHEN [Central Europe_E] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dcee
                                WHEN [Central Europe_E] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dcee
                                WHEN [Central Europe_E] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dcee
                                WHEN [Central Europe_E] is null and [ISE] is not null then [ISE]*1/ISEce
                                WHEN [Central Europe_E] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiace
                                WHEN [Central Europe_E] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Ecee
                                WHEN [Central Europe_E] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Ecee
                                WHEN [Central Europe_E] is null and [Latin America] is not null then [Latin America]*1/Latin_Americace
                                ELSE null
                                END as Central_Europe_E2
                                , CASE 
                                WHEN AP is not null THEN AP*1
                                WHEN AP is null and NA is not null THEN NA*APa
                                WHEN AP is null and NA is null and EU is not null then EU*APa2
                                WHEN AP is null and NA is null and EU is null and LA is not null then LA*1/LAa3
                                ELSE null 
                                END as AP2
                                , CASE
                                WHEN [India SL & BL] is not null then [India SL & BL]*1
                                WHEN [India SL & BL] is null and [North America] is not null then [North America]*Indiana
                                WHEN [India SL & BL] is null and [UK&I] is not null then [UK&I]*1/UKIia
                                WHEN [India SL & BL] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europeia
                                WHEN [India SL & BL] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europeia
                                WHEN [India SL & BL] is null and [ISE] is not null then [ISE]*1/ISEia
                                WHEN [India SL & BL] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europeia
                                WHEN [India SL & BL] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiaia
                                WHEN [India SL & BL] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinaia
                                WHEN [India SL & BL] is null and [Latin America] is not null then [Latin America]*1/Latin_Americaia
                                ELSE null
                                END as India2
                                , CASE
                                WHEN [India SL & BL] is not null then [India SL & BL]*1
                                WHEN [India SL & BL] is null and [North America] is not null then [North America]*Indiana
                                WHEN [India SL & BL] is null and [UK&I] is not null then [UK&I]*1/UKIia
                                WHEN [India SL & BL] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europeia
                                WHEN [India SL & BL] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europeia
                                WHEN [India SL & BL] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dia
                                WHEN [India SL & BL] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dia
                                WHEN [India SL & BL] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dia
                                WHEN [India SL & BL] is null and [ISE] is not null then [ISE]*1/ISEia
                                WHEN [India SL & BL] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Eia
                                WHEN [India SL & BL] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Eia
                                WHEN [India SL & BL] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Eia
                                WHEN [India SL & BL] is null and [Latin America] is not null then [Latin America]*1/Latin_Americaia 
                                ELSE null
                                END as India_E2 
                                , CASE
                                WHEN [Greater Asia] is not null then [Greater Asia]*1
                                WHEN [Greater Asia] is null and [North America] is not null then [North America]*Greater_Asiana
                                WHEN [Greater Asia] is null and [UK&I] is not null then [UK&I]*1/UKIga
                                WHEN [Greater Asia] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europega
                                WHEN [Greater Asia] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europega
                                WHEN [Greater Asia] is null and [ISE] is not null then [ISE]*1/ISEga
                                WHEN [Greater Asia] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiaga
                                WHEN [Greater Asia] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europega
                                WHEN [Greater Asia] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinaga
                                WHEN [Greater Asia] is null and [Latin America] is not null then [Latin America]*1/Latin_Americaga
                                ELSE null
                                END as Greater_Asia2
                                , CASE
                                WHEN [Greater Asia_D] is not null then [Greater Asia_D]*1
                                WHEN [Greater Asia_D] is null and [North America] is not null then [North America]*Greater_Asiana
                                WHEN [Greater Asia_D] is null and [UK&I] is not null then [UK&I]*1/UKIga
                                WHEN [Greater Asia_D] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europega
                                WHEN [Greater Asia_D] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europega
                                WHEN [Greater Asia_D] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dgad
                                WHEN [Greater Asia_D] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dgad
                                WHEN [Greater Asia_D] is null and [ISE] is not null then [ISE]*1/ISEga
                                WHEN [Greater Asia_D] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiaga
                                WHEN [Greater Asia_D] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Egad
                                WHEN [Greater Asia_D] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Egad
                                WHEN [Greater Asia_D] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Egad
                                WHEN [Greater Asia_D] is null and [Latin America] is not null then [Latin America]*1/Latin_Americaga
                                ELSE null
                                END as Greater_Asia_D2
                                , CASE
                                WHEN [Greater Asia_E] is not null then [Greater Asia_E]*1
                                WHEN [Greater Asia_E] is null and [North America] is not null then [North America]*Greater_Asiana
                                WHEN [Greater Asia_E] is null and [UK&I] is not null then [UK&I]*1/UKIga
                                WHEN [Greater Asia_E] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europega
                                WHEN [Greater Asia_E] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europega
                                WHEN [Greater Asia_E] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dgae
                                WHEN [Greater Asia_E] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dgae
                                WHEN [Greater Asia_E] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dgae
                                WHEN [Greater Asia_E] is null and [ISE] is not null then [ISE]*1/ISEga
                                WHEN [Greater Asia_E] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiaga
                                WHEN [Greater Asia_E] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Egae
                                WHEN [Greater Asia_E] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Egae
                                WHEN [Greater Asia_E] is null and [Latin America] is not null then [Latin America]*1/Latin_Americaga
                                ELSE null
                                END as Greater_Asia_E2
                                , CASE
                                WHEN [Greater China] is not null then [Greater China]*1
                                WHEN [Greater China] is null and [North America] is not null then [North America]*Greater_Chinana
                                WHEN [Greater China] is null and [UK&I] is not null then [UK&I]*1/UKIgc
                                WHEN [Greater China] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europegc
                                WHEN [Greater China] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europegc
                                WHEN [Greater China] is null and [ISE] is not null then [ISE]*1/ISEgc
                                WHEN [Greater China] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiagc
                                WHEN [Greater China] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europegc
                                WHEN [Greater China] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiagc
                                WHEN [Greater China] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
                                ELSE null
                                END as Greater_China2
                                , CASE
                                WHEN [Greater China_D] is not null then [Greater China_D]*1
                                WHEN [Greater China_D] is null and [North America] is not null then [North America]*Greater_China_Dna
                                WHEN [Greater China_D] is null and [UK&I] is not null then [UK&I]*1/UKIgc
                                WHEN [Greater China_D] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europegc
                                WHEN [Greater China_D] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europegc
                                WHEN [Greater China_D] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dgcd
                                WHEN [Greater China_D] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dgcd
                                WHEN [Greater China_D] is null and [ISE] is not null then [ISE]*1/ISEgc
                                WHEN [Greater China_D] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiagc
                                WHEN [Greater China_D] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Egcd
                                WHEN [Greater China_D] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Egcd
                                WHEN [Greater China_D] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Egcd
                                WHEN [Greater China_D] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
                                ELSE null
                                END as Greater_China_D2
                                , CASE
                                WHEN [Greater China_E] is not null then [Greater China_E]*1
                                WHEN [Greater China_E] is null and [North America] is not null then [North America]*Greater_China_Ena
                                WHEN [Greater China_E] is null and [UK&I] is not null then [UK&I]*1/UKIgc
                                WHEN [Greater China_E] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europegc
                                WHEN [Greater China_E] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europegc
                                WHEN [Greater China_E] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dgce
                                WHEN [Greater China_E] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dgce
                                WHEN [Greater China_E] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_Asia_Dgce
                                WHEN [Greater China_E] is null and [ISE] is not null then [ISE]*1/ISEgc
                                WHEN [Greater China_E] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiagc
                                WHEN [Greater China_E] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Egce
                                WHEN [Greater China_E] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Egce
                                WHEN [Greater China_E] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
                                ELSE null
                                END as Greater_China_E2
                                , CASE 
                                WHEN LA is not null THEN LA*1
                                WHEN LA is null and NA is not null THEN NA*LAa
                                WHEN LA is null and NA is null and EU is not null then EU*LAa2
                                WHEN LA is null and NA is null and EU is null and AP is not null then AP*LAa3
                                ELSE null 
                                END as LA2
                                , CASE
                                WHEN [Latin America] is not null then [Latin America]*1
                                WHEN [Latin America] is null and [North America] is not null then [North America]*Latin_Americana
                                WHEN [Latin America] is null and [UK&I] is not null then [UK&I]*1/UKIla
                                WHEN [Latin America] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europela
                                WHEN [Latin America] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europela
                                WHEN [Latin America] is null and [ISE] is not null then [ISE]*1/ISEla
                                WHEN [Latin America] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiala
                                WHEN [Latin America] is null and [Central Europe] is not null then [Central Europe]*1/Central_Europela
                                WHEN [Latin America] is null and [Greater Asia] is not null then [Greater Asia]*1/Greater_Asiala
                                WHEN [Latin America] is null and [Greater China] is not null then [Greater China]*1/Greater_Chinala
                                ELSE null
                                END as Latin_America2
                                , CASE
                                WHEN [Latin America] is not null then [Latin America]*1
                                WHEN [Latin America] is null and [North America] is not null then [North America]*Latin_Americana
                                WHEN [Latin America] is null and [UK&I] is not null then [UK&I]*1/UKIla
                                WHEN [Latin America] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europela
                                WHEN [Latin America] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europela
                                WHEN [Latin America] is null and [Central Europe_D] is not null then [Central Europe_D]*1/Central_Europe_Dla
                                WHEN [Latin America] is null and [Greater Asia_D] is not null then [Greater Asia_D]*1/Greater_Asia_Dla
                                WHEN [Latin America] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dla
                                WHEN [Latin America] is null and [ISE] is not null then [ISE]*1/ISEla
                                WHEN [Latin America] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiala
                                WHEN [Latin America] is null and [Central Europe_E] is not null then [Central Europe_E]*1/Central_Europe_Ela
                                WHEN [Latin America] is null and [Greater Asia_E] is not null then [Greater Asia_E]*1/Greater_Asia_Ela
                                WHEN [Latin America] is null and [Greater China_E] is not null then [Greater China_E]*1/Greater_China_Ela
                                ELSE null
                                END as Latin_America_E2
                                , CASE 
                                WHEN NA is not null THEN 'Self'
                                WHEN NA is null and EU is not null THEN 'EU'
                                WHEN NA is null and EU is null and AP is not null then 'AP'
                                WHEN NA is null and EU is null and AP is null and LA is not null then 'LA'
                                ELSE 'Six' 
                                END as NAroute
                                , CASE
                                WHEN [North America] is not null then 'Self'
                                WHEN [North America] is null and [UK&I] is not null then 'UK&I'
                                WHEN [North America] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [North America] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [North America] is null and [ISE] is not null then 'ISE'
                                WHEN [North America] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [North America] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [North America] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [North America] is null and [Greater China] is not null then 'Greater China'
                                WHEN [North America] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as North_AmericaRoute
                                , CASE
                                WHEN [North America_D] is not null then 'Self'
                                WHEN [North America_D] is null and [UK&I_D] is not null then 'UK'
                                WHEN [North America_D] is null and [Northern Europe_D] is not null then 'NE'
                                WHEN [North America_D] is null and [Southern Europe_D] is not null then 'SE'
                                WHEN [North America_D] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [North America_D] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [North America_D] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [North America_D] is null and [ISE_E] is not null then 'IS'
                                WHEN [North America_D] is null and [India SL & BL_E] is not null then 'IN'
                                WHEN [North America_D] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [North America_D] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [North America_D] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [North America_D] is null and [Latin America_E] is not null then 'LA'
                                ELSE null
                                END as North_America_DRoute
                                , CASE
                                WHEN EU is not null then 'SELF'
                                WHEN EU is null and NA is not null then 'NA'
                                WHEN EU is null and NA is null and AP is not null then 'AP'
                                WHEN EU is null and NA is null and AP is null and LA is not null then 'LA'
                                ELSE 'Six'
                                END as EUroute
                                , CASE
                                WHEN [UK&I] is not null then 'Self'
                                WHEN [UK&I] is null and [North America] is not null then 'North America'
                                WHEN [UK&I] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [UK&I] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [UK&I] is null and [ISE] is not null then 'ISE'
                                WHEN [UK&I] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [UK&I] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [UK&I] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [UK&I] is null and [Greater China] is not null then 'Greater China'
                                WHEN [UK&I] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as UKIRoute
                                , CASE
                                WHEN [UK&I] is not null then 'Self'
                                WHEN [UK&I] is null and [North America] is not null then 'NA'
                                WHEN [UK&I] is null and [Northern Europe] is not null then 'NE'
                                WHEN [UK&I] is null and [Southern Europe] is not null then 'SE'
                                WHEN [UK&I] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [UK&I] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [UK&I] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [UK&I] is null and [ISE] is not null then 'IS'
                                WHEN [UK&I] is null and [India SL & BL] is not null then 'IN'
                                WHEN [UK&I] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [UK&I] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [UK&I] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [UK&I] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as UKI_DRoute
                                , CASE
                                WHEN [Northern Europe] is not null then 'Self'
                                WHEN [Northern Europe] is null and [North America] is not null then 'North America'
                                WHEN [Northern Europe] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Northern Europe] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [Northern Europe] is null and [ISE] is not null then 'ISE'
                                WHEN [Northern Europe] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [Northern Europe] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [Northern Europe] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [Northern Europe] is null and [Greater China] is not null then 'Greater China'
                                WHEN [Northern Europe] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as Northern_EuropeRoute
                                , CASE
                                WHEN [Northern Europe] is not null then 'Self'
                                WHEN [Northern Europe] is null and [North America] is not null then 'NA'
                                WHEN [Northern Europe] is null and [UK&I] is not null then 'UK'
                                WHEN [Northern Europe] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Northern Europe] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [Northern Europe] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [Northern Europe] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [Northern Europe] is null and [ISE] is not null then 'IS'
                                WHEN [Northern Europe] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Northern Europe] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [Northern Europe] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [Northern Europe] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [Northern Europe] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as Northern_Europe_DRoute
                                , CASE
                                WHEN [Southern Europe] is not null then 'Self'
                                WHEN [Southern Europe] is null and [North America] is not null then 'North America'
                                WHEN [Southern Europe] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Southern Europe] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [Southern Europe] is null and [ISE] is not null then 'ISE'
                                WHEN [Southern Europe] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [Southern Europe] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [Southern Europe] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [Southern Europe] is null and [Greater China] is not null then 'Greater China'
                                WHEN [Southern Europe] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as Southern_EuropeRoute
                                , CASE
                                WHEN [Southern Europe] is not null then 'Self'
                                WHEN [Southern Europe] is null and [North America] is not null then 'NA'
                                WHEN [Southern Europe] is null and [UK&I] is not null then 'UK'
                                WHEN [Southern Europe] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Southern Europe] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [Southern Europe] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [Southern Europe] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [Southern Europe] is null and [ISE] is not null then 'IS'
                                WHEN [Southern Europe] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Southern Europe] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [Southern Europe] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [Southern Europe] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [Southern Europe] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as Southern_Europe_DRoute
                                , CASE
                                WHEN [ISE] is not null then 'Self'
                                WHEN [ISE] is null and [North America] is not null then 'North America'
                                WHEN [ISE] is null and [UK&I] is not null then 'UK&I'
                                WHEN [ISE] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [ISE] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [ISE] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [ISE] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [ISE] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [ISE] is null and [Greater China] is not null then 'Greater China'
                                WHEN [ISE] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as ISERoute
                                , CASE
                                WHEN [ISE] is not null then 'Self'
                                WHEN [ISE] is null and [North America] is not null then 'NA'
                                WHEN [ISE] is null and [UK&I] is not null then 'UK'
                                WHEN [ISE] is null and [Northern Europe] is not null then 'NE'
                                WHEN [ISE] is null and [Southern Europe] is not null then 'SE'
                                WHEN [ISE] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [ISE] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [ISE] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [ISE] is null and [India SL & BL] is not null then 'IN'
                                WHEN [ISE] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [ISE] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [ISE] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [ISE] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as ISE_ERoute
                                , CASE
                                WHEN [Central Europe] is not null then 'Self'
                                WHEN [Central Europe] is null and [North America] is not null then 'North America'
                                WHEN [Central Europe] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Central Europe] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [Central Europe] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [Central Europe] is null and [ISE] is not null then 'ISE'
                                WHEN [Central Europe] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [Central Europe] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [Central Europe] is null and [Greater China] is not null then 'Greater China'
                                WHEN [Central Europe] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as Central_EuropeRoute
                                , CASE
                                WHEN [Central Europe_D] is not null then 'Self'
                                WHEN [Central Europe_D] is null and [North America] is not null then 'NA'
                                WHEN [Central Europe_D] is null and [UK&I] is not null then 'UK'
                                WHEN [Central Europe_D] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Central Europe_D] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Central Europe_D] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [Central Europe_D] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [Central Europe_D] is null and [ISE] is not null then 'IS'
                                WHEN [Central Europe_D] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Central Europe_D] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [Central Europe_D] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [Central Europe_D] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [Central Europe_D] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as Central_Europe_DRoute
                                , CASE
                                WHEN [Central Europe_E] is not null then 'Self'
                                WHEN [Central Europe_E] is null and [North America] is not null then 'NA'
                                WHEN [Central Europe_E] is null and [UK&I] is not null then 'UK'
                                WHEN [Central Europe_E] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Central Europe_E] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Central Europe_E] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [Central Europe_E] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [Central Europe_E] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [Central Europe_E] is null and [ISE] is not null then 'IS'
                                WHEN [Central Europe_E] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Central Europe_E] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [Central Europe_E] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [Central Europe_E] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as Central_Europe_ERoute
                                , CASE 
                                WHEN AP is not null THEN 'Self'
                                WHEN AP is null and NA is not null THEN 'NA'
                                WHEN AP is null and NA is null and EU is not null then 'EU'
                                WHEN AP is null and NA is null and EU is null and LA is not null then 'LA'
                                ELSE 'Six'
                                END as AProute
                                , CASE
                                WHEN [India SL & BL] is not null then 'Self'
                                WHEN [India SL & BL] is null and [North America] is not null then 'North America'
                                WHEN [India SL & BL] is null and [UK&I] is not null then 'UK&I'
                                WHEN [India SL & BL] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [India SL & BL] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [India SL & BL] is null and [ISE] is not null then 'ISE'
                                WHEN [India SL & BL] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [India SL & BL] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [India SL & BL] is null and [Greater China] is not null then 'Greater China'
                                WHEN [India SL & BL] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as IndiaRoute
                                , CASE
                                WHEN [India SL & BL] is not null then 'Self'
                                WHEN [India SL & BL] is null and [North America] is not null then 'NA'
                                WHEN [India SL & BL] is null and [UK&I] is not null then 'UK'
                                WHEN [India SL & BL] is null and [Northern Europe] is not null then 'NE'
                                WHEN [India SL & BL] is null and [Southern Europe] is not null then 'SE'
                                WHEN [India SL & BL] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [India SL & BL] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [India SL & BL] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [India SL & BL] is null and [ISE] is not null then 'IS'
                                WHEN [India SL & BL] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [India SL & BL] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [India SL & BL] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [India SL & BL] is null and [Latin America] is not null then 'LA' 
                                ELSE null
                                END as India_ERoute 
                                , CASE
                                WHEN [Greater Asia] is not null then 'Self'
                                WHEN [Greater Asia] is null and [North America] is not null then 'North America'
                                WHEN [Greater Asia] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Greater Asia] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [Greater Asia] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [Greater Asia] is null and [ISE] is not null then 'ISE'
                                WHEN [Greater Asia] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [Greater Asia] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [Greater Asia] is null and [Greater China] is not null then 'Greater China'
                                WHEN [Greater Asia] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as Greater_AsiaRoute
                                , CASE
                                WHEN [Greater Asia_D] is not null then 'Self'
                                WHEN [Greater Asia_D] is null and [North America] is not null then 'NA'
                                WHEN [Greater Asia_D] is null and [UK&I] is not null then 'UK'
                                WHEN [Greater Asia_D] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Greater Asia_D] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Greater Asia_D] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [Greater Asia_D] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [Greater Asia_D] is null and [ISE] is not null then 'IS'
                                WHEN [Greater Asia_D] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Greater Asia_D] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [Greater Asia_D] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [Greater Asia_D] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [Greater Asia_D] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as Greater_Asia_DRoute
                                , CASE
                                WHEN [Greater Asia_E] is not null then 'Self'
                                WHEN [Greater Asia_E] is null and [North America] is not null then 'NA'
                                WHEN [Greater Asia_E] is null and [UK&I] is not null then 'UK'
                                WHEN [Greater Asia_E] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Greater Asia_E] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Greater Asia_E] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [Greater Asia_E] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [Greater Asia_E] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [Greater Asia_E] is null and [ISE] is not null then 'IS'
                                WHEN [Greater Asia_E] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Greater Asia_E] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [Greater Asia_E] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [Greater Asia_E] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as Greater_Asia_ERoute
                                , CASE
                                WHEN [Greater China] is not null then 'Self'
                                WHEN [Greater China] is null and [North America] is not null then 'North America'
                                WHEN [Greater China] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Greater China] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [Greater China] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [Greater China] is null and [ISE] is not null then 'ISE'
                                WHEN [Greater China] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [Greater China] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [Greater China] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [Greater China] is null and [Latin America] is not null then 'Latin America'
                                ELSE null
                                END as Greater_ChinaRoute
                                , CASE
                                WHEN [Greater China_D] is not null then 'Self'
                                WHEN [Greater China_D] is null and [North America] is not null then 'NA'
                                WHEN [Greater China_D] is null and [UK&I] is not null then 'UK'
                                WHEN [Greater China_D] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Greater China_D] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Greater China_D] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [Greater China_D] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [Greater China_D] is null and [ISE] is not null then 'IS'
                                WHEN [Greater China_D] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Greater China_D] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [Greater China_D] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [Greater China_D] is null and [Greater China_E] is not null then 'GCE'
                                WHEN [Greater China_D] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
                                ELSE null
                                END as Greater_China_DRoute
                                , CASE
                                WHEN [Greater China_E] is not null then 'Self'
                                WHEN [Greater China_E] is null and [North America] is not null then 'NA'
                                WHEN [Greater China_E] is null and [UK&I] is not null then 'UK'
                                WHEN [Greater China_E] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Greater China_E] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Greater China_E] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [Greater China_E] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [Greater China_E] is null and [Greater China_D] is not null then 'GCD'
                                WHEN [Greater China_E] is null and [ISE] is not null then 'IS'
                                WHEN [Greater China_E] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Greater China_E] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [Greater China_E] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [Greater China_E] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
                                ELSE null
                                END as Greater_China_ERoute
                              , CASE 
                                WHEN LA is not null THEN 'Self'
                                WHEN LA is null and NA is not null THEN 'NA'
                                WHEN LA is null and NA is null and EU is not null then 'EU'
                                WHEN LA is null and NA is null and EU is null and AP is not null then 'AP'
                                ELSE 'Six' 
                                END as LAroute
                                , CASE
                                WHEN [Latin America] is not null then 'Self'
                                WHEN [Latin America] is null and [North America] is not null then 'North America'
                                WHEN [Latin America] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Latin America] is null and [Northern Europe] is not null then 'Northern Europe'
                                WHEN [Latin America] is null and [Southern Europe] is not null then 'Southern Europe'
                                WHEN [Latin America] is null and [ISE] is not null then 'ISE'
                                WHEN [Latin America] is null and [India SL & BL] is not null then 'India SL & BL'
                                WHEN [Latin America] is null and [Central Europe] is not null then 'Central Europe'
                                WHEN [Latin America] is null and [Greater Asia] is not null then 'Greater Asia'
                                WHEN [Latin America] is null and [Greater China] is not null then 'Greater China'
                                ELSE null
                                END as Latin_AmericaRoute
                                , CASE
                                WHEN [Latin America] is not null then 'Self'
                                WHEN [Latin America] is null and [North America] is not null then 'NA'
                                WHEN [Latin America] is null and [UK&I] is not null then 'UK'
                                WHEN [Latin America] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Latin America] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Latin America] is null and [Central Europe_D] is not null then 'CED'
                                WHEN [Latin America] is null and [Greater Asia_D] is not null then 'GAD'
                                WHEN [Latin America] is null and [Greater China_D] is not null then [Greater China_D]*1/Greater_China_Dla
                                WHEN [Latin America] is null and [ISE] is not null then 'IS'
                                WHEN [Latin America] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Latin America] is null and [Central Europe_E] is not null then 'CEE'
                                WHEN [Latin America] is null and [Greater Asia_E] is not null then 'GAE'
                                WHEN [Latin America] is null and [Greater China_E] is not null then 'GCE'
                                ELSE null
                                END as Latin_America_ERoute
                                
                              from usageSummary2TE_D2
                              order by segment, biz
                              ")

# COMMAND ----------

#Using Market10, can switch to Market10D/E?
      sourceRiMPV <- sqldf("
                       SELECT a.printer_platform_name, a.printer_region_code, a.developed_emerging, a.market10, a.country_alpha2
                        ,CASE WHEN a.Source_vlook = 'country'  then b.iMPV
                             WHEN a.Source_vlook = 'dev/em'   then c.iMPV
                             WHEN a.Source_vlook = 'market10' then d.iMPV
                             WHEN a.Source_vlook = 'region5'  then e.iMPV
                             WHEN a.Source_vlook = 'None' then 
                              CASE
                              WHEN a.market10 = 'North America' then f.North_America2
                              WHEN a.market10 = 'Latin America' then f.Latin_America2
                              WHEN a.market10 = 'UK&I' then f.UKI2
                              WHEN a.market10 = 'Northern Europe' then f.Northern_Europe2
                              WHEN a.market10 = 'Southern Europe' then f.Southern_Europe2
                              WHEN a.market10 = 'Central Europe' AND a.developed_emerging='Developed' THEN f.Central_Europe_D2
                              WHEN a.market10 = 'Central Europe' AND a.developed_emerging='Emerging' THEN f.Central_Europe_E2
                              WHEN a.market10 = 'ISE' then f.ISE2
                              WHEN a.market10 = 'Greater Asia' AND a.developed_emerging='Developed' then f.Greater_Asia_D2
                              WHEN a.market10 = 'Greater Asia' AND a.developed_emerging='Emerging' then f.Greater_Asia_E2
                              WHEN a.market10 = 'Greater China' AND a.developed_emerging='Developed' then f.Greater_China_D2
                              WHEN a.market10 = 'Greater China' AND a.developed_emerging='Emerging' then f.Greater_China_E2
                              WHEN a.market10 = 'India SL & BL' then f.India2
                              END
                             ELSE null
                             END as iMPV
                          ,CASE 
                             WHEN a.Source_vlook = 'None' then 
                              CASE
                              WHEN a.market10 = 'North America' then f.North_AmericaRoute
                              WHEN a.market10 = 'Latin America' then f.Latin_AmericaRoute
                              WHEN a.market10 = 'UK&I' then f.UKIRoute
                              WHEN a.market10 = 'Northern Europe' then f.Northern_EuropeRoute
                              WHEN a.market10 = 'Southern Europe' then f.Southern_EuropeRoute
                              WHEN a.market10 = 'Central Europe' then f.Central_EuropeRoute
                              WHEN a.market10 = 'ISE' then f.ISERoute
                              WHEN a.market10 = 'Greater Asia' then f.Greater_AsiaRoute
                              WHEN a.market10 = 'Greater China' then f.Greater_ChinaRoute
                              WHEN a.market10 = 'India SL & BL' then f.IndiaRoute
                              END
                             ELSE Source_vlook
                        END as Route
                        ,CASE 
                             WHEN a.Source_vlook = 'None' then 
                              CASE
                              WHEN a.market10 = 'North America' then f.North_America_DRoute
                              WHEN a.market10 = 'Latin America' then f.Latin_America_ERoute
                              WHEN a.market10 = 'UK&I' then f.UKI_DRoute
                              WHEN a.market10 = 'Northern Europe' then f.Northern_Europe_DRoute
                              WHEN a.market10 = 'Southern Europe' then f.Southern_Europe_DRoute
                              WHEN a.market10 = 'Central Europe' AND a.developed_emerging='Developed' then f.Central_Europe_DRoute
                              WHEN a.market10 = 'Central Europe' AND a.developed_emerging='Emerging' then f.Central_Europe_ERoute
                              WHEN a.market10 = 'ISE' then f.ISE_ERoute
                              WHEN a.market10 = 'Greater Asia' AND a.developed_emerging='Developed' then f.Greater_Asia_DRoute
                              WHEN a.market10 = 'Greater Asia' AND a.developed_emerging='Emerging' then f.Greater_Asia_ERoute
                              WHEN a.market10 = 'Greater China' AND a.developed_emerging='Developed' then f.Greater_China_DRoute
                              WHEN a.market10 = 'Greater China' AND a.developed_emerging='Emerging' then f.Greater_China_ERoute
                              WHEN a.market10 = 'India SL & BL' then f.India_ERoute
                              END
                             ELSE Source_vlook
                        END as RouteDE
                       FROM sourceR a
                       LEFT JOIN usageSummary2 b
                        ON a.printer_platform_name=b.printer_platform_name and a.country_alpha2=b.country_alpha2
                       LEFT JOIN usageSummary2DE c
                        ON a.printer_platform_name=c.printer_platform_name and a.market10=c.market10 and a.developed_emerging=c.developed_emerging
                       LEFT JOIN usageSummary2Mkt d
                        ON a.printer_platform_name=d.printer_platform_name and a.market10=d.market10
                       LEFT JOIN usageSummary2R5 e
                        ON a.printer_platform_name=e.printer_platform_name and a.printer_region_code=e.printer_region_code 
                       LEFT JOIN usageSummary2TE_D3 f
                        ON a.printer_platform_name=f.printer_platform_name
                       ")

# COMMAND ----------

# Step 48 - combining AP results with all rows where iMPVs for NA and EU are populated

usagesummaryNAEUAP <- usageSummary2TE_D3

# COMMAND ----------

# Step 49 - Extracting PoR information for all platforms from DIM_PLATFORM table

  PoR <- sqldf("select distinct a.platform_subset as printer_platform_name 
                ,a.[Final Product Family Name] as supplies_family
                ,a.biz
                ,a.segment
                ,b.intro_date as introyr
                FROM zero_init2 a
                LEFT JOIN ibintrodt b
                  ON upper(a.platform_subset)=upper(b.platform_subset)

        ")

PoR_platformLIST <- sqldf('select distinct printer_platform_name, "product_ref" as Source from PoR')

# COMMAND ----------

# Step - 50 -- extracting platform specific general IntroDate and create Old-Future type

old <- sqldf(paste("select *,
             CASE WHEN ",end1,"-introyr > 0 THEN 'OLD'
             --ELSE 'FUTURE'
             ELSE 'OLD'
             END AS platform_type
             from PoR
             ",sep = " ", collapse = NULL))

  head(old)
  colnames(old)
  dim(old)
  str(old)
  
  # "dropList" is the list of platforms which are recorded both as Old and Future
  
  dropList <- old[old$platform_type=="FUTURE",]$printer_platform_name[old[old$platform_type=="FUTURE",]$printer_platform_name %in% (usagesummaryNAEUAP$printer_platform_name)]; dropList


# COMMAND ----------

# Step - 51 attching introdate, platform type with PoR table

  PoR2 <- old
  
  #PoR2$J90Mo <- (as.numeric(substr(PoR2$Intro_FYearMo, 1,4)) - 1990)*12 + (as.numeric(substr(PoR2$Intro_FYearMo, 5,6))-1)
  str(PoR2)

# COMMAND ----------

# Step 51A extracting platform-dim

  PoR2B <- old
  str(PoR2B)

# COMMAND ----------

# Step - 52 Creating raw_iMPV for all old Platforms using model information

regnpct <- sqldf("select platform_subset
                      ,sum(CASE WHEN market10='Central Europe' THEN ib ELSE 0 END) as nCE
                      ,sum(CASE WHEN market10='Greater Asia' THEN ib ELSE 0 END) as nGA
                      ,sum(CASE WHEN market10='Greater China' THEN ib ELSE 0 END) as nGC
                      ,sum(CASE WHEN market10='India SL & BL' THEN ib ELSE 0 END) as nIN
                      ,sum(CASE WHEN market10='ISE' THEN ib ELSE 0 END) as nIS
                      ,sum(CASE WHEN market10='Latin America' THEN ib ELSE 0 END) as nLA
                      ,sum(CASE WHEN market10='North America' THEN ib ELSE 0 END) as nNA
                      ,sum(CASE WHEN market10='Northern Europe' THEN ib ELSE 0 END) as nNE
                      ,sum(CASE WHEN market10='Southern Europe' THEN ib ELSE 0 END) as nSE
                      ,sum(CASE WHEN market10='UK&I' THEN ib ELSE 0 END) as nUK
                      ,sum(ib) as nToT
                    from ibtable
                    group by platform_subset 
                 
                 ")
regnpct$pctCE<- regnpct$nCE/regnpct$nToT
regnpct$pctGA<- regnpct$nGA/regnpct$nToT
regnpct$pctGC<- regnpct$nGC/regnpct$nToT
regnpct$pctIN<- regnpct$nIN/regnpct$nToT
regnpct$pctIS<- regnpct$nIS/regnpct$nToT
regnpct$pctLA<- regnpct$nLA/regnpct$nToT
regnpct$pctNA<- regnpct$nNA/regnpct$nToT
regnpct$pctNE<- regnpct$nNE/regnpct$nToT
regnpct$pctSE<- regnpct$nSE/regnpct$nToT
regnpct$pctUK<- regnpct$nUK/regnpct$nToT


PoR2model <- sqldf("select distinct a.*
                 ,b.Intro_FYearMo as fymchk
                 ,b.developed_emerging
                 ,c.pctCE as RCCE
                 ,c.pctGA as RCGA
                 ,c.pctGC as RCGC
                 ,c.pctIN as RCIN
                 ,c.pctIS as RCIS
                 ,c.pctLA as RCLA
                 ,c.pctNA as RCNA
                 ,c.pctNE as RCNE
                 ,c.pctSE as RCSE
                 ,c.pctUK as RCUK
                 
                 from PoR2 a
                 left join introYear3 b
                  on a.printer_platform_name=b.printer_platform_name
                 left join regnpct c
                  on a.printer_platform_name=c.platform_subset
                 --where a.printer_platform_name in (select printer_platform_name from usage)
                 ")

#ModelVal$MoSI <- round((as.Date(paste0(ModelVal$FYearMo,'01'),format="%Y%m%d")-as.Date(paste0(ModelVal$Intro_FYearMo,'01'),format="%Y%m%d"))/(365.25/12))
#ModelVal$M100q <- round((as.Date(paste0(ModelVal$FYearMo,'01'),format="%Y%m%d")-as.Date('1990-01-01',format="%Y-%m-%d"))/(365.25/12))
PoR2model$MoSI <- 30

PoR2model$M100q <- round(as.Date(paste0(PoR2model$introy,'01'),format="%Y%m%d")-as.Date('1990-01-01',format="%Y-%m-%d"))/(365.25/12)+30


PoR2model$segGPA <- ifelse(PoR2model$Segment=='GPA 60+'|PoR2model$Segment=='GPA 60-',1,0)
PoR2model$segGPW <- ifelse(PoR2model$Segment=='GPW',1,0)
PoR2model$segAq <- ifelse(PoR2model$Segment=='Aqueous',1,0)
PoR2model$segLFB <- ifelse(PoR2model$Segment=='Latex FB',1,0)
PoR2model$segLFH <- ifelse(PoR2model$Segment=='Latex Hybrid',1,0)
PoR2model$segLFLV <- ifelse(PoR2model$Segment=='Latex LV',1,0)
PoR2model$segLFMV <- ifelse(PoR2model$Segment=='Latex MV',1,0)
PoR2model$segTDPC <- ifelse(PoR2model$Segment=='TDP Core',1,0)
PoR2model$segTDPP <- ifelse(PoR2model$Segment=='TDP Premium',1,0)
PoR2model$segTLV <- ifelse(PoR2model$Segment=='Textiles LV',1,0)
PoR2model$segTMV <- ifelse(PoR2model$Segment=='Textiles MV',1,0)
PoR2model$segT24 <- ifelse(PoR2model$Segment=='TPW 24',1,0)
PoR2model$segT36 <- ifelse(PoR2model$Segment=='TPW 36+',1,0)

#PoRmodel$DE   <- ifelse(PoRmodel$developed_emerging=='D',1,0)
# PoR2model$PPM  <- as.numeric(ifelse(PoR2model$print_color_speed_pages==0,PoR2model$print_mono_speed_pages,(PoR2model$print_color_speed_pages+PoR2model$print_mono_speed_pages)/2))
# PoR2model$lavPPM <- as.numeric(log(PoR2model$PPM))
# PoR2model$FIntroYrn <- as.numeric(year(as.Date(paste0(PoR2model$Intro_FYearMo,'01'),format="%Y%m%d")))
# PoR2model$Qtr <- quarter(as.Date(paste0(PoR2model$Intro_FYearMo,'01'),format="%Y%m%d"))
# PoR2model$Qtr1 <- ifelse(PoR2model$Qtr==1,1,0)
# PoR2model$Qtr2 <- ifelse(PoR2model$Qtr==2,1,0)
# PoR2model$Qtr3 <- ifelse(PoR2model$Qtr==3,1,0)
# PoR2model$Qtr4 <- ifelse(PoR2model$Qtr==4,1,0)
# PoR2model$MktH <- ifelse(PoR2model$platform_finance_market_category_code=="WGP",1,0)
# PoR2model$CM   <- ifelse(grepl(pattern='Color',x=PoR2model$copier_segment),1,0)
# PoR2model$Seg1 <- ifelse(grepl(pattern='1',x=PoR2model$copier_segment),1,0)
# PoR2model$Seg2 <- ifelse(grepl(pattern='2',x=PoR2model$copier_segment),1,0)
# PoR2model$Seg3 <- ifelse(grepl(pattern='3',x=PoR2model$copier_segment),1,0)
# PoR2model$Seg4 <- ifelse(grepl(pattern='4',x=PoR2model$copier_segment),1,0)
# PoR2model$A4   <- ifelse(PoR2model$platform_page_category=="A3",0,1)
PoR2model$MoSI2 <- as.numeric(PoR2model$MoSI)
PoR2model$M100q2 <- as.numeric(PoR2model$M100q)

#####NEED TO PULL IN NEW MODELS#######  
PoR2model <- sqldf("select *
  ,(3.78767300 +0.03143493*RCCE + 0.39584367*RCGA + 0.75536537*RCGC + 0.52490714*RCIN + 0.53585136*RCIS +0.46014577*RCLA +0.0952870*RCNE -0.05674692*RCSE +0.21747250*RCUK
    +2.77690504*segGPA +0.97084863*segGPW +4.68947156*segLFB +5.65061650*segLFH +4.79638968*segLFLV +6.43142587*segLFMV +1.20695413*segTDPC +3.37085596*segTDPP +3.86502239*segTLV
    +5.16899544*segTMV -1.50515648*segT24
    +0.00055904*MoSI2  -0.01183001*M100q2) as model
     from PoR2model

     ", method="name__class")
   
   PoR2model$rawMPV <- exp(as.numeric(PoR2model$model))
   #PoR2model$rawMPV <- ifelse(PoR2model$rawMPV<1,1,PoR2model$rawMPV)
   str(PoR2model)
  
  #write.csv(paste(mainDir,subDir1,"/","PoR2model_iMPVmodeled_OLD_platforms",".csv", sep=''), x=PoR2model,row.names=FALSE, na="")

# COMMAND ----------

# Step - 53 attaching calculated_iMPV with raw_iMPV data

  PoR2model_iMPV <- sqldf('select aa1.*, aa2.NA2, aa2.EU2, aa2.AP2, aa2.LA2, North_America2, UKI2, Northern_Europe2, Southern_Europe2, ISE2, Central_Europe2, India2, Greater_Asia2, Greater_China2,Latin_America2, North_America_D2, UKI_D2, Northern_Europe_D2, Southern_Europe_D2, ISE_E2, Central_Europe_D2, Central_Europe_E2, India_E2, Greater_Asia_D2, Greater_Asia_E2,
Greater_China_D2, Greater_China_E2, Latin_America_E2
                          from PoR2model aa1 
                          left outer join 
                          usagesummaryNAEUAP aa2 
                          on
                          aa1.printer_platform_name = aa2.printer_platform_name
                          where PLATFORM_TYPE = "OLD"
                          ')

# COMMAND ----------

# Step - 54 Calculating the ratio of calculated_iMPV and raw_iMPV for NA 

  PoR2model_iMPV$ratio1 <- ifelse(is.na(PoR2model_iMPV$North_America2),1,PoR2model_iMPV$North_America2/PoR2model_iMPV$rawMPV)
  PoR2model_iMPV$ratio2 <- ifelse(is.na(PoR2model_iMPV$NA2),1,PoR2model_iMPV$NA2/PoR2model_iMPV$rawMPV)
  PoR2model_iMPV$ratio3 <- ifelse(is.na(PoR2model_iMPV$North_America_D2),1,PoR2model_iMPV$North_America_D2/PoR2model_iMPV$rawMPV)

# COMMAND ----------

# Step - 55 Calculating the average of iMPV ratio specific to groups defined by SM, CM

  avgRatio1 <- sqldf('select segment, biz, avg(ratio1) as avgratio1 from PoR2model_iMPV group by segment, biz')
  avgRatio2 <- sqldf('select segment, biz, avg(ratio2) as avgratio2 from PoR2model_iMPV group by segment, biz')
  avgRatio3 <- sqldf('select segment, biz, avg(ratio3) as avgratio3 from PoR2model_iMPV group by segment, biz')

# COMMAND ----------

# Step - 56 attaching the average iMPV ratio

PoR2model_iMPV2 <- sqldf(' select aa1.*, aa2.avgratio1, aa3.avgratio2, aa4.avgratio3
                           from 
                           PoR2model_iMPV aa1
                           inner join 
                           avgRatio1 aa2
                           on 
                           
                           aa1.segment= aa2.segment
                           and
                           aa1.biz = aa2.biz
                           inner join 
                           avgRatio2 aa3
                           on 
                           
                           aa1.segment= aa3.segment
                           and
                           aa1.biz = aa3.biz
                           inner join 
                           avgRatio3 aa4
                           on 
                           
                           aa1.segment= aa4.segment
                           and
                           aa1.biz = aa4.biz
                           order by segment,biz
                           ')
  
  PoR2model_iMPV3 <- sqldf('select *
                           , case
                           when NA2 is not null then NA2
                           else rawMPV*avgratio2 
                           end as NA3
                            , case
                           when North_America2 is not null then North_America2
                           else rawMPV*avgratio2 
                           end as North_America3
                           from
                           PoR2model_iMPV2
                           
                           ')

# COMMAND ----------

# ---- Step - 58 attaching regional coefficients -------------------------------------------#

  PoR2model_iMPV4 <- sqldf('select aa1.*, 1 as NAa, aa2.EUa, aa2.APa, aa2.LAa, 1 as North_Americana, aa2.UKIna, aa2.Northern_Europena, aa2.Southern_Europena, aa2.ISEna, aa2.Central_Europena, aa2.Indiana, aa2.Greater_Asiana, aa2.Greater_Chinana,aa2.Latin_Americana, aa2.Greater_Asia_Dna, aa2.Greater_Asia_Ena, aa2.Greater_China_Dna, aa2.Greater_China_Ena, aa2.Central_Europe_Dna, aa2.Central_Europe_Ena
                           from PoR2model_iMPV3 aa1
                           inner join 
                           wtaverage aa2
                           on 
                           aa1.biz =aa2.biz
                           and 
                           
                           aa1.segment=aa2.segment

                           ')

# COMMAND ----------

# Step - 59 populating the null iMPV of the remaining old platforms by multiplying
#         iMPV NA with the respective regional coefficients

PoR2model_iMPV5 <- sqldf('select *
                           , case when EU2 is null then NA3*EUa
                            else EU2
                            end as EU3
                           , case when AP2 is null then NA3*APa
                            else AP2
                            end as AP3
                           , case when LA2 is null then NA3*LAa
                            else LA2
                            end as LA3
                           , case when UKI2 is null then North_America3*UKIna
                            else UKI2
                            end as UKI3
                           , case when Northern_Europe2 is null then North_America3*Northern_Europena
                            else Northern_Europe2
                            end as Northern_Europe3
                           , case when Southern_Europe2 is null then North_America3*Southern_Europena
                            else Southern_Europe2
                            end as Southern_Europe3
                           , case when ISE2 is null then North_America3*ISEna
                            else ISE2
                            end as ISE3
                           , case when Central_Europe2 is null then North_America3*Central_Europena
                            else Central_Europe2
                            end as Central_Europe3
                            , case when Central_Europe2 is null then North_America3*Central_Europe_Dna
                            else Central_Europe2
                            end as Central_Europe_D3
                            , case when Central_Europe2 is null then North_America3*Central_Europe_Ena
                            else Central_Europe2
                            end as Central_Europe_E3
                           , case when India2 is null then North_America3*Indiana
                            else India2
                            end as India3
                           , case when Greater_Asia2 is null then North_America3*Greater_Asiana
                            else Greater_Asia2
                            end as Greater_Asia3
                            , case when Greater_Asia2 is null then North_America3*Greater_Asia_Dna
                            else Greater_Asia2
                            end as Greater_Asia_D3
                            , case when Greater_Asia2 is null then North_America3*Greater_Asia_Ena
                            else Greater_Asia2
                            end as Greater_Asia_E3
                           , case when Greater_China2 is null then North_America3*Greater_Chinana
                            else Greater_China2
                            end as Greater_China3
                            , case when Greater_China2 is null then North_America3*Greater_China_Dna
                            else Greater_China2
                            end as Greater_China_D3
                            , case when Greater_China2 is null then North_America3*Greater_China_Ena
                            else Greater_China2
                            end as Greater_China_E3
                           , case when Latin_America2 is null then North_America3*Latin_Americana
                            else Latin_America2
                            end as Latin_America3
                           from PoR2model_iMPV4
                           
                           ')

#write.csv('C:/UPM/iMPV_OLD_complete.csv', x=PoR2model_iMPV5,row.names=FALSE, na="")

# COMMAND ----------

# Step 60 - Populating the route matrix for iMPV creation based on modeled values

route5 <- sqldf("select distinct segment, biz, printer_platform_name
                  , case 
                  when NA2 is not null then 'populated'
                  when NA3 is null then null
                  else 'Modeled'
                  end as NAs
                 -- , case 
                 -- when North_America2 is not null then 'populated'
                 -- when North_America3 is null then null
                 -- else 'Modeled'
                 -- end as North_Americas
                  --, case 
                  --when EU2 is not null then 'populated'
                 -- when EU3 is null then null
                  --else 'Modeled'
                  --end as EUs  
                  , case 
                  when UKI2 is not null then 'populated'
                  when UKI3 is null then null
                  else 'Modeled'
                  end as UKs
                  , case
                  when Northern_Europe2 is not null then 'populated'
                  when Northern_Europe3 is null then null
                  else 'Modeled'
                  end as NEs
                  , case
                  when Southern_Europe2 is not null then 'populated'
                  when Southern_Europe3 is null then null
                  else 'Modeled'
                  end as SEs
                  --, case
                  --when Central_Europe2 is not null then 'populated'
                  --when Central_Europe3 is null then null
                  --else 'Modeled'
                  --end as Central_Europes
                  , case
                  when Central_Europe2 is not null then 'populated'
                  when Central_Europe_D3 is null then null
                  else 'Modeled'
                  end as CEDs
                  , case
                  when Central_Europe2 is not null then 'populated'
                  when Central_Europe_E3 is null then null
                  else 'Modeled'
                  end as CEEs
                  , case
                  when ISE2 is not null then 'populated'
                  when ISE3 is null then null
                  else 'Modeled'
                  end as ISs
                  --, case 
                  --when AP2 is not null then 'populated'
                  --when AP3 is null then null
                  --else 'Modeled'
                  --end as APs     
                  , case when India2 is not null then 'populated'
                  when India3 is null then null
                  else 'Modeled'
                  end as INs
                  --, case 
                  --when Greater_Asia2 is not null then 'populated'
                  --when Greater_Asia3 is null then null
                  --else 'Modeled'
                  --end as Greater_Asias
                  , case 
                  when Greater_Asia2 is not null then 'populated'
                  when Greater_Asia_D3 is null then null
                  else 'Modeled'
                  end as GADs
                  , case 
                  when Greater_Asia2 is not null then 'populated'
                  when Greater_Asia_E3 is null then null
                  else 'Modeled'
                  end as GAEs
                  --, case
                  --when Greater_China2 is not null then 'populated'
                  --when Greater_China3 is null then null
                  --else 'Modeled'
                  --end as Greater_Chinas
                  , case
                  when Greater_China2 is not null then 'populated'
                  when Greater_China_D3 is null then null
                  else 'Modeled'
                  end as GCDs
                  , case
                  when Greater_China2 is not null then 'populated'
                  when Greater_China_E3 is null then null
                  else 'Modeled'
                  end as GCEs
                  , case 
                  when LA2 is not null then 'populated'
                  when LA3 is null then null
                  else 'Modeled'
                  end as LAs 
                  --, case
                  --when Latin_America2 is not null then 'populated'
                  --when Latin_America3 is null then null
                  --else 'Modeled'
                  --end as Latin_Americas
 
                  
                  from PoR2model_iMPV5
                  ")
  

  route5B <- reshape2::melt(route5 , id.vars = c("Segment","Biz","printer_platform_name"),variable.name = "printer_region_code", value.name = "Route")
  
  route5B <- route5B[which((route5B$Route=="Modeled")|is.na(route5B$Route)),]
  route5B$printer_platform_name <- factor(route5B$printer_platform_name)

# COMMAND ----------

# Step 68 - Populating the route matrix of iMPV creations for Futre products

route1 <- usagesummaryNAEUAP[c(1:3, 509, 512, 514, 516, 518, 520, 521, 524, 526, 527 , 529, 530, 533)]
  route1B <- reshape2::melt(route1 , id.vars = c("biz","segment","printer_platform_name"),variable.name = "printer_region_code", value.name = "Route")
  route1B <- sqldf("SELECT distinct biz as Biz, segment as Segment, printer_platform_name
                  ,CASE WHEN printer_region_code='Central_Europe_DRoute' THEN 'CEDs'
                       WHEN printer_region_code='Central_Europe_ERoute' THEN 'CEEs'
                       WHEN printer_region_code='Greater_Asia_DRoute' THEN 'GADs'
                       WHEN printer_region_code='Greater_Asia_ERoute' THEN 'GAEs'
                       WHEN printer_region_code='Greater_China_DRoute' THEN 'GCDs'
                       WHEN printer_region_code='Greater_China_ERoute' THEN 'GCEs'
                       WHEN printer_region_code='India_ERoute' THEN 'INs'
                       WHEN printer_region_code='ISE_ERoute' THEN 'ISs'
                       WHEN printer_region_code='Latin_America_ERoute' THEN 'LAs'
                       WHEN printer_region_code='North_America_DRoute' THEN 'NAs'
                       WHEN printer_region_code='Northern_Europe_DRoute' THEN 'NEs'
                       WHEN printer_region_code='Southern_Europe_DRoute' THEN 'SEs'
                       WHEN printer_region_code='UKI_DRoute' THEN 'UKs'
                       END AS printer_region_code
                       ,Route
                       FROM route1B
                   ")
  route <- rbind(route1B, route5B)

  routeT <- reshape2::dcast(route, Biz + Segment+printer_platform_name ~printer_region_code, value.var="Route")

# rm(route1B)
# rm(route5B)
# rm(route6B)

# COMMAND ----------

# Step - 69 - combining old and future iMPV matrix to create complete iMPV matrix

 normdataOLD <- sqldf('select biz ,  segment
                       , printer_platform_name
                       , North_America3 AS NA
                       , Northern_Europe3 AS NE
                       , Southern_Europe3 AS SE
                       , Central_Europe_D3 AS CED
                       , Central_Europe_E3 AS CEE
                       , ISE3 AS ISE
                       , UKI3 AS UK
                       , India3 AS INE
                       , Greater_Asia_D3 AS GAD
                       , Greater_Asia_E3 AS GAE
                       , Greater_China_D3 AS GCD
                       , Greater_China_E3 AS GCE
                       , LA3 AS LA
                       FROM PoR2model_iMPV5
                       where North_America3 is not null
                       ')

 # normdataOLD2 <-  normdataOLD %>% 
 # group_by(Biz) %>%
 # summarise(across(everything(), mean))

normdataOLD2 <- sqldf("
        SELECT Biz
          , AVG(NA) AS NA
          , AVG(NE) AS NE
          , AVG(SE) AS SE
          , AVG(CED) AS CED
          , AVG(CEE) AS CEE
          , AVG(ISE) AS ISE
          , AVG(UK) AS UK
          , AVG(INE) AS INE
          , AVG(GAD) AS GAD
          , AVG(GAE) AS GAE
          , AVG(GCD) AS GCD
          , AVG(GCE) AS GCE
          , AVG(LA) AS LA
          FROM normdataOLD
          GROUP BY Biz
")


  normdataOLD2$Segment <- ifelse(normdataOLD2$Biz=='Pro','Echo',ifelse(normdataOLD2$Biz=='Design','NULL',NA))
   
# Create mising list a, b, c, d variables

    printer_platform_name  <- c('BEAM 36 CISS','BEAM 36 MFP','BEAM 36 PRO MFP','BEAM 36 PRO SF','BEAM 36 SF','BIG NARA','JUPITER 2R','KYOTO','NARA')
    Segment <- c('NULL','NULL','NULL','NULL','NULL','Echo','NULL','Echo','Echo')
    Biz <- c('Design','Design','Design','Design','Design','Pro','Design','Pro','Pro')
# Join the variables to create a data frame
  ndoldlst <- data.frame(printer_platform_name,Segment,Biz)
  
  normdataOLD2 <- sqldf("SELECT a.Biz,a.Segment,b.printer_platform_name,a.NA,a.NE,a.SE,a.CED,a.CEE,a.ISE,a.UK,a.INE,a.GAD,a.GAE,a.GCD,a.GCE,a.LA FROM normdataOLD2 a left join ndoldlst b on a.biz=b.biz and a.segment=b.segment")
  
  normdata <- dplyr::bind_rows(normdataOLD,normdataOLD2)

# COMMAND ----------

# Step - 70 Normalize complete iMPV matrix with respect to VV, CM, SM and Plat_Nm

  normdata2 <- reshape2::melt(normdata, id.vars = c("Biz","Segment", "printer_platform_name"),
                    variable.name = "printer_region_code", 
                    value.name = "iMPV")

# COMMAND ----------

# Step - 71 Extracting region specific introDate for each of the platforms

normdatadate <- sqldf('select distinct 
                        platform_subset as printer_platform_name
                        , INTRODATE_NA AS NA, INTRODATE_NE AS NE, INTRODATE_SE AS SE, INTRODATE_CED AS CED, INTRODATE_CEE AS CEE, INTRODATE_UK AS UK, INTRODATE_IS AS [IS], INTRODATE_IN AS [IN]
                        , INTRODATE_GAD AS GAD, INTRODATE_GAE AS GAE, INTRODATE_GCD AS GCD, INTRODATE_GCE AS GCE, INTRODATE_LA AS LA
                        FROM introdatetbl2
                        ')

# COMMAND ----------

# Step - 72 creating detailed iMPV matrix for all platforms

combined <- sqldf('select aa2.Biz
                    , aa2.Segment
                    , aa2.printer_platform_name
                    , aa2.NAs as NA_route
                    , aa2.CEDs as CED_route
                    , aa2.CEEs as CEE_route
                    , aa2.GADs as GAD_route
                    , aa2.GAEs as GAE_route
                    , aa2.GCDs as GCD_route
                    , aa2.GCEs as GCE_route
                    , aa2.INs as IN_route
                    , aa2.ISs as IS_route
                    , aa2.LAs as LA_route
                    , aa2.NEs as NE_route
                    , aa2.SEs as SE_route
                    , aa2.UKs as UK_route
                    , aa3.INTRODATE_NA
                    , aa3.INTRODATE_CED
                    , aa3.INTRODATE_CEE
                    , aa3.INTRODATE_GAD
                    , aa3.INTRODATE_GAE
                    , aa3.INTRODATE_GCD
                    , aa3.INTRODATE_GCE
                    , aa3.INTRODATE_IN
                    , aa3.INTRODATE_IS
                    , aa3.INTRODATE_LA
                    , aa3.INTRODATE_NE
                    , aa3.INTRODATE_SE
                    , aa3.INTRODATE_UK
                    from
                    routeT aa2
                    inner join
                    old aa1
                    on aa1.Biz =aa2.Biz
                    and aa1.Segment=aa2.Segment
                    and aa1.printer_platform_name = aa2.printer_platform_name
                    LEFT JOIN introdatetbl2 aa3
                    ON aa1.printer_platform_name = aa3.platform_subset
                    ')
  
  combined2 <- sqldf('select aa2.*
                     , aa1.NA as NA_iMPV
                     , aa1.NE as NE_iMPV
                     , aa1.SE as SE_iMPV
                     , aa1.CED as CED_iMPV
                     , aa1.CEE as CEE_iMPV
                     , aa1.ISE as ISE_iMPV
                     , aa1.UK as UK_iMPV
                     , aa1.INE as IN_iMPV
                     , aa1.GAD as GAD_iMPV
                     , aa1.GAE as GAE_iMPV
                     , aa1.GCD as GCD_iMPV
                     , aa1.GCE as GCE_iMPV
                     , aa1.LA as LA_iMPV
                     from
                     combined aa2
                     inner join
                     normdata aa1
                     on aa1.biz =aa2.biz
                      and aa1.segment=aa2.segment
                      and aa1.printer_platform_name = aa2.printer_platform_name
                     ')

# COMMAND ----------

# Step - 73 Normalize introDate data with respect to VV, CM, SM and Plat_Nm
 
  normdatadate2 <- reshape2::melt(normdatadate, id.vars = c("printer_platform_name"),
                        variable.name = "printer_region_code", 
                        value.name = "introdate")

# COMMAND ----------

# Step - 74 Combine Normalized introDate and iMPV data for each Platform
###HERE###  

sourceRiMPV$mde <- ifelse(sourceRiMPV$market10=='Central Europe',paste0('CE',substr(sourceRiMPV$developed_emerging,1,1)),
                     ifelse(sourceRiMPV$market10=='Greater Asia',paste0('GA',substr(sourceRiMPV$developed_emerging,1,1)), 
                     ifelse(sourceRiMPV$market10=='Greater China',paste0('GC',substr(sourceRiMPV$developed_emerging,1,1)),
                     ifelse(sourceRiMPV$market10=='North America','NA',
                     ifelse(sourceRiMPV$market10=='Northern Europe','NE',
                     ifelse(sourceRiMPV$market10=='Southern Europe','SE',
                     ifelse(sourceRiMPV$market10=='UK&I','UK',
                     ifelse(sourceRiMPV$market10=='ISE','IS',
                     ifelse(sourceRiMPV$market10=='India SL & BL','IN',
                     ifelse(sourceRiMPV$market10=='Latin America','LA',
                     NA
                        ))))))))))
  normdataFinal0 <- sqldf("
                     select distinct src.printer_platform_name
                     , src.printer_region_code
                     , SUBSTR(src.developed_emerging,1,1) as developed_emerging
                     , src.market10
                     , src.country_alpha2
                     , src.Route
                     , por.biz
                     , por.segment
                     , CASE WHEN src.Route='country' THEN ctry.iMPV 
                            WHEN src.Route='dev/em' THEN de.iMPV
                            WHEN src.Route='market10' THEN mkt.iMPV
                            WHEN src.Route='region5' THEN reg.iMPV
                            WHEN src.Route='Self' THEN mkt.iMPV
                            WHEN src.mde='LA' AND por.Latin_America3 IS NOT NULL THEN por.Latin_America3
                            WHEN src.mde='CEE' AND por.Central_Europe_E3 IS NOT NULL THEN por.Central_Europe_E3
                            WHEN src.mde='CED' AND por.Central_Europe_D3 IS NOT NULL THEN por.Central_Europe_D3
                            WHEN src.mde='GAD' AND por.Greater_Asia_D3 IS NOT NULL THEN por.Greater_Asia_D3
                            WHEN src.mde='GAE' AND por.Greater_Asia_E3 IS NOT NULL THEN por.Greater_Asia_E3
                            WHEN src.mde='GCD' AND por.Greater_China_D3 IS NOT NULL THEN por.Greater_China_D3
                            WHEN src.mde='GCE' AND por.Greater_China_E3 IS NOT NULL THEN por.Greater_China_E3
                            WHEN src.mde='IN' AND por.India3 IS NOT NULL THEN por.India3
                            WHEN src.mde='IS' AND por.ISE3 IS NOT NULL THEN por.ISE3
                            WHEN src.mde='NA' AND por.North_America3 IS NOT NULL THEN por.North_America3
                            WHEN src.mde='NE' AND por.Northern_Europe3 IS NOT NULL THEN Northern_Europe3
                            WHEN src.mde='SE' AND por.Southern_Europe3 IS NOT NULL THEN Southern_Europe3
                            WHEN src.mde='UK' AND por.UKI3 IS NOT NULL THEN por.UKI3
                            --WHEN src.Route IS NULL THEN ?
                            ELSE null
                      
                        END AS iMPV
                      , dt.introdate as introdate0
                     
                     FROM sourceRiMPV src
                     LEFT JOIN usageSummary2 ctry
                      ON src.printer_platform_name=ctry.printer_platform_name
                      AND src.country_alpha2=ctry.country_alpha2
                     LEFT JOIN usageSummary2DE de
                      ON src.printer_platform_name=de.printer_platform_name 
                       AND src.market10=de.market10 AND src.developed_emerging=de.developed_emerging
                     LEFT JOIN usageSummary2Mkt mkt
                      ON src.printer_platform_name=mkt.printer_platform_name 
                       AND src.market10=mkt.market10
                     LEFT JOIN usageSummary2R5 reg
                      ON src.printer_platform_name=reg.printer_platform_name 
                       AND src.printer_region_code=reg.printer_region_code
                     LEFT JOIN PoR2model_iMPV5 por
                      ON src.printer_platform_name=por.printer_platform_name
                     LEFT JOIN normdatadate2 dt
                      ON src.printer_platform_name=dt.printer_platform_name AND src.mde=dt.printer_region_code
                     ")
  
  normdataFinal <- sqldf('select distinct aa1.*, aa2.minYear as introdate from normdataFinal0 aa1
                         inner join
                         introYear2a aa2
                         on
                         aa1.printer_platform_name = aa2.printer_platform_name')
  #normdataFinal$ep <- ifelse(normdataFinal$ep=="E","ENT","PRO")
  #cols <-c( 'segment', 'biz', 'geography' )
  #cols2 <- c( 'copier_segment', 'PL','platform_finance_market_category_code', 'Route','developed_emerging' )
  #cols <- c( 'copier_segment','PL','platform_finance_market_category_code', 'printer_region_code' )
  #cols2 <- c( 'copier_segment', 'PL','platform_finance_market_category_code', 'country_alpha2' )
  #cols <- c('copier_segment', 'PL','platform_finance_market_category_code', 'market10','developed_emerging')
  #normdataFinal$strata2 <- apply( normdataFinal[ , cols2 ] , 1 , paste , collapse = "_" )
  normdataFinal$strata1 <- apply( normdataFinal[ , cols ] , 1 , paste , collapse = "_" )

# COMMAND ----------

# Step 75 - Creating two artifical columns to capture year and month since Jan 1990

s1 <- as.data.frame(seq(max(1990,firstAvaiableYear), lastAvaiableYear, 1))
s2 <- as.data.frame(seq(1, 12, 1))
s3 <- merge(s1,s2,all=TRUE)
# names(s3)[names(s3)=="seq(1990, 2020, 1)"] <- "year"
# names(s3)[names(s3)=="seq(1, 12, 1)"] <- "month"
names(s3)[1] <- "year"
names(s3)[2] <- "month"
names(s2)[1] <- "month"
tempdir(check=TRUE)

#   rm(PoR, PoR_1, zeroi, usage2, usage3, usage4, usage5, zero, two, three, four, outcome0, sourceR, sourceRiMPV, introYear3, introYear, iblist )
gc()

# COMMAND ----------

  s1 <- as.data.frame(seq(1990, lastAvaiableYear, 1))
  s2 <- as.data.frame(seq(1, 12, 1))
  s3 <- merge(s1,s2,all=TRUE)
  # names(s3)[names(s3)=="seq(1990, 2020, 1)"] <- "year"
  # names(s3)[names(s3)=="seq(1, 12, 1)"] <- "month"
  names(s3)[1] <- "year"
  names(s3)[2] <- "month"

# COMMAND ----------

outcome_spark <- as.DataFrame(outcome)
normdataFinal_spark <- as.DataFrame(normdataFinal)
s3_spark <- as.DataFrame(s3)
s2_spark <- as.DataFrame(s2)

# COMMAND ----------

# Step 76 - SparkR

outcome_filtered <- outcome_spark

normdataFinal_filtered <- SparkR::filter(normdataFinal_spark, !isNull(normdataFinal_spark$introdate) & !isNull(normdataFinal_spark$iMPV))

# for loop 0 replacement
d2 <- crossJoin(s3_spark, normdataFinal_filtered)

d2$yyyy <- cast(substr(d2$introdate, 1,4), "double")
d2$mm <- cast(substr(d2$introdate, 5,6), "double")

d2$year <- cast(d2$year, "integer")
d2$month <- cast(d2$month, "integer")
d2$yyyymm <- ifelse(d2$month<10, concat_ws(sep="0", d2$year, d2$month), concat(d2$year, d2$month))
d2 <- filter(d2, d2$yyyymm >= d2$introdate)

createOrReplaceTempView(d2, "d2")

# dseason
d <- outcome_filtered
createOrReplaceTempView(d, "d")

dsea1 <- crossJoin(SparkR::sql("SELECT DISTINCT strata, b1 FROM d"), s3_spark)
createOrReplaceTempView(dsea1, "dsea1")

dsea <- SparkR::sql("select distinct m.strata, m.b1, m.month, d.seasonality
              from dsea1 m
              left join d
                on m.month=d.month
                  and m.strata=d.strata
                  and m.b1=d.b1
              --group by m.strata, m.month, d.seasonality 
              ")

createOrReplaceTempView(dsea, "dsea")

dseason <- SparkR::sql('select distinct strata, b1, month, seasonality
                     from dsea')

createOrReplaceTempView(dseason, "dseason")

d3 <- SparkR::sql('select aa1.*, aa2.b1, CASE WHEN aa2.seasonality is null then 0 ELSE aa2.seasonality END as seasonality
            from d2 aa1
            inner join 
            dseason aa2
            on
            aa1.strata1 = aa2.strata
            and 
            aa1.month = aa2.month
            order by 
            year, month')

createOrReplaceTempView(d3, "d3")

d4 <- SparkR::sql('select aa1.*, CASE WHEN aa2.mo_Smooth is null then 0 ELSE aa2.mo_Smooth END as Cyclical
            from d3 aa1
            left outer join 
            d aa2
            on
            aa1.strata1 = aa2.strata
            and 
            aa1.month = aa2.month
            order by 
            year, month')

d4$rFYearMo <- (d4$year)*1 + (d4$month-2)/12
d4$MoSI <- (d4$year - d4$yyyy)*12 + (d4$month - d4$mm)

d4$Trend <-exp(log(d4$iMPV) + d4$MoSI*d4$b1/12)
d4$UPM_MPV <- ifelse(isNull(d4$Cyclical), exp(log(d4$Trend) + d4$seasonality), exp(log(d4$Trend) + d4$seasonality + d4$Cyclical))
d4$TS_MPV <-  exp(log(d4$Trend) + d4$seasonality) # without considering the cyclical part
d4$TD_MPV <-  exp(log(d4$Trend)) # Trend Only

d4 <- withColumnRenamed(d4, "b1", "Decay")
d4 <- withColumnRenamed(d4, "seasonality", "Seasonality")

createOrReplaceTempView(d4, "final1")

# COMMAND ----------

# Step 80 - Attaching Segment Specific MPV and N

createOrReplaceTempView(as.DataFrame(usage), "usage")
createOrReplaceTempView(as.DataFrame(outcome2), "outcome2")

  final2 <- SparkR::sql('select distinct aa1.*
                  , aa5.mpva
                  , aa5.na
                  --, aa7.MUT
                  from 
                  final1 aa1
                  left join 
                  usage aa5
                  on 
                  aa1.printer_platform_name = aa5.printer_platform_name
                  and 
                  aa1.country_alpha2 = aa5.country_alpha2
                  and
                  aa1.yyyymm = aa5.FYearMo
                  left outer join 
                  outcome2 aa7
                  on 
                  aa1.market10 = aa7.market10
                  and 
                  aa1.developed_emerging=aa7.developed_emerging
                  and
                  CAST(substr(aa1.yyyymm, 5,2) AS INT)= aa7.month
                  and
                  aa1.segment = aa7.segment
                  and 
                  aa1.biz=aa7.biz
                  ')

createOrReplaceTempView(final2, "final2")

# COMMAND ----------

# Step 83 - Exatracting and Attaching Installed Base infomration

######Not keeping all IB######

caldts <- dbGetQuery(sfai,"SELECT distinct Calendar_Yr_Mo, Fiscal_Year_Qtr FROM IE2_Prod.dbo.calendar WHERE Day_of_Month=1")

createOrReplaceTempView(as.DataFrame(caldts), "caldts")

createOrReplaceTempView(as.DataFrame(ibtable), "ibtable")

final8 <- SparkR::sql('select aa1.*
                , aa2.ib as IB
                , cd.Fiscal_Year_Qtr as FYearQtr
                from final2 aa1
                left outer join 
                ibtable aa2
                on 
                aa1.printer_platform_name = aa2.platform_subset and aa1.country_alpha2 = aa2.country_alpha2 and aa1.yyyymm = aa2.month_begin
                left join caldts cd
                on aa1.yyyymm=cd.Calendar_Yr_Mo
                ')


final8$rFYearQtr <- cast(substr(final8$FYearQtr,1,4), "integer")*1 +  ((cast(substr(final8$FYearQtr,6,6), "integer") - 1)/4) + (1/8)

final8$rFYearMo_Ad <- final8$rFYearMo + (1/24)

createOrReplaceTempView(final8, "final8")

# COMMAND ----------

# Step 84 - Renaming and aligning Variables

final9 <- SparkR::sql('
                select
                  printer_platform_name as Platform_Subset_Nm
                  , CASE 
                      WHEN SUBSTR(printer_region_code,1,2) = "AP" THEN "APJ"
                      WHEN SUBSTR(printer_region_code,1,2) = "JP" THEN "APJ"
                      WHEN SUBSTR(printer_region_code,1,2) = "EU" THEN "EMEA"
                      ELSE "AMS"
                    END AS Region_3
                  , printer_region_code AS Region
                  , developed_emerging AS Region_DE
                  , market10 AS market10
                  , country_alpha2 AS Country_Cd
                  , CAST(NULL AS STRING) AS Country_Nm
                  , yyyymm as FYearMo
                  , rFYearMo_Ad as rFYearMo
                  , FYearQtr
                  , rFYearQtr
                  , year as FYear
                  , month as FMo
                  , MoSI
                  , UPM_MPV as MPV_UPM
                  , TS_MPV AS MPV_TS
                  , TD_MPV as MPV_Trend
                  , MPVA as MPV_Raw
                  , NA as MPV_N
                  , IB
                  , introdate as FIntroDt
                  , biz
                  , segment
                  , iMPV as MPV_Init
                  , Decay
                  , Seasonality
                  --, Cyclical
                  --, MUT
                  , Trend
                  , Route as IMPV_Route
                  from final8  
                  where IB is not null 
                ')

final9$FYearMo <- cast(final9$FYearMo, "string")

createOrReplaceTempView(final9, "final9")

# COMMAND ----------

printSchema(final9)


# COMMAND ----------

# -------Can you create an Access database from server?  
# Step 85 - exporting final10 Table into Access database

start.time2 <- Sys.time()

# odbcDataSources()
# 
# ch2 <- odbcConnect(db1)
# sqlTables(ch2)
# 
# variableTypes = c(Color_Pct="number", K_EYR="number"
#                   , KCYM_EYR="number", POR_ColorPct="number"
#                   ,POR_KEYR="number", POR_KCYMEYR="number")
# 
# 
# sqlSave(ch2, final10, tablename = tb1, append = FALSE,
#         rownames = FALSE, colnames = FALSE, safer = FALSE 
#         # If "safer" = false, allow sqlSave to attempt to delete all the rows of an existing table, or to drop it
#         ,varTypes=variableTypes
# )
# 
# close(ch2)  


#s3write_using(x=final9,FUN = write.csv, object = paste0("s3://insights-environment-sandbox/BrentT/UPM_ctry(",Sys.Date(),").csv"), row.names=FALSE, na="")
# s3write_using(x=final9,FUN = write_parquet, object = paste0("s3://insights-environment-sandbox/BrentT/UPM_ctry(",Sys.Date(),").parquet"))

output_file_name <- paste0("s3://", aws_bucket_name, "LF_UPM_ctry(", todaysDate, ").parquet")

SparkR::write.parquet(x=final9, path=output_file_name, mode="overwrite")

print(output_file_name)

end.time2 <- Sys.time()
time.taken.accesssDB <- end.time2 - start.time2;time.taken.accesssDB

# COMMAND ----------

  # ---- Step 86 - exporting final10 Table into R database ----------------------------------#

  #s3saveRDS(x=final10,object="BrentT/UPM_Data.RDS", bucket="s3://insights-environment-sandbox/")
# Cannot add "subdirectories" in S3, subdirectory name is part of the filename.  

# COMMAND ----------

notebook_end_time <- Sys.time()
notebook_total_time <- notebook_end_time - notebook_start_time;notebook_total_time

# COMMAND ----------


