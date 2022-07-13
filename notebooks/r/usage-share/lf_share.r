# Databricks notebook source
# Databricks notebook source
# ---
# #Version 2021.05.05.1#
# title: "100% IB LF Share (country level)"
# output:
#   html_notebook: default
#   pdf_document: default
# ---

# COMMAND ----------

# MAGIC %run ../../scala/common/Constants

# COMMAND ----------

# MAGIC %run ../../python/common/secrets_manager_utils

# COMMAND ----------

notebook_start_time <- Sys.time()

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", list("dev", "itg", "prd"))
dbutils.widgets.text("upm_date", "")
dbutils.widgets.text("outname_dt","")
dbutils.widgets.text("fcst_nt","LF Q2 QE 2022")


# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # retrieve secrets based on incoming/inputted secrets name - variables will be accessible across languages
# MAGIC  
# MAGIC sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")
# MAGIC spark.conf.set("sfai_username", sqlserver_secrets["username"])
# MAGIC spark.conf.set("sfai_password", sqlserver_secrets["password"])
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

packages <- c("rJava", "RJDBC", "DBI", "sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR", "spatstat","readxl","nls2")

# COMMAND ----------

# MAGIC %run ../common/package_check

# COMMAND ----------

options(java.parameters = "-Xmx40g" )

# setwd("~/work") #Set Work Directory ("~/NPI_Model") ~ for root directory
options(scipen=999) #remove scientific notation
tempdir(check=TRUE)

UPMDate <- dbutils.widgets.get("upm_date")

# COMMAND ----------

aws_bucket_name <- "insights-environment-sandbox/BrentT/"
mount_name <- "insights-environment-sandbox"

sparkR.session(sparkConfig = list(
  aws_bucket_name = aws_bucket_name,
  mount_name = mount_name
))

tryCatch(dbutils.fs.mount(paste0("s3a://", aws_bucket_name), paste0("/mnt/", mount_name)),
 error = function(e)
print("Mount does not exist or is already mounted to cluster"))

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

#Design
#design_in1 <- read.xlsx(file="/dbfs/mnt/insights-environment-sandbox/FIJI AMS.xlsx",sheetName='Design FY91-FY07', header=TRUE, na="", startRow=2, colClasses=rep("character",130),stringsAsFactors=FALSE)
#design_in2 <- read.xlsx(file="/dbfs/mnt/insights-environment-sandbox/FIJI AMS.xlsx",sheetName='Design FY08-FY26', header=TRUE, na="", startRow=2, colClasses=rep("character",130),stringsAsFactors=FALSE)
#design_in3 <- read.xlsx(file="/dbfs/mnt/insights-environment-sandbox/FIJI AMS FY22-FY27 24.03.2022.xlsx",sheetName='LFD - FY22-FY27', header=TRUE, na="", startRow=2, colClasses=rep("character",130),stringsAsFactors=FALSE)
design_in1 <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/FIJI AMS.xlsx",sheet='Design FY91-FY07', col_names=TRUE, na="", skip=1, col_types="text")
design_in2 <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/FIJI AMS.xlsx",sheet='Design FY08-FY26', col_names=TRUE, na="", skip=1, col_types="text")
##design_in3 <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/FIJI AMS FY22-FY27 24.03.2022.xlsx",sheet='LFD - FY22-FY27', col_names=TRUE, na="", skip=1, col_types="text")
design_inEMEA <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/LFD LTP Usage-AMS% V2.xlsx",sheet='EMEA', col_names=TRUE, na="",skip=0, col_types="text", .name_repair= ~make.unique(.x, sep="."))
design_inAMS <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/LFD LTP Usage-AMS% V2.xlsx",sheet='AMS', col_names=TRUE, na="", skip=0, col_types="text", .name_repair= ~make.unique(.x, sep="."))
design_inAPJ <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/LFD LTP Usage-AMS% V2.xlsx",sheet='APJ', col_names=TRUE, na="", skip=0, col_types="text", .name_repair= ~make.unique(.x, sep="."))


# COMMAND ----------

design1 <- pivot_longer(data=design_in1,names_to='date',cols = !starts_with('...'),values_to='pageshare',values_drop_na=TRUE)
design2 <- pivot_longer(data=design_in2,names_to='date',cols = !starts_with('...'),values_to='pageshare',values_drop_na=TRUE)
##design3 <- pivot_longer(data=design_in3,names_to='date',cols = !starts_with('...'),values_to='pageshare',values_drop_na=TRUE)
designEMEAa <- pivot_longer(data=design_inEMEA,names_to='datea',cols = starts_with('Q'),values_to='pageshare',values_drop_na=TRUE)
designAMSa <- pivot_longer(data=design_inAMS,names_to='datea',cols = starts_with('Q'),values_to='pageshare',values_drop_na=TRUE)
designAPJa <- pivot_longer(data=design_inAPJ,names_to='datea',cols = starts_with('Q'),values_to='pageshare',values_drop_na=TRUE)

# COMMAND ----------

designEMEA <- subset(designEMEAa, substr(datea,5,7)=='.1')
designAMS  <- subset(designAMSa, substr(datea,5,7)=='.1')
designAPJ  <- subset(designAPJa, substr(datea,5,7)=='.1')
designEMEA$dateq <- paste0("20",substr(designEMEA$datea,3,4),'Q',substr(designEMEA$datea,2,2))
designAMS$dateq <- paste0("20",substr(designAMS$datea,3,4),'Q',substr(designAMS$datea,2,2))
designAPJ$dateq <- paste0("20",substr(designAPJ$datea,3,4),'Q',substr(designAPJ$datea,2,2))

# COMMAND ----------

calendar <- dbGetQuery(sfai,"select * from ie2_prod.dbo.calendar")

# COMMAND ----------

designEMEA <- sqldf("
              SELECT a.[LTP Category] as LTP_Category,b.Calendar_Yr_Mo as date,a.pageshare, 'EMEA' as region
              FROM designEMEA a
              LEFT JOIN calendar b
                ON a.dateq=b.Fiscal_Year_Qtr
                    ")
designAMS <- sqldf("
              SELECT a.[LTP Category] as LTP_Category,b.Calendar_Yr_Mo as date,a.pageshare, 'AMS' as region
              FROM designAMS a
              LEFT JOIN calendar b
                ON a.dateq=b.Fiscal_Year_Qtr
                    ")
designAPJ <- sqldf("
              SELECT a.[LTP Category] as LTP_Category,b.Calendar_Yr_Mo as date,a.pageshare, 'APJ' as region
              FROM designAPJ a
              LEFT JOIN calendar b
                ON a.dateq=b.Fiscal_Year_Qtr
                    ")

# COMMAND ----------

design_lt14 <- rbind(design1,design2)  
design_lt14$date<- as.character(gsub('X','',as.character(design_lt14$date)))
design_lt14 <- subset(design_lt14,date < "201311")

# COMMAND ----------

#less than 2014Q1 by SKU
colnames(design_lt14) <- c('record','geography','group','sku','date','pageshare')
design_lt14$pageshare <- as.numeric(design_lt14$pageshare)
design_lt14 <- subset(design_lt14,!is.na(pageshare))
design_lt14$biz='design'

#greater than 2014Q1 by LTP Category
design_gt14 <- rbind(designEMEA,designAMS,designAPJ)
design_gt14$pageshare <- as.numeric(design_gt14$pageshare)
design_gt14 <- subset(design_gt14,!is.na(pageshare))
design_gt14$biz='design'

# COMMAND ----------

#pro
#pro_in1 <- read.xlsx(file="/dbfs/mnt/insights-environment-sandbox/FIJI AMS.xlsx",sheetName='Pro FY06-FY15', header=TRUE, na="", startRow=2, colClasses=rep("character",130),stringsAsFactors=FALSE)
#pro_in2 <- read.xlsx(file="/dbfs/mnt/insights-environment-sandbox/FIJI AMS.xlsx",sheetName='Pro FY16-FY26', header=TRUE, na="", startRow=2, colClasses=rep("character",130),stringsAsFactors=FALSE)
#pro_in3 <- read.xlsx(file="/dbfs/mnt/insights-environment-sandbox/FIJI AMS FY22-FY27 24.03.2022.xlsx",sheetName='LFP - FY22-FY27', header=TRUE, na="", startRow=2, colClasses=rep("character",130),stringsAsFactors=FALSE)
pro_in1 <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/FIJI AMS.xlsx",sheet='Pro FY06-FY15', col_names=TRUE, na="", skip=1, col_types="text")
pro_in2 <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/FIJI AMS.xlsx",sheet='Pro FY16-FY26', col_names=TRUE, na="", skip=1, col_types="text")
#pro_in3 <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/FIJI AMS FY22-FY27 24.03.2022.xlsx",sheet='LFP - FY22-FY27', col_names=TRUE, na="", skip=1, col_types="text")
pro_inEMEA <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/LFP LTP Usage-AMS%.xlsx",sheet='EMEA', col_names=TRUE, na="", skip=0, col_types="text", .name_repair= ~make.unique(.x, sep="."))
pro_inAMS <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/LFP LTP Usage-AMS%.xlsx",sheet='AMS', col_names=TRUE, na="", skip=0, col_types="text", .name_repair= ~make.unique(.x, sep="."))
pro_inAPJ <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/LFP LTP Usage-AMS%.xlsx",sheet='APJ', col_names=TRUE, na="", skip=0, col_types="text", .name_repair= ~make.unique(.x, sep="."))

# COMMAND ----------

pro1 <- pivot_longer(data=pro_in1,names_to='date',cols = !starts_with('...'),values_to='pageshare',values_drop_na=TRUE)
pro2 <- pivot_longer(data=pro_in2,names_to='date',cols = !starts_with('...'),values_to='pageshare',values_drop_na=TRUE)
#pro3 <- pivot_longer(data=pro_in3,names_to='date',cols = !starts_with('...'),values_to='pageshare',values_drop_na=TRUE)
proEMEAa <- pivot_longer(data=pro_inEMEA,names_to='datea',cols = starts_with('Q'),values_to='pageshare',values_drop_na=TRUE)
proAMSa <- pivot_longer(data=pro_inAMS,names_to='datea',cols = starts_with('Q'),values_to='pageshare',values_drop_na=TRUE)
proAPJa <- pivot_longer(data=pro_inAPJ,names_to='datea',cols = starts_with('Q'),values_to='pageshare',values_drop_na=TRUE)


# COMMAND ----------

proEMEA <- subset(proEMEAa, substr(datea,5,7)=='.1')
proAMS  <- subset(proAMSa, substr(datea,5,7)=='.1')
proAPJ  <- subset(proAPJa, substr(datea,5,7)=='.1')
proEMEA$dateq <- paste0("20",substr(proEMEA$datea,3,4),'Q',substr(proEMEA$datea,2,2))
proAMS$dateq <- paste0("20",substr(proAMS$datea,3,4),'Q',substr(proAMS$datea,2,2))
proAPJ$dateq <- paste0("20",substr(proAPJ$datea,3,4),'Q',substr(proAPJ$datea,2,2))

# COMMAND ----------

proEMEA <- sqldf("
              SELECT a.[LTP Category] as LTP_Category,b.Calendar_Yr_Mo as date,a.pageshare, 'EMEA' as region
              FROM proEMEA a
              LEFT JOIN calendar b
                ON a.dateq=b.Fiscal_Year_Qtr
                    ")
proAMS <- sqldf("
              SELECT a.[LTP Category] as LTP_Category,b.Calendar_Yr_Mo as date,a.pageshare, 'AMS' as region
              FROM proAMS a
              LEFT JOIN calendar b
                ON a.dateq=b.Fiscal_Year_Qtr
                    ")
proAPJ <- sqldf("
              SELECT a.[LTP Category] as LTP_Category,b.Calendar_Yr_Mo as date,a.pageshare, 'APJ' as region
              FROM proAPJ a
              LEFT JOIN calendar b
                ON a.dateq=b.Fiscal_Year_Qtr
                    ")

# COMMAND ----------

pro_lt14 <- rbind(pro1,pro2)  
pro_lt14$date<- as.character(gsub('X','',as.character(pro_lt14$date)))
pro_lt14 <- subset(pro_lt14,date < "201311")

# COMMAND ----------

#pro3 <- subset(pro3,select=-val)

pro_lt14$date<- as.character(gsub('X','',as.character(pro_lt14$date)))
colnames(pro_lt14) <- c('record','geography','group','sku','date','pageshare')
pro_lt14$pageshare <- as.numeric(pro_lt14$pageshare)
pro_lt14 <- subset(pro_lt14,!is.na(pageshare))
pro_lt14$biz='pro'

#greater than 2014Q1 by LTP Category
pro_gt14 <- rbind(proEMEA,proAMS,proAPJ)
pro_gt14$pageshare <- as.numeric(pro_gt14$pageshare)
pro_gt14 <- subset(pro_gt14,!is.na(pageshare))
pro_gt14$biz='pro'

# COMMAND ----------

share_data_in_lt14 <- rbind(pro_lt14,design_lt14)
share_data_in_gt14 <- rbind(pro_gt14,design_gt14)

share_data_in_lt14$region_3 <- ifelse(share_data_in_lt14$geography=='AP','APJ',ifelse(share_data_in_lt14$geography=='Asia Pacific', 'APJ', ifelse(share_data_in_lt14$geography=='EU','EMEA',ifelse(share_data_in_lt14$geography=='Europe','EMEA',
                            ifelse(share_data_in_lt14$geography=='Japan','APJ',
                            ifelse(share_data_in_lt14$geography=='Latin America','AMS',ifelse(share_data_in_lt14$geography=='NA','AMS',ifelse(share_data_in_lt14$geography=='US','AMS',
                            ifelse(share_data_in_lt14$geography=='LA','AMS',NA)))))))))

share_data_in_lt14 <- subset(share_data_in_lt14, pageshare >0)
share_data_in_gt14 <- subset(share_data_in_gt14, pageshare >0)

# COMMAND ----------

#hw info
  #prod_in2 <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/RDMA Categories and Subsets - From Fiji to IE v7 25.04.2022.xlsx",sheet='LF', col_names=TRUE, na="")
  penmap <- read_xlsx(path="/dbfs/mnt/insights-environment-sandbox/PensxPrinter Mapping.xlsx",sheet='Map', col_names=TRUE, na="")
  prod_inp <- dbGetQuery(sfai,"SELECT * from [IE2_Landing].[lfd].[lf_rdma_cat] where [SKU Yes/No]='Yes' and [HW/Supp]='HW' and [Final Product Family Name] != 'ACCESSORIES'")

# COMMAND ----------

hw_info <- dbGetQuery(sfai,"
                      SELECT platform_subset, business_feature as biz, business_segment as segment, hw_product_family 
                      FROM IE2_Prod.dbo.hardware_xref with (NOLOCK)
                      WHERE (upper(technology)='LF') 
                      ")
    
    zero_init2 <- sqldf("select distinct b.SKU
      ,a.[Base Product Number] as SKUp
      , a.[Product Status]
      , a.[End of life]
      , CASE WHEN c.biz is not null then c.biz ELSE a.[Biz] end as Biz
      , CASE WHEN c.segment is not null then c.biz ELSE a.[segment] end as segment
      , a.[Final Product Family Name] 
      , upper(c.platform_subset) as platform_subset
      , a.[Predecessor]
      --, a.[Financial Market Category Name]
      , a.[Product Class Code]
      , a.[Product Type]
      FROM hw_info c
      LEFT JOIN prod_inp a
        ON a.[New RDMA Subset Name]=c.[platform_subset]
      LEFT JOIN penmap b
        ON b.[Fiji Subset]=a.[Fiji Platform Subset]
        --where b.SKU is not null
      ")

# COMMAND ----------

ibtable <- dbGetQuery(sfai,paste0("
                     select  upper(a.platform_subset) as platform_subset
                            , a.month_begin as cal_date
                            , YEAR(a.month_begin)*100+MONTH(a.month_begin) AS month_begin
                            , cal.fiscal_year_qtr
                            ,d.technology AS hw_type
                            ,a.hps_ops
                            ,b.region_3
                            --,b.market10
                            --,a.country_alpha2
                            ,sum(a.ib) as ib
                      from IE2_Staging.test.ib_18_ce_splits_post_scrub a with (NOLOCK)
                      left join IE2_Prod.dbo.iso_country_code_xref b with (NOLOCK)
                        on (a.country_alpha2=b.country_alpha2)
                      left join IE2_Prod.dbo.hardware_xref d with (NOLOCK)
                         on (a.platform_subset=d.platform_subset)
                      left join IE2_Prod.dbo.calendar cal
                         on a.month_begin=cal.date_key
                      where 1=1
                          and ib>0
                          and d.technology='LF'
                      group by a.platform_subset 
                              , cal.fiscal_year_qtr  , a.month_begin
                              , d.technology , a.hps_ops
                              ,b.region_3 --, a.country_alpha2, b.market10
                     "))

# COMMAND ----------


  caldts <- dbGetQuery(sfai,"select Date, Fiscal_Year_Qtr, Calendar_Yr_Mo FROM ie2_prod.dbo.calendar")
  ibintrodt <- sqldf("
              SELECT  platform_subset
                      ,min(cal_date) AS intro_date
                      ,min(month_begin) AS intro_month
                      FROM ibtable
                      GROUP BY platform_subset
                     ")
  ibintrodt$intro_date <- as.Date(ibintrodt$intro_date)
  

# COMMAND ----------

hwval <- dbGetQuery(sfai,"
                    SELECT distinct upper(platform_subset) as platform_subset
                      , predecessor
                      , pl as product_line_code
                      , CASE WHEN vc_category in ('DEPT','Dept') THEN 'DPT'
                            WHEN vc_category in ('PLE-H','PLE-L','ULE') THEN 'DSK'
                            WHEN vc_category in ('SWT-H','SWT-H Pro') THEN 'SWH'
                            WHEN vc_category = 'SWT-L' THEN 'SWL'
                            WHEN vc_category = 'WG' THEN 'WGP'
                      END AS platform_market_code
                      , sf_mf AS platform_function_code
                       , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                        WHEN vc_category in ('SWT-H','SWT-H Pro','Dept','Dept-High','WG') then 'ENT'
                        ELSE NULL
                        END as platform_business_code
                      , SUBSTRING(mono_color,1,1) as platform_chrome_code
                      , mono_ppm AS print_mono_speed_pages
                      , color_ppm AS print_color_speed_pages
                      , por_ampv 
                      , technology as printer_technology_type
                      , hw_product_family as product_group
                      , product_structure as platform_speed_segment_code
                      , business_feature as platform_division_code  
                    FROM ie2_Prod.dbo.hardware_xref with (NOLOCK)
                    where technology in ('LF')")

# COMMAND ----------

share_data_month_lt14 <- sqldf("
                      with sub0 as (select a.sku 
                      , b2.platform_subset 
                      , ib.region_3 as printer_region_code
                      , c.intro_date as printer_intro_month
                      , ib.cal_date
                      , a.date
                      , hw.platform_chrome_code AS CM
                      , hw.platform_business_code AS EP
                      , hw.platform_function_code AS platform_function_code
                      , hw.platform_market_code
                      --, c.developed_emerging as de
                      , b2.[Segment] as segment
                      , b2.[Final Product Family name] as family
                      , b2.[Predecessor]
                      --, b2.[Financial Market Category Name] as fmc
                      --, b2.[Product Class Code] as product_class
                      --, b2.[Product Type] as product_type
                      , cal.Fiscal_Year_Qtr
                      , avg(a.pageshare) as pageshare 
                  
              FROM share_data_in_lt14 a
              LEFT JOIN zero_init2 b2
                ON (a.sku=b2.SKU)
              LEFT JOIN zero_rdma b
                ON (b2.skup=b.Base_Prod_Number)
              LEFT JOIN ibtable ib
                ON (a.region_3=ib.region_3 and a.date=ib.month_begin and b2.platform_subset=ib.platform_subset)
              LEFT JOIN ibintrodt c
                ON b2.platform_subset =c.platform_subset
              LEFT JOIN hwval hw
                ON b2.platform_subset =hw.platform_subset
              LEFT JOIN caldts cal
                ON a.date=cal.Calendar_Yr_Mo
                GROUP BY  1,2,3,4,5,6,7,8,9,10,11,12,13,14)
              , sub_1 AS (
              SELECT distinct platform_subset
                      , printer_region_code 
                      , printer_intro_month
                      , cal_date
                      , date
                      , CM
                      , EP
                      , platform_function_code
                      , platform_market_code
                      , segment
                      , family
                      , predecessor
                      --, fmc
                      --, product_class
                      --,product_type
                      , Fiscal_Year_Qtr
                      , pageshare
                     FROM sub0
                      WHERE platform_subset is not null
                      GROUP BY
                        platform_subset
                      , printer_region_code 
                      , printer_intro_month
                      , cal_date
                      , date
                      , CM
                      , EP
                      , platform_function_code
                      , platform_market_code
                      , segment
                      , family
                      , predecessor
                      --, fmc
                      --, product_class
                      --, product_type
                      , Fiscal_Year_Qtr
                      , pageshare
                      )
                      select * from sub_1
                            ")

# COMMAND ----------

share_data_month_gt14 <- sqldf("
                      with sub0 as (select  
                      b2.[New RDMA Subset Name] as platform_subset 
                      , ib.region_3 as printer_region_code
                      , c.intro_date as printer_intro_month
                      , ib.cal_date
                      , a.date
                      , hw.platform_chrome_code AS CM
                      , hw.platform_business_code AS EP
                      , hw.platform_function_code AS platform_function_code
                      , hw.platform_market_code
                      --, c.developed_emerging as de
                      , b2.[Segment] as segment
                      , b2.[Final Product Family name] as family
                      , b2.[Predecessor]
                      --, b2.[Financial Market Category Name] as fmc
                      --, b2.[Product Class Code] as product_class
                      --, b2.[Product Type] as product_type
                      , cal.Fiscal_Year_Qtr
                      , avg(a.pageshare) as pageshare 
                  
              FROM share_data_in_gt14 a
              LEFT JOIN prod_inp b2
                ON (a.LTP_Category=b2.[LTP Category])
              LEFT JOIN ibtable ib
                ON (a.region=ib.region_3 and a.date=ib.month_begin and b2.[New RDMA Subset Name]=ib.platform_subset)
              LEFT JOIN ibintrodt c
                ON b2.[New RDMA Subset Name] =c.platform_subset
              LEFT JOIN hwval hw
                ON b2.[New RDMA Subset Name] =hw.platform_subset
              LEFT JOIN caldts cal
                ON a.date=cal.Calendar_Yr_Mo
                GROUP BY  1,2,3,4,5,6,7,8,9,10,11,12,13)
              , sub_1 AS (
              SELECT distinct platform_subset
                      , printer_region_code 
                      , printer_intro_month
                      , cal_date
                      , date
                      , CM
                      , EP
                      , platform_function_code
                      , platform_market_code
                      , segment
                      , family
                      , predecessor
                      --, fmc
                      --, product_class
                      --,product_type
                      , Fiscal_Year_Qtr
                      , pageshare
                     FROM sub0
                      WHERE platform_subset is not null
                      GROUP BY
                        platform_subset
                      , printer_region_code 
                      , printer_intro_month
                      , cal_date
                      , date
                      , CM
                      , EP
                      , platform_function_code
                      , platform_market_code
                      , segment
                      , family
                      , predecessor
                      --, fmc
                      --, product_class
                      --, product_type
                      , Fiscal_Year_Qtr
                      , pageshare
                      )
                      select * from sub_1
                            ")

# COMMAND ----------

share_data_month <- rbind(share_data_month_lt14,share_data_month_gt14)  

# COMMAND ----------

share_data_qtr <- sqldf("select platform_subset
                      , printer_region_code 
                      , printer_intro_month
                      , min(cal_date) as cal_date
                      , min(date) as date
                      , CM
                      , EP
                      , platform_function_code
                      , platform_market_code
                      , segment
                      , family
                      , predecessor
                      --, fmc
                      --, product_class
                      --, product_type
                      , Fiscal_Year_Qtr
                      , avg(pageshare) as pageshare
                      FROM share_data_month
                      Group By
                      platform_subset
                      , printer_region_code 
                      , printer_intro_month
                      --, cal_date
                      --, date
                      , CM
                      , EP
                      , platform_function_code
                      , platform_market_code
                      , segment
                      , family
                      , predecessor
                      --, fmc
                      --, product_class
                      --, product_type
                      , Fiscal_Year_Qtr
                    ")
share_data <- share_data_qtr
share_data$grp <- paste0(share_data$platform_subset,"_",share_data$printer_region_code)
share_data <- subset(share_data,pageshare >0)

# COMMAND ----------

str(share_data)

# COMMAND ----------

data_coefr <- list()

for (printer in unique(share_data$grp)){
  
  dat1 <- subset(share_data,grp==printer)  # & fiscal_year_quarter != '2020Q1' & fiscal_year_quarter != '2020Q2')
  #dat1 <- subset(page_share_reg,grp=='CICADA PLUS ROW_NA' & fiscal_year_quarter != '2020Q1' & fiscal_year_quarter != '2020Q2')
  if (nrow(dat1) <4 ) {next}
  if(is.na(dat1$printer_region_code[1])){next}
  if(is.na(dat1$printer_intro_month[1])){next}
  minchk <- min(dat1$pageshare)
  maxchk <- max(dat1$pageshare)
  if(minchk==maxchk) { 
    min <- minchk
    max <- maxchk
    med <- 50
    spread <- 0
    a <- maxchk
    b <- 0
    grp <- unique(as.character(dat1$grp))
    coeffout<-  data.frame(min,max,med,spread,a,b,grp)
    } else {
  #mindt <- min(dat1$fiscal_year_quarter)
  mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
  dat1$timediff <- round(as.numeric(difftime(as.Date(dat1$cal_date,format="%Y-%m-%d"),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  #dat1$timediff2 <- round(as.numeric(difftime(as.Date(dat1$printer_high_month,format="%Y-%m-%d"),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
  mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
  
  #values for sigmoid curve start
  midtime <- (min(dat1$timediff)+min(dat1$timediff))/2 #median(dat1$timediff)
  #midtime2 <- min(dat1$timediff2) #median(dat1$timediff)
  maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
  #maxtimediff2 <- max(dat1$timediff)
  
  dat1$pageshare <- as.numeric(dat1$pageshare)
  maxv=min(max(dat1$pageshare)+max(dat1$pageshare)*(0.002*mintimediff),1.5)
  minv=max(min(dat1$pageshare)-min(dat1$pageshare)*(0.002*maxtimediff),0.05)
  spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2,.1)
  frstshr <- dat1[1,13]
 
  #Sigmoid Model  
  sigmoid <- pageshare ~ max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
 
  fitmodel <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                    lower=c(min=0.05,max=maxv,med=0, spread=0.01),
                    upper=c(min=minv,max=2,med=1000, spread=.1),
                    algorithm = "grid-search", 
                    #weights=supply_count_share,
                    weights = timediff,
                    control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
  #linear model
    dat1b <- sqldf("select * from dat1 order by timediff desc limit 8")
    fitmodel2 <- lm(pageshare ~ timediff, data=dat1b)
    # fitmodel3 <- nls2(formula=sigmoid, 
    #                  data=dat1,
    #                  start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
    #                  #lower=c(min=0.05,max=minv,med=0, spread=0.05),
    #                  #upper=c(min=maxv,max=2,med=1000, spread=1),
    #                  algorithm = "grid-search", 
    #                  #weights=supply_count_share,
    #                  weights = timediff,
    #                  control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
  
  timediff <- c(0:100)
  preddata <- as.data.frame(timediff)

  #dat2<- dat1[,c("timediff")]
  preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)

  fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
  colnames(fit1) <- "fit1"
  preddata <- cbind(preddata,fit1)
  preddata <- sqldf(paste("select a.*, b.pageshare as obs
                            --, b.supply_count_share
                          from preddata a left join dat1 b on a.timediff=b.timediff"))
  #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
   #                               ifelse(preddata$supply_count_share<10000,3,4)))

   #plot(y=preddata$pageshare,x=preddata$timediff, type="p"
   #     ,main=paste("Share for ",printer)
   #      ,xlab="Date", ylab="HP Share", ylim=c(0,1))
   #lines(y=preddata$pageshare,x=preddata$timediff, col='black')
   #lines(y=preddata$fit1,x=preddata$timediff,col="blue")
   #legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
  
    coeffo <- as.data.frame(coef(fitmodel))
    coeffo2 <- as.data.frame(coef(fitmodel2))
    #coeffo3 <- as.data.frame(coef(fitmodel3))
    coeffout<-  as.data.frame(cbind(coeffo[1,],coeffo[2,],coeffo[3,]
                          ,coeffo[4,],coeffo2[1,],coeffo2[2,],unique(as.character(dat1$grp))
                          ))
    # coeffout<-  as.data.frame(cbind(coeffo[1,],coeffo[2,],coeffo[3,]
    #                     ,coeffo[4,],coeffo2[1,],coeffo2[2,]
    #                     #,unique(as.character(dat1$Platform.Subset))
    #                     #,unique(as.character(dat1$Region_5))
    #                     #,unique(as.character(dat1$RTM))
    #                     ,unique(as.character(dat1$grp))
    #                     ,coeffo3[1,],coeffo3[2,],coeffo3[3,]
    #                     ,coeffo3[4,]
    # ))
    colnames(coeffout)[1] <- "min"
    colnames(coeffout)[2] <- "max"
    colnames(coeffout)[3] <- "med"
    colnames(coeffout)[4] <- "spread"
    colnames(coeffout)[5] <- "a"
    colnames(coeffout)[6] <- "b"
    colnames(coeffout)[7] <- "grp"
    # colnames(coeffout)[8] <- "min2"
    # colnames(coeffout)[9] <- "max2"
    # colnames(coeffout)[10] <- "med2"
    # colnames(coeffout)[11] <- "spread2"

    }  
  coeffout$min <- as.numeric(coeffout$min)
  coeffout$max <- as.numeric(coeffout$max)
  coeffout$med <- as.numeric(coeffout$med)
  coeffout$spread <- as.numeric(coeffout$spread)
  coeffout$a <- as.numeric(coeffout$a)
  coeffout$b <- as.numeric(coeffout$b)
  coeffout$grp <- as.character(coeffout$grp)
  data_coefr[[printer]] <- coeffout

}

# COMMAND ----------

data_coeffr <- as.data.frame(do.call("bind_rows", data_coefr))


printer_list <- sqldf("SELECT pi.platform_subset as platform_subset_name
                      , pi.Predecessor
                      , pi.product_line_code, pi.platform_market_code, pi.platform_function_code, pi.platform_business_code, pi.platform_chrome_code, pi.print_mono_speed_pages
                      , pi.print_color_speed_pages, pi.por_ampv, pi.printer_technology_type, pi.product_group, pi.platform_speed_segment_code, pi.platform_division_code, min(dt.intro_date) as printer_intro_month
                      from hwval pi
                      left join ibintrodt dt
                      on pi.platform_subset=dt.platform_subset
                      group by pi.platform_subset
                      , pi.Predecessor
                      , pi.product_line_code, pi.platform_market_code, pi.platform_function_code, pi.platform_business_code, pi.platform_chrome_code, pi.print_mono_speed_pages
                      , pi.print_color_speed_pages, pi.por_ampv, pi.printer_technology_type, pi.product_group, pi.platform_speed_segment_code, pi.platform_division_code
                      ")

cntry <- as.data.frame(unique(share_data_month$printer_region_code))
colnames(cntry) <- 'region3'
printer_list_ctr <- printer_list[rep(seq_len(nrow(printer_list)),each=nrow(cntry)),]
printer_list_ctr <- cbind(printer_list_ctr,cntry)

printer_list_ctr$grpr <-paste0(printer_list_ctr$platform_subset_name,"_",printer_list_ctr$region3)

combined1 <- sqldf("select a.platform_subset_name
                            , a.Predecessor
                            , a.region3 as regions
                            --, SUBSTR(a.developed_emerging,1,1) as emdm
                            --, a.cntry as country_alpha2
                            , r.min
                            , r.max
                            , r.med
                            , r.spread
                            , r.a
                            , r.b
                            , 'Region_3' as source
                            --, a.FMC
                            , a.printer_intro_month
                           
                    FROM printer_list_ctr a 
                    left join data_coeffr r
                      on (a.grpr=r.grp)
                   
                   "
                    )
#detach(package:RH2)

colnames(combined1)[names(combined1)=="platform_subset_name"] <- "printer_platform_name"

output1 <- subset(combined1,!is.na(min))

combined1b <- sqldf("select
                      a.*,
                      CASE WHEN
                        a.min IS NULL THEN 'N'
                      ELSE 'Y'
                      END AS curve_exists
                      from combined1 a
                  ")

# COMMAND ----------

str(output1)

# COMMAND ----------

predlist <- combined1b[c(1,3,2,12)]  #printer_platform_name, regions, Predecessor, curve_exists#
proxylist1 <- subset(predlist,curve_exists=="Y")
proxylist1$proxy <- proxylist1$printer_platform_name
proxylist1$gen   <- 0



predlist_use <- anti_join(predlist,proxylist1,by=c("printer_platform_name","regions"))

# COMMAND ----------

predlist_use$pred_iter <- predlist_use$predecessor
predlist_use <- anti_join(predlist,proxylist1,by=c("printer_platform_name","regions"))
predlist_use$pred_iter <- predlist_use$predecessor
####Predecessors####
y <- 0
repeat{
predlist_iter2 <- sqldf("
                  select a.printer_platform_name
                  , a.regions
                  --, a.country_alpha2
                  , a.predecessor
                  --, a.source
                  , CASE 
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.predecessor as pred_iter
                  from predlist_use a
                  left join predlist b
                    on (a.pred_iter=b.printer_platform_name and a.regions=b.regions)
                  ")
predlist_iter <- predlist_iter2
proxylist_iter <- subset(predlist_iter,curve_exists=="Y")
y<- y-1

#print(y)

if(dim(proxylist_iter)[1] != 0){

proxylist_iter$proxy <- proxylist_iter$predecessor
proxylist_iter <- proxylist_iter[c(1,2,3,4,6)]
proxylist_iter$gen   <- y
proxylist1 <-rbind(proxylist1,proxylist_iter)
}
predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","regions"))

if(all(is.na(predlist_use$pred_iter))){break}
if(y< -15){break}
}

predlist_use$pred_iter <- predlist_use$printer_platform_name
#####Successors#####
y=0
repeat{
predlist_iter2 <- sqldf("
                  select a.printer_platform_name
                  , a.regions
                  --, a.regions
                  , a.predecessor
                  --, a.source
                  , CASE 
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.printer_platform_name as pred_iter
                  from predlist_use a
                  left join predlist b
                    on (a.pred_iter=b.predecessor and a.regions=b.regions)
                  ")

predlist_iter <- predlist_iter2
proxylist_iter <- subset(predlist_iter,curve_exists=="Y")

y<- y+1

#print(y)

if(dim(proxylist_iter)[1] != 0){

proxylist_iter$proxy <- proxylist_iter$pred_iter
proxylist_iter <- proxylist_iter[c(1,2,3,4,6)]
proxylist_iter$gen   <- y
proxylist1 <-rbind(proxylist1,proxylist_iter)
}

predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","regions"))

if(all(is.na(predlist_use$pred_iter))){break}
if(y>15){break}
}
#successor list empty

proxylist_temp <- proxylist1

leftlist <- anti_join(predlist,proxylist_temp,by=c("printer_platform_name","regions"))

# COMMAND ----------

proxylist_temp <- bind_rows(proxylist1,predlist_use)

output2<- sqldf("select a.printer_platform_name
                        , a.regions
                        --, a.country_alpha2
                        , a.predecessor
                        , c.proxy
                        , b.min
                        , b.max
                        , b.med
                        , b.spread
                        , b.a
                        , b.b
                        , a.source
                        --, a.FMC
                        , a.printer_intro_month
                        from combined1 a
                        left join proxylist_temp c
                        on (a.printer_platform_name=c.printer_platform_name and a.regions=c.regions)
                        left join combined1 b
                        on (c.proxy=b.printer_platform_name and c.regions=b.regions)
                        --left join combined1na d
                        --on (c.printer_platform_name=d.printer_platform_name)
              ")

proxylist_f1 <- subset(output2,!is.na(min))

# COMMAND ----------

proxylist_final2 <- sqldf("
                          SELECT distinct
                              a.printer_platform_name
                              --, c.Supplies_Product_Family
                              --, d.selectability
                              --, e.Crg_PL_Name
                              --, a.printer_platform_name
                              , a.regions
                              --, a.country_alpha2
                              , a.Predecessor
                              , a.proxy as ps_proxy
                              , a.min as ps_min
                              , a.max as ps_max
                              , a.med as ps_med
                              , a.spread as ps_spread
                              , a.a as ps_a
                              , a.b as ps_b
                              --, a.FMC
                              , a.source
                              --, f.proxy as cu_proxy
                              --, f.min as cu_min
                              --, f.max as cu_max
                              --, f.med as cu_med
                              --, f.spread as cu_spread
                              , c.intro_date as printer_intro_month
                            FROM proxylist_f1 a
                            LEFT JOIN hwval b
                              ON a.printer_platform_name = b.platform_subset
                            LEFT JOIN ibintrodt c
                              ON a.printer_platform_name-c.platform_subset
                            --LEFT JOIN (SELECT Supplies_Product_Family, Platform_Subset FROM SupplyFam GROUP BY Platform_Subset) c
                              --ON (a.printer_platform_name = c. Platform_Subset)
                            --LEFT JOIN selectability d
                              --ON a.printer_platform_name = d.Platform_Subset_Nm
                            --LEFT JOIN pcplnm e
                              --ON a.printer_platform_name = e.Platform_Subset
                            --LEFT JOIN proxylist_f1c f
                              --ON (a.printer_platform_name = f.printer_platform_name AND a.country_alpha2=f.country_alpha2)
                          ")

# COMMAND ----------

UPM <- SparkR::read.parquet(path=paste0("s3://", aws_bucket_name, "LF_UPM_ctry(", UPMDate, ").parquet"))

createOrReplaceTempView(UPM, "UPM")

# COMMAND ----------

nrow(UPM)

# COMMAND ----------

UPM2 <- SparkR::sql('
              SELECT distinct
                a.Platform_Subset_Nm as printer_platform_name
              , a.Region_3
              , a.Region
              , a.Region_DE
              , a.Country_Cd
              , a.market10
              , a.FYearMo
              , a.FYearQtr
              , a.rFYearQtr
              , a.FYear
              , substr(a.FYearQtr,6,6) as FQtr
              , CEIL(avg(a.MoSI)/3) as QSI
              , sum(a.MPV_UPM*a.IB) as MPV_UPMIB
              , sum(a.MPV_TS*a.IB) as MPV_TSIB
              , sum(a.MPV_Trend*a.IB) as MPV_TDIB
              , sum(a.MPV_Raw*a.IB) as MPV_RawIB
              , sum(a.MPV_N) as MPV_n
              , sum(a.IB) as IB
              , sum(a.MPV_Init) as Init_MPV
              --, avg(b.MUT) as MUT
              , avg(a.Trend) as Trend
              --, avg(b.Seasonality) as Seasonality
              --, avg(b.Cyclical) as Cyclical
              , avg(a.Decay) as Decay
              FROM UPM a 
              GROUP BY 
                a.Platform_Subset_Nm
              , a.Region_3
              , a.Region
              , a.Region_DE
              , a.Country_Cd
              , a.market10
              , a.FYearMo
              , a.FYearQtr
              , a.rFYearQtr
              , a.FYear
              ')

UPM3 <- SparkR::sql('
              SELECT 
                DISTINCT Platform_Subset_Nm as printer_platform_name
              , Region_3
              , Region
              , Region_DE
              , market10
              , Country_Cd 
              , FIntroDt
              , Decay
              , Seasonality
              , IMPV_Route
              FROM UPM
              GROUP BY 
                Platform_Subset_Nm
              , Region_3
              , Region
              , Region_DE
              , market10
              , Country_Cd
              , FIntroDt
              , Decay
              , Seasonality
              , IMPV_Route
              ')


UPM2$MPV_UPM <- (UPM2$MPV_UPMIB/UPM2$IB)
UPM2$MPV_Raw <- (UPM2$MPV_RawIB/UPM2$IB)
UPM2$MPV_TS  <- (UPM2$MPV_TSIB/UPM2$IB)
UPM2$MPV_TD  <- (UPM2$MPV_TDIB/UPM2$IB)


# COMMAND ----------

months <- as.data.frame(seq(as.Date("1989101",format="%Y%m%d"),as.Date("20500101",format="%Y%m%d"),"month"))
colnames(months) <- "CalYrDate"
quarters2 <- as.data.frame(months[rep(seq_len(nrow(months)),nrow(proxylist_final2)),])
colnames(quarters2) <- "CalDate"

# COMMAND ----------

hwlookup <- as.DataFrame(dbGetQuery(sfai,"
                      SELECT distinct platform_subset, technology, pl, sf_mf as SM
                      FROM IE2_Prod.dbo.hardware_xref"))

ibtablef <- as.DataFrame(dbGetQuery(sfai,paste0("
                     select  upper(a.platform_subset) as platform_subset
                            , a.month_begin as cal_date
                            , YEAR(a.month_begin)*100+MONTH(a.month_begin) AS month_begin
                            , cal.fiscal_year_qtr
                            ,d.technology AS hw_type
                            ,a.hps_ops
                            ,b.region_3
                            ,b.market10
                            ,a.country_alpha2
                            ,sum(a.ib) as ib
                      from IE2_Staging.test.ib_18_ce_splits_post_scrub a with (NOLOCK)
                      left join IE2_Prod.dbo.iso_country_code_xref b with (NOLOCK)
                        on (a.country_alpha2=b.country_alpha2)
                      left join IE2_Prod.dbo.hardware_xref d with (NOLOCK)
                         on (a.platform_subset=d.platform_subset)
                      left join IE2_Prod.dbo.calendar cal
                         on a.month_begin=cal.date_key
                      where 1=1
                          and ib>0
                          and d.technology='LF'
                      group by a.platform_subset 
                              , cal.fiscal_year_qtr  , a.month_begin
                              , d.technology , a.hps_ops
                              ,b.region_3 , a.country_alpha2, b.market10
                     ")))

# COMMAND ----------

createOrReplaceTempView(UPM2, "UPM2")
createOrReplaceTempView(as.DataFrame(proxylist_final2), "proxylist_final2")
createOrReplaceTempView(as.DataFrame(ibtable), "ibtable")
createOrReplaceTempView(as.DataFrame(share_data), "share_data")
createOrReplaceTempView(UPM3, "UPM3")
createOrReplaceTempView(hwlookup, "hwlookup")
createOrReplaceTempView(as.DataFrame(ibintrodt), "ibintrodt")

# COMMAND ----------

final_list2 <- SparkR::sql("
                     SELECT distinct b.printer_platform_name AS Platform_Subset_Nm
                      , hw.technology
                      , hw.pl
                      --, a.Supplies_Product_Family
                      --, a.selectability AS Selectability
                      --, a.Crg_PL_Name
                      , b.Region_3
                      , b.Region
                      , b.Region_DE
                      , b.Country_Cd
                      , b.market10
                      , b.FYearQtr AS Fiscal_Quarter
                      , c.ib AS IB
                      , b.FYearMo
                      , b.rFyearQtr
                      , b.FYear
                      , b.FQtr
                      , b.QSI
                      , b.MPV_UPM
                      , b.MPV_TS
                      , b.MPV_TD
                      , b.MPV_Raw
                      , b.MPV_N
                      , idt.intro_date as FIntroDt
                      --, g.EP
                      --, g.CM
                      --, hw.SM
                      --, g.Mkt
                      --, g.K_PPM
                      --, g.Clr_PPM
                      --, g.Intro_Price
                      , b.Init_MPV
                      --, CASE WHEN g.CM='C' THEN b.MPV_UPM*bb.color_pct ELSE 0 END AS MPV_UPMc
                      --, CASE WHEN g.CM='C' THEN b.MPV_TS*bb.color_pct ELSE 0 END AS MPV_TSc
                      --, CASE WHEN g.CM='C' THEN b.MPV_TD*bb.color_pct ELSE 0 END AS MPV_TDc
                      --, CASE WHEN g.CM='C' THEN b.MPV_Raw*bb.color_pct ELSE 0 END AS MPV_Rawc
                      --, CASE WHEN g.CM='C' THEN bb.MPV_N ELSE 0 END AS MPV_Nc
                      , g.IMPV_Route
                      , a.Predecessor
                      --, e.usage_region_incl_flag AS BD_Usage_Flag
                      --, e.usage_country_incl_flag AS BD_Usage_Flag
                      , b.MPV_Raw AS MPV_Dash
                      --, e.campv AS MPV_DashC
                      , a.ps_proxy AS Proxy_PS
                      --, d.page_share_region_incl_flag AS BD_Share_Flag_PS
                      , a.source
                      , a.ps_min AS Model_Min_PS
                      , a.ps_max AS Model_Max_PS
                      , a.ps_med AS Model_Median_PS
                      , a.ps_spread AS Model_Spread_PS
                      , a.ps_a AS Model_a_PS
                      , a.ps_b AS Model_b_PS
                      , d.pageshare AS Share_Raw_PS
                      --, d.printer_count_month_ps AS Share_Raw_N_PS
                      , b.MPV_N as usage_n
                      --, a.cu_proxy AS Proxy_CU
                      --, f.share_region_incl_flag AS BD_Share_Flag_CU
                      --, a.cu_min AS Model_Min_CU
                      --, a.cu_max AS Model_Max_CU
                      --, a.cu_med AS Model_Median_CU
                      --, a.cu_spread AS Model_Spread_CU
                      --, f.value AS Share_Raw_CU
                      --, f.printer_count_month_ci AS Share_Raw_N_CU
                      --, bb.color_pct

                      FROM UPM2 b
                      LEFT JOIN proxylist_final2 a
                        ON (a.printer_platform_name=b.printer_platform_name and a.regions=b.Region_3)
                      --LEFT JOIN UPM2C bb
                        --ON (b.printer_platform_name=bb.printer_platform_name and b.Country_cd=bb.Country_cd
                            --and b.FYearMo=bb.FYearMo)
                      LEFT JOIN ibtablef c
                        ON (b.printer_platform_name=c.platform_subset and b.Country_cd=bb.Country_cd
                              and b.FYearMo=c.month_begin)
                      LEFT JOIN share_data d
                        ON (b.printer_platform_name=d.platform_subset AND b.Region_3=d.printer_region_code
                            AND b.FYearQtr=d.Fiscal_Year_Qtr)
                      --LEFT JOIN dashampv2 e
                        --ON (b.printer_platform_name=e.platform_name AND b.Country_cd=e.iso_country_code  
                          --AND b.FYearMo=e.calendar_year_month)
                      --LEFT JOIN crg_share_ctr f
                        --ON (b.printer_platform_name=f.platform_name AND b.Country_cd=f.country_alpha2  
                            --AND b.FYearQtr=f.fiscal_year_quarter)
                      LEFT JOIN UPM3 g
                        ON (b.printer_platform_name=g.printer_platform_name and b.Country_cd=g.Country_cd)
                      LEFT JOIN hwlookup hw
                        ON (b.printer_platform_name=hw.platform_subset)
                      LEFT JOIN ibintrodt idt
                        ON (b.printer_platform_name=idt.platform_subset)
                     ")

# COMMAND ----------

sigmoid_pred <- function(input_min, input_max, input_med, input_spread, input_timediff) {
  input_max-(exp((input_timediff-input_med)*input_spread)/(exp((input_timediff-input_med)*input_spread)+lit(1))*(lit(1)-(input_min+(lit(1)-input_max))))
}
#sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))

 
final_list2$timediff <- round(datediff(concat_ws(sep="-", substr(final_list2$FYearMo, 1, 4), substr(final_list2$FYearMo, 5, 6), lit("01")), concat_ws(sep="-", substr(final_list2$FIntroDt, 1, 4), substr(final_list2$FIntroDt, 5, 6), lit("01")))/(365.25/12))
#dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)

final_list2$Share_Model_PS <- sigmoid_pred(final_list2$Model_Min_PS, final_list2$Model_Max_PS, final_list2$Model_Median_PS, final_list2$Model_Spread_PS, final_list2$timediff)
final_list2$Share_Model_PS <- ifelse(final_list2$Share_Model_PS > 1,1,ifelse(final_list2$Share_Model_PS<0.05,0.05,final_list2$Share_Model_PS))
final_list2$Share_Model_PSlin <- (final_list2$Model_a_PS+final_list2$Model_b_PS*final_list2$timediff)


# COMMAND ----------

createOrReplaceTempView(final_list2, "final_list2")

# COMMAND ----------

final_list2 <- SparkR::sql("
                  select distinct a.*
                      , case when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.Share_Raw_PS IN (NULL, 0) then 'Modelled at Region 3 Level'
                            else 'Have Data'
                            end
                          else 'Modelled by Proxy'
                          end as Share_Source_PS
                      , case when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.Share_Raw_PS IN (NULL, 0) then a.Share_Model_PS
                            else a.Share_Raw_PS
                            end
                          else a.Share_Model_PS
                          end as Page_Share_sig
                      , case when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.Share_Raw_PS IN (NULL, 0) then a.Share_Model_PSlin
                            else a.Share_Raw_PS
                            end
                          else a.Share_Model_PSlin
                        end as Page_Share_lin

                      , case
                          when a.MPV_Raw is NULL then 'UPM'
                          when a.MPV_n < 30 then 'UPM Sample Size'
                            else 'Dashboard'
                            end as Usage_Source
                      , case when a.MPV_Raw is NULL then a.MPV_TD
                          when a.MPV_n < 30 then a.MPV_TD
                            else a.MPV_Raw
                            end as Usage
                        
                     from final_list2 a
                     ")

# COMMAND ----------

 SparkR::count(final_list2)

# COMMAND ----------

createOrReplaceTempView(final_list2, "final_list2")

# COMMAND ----------

ws <- orderBy(windowPartitionBy("Platform_Subset_Nm", "Country_Cd"), "FYearMo")
final_list7 <- mutate(final_list2
                     ,lagShare_Source_PS = over(lag("Share_Source_PS"), ws)
                     ,lagUsage_Source = over(lag("Usage_Source"), ws)
                     ,lagShare_PS = over(lag("Page_Share_sig"), ws)
                     ,lagShare_Usage = over(lag("Usage"), ws)
                     ,index1 = over(dense_rank(), ws)
                     )

# COMMAND ----------

final_list7$hd_mchange_ps <- ifelse(substr(final_list7$Share_Source_PS,1,8)=="Modelled",ifelse(final_list7$lagShare_Source_PS=="Have Data",final_list7$Page_Share_sig-final_list7$lagShare_PS, NA ),NA)
final_list7$hd_mchange_ps_i <- ifelse(!isNull(final_list7$hd_mchange_ps),final_list7$index1,NA)
final_list7$hd_mchange_psb <- ifelse(final_list7$Share_Source_PS=="Have Data",ifelse(substr(final_list7$Share_Source_PS,1,8)=="Modelled",final_list7$Page_Share_sig, NA ),NA)
final_list7$hd_mchange_ps_j <- ifelse(!isNull(final_list7$hd_mchange_psb),final_list7$index1,NA)
final_list7$hd_mchange_use <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage-final_list7$lagShare_Usage, NA ),NA)
final_list7$hd_mchange_used <- ifelse(final_list7$Usage_Source=="Dashboard",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage-final_list7$lagShare_Usage, NA ),NA)
final_list7$hd_mchange_use_i <- ifelse(!isNull(final_list7$hd_mchange_use),final_list7$index1,NA)

# COMMAND ----------

createOrReplaceTempView(final_list7, "final_list7")

# COMMAND ----------

final_list7 <- SparkR::sql("
with sub0 as (
                    SELECT distinct Platform_Subset_Nm,Country_Cd
                        ,max(hd_mchange_ps_i) as hd_mchange_ps_i
                        ,min(hd_mchange_ps_j) as hd_mchange_ps_j
                        ,max(hd_mchange_use_i) as hd_mchange_use_i
                        FROM final_list7
                        GROUP BY Platform_Subset_Nm, Country_Cd
                )
                , subusev as (
                     SELECT distinct Platform_Subset_Nm,Country_Cd, FYearMo, Usage, IB
                     FROM final_list7
                     WHERE Usage_Source='Dashboard'
                   )
                 , subusev2 as (
                     SELECT distinct Platform_Subset_Nm,Country_Cd, FYearMo, Usage*IB as UIB, IB, ROW_NUMBER() 
                          OVER (PARTITION BY Platform_Subset_Nm,Country_Cd
                          ORDER BY Platform_Subset_Nm,Country_Cd, FYearMo DESC ) AS Rank
                     FROM subusev 
                     order by FYearMo
                   )
                 , subusev3 as (
                     SELECT Platform_Subset_Nm,Country_Cd, max(FYearMo) as FYearMo, sum(UIB) as sumUIB, sum(IB) as IB
                     FROM subusev2
                     WHERE Rank < 4
                     GROUP BY Platform_Subset_Nm,Country_Cd
                   )
                  , subusev4 as (
                    SELECT distinct Platform_Subset_Nm,Country_Cd, sumUIB/IB as avgUsage
                    FROM subusev3
                    --GROUP BY Platform_Subset_Nm,Country_Cd
                  )
                , subusev5 as (
                     SELECT distinct Platform_Subset_Nm,Country_Cd, FYearMo, Usage*IB as UIB, IB, ROW_NUMBER() 
                          OVER (PARTITION BY Platform_Subset_Nm,Country_Cd
                          ORDER BY Platform_Subset_Nm,Country_Cd, FYearMo ASC ) AS Rank
                     FROM subusev 
                     order by FYearMo
                   )
                 , subusev6 as (
                     SELECT Platform_Subset_Nm,Country_Cd, max(FYearMo) as FYearMo, sum(UIB) as sumUIB, sum(IB) as IB
                     FROM subusev5
                     WHERE Rank < 4
                     GROUP BY Platform_Subset_Nm,Country_Cd
                   )
                  , subusev7 as (
                    SELECT distinct Platform_Subset_Nm,Country_Cd, sumUIB/IB as avgUsage
                    FROM subusev6
                    --GROUP BY Platform_Subset_Nm,Country_Cd
                  )  
                  
                , sub1ps as( 
                    SELECT distinct final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,sub0.hd_mchange_ps_i
                        ,final_list7.Page_Share_sig-final_list7.lagShare_PS AS hd_mchange_ps
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.hd_mchange_ps_i=sub0.hd_mchange_ps_i
                  )
                  , sub1psb as( 
                    SELECT distinct final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,sub0.hd_mchange_ps_j
                        ,final_list7.Page_Share_sig AS hd_mchange_psb
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.hd_mchange_ps_j=sub0.hd_mchange_ps_j
                  )
                  , sub1use as( 
                    SELECT distinct final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,sub0.hd_mchange_use_i,final_list7.Usage
                        ,final_list7.Usage-final_list7.lagShare_Usage AS hd_mchange_use
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.index1 = sub0.hd_mchange_use_i 
                  )
                  , sub1used as( 
                    SELECT distinct final_list7.Platform_Subset_Nm,final_list7.Country_Cd, final_list7.FYearMo,sub0.hd_mchange_use_i
                        ,final_list7.Usage-final_list7.lagShare_Usage AS hd_mchange_used
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.index1=(sub0.hd_mchange_use_i-1)
                  )
                  , sub1used2 as( 
                    SELECT distinct final_list7.Platform_Subset_Nm,final_list7.Country_Cd, final_list7.FYearMo, subusev4.avgUsage, sub1use.hd_mchange_use_i,final_list7.Usage
                        ,sub1use.Usage-subusev4.avgUsage AS hd_mchange_useavg, sub1use.hd_mchange_use
                        FROM final_list7
                        LEFT JOIN 
                        subusev4 ON final_list7.Platform_Subset_Nm=subusev4.Platform_Subset_Nm and final_list7.Country_Cd=subusev4.Country_Cd
                        INNER JOIN 
                        sub1use ON final_list7.Platform_Subset_Nm=sub1use.Platform_Subset_Nm and final_list7.Country_Cd=sub1use.Country_Cd  
                          and final_list7.index1=sub1use.hd_mchange_use_i-1
                  )
                  , sub1used3 as( 
                    SELECT distinct final_list7.Platform_Subset_Nm,final_list7.Country_Cd, final_list7.FYearMo, subusev7.avgUsage, sub1use.hd_mchange_use_i,final_list7.Usage
                        ,sub1use.Usage-subusev7.avgUsage AS hd_mchange_useavg, sub1use.hd_mchange_use
                        FROM final_list7
                        LEFT JOIN 
                        subusev7 ON final_list7.Platform_Subset_Nm=subusev7.Platform_Subset_Nm and final_list7.Country_Cd=subusev7.Country_Cd
                        INNER JOIN 
                        sub1use ON final_list7.Platform_Subset_Nm=sub1use.Platform_Subset_Nm and final_list7.Country_Cd=sub1use.Country_Cd  
                          and final_list7.index1=sub1use.hd_mchange_use_i-1
                  )
                  
                  , sub2 as (
                     SELECT distinct a.*
                      ,sub1ps.hd_mchange_ps as adjust_ps
                      ,sub1ps.hd_mchange_ps_i as adjust_ps_i
                      ,sub1psb.hd_mchange_psb as adjust_psb
                      ,sub1psb.hd_mchange_ps_j as adjust_ps_j
                      ,sub1use.hd_mchange_use as adjust_use
                      ,sub1used.hd_mchange_used as adjust_used
                      ,sub1use.hd_mchange_use_i as adjust_use_i
                      ,subusev4.avgUsage as avgUsage
                      ,subusev7.avgUsage as avgUsageBeg
                      ,sub1used2.hd_mchange_useavg as adjust_useav
                      ,sub1used3.hd_mchange_useavg as adjust_useav_beg
                      
                      FROM final_list7 a
                      LEFT JOIN 
                        sub1ps
                        ON a.Platform_Subset_Nm=sub1ps.Platform_Subset_Nm and a.Country_Cd=sub1ps.Country_Cd --and a.FYearMo=sub1ps.FYearMo
                      LEFT JOIN 
                        sub1psb
                        ON a.Platform_Subset_Nm=sub1psb.Platform_Subset_Nm and a.Country_Cd=sub1psb.Country_Cd --and a.FYearMo=sub1psb.FYearMo
                      LEFT JOIN 
                        sub1use
                        ON a.Platform_Subset_Nm=sub1use.Platform_Subset_Nm and a.Country_Cd=sub1use.Country_Cd --and a.FYearMo=sub1use.FYearMo
                      LEFT JOIN 
                        sub1used
                        ON a.Platform_Subset_Nm=sub1used.Platform_Subset_Nm and a.Country_Cd=sub1used.Country_Cd --and a.FYearMo=sub1used.FYearMo
                      LEFT JOIN 
                        subusev4
                        ON a.Platform_Subset_Nm=subusev4.Platform_Subset_Nm and a.Country_Cd=subusev4.Country_Cd --and a.FYearMo=subusev4.FYearMo
                      LEFT JOIN 
                        subusev7
                        ON a.Platform_Subset_Nm=subusev7.Platform_Subset_Nm and a.Country_Cd=subusev7.Country_Cd --and a.FYearMo=subusev7.FYearMo
                      LEFT JOIN 
                        sub1used2
                        ON a.Platform_Subset_Nm=sub1used2.Platform_Subset_Nm and a.Country_Cd=sub1used2.Country_Cd --and a.FYearMo=sub1used2.FYearMo
                      LEFT JOIN 
                        sub1used3
                        ON a.Platform_Subset_Nm=sub1used3.Platform_Subset_Nm and a.Country_Cd=sub1used3.Country_Cd --and a.FYearMo=sub1used3.FYearMo
                  )
                  SELECT *
                  FROM sub2
                     ")

# COMMAND ----------

final_list7$adjust_ps <- ifelse(isNull(final_list7$adjust_ps), 0, final_list7$adjust_ps)
final_list7$adjust_psb <- ifelse(isNull(final_list7$adjust_psb), 0, final_list7$adjust_psb)
final_list7$adjust_ps_i <- ifelse(isNull(final_list7$adjust_ps_i), 10000, final_list7$adjust_ps_i)
final_list7$adjust_ps_j <- ifelse(isNull(final_list7$adjust_ps_j), 0, final_list7$adjust_ps_j)
final_list7$adjust_ps <- ifelse(final_list7$adjust_ps == -Inf, 0, final_list7$adjust_ps)
final_list7$adjust_used <- ifelse(isNull(final_list7$adjust_used),0,final_list7$adjust_used)
final_list7$adjust_use <- ifelse(isNull(final_list7$adjust_use),0,final_list7$adjust_use)
final_list7$adjust_useav <- ifelse(isNull(final_list7$adjust_useav),0,final_list7$adjust_useav)
final_list7$adjust_useav_beg <- ifelse(isNull(final_list7$adjust_useav_beg),0,final_list7$adjust_useav_beg)

# COMMAND ----------

adjval <- 0.99

final_list7$Page_Share_Adj <- ifelse(substr(final_list7$Share_Source_PS,1,8)=="Modelled"
                                     ,ifelse(final_list7$adjust_ps_i <= final_list7$index1, 
                                             lit(adjval)^(final_list7$index1-final_list7$adjust_ps_i+1)*final_list7$Page_Share_lin +(lit(1)-(lit(adjval)^(final_list7$index1-final_list7$adjust_ps_i+1)))*final_list7$Page_Share_sig 
                                             ,ifelse(final_list7$adjust_ps_j>final_list7$index1
                                             ,ifelse(final_list7$Page_Share_sig > final_list7$adjust_psb, final_list7$Page_Share_sig, final_list7$adjust_psb)
                                             ,final_list7$Page_Share_sig))
                                     ,final_list7$Share_Raw_PS)

final_list7$Page_Share_Adj <- ifelse(final_list7$Page_Share_Adj>1,1,ifelse(final_list7$Page_Share_Adj<0.05,0.05,final_list7$Page_Share_Adj))


# COMMAND ----------

###ADJUST USAGE
final_list7$adjust_used <- ifelse(final_list7$adjust_used==0,1,final_list7$adjust_used)
final_list7$adjust_use_i <- ifelse(isNull(final_list7$adjust_use_i),0,final_list7$adjust_use_i)
final_list7$Usage_Adj <- ifelse(substr(final_list7$Usage_Source,1,3)=="UPM",
                                              ifelse(final_list7$adjust_use_i <= final_list7$index1,
                                                     ifelse((final_list7$Usage-final_list7$adjust_useav) > 0.05, (final_list7$Usage-final_list7$adjust_useav), 0.05)
                                                    ,ifelse((final_list7$Usage-final_list7$adjust_useav_beg) > 0.05, (final_list7$Usage-final_list7$adjust_useav_beg), 0.05))
                                              ,final_list7$Usage)

#final_list7$Usage_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$adjust_use_i <= final_list7$index1, ifelse((final_list7$Usage-final_list7$adjust_useav) > 0.05, (final_list7$Usage-final_list7$adjust_useav), 0.05), final_list7$Usage), final_list7$Usage)

# COMMAND ----------

final_list7$Page_Share_old <- cast(final_list7$Page_Share_sig, "double")
final_list7$Page_Share <- cast(final_list7$Page_Share_Adj, "double")
final_list7$Usage <- cast(final_list7$Usage_Adj, "double")

final_list7$Pages_Device_UPM <- final_list7$MPV_TS*final_list7$IB
final_list7$Pages_Device_Raw <- final_list7$MPV_Raw*final_list7$IB
final_list7$Pages_Device_Dash <- final_list7$MPV_Dash*final_list7$IB
final_list7$Pages_Device_Use <- final_list7$Usage*final_list7$IB
final_list7$Pages_Share_Model_PS <- final_list7$MPV_TS*final_list7$IB*final_list7$Share_Model_PS
final_list7$Pages_Share_Raw_PS <- final_list7$MPV_Raw*final_list7$IB*cast(final_list7$Share_Raw_PS, "double")
final_list7$Pages_Share_Dash_PS <- final_list7$MPV_Dash*final_list7$IB*cast(final_list7$Share_Raw_PS, "double")
final_list7$Pages_PS <- final_list7$Pages_Device_Use*cast(final_list7$Page_Share, "double")

#final_list7 <- as.data.frame(final_list7)
#final_list7$cal_date <- as.Date(paste(final_list7$FYearMo,"01"),format='%Y%m%d')

final_list7$total_cc <- final_list7$Usage*final_list7$IB
final_list7$hp_cc <- final_list7$Usage*final_list7$IB*final_list7$Page_Share

# COMMAND ----------

printSchema(final_list2)

# COMMAND ----------

final_list8 <- filter(final_list7, !isNull(final_list7$Page_Share))   #missing intro date

final_list8$fiscal_date <- concat_ws(sep = "-", substr(final_list8$FYearMo, 1, 4), substr(final_list8$FYearMo, 5, 6), lit("01"))
#final_list8$model_group <- concat(final_list8$CM, final_list8$SM ,final_list8$Mkt, lit("_"), final_list8$market10, lit("_"), final_list8$Region_DE)
final_list8$year_month_float <- to_date(final_list8$fiscal_date, "yyyy-MM-dd")

today <- Sys.Date()
vsn <- '2021.05.05.1'  #for DUPSM
#vsn <- 'New Version'
rec1 <- 'usage_share'
geog1 <- 'country'
ibversion <- 'LF IB'
fcstnt <-  dbutils.widgets.get("fcst_nt")


# COMMAND ----------

createOrReplaceTempView(final_list8, "final_list8")
cacheTable("final_list8")

# COMMAND ----------

mdm_tbl_share <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , '",fcstnt,"' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Share_Source_PS as data_source
                , '",vsn,"' as version
                , 'hp_share' as measure
                , Page_Share as units
                , Proxy_PS as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                --, model_group
                from final_list8
                 
                 "))


mdm_tbl_usage <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , '",fcstnt,"' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'usage' as measure
                , Usage as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                --, model_group
                from final_list8
                WHERE Usage is not null
                 
                 "))
mdm_tbl_usagen <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , '",fcstnt,"' as forecast_process_note
                , '",today,"' as forecast_created_date
                , 'n' as data_source
                , '",vsn,"' as version
                , 'Usage_n' as measure
                , MPV_n as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                --, model_group
                from final_list8
                WHERE MPV_n is not null AND MPV_n >0
                 
                 "))
###add totals
mdm_tbl_totalcc <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , '",fcstnt,"' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'total_pages' as measure
                , total_cc as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                --, model_group
                from final_list8
                WHERE Usage is not null
                 
                 "))
mdm_tbl_hpcc <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , '",fcstnt,"' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'hp_pages' as measure
                , hp_cc as units
                , Proxy_PS as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                --, model_group
                from final_list8
                WHERE Usage is not null
                 
                 "))
mdm_tbl_ib <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , '",fcstnt,"' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'ib' as measure
                , ib as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                --, model_group
                from final_list8
                WHERE Usage is not null
                 
                 "))


# COMMAND ----------

mdm_tbl <- rbind(mdm_tbl_share, mdm_tbl_usage, mdm_tbl_totalcc, mdm_tbl_hpcc, mdm_tbl_ib, mdm_tbl_usagen)

#mdm_tbl <- rbind(mdm_tbl_share, mdm_tbl_usage, mdm_tbl_usagen, mdm_tbl_sharen, mdm_tbl_kusage, mdm_tbl_cusage)

mdm_tbl$cal_date <- to_date(mdm_tbl$cal_date,format="yyyy-MM-dd")
mdm_tbl$forecast_created_date <- to_date(mdm_tbl$forecast_created_date,format="yyyy-MM-dd")
mdm_tbl$load_date <- to_date(mdm_tbl$load_date,format="yyyy-MM-dd")

createOrReplaceTempView(mdm_tbl, "mdm_tbl")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val mdmTbl = spark.sql("SELECT * FROM mdm_tbl")
# MAGIC 
# MAGIC mdmTbl.write
# MAGIC   .format("parquet")
# MAGIC   .mode("overwrite")
# MAGIC   .partitionBy("measure")
# MAGIC   //.save(s"""s3://${spark.conf.get("aws_bucket_name")}LF_usage_share_${dbutils.widgets.get("outname_dt")}.parquet""")

# COMMAND ----------

notebook_end_time <- Sys.time()
notebook_total_time <- notebook_end_time - notebook_start_time;notebook_total_time
