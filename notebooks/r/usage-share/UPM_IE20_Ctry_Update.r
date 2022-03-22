# Databricks notebook source
# ---
# #Version 2021.03.14.1#
# title: "UPM with IE2.0 IB Country Level"
# output: html_notebook
# ---
# 
# UPM from Cumulus

# COMMAND ----------

notebook_start_time <- Sys.time()

# COMMAND ----------

dbutils.widgets.text("ib_version", "")
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", list("dev", "itg", "prd"))
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("bdtbl", "")

# COMMAND ----------

# MAGIC %run ../../scala/common/Constants

# COMMAND ----------

# MAGIC %run ../../scala/common/DatabaseUtils

# COMMAND ----------

# MAGIC %run ../../python/common/secrets_manager_utils

# COMMAND ----------

# MAGIC %python
# MAGIC # retrieve secrets based on incoming/inputted secrets name - variables will be accessible across languages
# MAGIC 
# MAGIC redshift_secrets = secrets_get(dbutils.widgets.get("redshift_secrets_name"), "us-west-2")
# MAGIC spark.conf.set("redshift_username", redshift_secrets["username"])
# MAGIC spark.conf.set("redshift_password", redshift_secrets["password"])
# MAGIC 
# MAGIC sqlserver_secrets = secrets_get(dbutils.widgets.get("sqlserver_secrets_name"), "us-west-2")
# MAGIC spark.conf.set("sfai_username", sqlserver_secrets["username"])
# MAGIC spark.conf.set("sfai_password", sqlserver_secrets["password"])

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /usr/bin/java
# MAGIC ls -l /etc/alternatives/java
# MAGIC ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/default-java
# MAGIC R CMD javareconf

# COMMAND ----------

packages <- c("rJava", "RJDBC", "DBI", "sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR")

# COMMAND ----------

# MAGIC %run ../common/package_check

# COMMAND ----------

options(java.parameters = "-Xmx60g" )

# setwd("~/work") #Set Work Directory ("~/NPI_Model") ~ for root directory
options(scipen=999) #remove scientific notation
tempdir(check=TRUE)

# COMMAND ----------

# mount s3 bucket to cluster
aws_bucket_name <- "insights-environment-sandbox/BrentT/"
mount_name <- "insights-environment-sandbox"

tryCatch(dbutils.fs.mount(paste0("s3a://", aws_bucket_name), paste0("/mnt/", mount_name)),
 error = function(e)
 print("Mount does not exist or is already mounted to cluster"))

# COMMAND ----------

# MAGIC %scala
# MAGIC var configs: Map[String, String] = Map()
# MAGIC configs += ("stack" -> dbutils.widgets.get("stack"),
# MAGIC             "sfaiUsername" -> spark.conf.get("sfai_username"),
# MAGIC             "sfaiPassword" -> spark.conf.get("sfai_password"),
# MAGIC             "sfaiUrl" -> SFAI_URL,
# MAGIC             "sfaiDriver" -> SFAI_DRIVER,
# MAGIC             "redshiftUsername" -> spark.conf.get("redshift_username"),
# MAGIC             "redshiftPassword" -> spark.conf.get("redshift_password"),
# MAGIC             "redshiftAwsRole" -> dbutils.widgets.get("aws_iam_role"),
# MAGIC             "redshiftUrl" -> s"""jdbc:redshift://${REDSHIFT_URLS(dbutils.widgets.get("stack"))}:${REDSHIFT_PORTS(dbutils.widgets.get("stack"))}/${dbutils.widgets.get("stack")}?ssl_verify=None""",
# MAGIC             "redshiftTempBucket" -> s"""${S3_BASE_BUCKETS(dbutils.widgets.get("stack"))}redshift_temp/""")

# COMMAND ----------

# MAGIC %scala
# MAGIC val zeroiQuery = s""" 
# MAGIC SELECT  tri_printer_usage_sn.printer_platform_name  
# MAGIC                     , tri_printer_usage_sn.printer_region_code 
# MAGIC                     , tri_printer_usage_sn.printer_country_iso_code
# MAGIC                     , tri_printer_usage_sn.date_month_dim_ky as yyyymm
# MAGIC                     --, CASE
# MAGIC                         --WHEN SUBSTRING(tri_printer_usage_sn.date_month_dim_ky,5,6) IN ('11','12') 
# MAGIC                               --THEN (CAST(tri_printer_usage_sn.date_month_dim_ky AS int) + 100 -10)
# MAGIC                         --WHEN SUBSTRING(tri_printer_usage_sn.date_month_dim_ky,5,6) BETWEEN '01' AND'10' 
# MAGIC                               --THEN (CAST(tri_printer_usage_sn.date_month_dim_ky AS int) + 2)
# MAGIC                         --ELSE tri_printer_usage_sn.date_month_dim_ky 
# MAGIC                         --END as FYearMo
# MAGIC                     , CAST(tri_printer_usage_sn.date_month_dim_ky AS int) as FYearMo
# MAGIC                     , tri_printer_usage_sn.printer_chrome_code AS CM
# MAGIC                     , tri_printer_usage_sn.platform_business_code AS EP
# MAGIC                     , tri_printer_usage_sn.printer_function_code as platform_function_code
# MAGIC                     , tri_printer_usage_sn.platform_market_code
# MAGIC                     , SUM(COALESCE(tri_printer_usage_sn.print_pages_total_ib_ext_sum,0)) as UsageNumerator
# MAGIC                     , SUM(COALESCE(tri_printer_usage_sn.print_months_ib_ext_sum,0)) AS UsageDenominator
# MAGIC                     , SUM(tri_printer_usage_sn.printer_count_month_usage_flag_sum) AS SumN
# MAGIC                     FROM 
# MAGIC                       ${dbutils.widgets.get("bdtbl")} tri_printer_usage_sn with (NOLOCK)
# MAGIC                     WHERE 
# MAGIC                       1=1
# MAGIC                       AND printer_route_to_market_ib='Aftermarket'
# MAGIC                    GROUP BY
# MAGIC                       printer_platform_name  
# MAGIC                     , printer_region_code 
# MAGIC                     , printer_country_iso_code
# MAGIC                     , yyyymm
# MAGIC                     , FYearMo
# MAGIC                     , CM
# MAGIC                     , EP
# MAGIC                     , platform_function_code
# MAGIC                     , platform_market_code
# MAGIC """
# MAGIC 
# MAGIC val zeroi = readRedshiftToDF(configs)
# MAGIC   .option("query", zeroiQuery)
# MAGIC   .load()
# MAGIC 
# MAGIC zeroi.createOrReplaceTempView("zeroi")

# COMMAND ----------

sqlserver_driver <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/dbfs/FileStore/jars/801b0636_e136_471a_8bb4_498dc1f9c99b-mssql_jdbc_9_4_0_jre8-13bd8.jar")

cprod <- dbConnect(sqlserver_driver, paste0("jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database=IE2_Prod;user=", sparkR.conf("sfai_username"), ";password=", sparkR.conf("sfai_password")))
clanding <- dbConnect(sqlserver_driver, paste0("jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database=IE2_Landing;user=", sparkR.conf("sfai_username"), ";password=", sparkR.conf("sfai_password")))

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

minSize <- 200

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

#--------Lock weights-----------------------------------------------------------------------------#
lock_weights <- 0   #0 for use calculated, 1 for use stated version
lockwt_file <- 'toner_weights_75_Q4_qe_2021-11-16'

#--------Big Data Table Name----------------------------------------------------------------------#
#bdtbl <- 'dashboard.print_share_usage_agg_stg'
bdtbl <- dbutils.widgets.get("bdtbl")

#--------Ouput Qtr Pulse or Quarter End-----------------------------------------------------------#
outnm_dt <- dbutils.widgets.get("outnm_dt")

# COMMAND ----------

# Step 1 - query for Normalized extract specific to PE and RM

zeroi <- SparkR::collect(SparkR::sql("SELECT * FROM zeroi"))

zeroi$SumMPV <- ifelse(zeroi$usagedenominator>0,zeroi$sumn*zeroi$usagenumerator/zeroi$usagedenominator,0)
# # s3write_using(x=zeroi,FUN = write_parquet, object = paste0("s3://insights-environment-sandbox/BrentT/BD_Zeroi(",Sys.Date(),").parquet"))

#Get Market10 Information
country_info <- dbGetQuery(cprod,"
                      WITH mkt10 AS (
                           SELECT country_alpha2, country_level_2 as market10, country_level_4 as emdm
                           FROM IE2_Prod.dbo.iso_cc_rollup_xref with (NOLOCK)
                           WHERE country_scenario='Market10'
                      ),
                      rgn5 AS (
                            SELECT country_alpha2, region_5, developed_emerging, country 
                            FROM IE2_Prod.dbo.iso_country_code_xref with (NOLOCK)
                      )
                      SELECT a.country_alpha2, a.region_5, b.market10, a.developed_emerging, country
                            FROM rgn5 a
                            LEFT JOIN mkt10 b
                            ON a.country_alpha2=b.country_alpha2
                           ")

#######SELECT IB VERSION#####
#ib_version <- as.character(dbGetQuery(ch,"select max(ib_version) as ib_version from biz_trans.tri_platform_measures_ib"))  #FROM BD Dashboard
ib_version <- dbutils.widgets.get("ib_version")
#ib_version <- as.character(dbGetQuery(cprod,"select max(version) as ib_version from IE2_Prod.dbo.ib with (NOLOCK)"))  #Phoenix

ibtable <- dbGetQuery(cprod,paste0("
                   select  a.platform_subset
                          , a.cal_date
                          ,YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          ,d.technology AS hw_type
                          --,a.customer_engagement  AS RTM
                          ,b.region_5
                          ,a.country
                          --,substring(b.developed_emerging,1,1) as de
                          ,sum(a.units) as ib
                          ,a.version
                    from IE2_Prod.dbo.ib a with (NOLOCK)
                    left join IE2_Prod.dbo.iso_country_code_xref b with (NOLOCK)
                      on (a.country=b.country_alpha2)
                    left join IE2_Prod.dbo.hardware_xref d with (NOLOCK)
                       on (a.platform_subset=d.platform_subset)
                    where a.measure='ib'
                      --and a.version = (select max(version) from IE2_Prod.dbo.ib)
                      and (upper(d.technology)='LASER' or (d.technology='PWA' and (upper(d.hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3')) 
                          or a.platform_subset like 'PANTHER%' or a.platform_subset like 'JAGUAR%'))
                      --and YEAR(a.cal_date) < 2026
                      and a.version='",ib_version,"'
                    group by a.platform_subset, a.cal_date, d.technology  --, a.customer_engagement
                            ,a.version
                          --, b.developed_emerging
                            , b.region_5, a.country
                   "))

lastAvaiableYear <- as.numeric(dbGetQuery(cprod,paste0("
                     select  max(YEAR(cal_date)) 
                      from IE2_Prod.dbo.ib with (NOLOCK)
                      where 1=1
                        --and version = (select max(version) from IE2_Prod.dbo.ib)
                        --and YEAR(a.cal_date) < 2026
                        and version='",ib_version,"'
                     ")))
firstAvaiableYear <- as.numeric(dbGetQuery(cprod,paste0("
                     select  min(YEAR(cal_date)) 
                      from IE2_Prod.dbo.ib with (NOLOCK)
                      where 1=1
                        --and version = (select max(version) from IE2_Prod.dbo.ib)
                        --and YEAR(a.cal_date) < 2026
                        and version='",ib_version,"'
                     ")))
  
hw_info <- dbGetQuery(cprod,"
                      SELECT platform_subset, technology, pl, mono_ppm
                      , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                        WHEN vc_category in ('SWT-H','SWT-H Pro','Dept','Dept-High','WG') then 'ENT'
                        ELSE NULL
                        END as pro_vs_ent
                      , format, mono_color, sf_mf, vc_category, product_structure
                              , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H') THEN 'DSK'
                                    WHEN vc_category in ('SWT-L') THEN 'SWL'
                                    WHEN vc_category in ('SWT-H','SWT-H Pro') THEN 'SWH'
                                    WHEN vc_category in ('Dept','Dept-High') THEN 'DPT'
                                    WHEN vc_category in ('WG') THEN 'WGP'
                                    ELSE NULL
                                END AS platform_market_code
                      FROM IE2_Prod.dbo.hardware_xref with (NOLOCK)
                      WHERE (upper(technology)='LASER' or (technology='PWA' and (upper(hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3')) 
                            or platform_subset like 'PANTHER%' or platform_subset like 'JAGUAR%'))
                      ")
  
zero <- sqldf("with sub0 as (select a.printer_platform_name,a.printer_region_code, c.market10, c.developed_emerging, a.FYearMo, SUBSTR(d.mono_color,1,1) as CM, d.pro_vs_ent as EP, a.platform_function_code, a.platform_market_code, d.product_structure
                  , SUM(SumMPV/SumN*b.ib) AS SumMPV, sum(a.SumN) as SumN, sum(b.ib) as SUMib
              from zeroi a
              left join ibtable b
              on (a.printer_country_iso_code=b.country and a.yyyymm=b.month_begin and a.printer_platform_name=b.platform_subset)
              left join country_info c
              on a.printer_country_iso_code=c.country_alpha2
              left join hw_info d
              on a.printer_platform_name=d.platform_subset
              group by a.printer_platform_name,a.printer_region_code, c.market10, c.developed_emerging, a.FYearMo, d.mono_color, d.pro_vs_ent, a.platform_function_code, a.platform_market_code, d.product_structure)
              , sub_1 AS (
              SELECT printer_platform_name
                      , printer_region_code
                      , market10
                      , SUBSTR(developed_emerging,1,1) as developed_emerging
                      , FYearMo
                      , CM
                      , EP
                      , product_structure
                      , platform_function_code
                      , platform_market_code
                      , SUM(SumMPV) AS SumMPVwtN
                      , SUM(SUMib) AS SumibWt
                      , AVG(SumMPV/SUMib) AS meansummpvwtn
                      , SUM(SumN) AS SumN
                     FROM sub0
                      GROUP BY
                        printer_platform_name
                      , printer_region_code
                      , market10
                      , developed_emerging
                      , FYearMo
                      , CM
                      , EP
                      , product_structure
                      , platform_function_code
                      , platform_market_code
                      ORDER BY
                        printer_platform_name
                      , printer_region_code
                      , FYearMo
                      , CM
                      , EP)
                      select * from sub_1
                      where SumibWt is not null
              ")

zero$pMPV <- ifelse(((zero$FYearMo) >= start1 & (zero$FYearMo <=end1)), zero$meansummpvwtn, NA)
zero$pN <- ifelse(((zero$FYearMo) >= start1 & (zero$FYearMo <=end1)), zero$SumN, NA)
zero$pMPVN <- zero$pMPV*zero$pN

zero_platform <- sqldf('select distinct printer_platform_name, printer_region_code, market10 from zero order by printer_platform_name, printer_region_code')
zero_platform$source1 <-"tri_printer_usage_sn"
zero_platformList <- reshape2::dcast(zero_platform, printer_platform_name ~market10, value.var="source1")

countFACT_LP_MONTH <- sum(!is.na(zero_platformList[, c("Central Europe","Greater Asia", "Greater China","India SL & BL","ISE","Latin America"
                                                       ,"North America","Northern Europe","Southern Europe","UK&I")]))
paste("The total numbers of platform - region combinations that could be retrieved from the FACT PRINTER LASER MONTH table =", countFACT_LP_MONTH)

head(zero)

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
                   SELECT printer_platform_name, market10, developed_emerging
                   , SUM(pMPVN) AS SUMpMPVN										
                   , SUM(pN) AS SUMpN										
                   FROM										
                   zero										
                   GROUP BY printer_platform_name, market10, developed_emerging
                   )AA0	
                   where SUMpN >=", MUTminSize, "
                   and SUMpMPVN is not null
                   ORDER BY printer_platform_name, market10, developed_emerging
                   ", sep = " "))

head(one)
colnames(one)
dim(one)
str(one)

oneT <- reshape2::dcast(one, printer_platform_name ~ market10 + developed_emerging, value.var="dummy")

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
               aa1.printer_platform_name = aa2.printer_platform_name
               and 
               aa1.market10 = aa2.market10
               and 
               aa1.developed_emerging=aa2.developed_emerging
               where 
               aa2.NormMPV is NOT NULL 
               or 
               aa2.NormMPV != 0
               order by 
               printer_platform_name ASC, 								
               printer_region_code ASC, 								
               fyearmo ASC, 								
               CM ASC, 								
               market10 ASC')
  
  two$MPV_Pct <- two$meansummpvwtn/two$NormMPV
  two$MPVPN <- two$MPV_Pct*two$SumN
  str(two)

# COMMAND ----------

# Step 4 - summary stat for a group  defined by EP, CM, printer_region_code and FYearMo

# Using the extract from Step 3, create the following summary stat for a group defined by EP,CM, printer_region_code 
# and FYearMo (notice, Plat_Nm is no more a part of a group); Sum of MPVPN as SumMPVPN and Sum of SumN as SumN

three <- sqldf('select CM, platform_market_code, market10, developed_emerging, FYearMo
               , sum(MPVPN) as SumMPVPN
               , sum(SumN) as SumN
               FROM two
               WHERE CM is not null 
               GROUP by CM, platform_market_code, market10, developed_emerging, FYearMo
               ORDER by CM, platform_market_code, market10, developed_emerging, FYearMo
               ')

# COMMAND ----------

# Step 5 - drop groups if respective number < 200 and create MUT

four <- sqldf(paste("select CM,platform_market_code, market10, developed_emerging, FYearMo 
                      , SumMPVPN
                      , SumN
                      , SumMPVPN/SumN as MUT
                      from three
                      where SumN >=", minSize, "
                      and platform_market_code is not NULL
                      --and EP is not NULL
                      order by CM, platform_market_code, market10, developed_emerging, FYearMo", sep = " "))

# COMMAND ----------

#Step 6 - Create variable STRATA by concatenating 3 variables

# columns to paste together
cols <- c( 'CM', 'platform_market_code', 'market10' , 'developed_emerging')

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

  print(cat)

  d <- subset(four, strata==cat)
  d <- d[d$FYearMo >= startMUT & d$FYearMo < endMUT,]
  d <- d[d$FYearMo < 202003 | d$FYearMo > 202005,]  #remove covid months

  if (nrow(d) == 0) next

  yyyy <- as.numeric(substr(d$FYearMo, 1,4))
  mm <- as.numeric(substr(d$FYearMo, 5,6))

  d$J90Mo <- (yyyy - 1990)*12 + (mm-1)

  d$x <- yyyy + (mm-1)/12

  d$y <- log(d$MUT)

  fit <- lm(y ~ x, data=d)
  fit2 <- lm(y ~ x, data=d, weights=(x-min(x))^2)

  dsub <- tail(d,8)
  fit3 <- lm(y ~ x, data=dsub)
  #abline(fit)
  #summary(fit)
  d$a0 <- fit$coefficients[[1]]
  d$b1 <- fit$coefficients[[2]]
  d$yhat <- d$a0 + d$b1*d$x

  d$a0b <- fit2$coefficients[[1]]
  d$b1b <- fit2$coefficients[[2]]
  d$yhatb <- d$a0b + d$b1b*d$x

  d$a0b <- fit2$coefficients[[1]]
  d$b1b <- fit2$coefficients[[2]]
  d$yhatb <- d$a0b + d$b1b*d$x

  d$a0c <- fit3$coefficients[[1]]
  d$b1c <- fit3$coefficients[[2]]
  d$yhatc <- d$a0c + d$b1c*d$x

  d$Detrend <- d$y - d$yhat
  d$month <- mm

  temp <- sqldf('select month, avg(Detrend) as avgtr from d group by month order by month')
  sumtemp <- sum(temp$avgtr)
  temp$avgtr2 <- temp$avgtr-(sumtemp/12)

  d2 <- sqldf('select aa1.*, aa2.avgtr2 as seasonality from d aa1 left join temp aa2 on aa1.month=aa2.month
              order by FYearMo')

  d2$DTDS <- d2$Detrend - d2$seasonality
  d2$mo_Smooth <-rollmean(d2$DTDS, 5,fill = NA)
  d2$Irregular <- d2$y-d2$yhat-d2$seasonality-d2$mo_Smooth

  d2$Eyhat <- exp(d2$yhat)
  d2$EDetrend <- exp(d2$Detrend)
  d2$Eseasonality <- exp(d2$seasonality)
  d2$Emo_Smooth <- exp(d2$mo_Smooth)
  d2$EIrregular <- exp(d2$Irregular)
  d2$Eyhat2 <- exp(d2$yhatb)
  d2$Eyhat3 <- exp(d2$yhatc)

  ymin <- 0 #min(d2$MUT,d2$Eyhat, d2$EIrregular, d2$Eseasonality, d2$Emo_Smooth, na.rm=TRUE )
  ymax <- max(d2$MUT,d2$Eyhat, d2$EIrregular, d2$Eseasonality, d2$Emo_Smooth, na.rm=TRUE )
  ymid <- (ymin+ymax)/2
  xmin <- min(d2$x)
  xmax <- max(d2$x)
  textout <- paste("old slope:",d$b1[1],"\n wgt slope:",d$b1b[1] ,"\n short slope:",d$b1c[1])


  plot(d2$x, d2$MUT, typ='l', col = "#0096d6", main=paste("Decomposition for",cat),
       xlab="Year", ylab="MUT and Other Series", xlim=c(2011, 2022), ylim=c(ymin, ymax)
       , lwd = 1, frame.plot=FALSE, las=1, xaxt='n'
  )#Blue, las=0: parallel to the axis, 1: always horizontal, 2: perpendicular to the axis, 3: always vertical
  axis(side=1,seq(2011,2019, by=1)) #increase number of years on x axis
  lines(d2$x, d2$Eyhat,col="#822980",lwd=1) #purple
  lines(d2$x, d2$EIrregular,col="#838B8B",lwd = 1)
  lines(d2$x, d2$Eseasonality,col="#fdc643",lwd = 1) #yellow
  lines(d2$x, d2$Emo_Smooth,col="#de2e43",lwd=1) #red
  lines(d2$x, d2$Eyhat2,col="#822980",lwd=1, lty=3) #purple
  lines(d2$x, d2$Eyhat3,col="#822980",lwd=1, lty=3) #purple
  text(2017, ymid, textout, pos = 4)
  box(bty="l") #puts x and y axis lines back in
  grid(nx=NULL, ny=NULL, col="cornsilk2", lty="dotted", lwd=par("lwd"))
  #grid(nx=96, ny=NULL, col="cornsilk2", lty="dotted", lwd=par("lwd"))

  legend("bottom", bty="n", # places a legend at the appropriate place 
         c("Original", "Trend", "Irregular", "Seasonality", "Cyclical","minslope"), # puts text in the legend
         xpd = TRUE, horiz = TRUE,
         lty=c(1,1,1,1,1,3), # gives the legend appropriate symbols (lines)
         lwd=c(1,1,1,1,1,1),col=c("#0096d6", "#822980", "#838B8B", "#fdc643", "#de2e43","#822780"))

  data[[cat]] <- d2

  #print(cat)

}

# COMMAND ----------

# Step 8 - Combining extended MUT dataset for each Strata

# ---- Also implementing the Source Replacement ---------------------------- #

outcome0 <- as.data.frame(do.call("rbind", data))
str(outcome0)
rownames(outcome0) <- NULL

stratpl <- sqldf("SELECT distinct platform_market_code from outcome0")
  stratcs <- sqldf("SELECT distinct CM from outcome0")
  stratmk <- sqldf("SELECT distinct market10, developed_emerging from outcome0")
  stratmn <- sqldf("SELECT distinct month from outcome0")
  stratjn <- sqldf("SELECT a.*, b.*, d.*, e.*
                   from stratpl a , stratcs b ,stratmk d, stratmn e")
  stratjn <- sqldf("SELECT a.*, b.region_5
                   from stratjn a
                   left join (select distinct region_5, market10 from country_info) b
                   on a.market10=b.market10")
  outcome0 <- sqldf("SELECT a.*, b.region_5
                   from outcome0 a
                   left join (select distinct region_5, market10 from country_info) b
                   on a.market10=b.market10")
 
  #Add region, get regional estimates...what else is missing for C4010 E0--India missing
  outcome_o <- sqldf("SELECT platform_market_code, CM, developed_emerging, month, market10, region_5, avg(b1) as b1, avg(seasonality) as seasonality , avg(mo_Smooth) as mo_Smooth
                     from outcome0
                     group by platform_market_code, CM, developed_emerging, month, market10, region_5")
  outcome_r <- sqldf("SELECT platform_market_code, CM, developed_emerging, month, region_5, avg(b1) as b1, avg(seasonality) as seasonality , avg(mo_Smooth) as mo_Smooth
                     from outcome_o
                     group by platform_market_code, CM, developed_emerging, month, region_5")
  
  outcome_a <- sqldf("SELECT platform_market_code, CM, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality , avg(mo_Smooth) as mo_Smooth
                     from outcome_o
                     group by platform_market_code, CM, developed_emerging, month")
  
  outcome_b <- sqldf("SELECT platform_market_code, market10, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality , avg(mo_Smooth) as mo_Smooth
                     from outcome_o
                     group by platform_market_code, market10, developed_emerging, month")
  outcome_c <- sqldf("SELECT CM, market10, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality , avg(mo_Smooth) as mo_Smooth
                     from outcome_o
                     group by CM, market10, developed_emerging, month")
  outcome_d <- sqldf("SELECT CM, market10, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality , avg(mo_Smooth) as mo_Smooth
                     from outcome_o
                     group by CM, market10, developed_emerging, month")
  outcome_e <- sqldf("SELECT  platform_market_code, CM,  market10, developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality , avg(mo_Smooth) as mo_Smooth
                     from outcome_o
                     group by  platform_market_code, CM,  market10, developed_emerging, month")
  outcome_f <- sqldf("SELECT CM,  developed_emerging, month, avg(b1) as b1, avg(seasonality) as seasonality , avg(mo_Smooth) as mo_Smooth
                     from outcome_o
                     group by CM, developed_emerging, month")
  outcome_w <- sqldf("SELECT CM, month, avg(b1) as b1, avg(seasonality) as seasonality, avg(mo_Smooth) as mo_Smooth
                     from outcome_o
                     group by CM, month")
                     
  outcome <- sqldf(" SELECT distinct s.platform_market_code, s.CM, s.market10, s.developed_emerging, s.month
                  , CASE WHEN o.b1 is not null then o.b1
                         WHEN r.b1 is not null then r.b1
                         WHEN a.b1 is not null then a.b1
                         WHEN b.b1 is not null then b.b1
                         WHEN c.b1 is not null then c.b1
                         WHEN d.b1 is not null then d.b1
                         WHEN e.b1 is not null then e.b1
                         WHEN f.b1 is not null then f.b1
                         WHEN w.b1 is not null then w.b1
                         ELSE NULL
                    END AS b1
                  , CASE WHEN o.seasonality is not null then o.seasonality
                         WHEN r.seasonality is not null then r.seasonality
                         WHEN a.seasonality is not null then a.seasonality
                         WHEN b.seasonality is not null then b.seasonality
                         WHEN c.seasonality is not null then c.seasonality
                         WHEN d.seasonality is not null then d.seasonality
                         WHEN e.seasonality is not null then e.seasonality
                         WHEN f.seasonality is not null then f.seasonality
                         WHEN w.seasonality is not null then w.seasonality
                         ELSE NULL
                         END as seasonality
                 , CASE WHEN o.mo_Smooth is not null then o.mo_Smooth
                         WHEN r.mo_Smooth is not null then r.mo_Smooth
                         WHEN a.mo_Smooth is not null then a.mo_Smooth
                         WHEN b.mo_Smooth is not null then b.mo_Smooth
                         WHEN c.mo_Smooth is not null then c.mo_Smooth
                         WHEN d.mo_Smooth is not null then d.mo_Smooth
                         WHEN e.mo_Smooth is not null then e.mo_Smooth
                         WHEN f.mo_Smooth is not null then f.mo_Smooth
                         ELSE NULL
                       END as mo_Smooth
                  , CASE WHEN o.b1 is not null then 'self'
                         WHEN r.b1 is not null then 'reg5'
                         WHEN a.b1 is not null then 'nomkt'
                         WHEN b.b1 is not null then 'nocs'
                         WHEN c.b1 is not null then 'nopl'
                         WHEN d.b1 is not null then 'nomc'
                         WHEN e.b1 is not null then 'nopfc'
                         WHEN f.b1 is not null then 'noplmkt'
                         WHEN w.b1 is not null then 'node'
                         ELSE NULL
                    END AS src
                
                  from stratjn s
                  
                  left join outcome_o o
                  on s.platform_market_code=o.platform_market_code and s.CM=o.CM  
                    and s.market10=o.market10 and s.developed_emerging=o.developed_emerging and s.month=o.month 
                  left join outcome_r r
                  on s.platform_market_code=r.platform_market_code and s.CM=r.CM  
                    and s.developed_emerging=r.developed_emerging and s.month=r.month and s.region_5=r.region_5 
                  left join outcome_a a
                  on s.platform_market_code=a.platform_market_code and s.CM=a.CM 
                    and s.developed_emerging=a.developed_emerging and s.month=a.month 
                  left join outcome_b b
                  on s.platform_market_code=b.platform_market_code 
                    and s.market10=b.market10 and s.developed_emerging=b.developed_emerging and s.month=b.month 
                  left join outcome_c c
                  on s.CM=c.CM 
                    and s.market10=c.market10 and s.developed_emerging=c.developed_emerging and s.month=c.month 
                  left join outcome_d d
                  on s.CM=d.CM  
                    and s.market10=d.market10 and s.developed_emerging=d.developed_emerging and s.month=d.month 
                  left join outcome_e e
                  on s.platform_market_code=e.platform_market_code and s.CM=e.CM 
                    and s.market10=e.market10 and s.developed_emerging=e.developed_emerging and s.month=e.month 
                  left join outcome_f f
                  on s.CM=f.CM 
                    and s.developed_emerging=f.developed_emerging and s.month=f.month 
                  left join outcome_w w
                  on s.CM=w.CM 
                    and s.month=w.month
                   ")
  outcome$strata <- apply( outcome[ , cols ] , 1 , paste , collapse = "_" )
         
  outcome$b1check <- outcome$b1                      #For checking limits
  outcome$b1 <- ifelse(outcome$b1 > -0.01, -.01,outcome$b1)
  outcome$b1 <- ifelse(outcome$b1 < -0.10, -0.10,outcome$b1)
  outcome$b1c <- ifelse(outcome$b1c > -0.01, -.01,outcome$b1c)
  outcome$b1c <- ifelse(outcome$b1c < -0.10, -0.10,outcome$b1c)
  # outcome$b1 <- -0.1
  #outcome$b1c <- -0.1
  ## What to do when decay is not negative ###


# COMMAND ----------

# Step 9 - creating decay matrix

# Create a decay matrix by extracting "b1", specific to each strata (i.e. the combination 
# of EP, CM, and Region_Cd), from the extended MUT dataset.

decay <- sqldf('
               select distinct CM, platform_market_code, market10, developed_emerging, strata, 
               CASE WHEN b1c > 0 THEN 
                  CASE WHEN b1 < 0 THEN b1
                  ELSE -0.01
                  END
                  ELSE 
                  CASE WHEN b1c > b1 THEN b1c
                    ELSE 
                    CASE WHEN b1 < 0 THEN b1
                    ELSE -0.01
                    END
                  END
              END AS b1
              from outcome 
              order by CM, platform_market_code, market10
               ')
str(decay)

# COMMAND ----------

# Step 10 - Query to create Usage output in de-normalized form

  #since changing to Cumulus, and sources are no longer separate, zero is not limited to time (just the values zero$pMPV and zero$pN are), so is not different anymore
usage <- sqldf("with sub0 as (select a.printer_platform_name,a.printer_region_code, c.country_alpha2, c.market10, c.developed_emerging, a.FYearMo, SUBSTR(d.mono_color,1,1) as CM 
                  , d.pro_vs_ent as EP, a.platform_function_code, d.platform_market_code
                  , SUM(SumMPV/SumN*b.ib) AS SumMPV, sum(a.SumN) as SumN, sum(b.ib) as SUMib
              from zeroi a
              left join ibtable b
              on (a.printer_country_iso_code=b.country and a.yyyymm=b.month_begin and a.printer_platform_name=b.platform_subset)
              left join country_info c
              on a.printer_country_iso_code=c.country_alpha2
              left join hw_info d
              on a.printer_platform_name=d.platform_subset 
              group by  a.printer_platform_name,a.printer_region_code, c.country_alpha2, c.market10, c.developed_emerging, a.FYearMo, d.mono_color, d.pro_vs_ent, a.platform_function_code, a.platform_market_code)
              , sub_1 AS (
              SELECT printer_platform_name
                      , printer_region_code
                      , country_alpha2
                      , market10
                      , developed_emerging
                      , FYearMo
                      , CM
                      , EP
                      , platform_function_code
                      , platform_market_code
                      , SUM(SumMPV) AS SumMPVwtN
                      , SUM(SUMib) AS SumibWt
                      , AVG(SumMPV/SUMib) AS MPVa
                      , SUM(SumN) AS Na
                     FROM sub0
                      GROUP BY
                        printer_platform_name
                      , printer_region_code
                      , country_alpha2
                      , market10
                      , developed_emerging
                      , FYearMo
                      , CM
                      , EP
                      , platform_function_code
                      , platform_market_code
                      ORDER BY
                        printer_platform_name
                      , printer_region_code
                      , FYearMo
                      , CM
                      , EP)
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
            group by printer_platform_name, printer_region_code, developed_emerging,FYearMo
            order by printer_platform_name, printer_region_code, developed_emerging,FYearMo
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
iblist <- sqldf("select distinct ib.platform_subset, ci.country_alpha2, ci.region_5, ci.market10, ci.developed_emerging
                from ibtable ib left join country_info ci
                on ib.country=ci.country_alpha2")

sourceR <- sqldf("
                 select ib.platform_subset as printer_platform_name, ib.region_5 as printer_region_code, ib.developed_emerging, ib.market10, ib.country_alpha2,
                 COALESCE(u2.prcN,0) as prcN, COALESCE(u2.mktN,0) as mktN, COALESCE(u2.demN,0) as demN,COALESCE(u2.ctyN,0) as ctyN
                 , case 
                 when u2.ctyN >= 200 then 'country'
                 when u2.demN >=200 then 'dev/em'
                 when u2.mktN >=200 then 'market10'
                 when u2.prcN >=200 then 'region5'
                 when ctyN >= 200 then 'country'
                 else 'None'
                 end as Source_vlook
                 ,oc.src
                 from iblist ib
                 left join u2
                 on ib.platform_subset=u2.printer_platform_name and ib.region_5=u2.printer_region_code and ib.market10=u2.market10 and ib.country_alpha2=u2.country_alpha2
                 left join hw_info hw
                 on ib.platform_subset=hw.platform_subset
                 left join outcome oc
                 on substr(upper(hw.mono_color),1,1)=oc.CM and oc.market10=ib.market10 and substr(upper(ib.developed_emerging),1,1)=oc.developed_emerging
                 and hw.platform_market_code=oc.platform_market_code
                 order by printer_platform_name, printer_region_code
                 ")

str(sourceR)

# COMMAND ----------

# Step 13 - Append Source_vlook to the Usage Matrix

# Append SNIS, SNRM, SNPE, SNWJ, SNJA and Source_vlook to the Usage Matrix (Step 10 outcome) 
# for each of the Plat_Nm and Region_Cd combinations.

usage2 <- sqldf('select aa1.*
                , aa2.Source_vlook, aa2.src 
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
                printer_platform_name, printer_region_code, country_alpha2, fyearmo, CM, EP')

# COMMAND ----------

# Step 14 - Create MPV and N that would be finally worked upon

# Using the outcome of Step 13, Create MPV and N that would be finally worked upon. 

usage3 <- sqldf("with reg as (
                select printer_platform_name, printer_region_code, fyearmo, CM, platform_market_code, SUM(SumMPVwtN) as SMPV, SUM(SumibWt) as SIB
                , SUM(Na) as N
                from usage2
                GROUP BY printer_platform_name, printer_region_code, fyearmo, CM, platform_market_code
                ORDER BY
                printer_platform_name, printer_region_code, fyearmo, CM, platform_market_code
                )
                ,mkt as (
                select printer_platform_name, market10, fyearmo, CM, platform_market_code, SUM(SumMPVwtN) as SMPV, SUM(SumibWt) as SIB
                , SUM(Na) as N
                from usage2
                GROUP BY printer_platform_name, market10, fyearmo, CM, platform_market_code
                ORDER BY
                printer_platform_name, market10, fyearmo, CM, platform_market_code
                )
                ,de as (
                select printer_platform_name, market10, developed_emerging, fyearmo, CM, platform_market_code, SUM(SumMPVwtN) as SMPV, SUM(SumibWt) as SIB
                , SUM(Na) as N
                from usage2
                GROUP BY printer_platform_name, market10, developed_emerging, fyearmo, CM, platform_market_code
                ORDER BY
                printer_platform_name, market10, developed_emerging, fyearmo, CM, platform_market_code
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
                printer_platform_name, printer_region_code, fyearmo, CM, platform_market_code
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
              and aa1.platform_market_code = aa2.platform_market_code
              and aa1.CM = aa2.CM
              and SUBSTR(aa1.developed_emerging,1,1)=aa2.developed_emerging
              order by aa1.printer_platform_name, aa1.printer_region_code, aa1.fyearmo
              ' )

# COMMAND ----------

# Step 16 - extracting Intro year for a given Platform n region

  introYear <- sqldf(" SELECT a.platform_subset as printer_platform_name 
                          , a.Region_5 AS printer_region_code
                          , b.country_alpha2
                          , b.market10
                          , b.developed_emerging
                          , min(a.month_begin) as Intro_FYearMo
                    FROM ibtable a
                    left join country_info b
                      on a.country=b.country_alpha2
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

usage5 <- sqldf('select aa1.*, aa2.Intro_FYearMo from usage4 aa1 inner join introYear3 aa2
                  on 
                  aa1.printer_platform_name=aa2.printer_platform_name
                  and
                  aa1.country_alpha2 = aa2.country_alpha2
                  order by aa1.printer_platform_name, aa1.country_alpha2, aa1.FYearMo
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
  strata <- unique(usage[c("CM", "platform_market_code", "printer_platform_name")])

  usageSummary2TEr5 <- sqldf('select aa1.printer_platform_name, aa2.CM, aa2.platform_market_code, aa1.NA, aa1.EU, aa1.AP, aa1.LA
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
    
  usageSummary2TEmkt10 <- sqldf('select aa1.printer_platform_name, aa2.CM, aa2.platform_market_code, aa1.[Central Europe], aa1.[Greater Asia], aa1.[Greater China], aa1.[India SL & BL], aa1.[ISE], aa1.[Latin America], aa1.[North America], aa1.[Northern Europe], aa1.[Southern Europe], aa1.[UK&I]
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

usageSummary2TEde <- sqldf("select aa1.printer_platform_name, aa2.CM, aa2.platform_market_code,aa1.[Central Europe_Developed],aa1.[Central Europe_Emerging]
                  ,aa1.[Greater Asia_Developed],aa1.[Greater Asia_Emerging],aa1.[Greater China_Developed],aa1.[Greater China_Emerging]
                  ,aa1.[India SL & BL_Emerging],aa1.[ISE_Emerging],aa1.[Latin America_Emerging],aa1.[North America_Developed]
                  ,aa1.[Northern Europe_Developed],aa1.[Southern Europe_Developed],aa1.[UK&I_Developed]
                           --North America_Developed
                           , COALESCE(aa1.[North America_Developed]/aa1.[North America_Developed],0) as [North America_Developed_wt]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[North America_Developed],0) as [Central Europe_Developed_wt]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[North America_Developed],0) as [Central Europe_Emerging_wt]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[North America_Developed],0) as [Greater Asia_Developed_wt]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[North America_Developed],0) as [Greater Asia_Emerging_wt]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[North America_Developed],0) as [Greater China_Developed_wt]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[North America_Developed],0) as [Greater China_Emerging_wt]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[North America_Developed],0) as [India SL & BL_Emerging_wt]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[North America_Developed],0) as [ISE_Emerging_wt]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[North America_Developed],0) as [Latin America_Emerging_wt]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[North America_Developed],0) as [Northern Europe_Developed_wt]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[North America_Developed],0) as [Southern Europe_Developed_wt]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[North America_Developed],0) as [UK&I_Developed_wt]
                           --Central Europe_Developed
                           , COALESCE(aa1.[North America_Developed]/aa1.[Central Europe_Developed],0) as [North America_Developed_wt2]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Central Europe_Developed],0) as [Central Europe_Developed_wt2]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Central Europe_Developed],0) as [Central Europe_Emerging_wt2]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Central Europe_Developed],0) as [Greater Asia_Developed_wt2]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Central Europe_Developed],0) as [Greater Asia_Emerging_wt2]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Central Europe_Developed],0) as [Greater China_Developed_wt2]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Central Europe_Developed],0) as [Greater China_Emerging_wt2]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Central Europe_Developed],0) as [India SL & BL_Emerging_wt2]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Central Europe_Developed],0) as [ISE_Emerging_wt2]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Central Europe_Developed],0) as [Latin America_Emerging_wt2]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Central Europe_Developed],0) as [Northern Europe_Developed_wt2]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Central Europe_Developed],0) as [Southern Europe_Developed_wt2]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Central Europe_Developed],0) as [UK&I_Developed_wt2]
                           --Central Europe_Emerging
                           , COALESCE(aa1.[North America_Developed]/aa1.[Central Europe_Emerging],0) as [North America_Developed_wt3]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Central Europe_Emerging],0) as [Central Europe_Developed_wt3]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Central Europe_Emerging],0) as [Central Europe_Emerging_wt3]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Central Europe_Emerging],0) as [Greater Asia_Developed_wt3]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Central Europe_Emerging],0) as [Greater Asia_Emerging_wt3]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Central Europe_Emerging],0) as [Greater China_Developed_wt3]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Central Europe_Emerging],0) as [Greater China_Emerging_wt3]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Central Europe_Emerging],0) as [India SL & BL_Emerging_wt3]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Central Europe_Emerging],0) as [ISE_Emerging_wt3]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Central Europe_Emerging],0) as [Latin America_Emerging_wt3]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Central Europe_Emerging],0) as [Northern Europe_Developed_wt3]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Central Europe_Emerging],0) as [Southern Europe_Developed_wt3]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Central Europe_Emerging],0) as [UK&I_Developed_wt3]
                           --Greater Asia_Developed
                           , COALESCE(aa1.[North America_Developed]/aa1.[Greater Asia_Developed],0) as [North America_Developed_wt4]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Greater Asia_Developed],0) as [Central Europe_Developed_wt4]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Greater Asia_Developed],0) as [Central Europe_Emerging_wt4]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Greater Asia_Developed],0) as [Greater Asia_Developed_wt4]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Greater Asia_Developed],0) as [Greater Asia_Emerging_wt4]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Greater Asia_Developed],0) as [Greater China_Developed_wt4]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Greater Asia_Developed],0) as [Greater China_Emerging_wt4]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Greater Asia_Developed],0) as [India SL & BL_Emerging_wt4]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Greater Asia_Developed],0) as [ISE_Emerging_wt4]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Greater Asia_Developed],0) as [Latin America_Emerging_wt4]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Greater Asia_Developed],0) as [Northern Europe_Developed_wt4]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Greater Asia_Developed],0) as [Southern Europe_Developed_wt4]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Greater Asia_Developed],0) as [UK&I_Developed_wt4]
                           --Greater Asia_Emerging
                           , COALESCE(aa1.[North America_Developed]/aa1.[Greater Asia_Emerging],0) as [North America_Developed_wt5]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Greater Asia_Emerging],0) as [Central Europe_Developed_wt5]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Greater Asia_Emerging],0) as [Central Europe_Emerging_wt5]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Greater Asia_Emerging],0) as [Greater Asia_Developed_wt5]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Greater Asia_Emerging],0) as [Greater Asia_Emerging_wt5]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Greater Asia_Emerging],0) as [Greater China_Developed_wt5]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Greater Asia_Emerging],0) as [Greater China_Emerging_wt5]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Greater Asia_Emerging],0) as [India SL & BL_Emerging_wt5]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Greater Asia_Emerging],0) as [ISE_Emerging_wt5]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Greater Asia_Emerging],0) as [Latin America_Emerging_wt5]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Greater Asia_Emerging],0) as [Northern Europe_Developed_wt5]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Greater Asia_Emerging],0) as [Southern Europe_Developed_wt5]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Greater Asia_Emerging],0) as [UK&I_Developed_wt5]
                           --Greater China_Developed
                           , COALESCE(aa1.[North America_Developed]/aa1.[Greater China_Developed],0) as [North America_Developed_wt6]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Greater China_Developed],0) as [Central Europe_Developed_wt6]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Greater China_Developed],0) as [Central Europe_Emerging_wt6]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Greater China_Developed],0) as [Greater Asia_Developed_wt6]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Greater China_Developed],0) as [Greater Asia_Emerging_wt6]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Greater China_Developed],0) as [Greater China_Developed_wt6]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Greater China_Developed],0) as [Greater China_Emerging_wt6]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Greater China_Developed],0) as [India SL & BL_Emerging_wt6]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Greater China_Developed],0) as [ISE_Emerging_wt6]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Greater China_Developed],0) as [Latin America_Emerging_wt6]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Greater China_Developed],0) as [Northern Europe_Developed_wt6]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Greater China_Developed],0) as [Southern Europe_Developed_wt6]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Greater China_Developed],0) as [UK&I_Developed_wt6]
                           --Greater China_Emerging
                           , COALESCE(aa1.[North America_Developed]/aa1.[Greater China_Emerging],0) as [North America_Developed_wt7]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Greater China_Emerging],0) as [Central Europe_Developed_wt7]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Greater China_Emerging],0) as [Central Europe_Emerging_wt7]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Greater China_Emerging],0) as [Greater Asia_Developed_wt7]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Greater China_Emerging],0) as [Greater Asia_Emerging_wt7]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Greater China_Emerging],0) as [Greater China_Developed_wt7]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Greater China_Emerging],0) as [Greater China_Emerging_wt7]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Greater China_Emerging],0) as [India SL & BL_Emerging_wt7]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Greater China_Emerging],0) as [ISE_Emerging_wt7]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Greater China_Emerging],0) as [Latin America_Emerging_wt7]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Greater China_Emerging],0) as [Northern Europe_Developed_wt7]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Greater China_Emerging],0) as [Southern Europe_Developed_wt7]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Greater China_Emerging],0) as [UK&I_Developed_wt7]
                           --India SL & BL_Emerging
                           , COALESCE(aa1.[North America_Developed]/aa1.[India SL & BL_Emerging],0) as [North America_Developed_wt8]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[India SL & BL_Emerging],0) as [Central Europe_Developed_wt8]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[India SL & BL_Emerging],0) as [Central Europe_Emerging_wt8]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[India SL & BL_Emerging],0) as [Greater Asia_Developed_wt8]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[India SL & BL_Emerging],0) as [Greater Asia_Emerging_wt8]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[India SL & BL_Emerging],0) as [Greater China_Developed_wt8]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[India SL & BL_Emerging],0) as [Greater China_Emerging_wt8]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[India SL & BL_Emerging],0) as [India SL & BL_Emerging_wt8]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[India SL & BL_Emerging],0) as [ISE_Emerging_wt8]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[India SL & BL_Emerging],0) as [Latin America_Emerging_wt8]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[India SL & BL_Emerging],0) as [Northern Europe_Developed_wt8]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[India SL & BL_Emerging],0) as [Southern Europe_Developed_wt8]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[India SL & BL_Emerging],0) as [UK&I_Developed_wt8]
                           --ISE_Emerging
                           , COALESCE(aa1.[North America_Developed]/aa1.[ISE_Emerging],0) as [North America_Developed_wt9]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[ISE_Emerging],0) as [Central Europe_Developed_wt9]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[ISE_Emerging],0) as [Central Europe_Emerging_wt9]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[ISE_Emerging],0) as [Greater Asia_Developed_wt9]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[ISE_Emerging],0) as [Greater Asia_Emerging_wt9]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[ISE_Emerging],0) as [Greater China_Developed_wt9]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[ISE_Emerging],0) as [Greater China_Emerging_wt9]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[ISE_Emerging],0) as [India SL & BL_Emerging_wt9]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[ISE_Emerging],0) as [ISE_Emerging_wt9]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[ISE_Emerging],0) as [Latin America_Emerging_wt9]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[ISE_Emerging],0) as [Northern Europe_Developed_wt9]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[ISE_Emerging],0) as [Southern Europe_Developed_wt9]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[ISE_Emerging],0) as [UK&I_Developed_wt9]
                           --Latin America_Emerging
                           , COALESCE(aa1.[North America_Developed]/aa1.[Latin America_Emerging],0) as [North America_Developed_wt10]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Latin America_Emerging],0) as [Central Europe_Developed_wt10]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Latin America_Emerging],0) as [Central Europe_Emerging_wt10]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Latin America_Emerging],0) as [Greater Asia_Developed_wt10]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Latin America_Emerging],0) as [Greater Asia_Emerging_wt10]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Latin America_Emerging],0) as [Greater China_Developed_wt10]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Latin America_Emerging],0) as [Greater China_Emerging_wt10]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Latin America_Emerging],0) as [India SL & BL_Emerging_wt10]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Latin America_Emerging],0) as [ISE_Emerging_wt10]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Latin America_Emerging],0) as [Latin America_Emerging_wt10]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Latin America_Emerging],0) as [Northern Europe_Developed_wt10]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Latin America_Emerging],0) as [Southern Europe_Developed_wt10]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Latin America_Emerging],0) as [UK&I_Developed_wt10]
                           --Northern Europe_Developed
                           , COALESCE(aa1.[North America_Developed]/aa1.[Northern Europe_Developed],0) as [North America_Developed_wt11]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Northern Europe_Developed],0) as [Central Europe_Developed_wt11]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Northern Europe_Developed],0) as [Central Europe_Emerging_wt11]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Northern Europe_Developed],0) as [Greater Asia_Developed_wt11]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Northern Europe_Developed],0) as [Greater Asia_Emerging_wt11]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Northern Europe_Developed],0) as [Greater China_Developed_wt11]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Northern Europe_Developed],0) as [Greater China_Emerging_wt11]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Northern Europe_Developed],0) as [India SL & BL_Emerging_wt11]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Northern Europe_Developed],0) as [ISE_Emerging_wt11]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Northern Europe_Developed],0) as [Latin America_Emerging_wt11]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Northern Europe_Developed],0) as [Northern Europe_Developed_wt11]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Northern Europe_Developed],0) as [Southern Europe_Developed_wt11]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Northern Europe_Developed],0) as [UK&I_Developed_wt11]
                           --Southern Europe_Developed
                           , COALESCE(aa1.[North America_Developed]/aa1.[Southern Europe_Developed],0) as [North America_Developed_wt12]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[Southern Europe_Developed],0) as [Central Europe_Developed_wt12]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[Southern Europe_Developed],0) as [Central Europe_Emerging_wt12]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[Southern Europe_Developed],0) as [Greater Asia_Developed_wt12]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[Southern Europe_Developed],0) as [Greater Asia_Emerging_wt12]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[Southern Europe_Developed],0) as [Greater China_Developed_wt12]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[Southern Europe_Developed],0) as [Greater China_Emerging_wt12]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[Southern Europe_Developed],0) as [India SL & BL_Emerging_wt12]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[Southern Europe_Developed],0) as [ISE_Emerging_wt12]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[Southern Europe_Developed],0) as [Latin America_Emerging_wt12]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[Southern Europe_Developed],0) as [Northern Europe_Developed_wt12]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[Southern Europe_Developed],0) as [Southern Europe_Developed_wt12]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[Southern Europe_Developed],0) as [UK&I_Developed_wt12]
                           --UK&I_Developed
                           , COALESCE(aa1.[North America_Developed]/aa1.[UK&I_Developed],0) as [North America_Developed_wt13]
                           , COALESCE(aa1.[Central Europe_Developed]/aa1.[UK&I_Developed],0) as [Central Europe_Developed_wt13]
                           , COALESCE(aa1.[Central Europe_Emerging]/aa1.[UK&I_Developed],0) as [Central Europe_Emerging_wt13]
                           , COALESCE(aa1.[Greater Asia_Developed]/aa1.[UK&I_Developed],0) as [Greater Asia_Developed_wt13]
                           , COALESCE(aa1.[Greater Asia_Emerging]/aa1.[UK&I_Developed],0) as [Greater Asia_Emerging_wt13]
                           , COALESCE(aa1.[Greater China_Developed]/aa1.[UK&I_Developed],0) as [Greater China_Developed_wt13]
                           , COALESCE(aa1.[Greater China_Emerging]/aa1.[UK&I_Developed],0) as [Greater China_Emerging_wt13]
                           , COALESCE(aa1.[India SL & BL_Emerging]/aa1.[UK&I_Developed],0) as [India SL & BL_Emerging_wt13]
                           , COALESCE(aa1.[ISE_Emerging]/aa1.[UK&I_Developed],0) as [ISE_Emerging_wt13]
                           , COALESCE(aa1.[Latin America_Emerging]/aa1.[UK&I_Developed],0) as [Latin America_Emerging_wt13]
                           , COALESCE(aa1.[Northern Europe_Developed]/aa1.[UK&I_Developed],0) as [Northern Europe_Developed_wt13]
                           , COALESCE(aa1.[Southern Europe_Developed]/aa1.[UK&I_Developed],0) as [Southern Europe_Developed_wt13]
                           , COALESCE(aa1.[UK&I_Developed]/aa1.[UK&I_Developed],0) as [UK&I_Developed_wt13]
                           from  usageSummary2Tde aa1
                           left join strata aa2
                             on 
                             aa1.printer_platform_name = aa2.printer_platform_name
                           ")
usageSummary2TEde[usageSummary2TEde == 0] <- NA

usageSummary2TE <- sqldf("select a.[printer_platform_name],a.[CM],a.[platform_market_code]
,a.[NA],a.[EU],a.[AP],a.[LA] ,b.[Central Europe],b.[Greater Asia],b.[Greater China],b.[India SL & BL],b.[ISE],b.[Latin America],b.[North America]
,b.[Northern Europe],b.[Southern Europe],b.[UK&I]
,a.[NA_wt],a.[EU_wt],a.[AP_wt],a.[LA_wt],a.[NA_wt2] ,a.[EU_wt2],a.[AP_wt2],a.[LA_wt2],a.[NA_wt3] ,a.[EU_wt3],a.[AP_wt3],a.[LA_wt3] ,b.[North America_wt],b.[Central Europe_wt],b.[Greater Asia_wt],b.[Greater China_wt],b.[India_wt],b.[ISE_wt]    ,b.[Latin America_wt],b.[Northern Europe_wt],b.[Southern Europe_wt],b.[UK&I_wt],b.[North America_wt2],b.[Central Europe_wt2],b.[Greater Asia_wt2],b.[Greater China_wt2] ,b.[India_wt2],b.[ISE_wt2],b.[Latin America_wt2],b.[Northern Europe_wt2],b.[Southern Europe_wt2],b.[UK&I_wt2],b.[North America_wt3],b.[Central Europe_wt3]
,b.[Greater Asia_wt3],b.[Greater China_wt3],b.[India_wt3],b.[ISE_wt3],b.[Latin America_wt3],b.[Northern Europe_wt3],b.[Southern Europe_wt3],b.[UK&I_wt3]            ,b.[North America_wt4],b.[Central Europe_wt4],b.[Greater Asia_wt4],b.[Greater China_wt4],b.[India_wt4],b.[ISE_wt4],b.[Latin America_wt4],b.[Northern Europe_wt4] ,b.[Southern Europe_wt4],b.[UK&I_wt4],b.[North America_wt5],b.[Central Europe_wt5],b.[Greater Asia_wt5],b.[Greater China_wt5],b.[India_wt5],b.[ISE_wt5]             ,b.[Latin America_wt5],b.[Northern Europe_wt5],b.[Southern Europe_wt5],b.[UK&I_wt5],b.[North America_wt6],b.[Central Europe_wt6],b.[Greater Asia_wt6]
,b.[Greater China_wt6],b.[India_wt6],b.[ISE_wt6],b.[Latin America_wt6],b.[Northern Europe_wt6],b.[Southern Europe_wt6],b.[UK&I_wt6],b.[North America_wt7]
,b.[Central Europe_wt7],b.[Greater Asia_wt7],b.[Greater China_wt7],b.[India_wt7],b.[ISE_wt7],b.[Latin America_wt7],b.[Northern Europe_wt7],b.[Southern Europe_wt7] ,b.[UK&I_wt7],b.[North America_wt8],b.[Central Europe_wt8],b.[Greater Asia_wt8],b.[Greater China_wt8],b.[India_wt8],b.[ISE_wt8],b.[Latin America_wt8]
,b.[Northern Europe_wt8],b.[Southern Europe_wt8],b.[UK&I_wt8],b.[North America_wt9],b.[Central Europe_wt9],b.[Greater Asia_wt9],b.[Greater China_wt9],b.[India_wt9]      ,b.[ISE_wt9],b.[Latin America_wt9],b.[Northern Europe_wt9],b.[Southern Europe_wt9],b.[UK&I_wt9],b.[North America_wt10],b.[Central Europe_wt10],b.[Greater Asia_wt10]   ,b.[Greater China_wt10],b.[India_wt10],b.[ISE_wt10],b.[Latin America_wt10],b.[Northern Europe_wt10],b.[Southern Europe_wt10],b.[UK&I_wt10] 
,c.[Central Europe_Developed],c.[Central Europe_Emerging],c.[Greater Asia_Developed],c.[Greater Asia_Emerging],c.[Greater China_Developed],c.[Greater China_Emerging]
,c.[India SL & BL_Emerging],c.[ISE_Emerging],c.[Latin America_Emerging],c.[North America_Developed],c.[Northern Europe_Developed]
,c.[Southern Europe_Developed],c.[UK&I_Developed],c.[North America_Developed_wt],c.[Central Europe_Developed_wt],c.[Central Europe_Emerging_wt]
,c.[Greater Asia_Developed_wt],c.[Greater Asia_Emerging_wt],c.[Greater China_Developed_wt],c.[Greater China_Emerging_wt],c.[India SL & BL_Emerging_wt]
,c.[ISE_Emerging_wt],c.[Latin America_Emerging_wt],c.[Northern Europe_Developed_wt],c.[Southern Europe_Developed_wt],c.[UK&I_Developed_wt]
,c.[North America_Developed_wt2],c.[Central Europe_Developed_wt2],c.[Central Europe_Emerging_wt2],c.[Greater Asia_Developed_wt2],c.[Greater Asia_Emerging_wt2]
,c.[Greater China_Developed_wt2],c.[Greater China_Emerging_wt2],c.[India SL & BL_Emerging_wt2],c.[ISE_Emerging_wt2],c.[Latin America_Emerging_wt2]
,c.[Northern Europe_Developed_wt2],c.[Southern Europe_Developed_wt2],c.[UK&I_Developed_wt2],c.[North America_Developed_wt3],c.[Central Europe_Developed_wt3]
,c.[Central Europe_Emerging_wt3],c.[Greater Asia_Developed_wt3],c.[Greater Asia_Emerging_wt3],c.[Greater China_Developed_wt3],c.[Greater China_Emerging_wt3]
,c.[India SL & BL_Emerging_wt3],c.[ISE_Emerging_wt3],c.[Latin America_Emerging_wt3],c.[Northern Europe_Developed_wt3],c.[Southern Europe_Developed_wt3]
,c.[UK&I_Developed_wt3],c.[North America_Developed_wt4],c.[Central Europe_Developed_wt4],c.[Central Europe_Emerging_wt4],c.[Greater Asia_Developed_wt4]
,c.[Greater Asia_Emerging_wt4],c.[Greater China_Developed_wt4],c.[Greater China_Emerging_wt4],c.[India SL & BL_Emerging_wt4],c.[ISE_Emerging_wt4]
,c.[Latin America_Emerging_wt4],c.[Northern Europe_Developed_wt4],c.[Southern Europe_Developed_wt4],c.[UK&I_Developed_wt4],c.[North America_Developed_wt5]
,c.[Central Europe_Developed_wt5],c.[Central Europe_Emerging_wt5],c.[Greater Asia_Developed_wt5],c.[Greater Asia_Emerging_wt5],c.[Greater China_Developed_wt5]
,c.[Greater China_Emerging_wt5],c.[India SL & BL_Emerging_wt5],c.[ISE_Emerging_wt5],c.[Latin America_Emerging_wt5],c.[Northern Europe_Developed_wt5]
,c.[Southern Europe_Developed_wt5],c.[UK&I_Developed_wt5],c.[North America_Developed_wt6],c.[Central Europe_Developed_wt6],c.[Central Europe_Emerging_wt6]
,c.[Greater Asia_Developed_wt6],c.[Greater Asia_Emerging_wt6],c.[Greater China_Developed_wt6],c.[Greater China_Emerging_wt6],c.[India SL & BL_Emerging_wt6]
,c.[ISE_Emerging_wt6],c.[Latin America_Emerging_wt6],c.[Northern Europe_Developed_wt6],c.[Southern Europe_Developed_wt6],c.[UK&I_Developed_wt6]
,c.[North America_Developed_wt7],c.[Central Europe_Developed_wt7],c.[Central Europe_Emerging_wt7],c.[Greater Asia_Developed_wt7],c.[Greater Asia_Emerging_wt7]
,c.[Greater China_Developed_wt7],c.[Greater China_Emerging_wt7],c.[India SL & BL_Emerging_wt7],c.[ISE_Emerging_wt7],c.[Latin America_Emerging_wt7]
,c.[Northern Europe_Developed_wt7],c.[Southern Europe_Developed_wt7],c.[UK&I_Developed_wt7],c.[North America_Developed_wt8],c.[Central Europe_Developed_wt8]
,c.[Central Europe_Emerging_wt8],c.[Greater Asia_Developed_wt8],c.[Greater Asia_Emerging_wt8],c.[Greater China_Developed_wt8],c.[Greater China_Emerging_wt8]
,c.[India SL & BL_Emerging_wt8],c.[ISE_Emerging_wt8],c.[Latin America_Emerging_wt8],c.[Northern Europe_Developed_wt8],c.[Southern Europe_Developed_wt8]
,c.[UK&I_Developed_wt8],c.[North America_Developed_wt9],c.[Central Europe_Developed_wt9],c.[Central Europe_Emerging_wt9],c.[Greater Asia_Developed_wt9]
,c.[Greater Asia_Emerging_wt9],c.[Greater China_Developed_wt9],c.[Greater China_Emerging_wt9],c.[India SL & BL_Emerging_wt9],c.[ISE_Emerging_wt9]
,c.[Latin America_Emerging_wt9],c.[Northern Europe_Developed_wt9],c.[Southern Europe_Developed_wt9],c.[UK&I_Developed_wt9],c.[North America_Developed_wt10]
,c.[Central Europe_Developed_wt10],c.[Central Europe_Emerging_wt10],c.[Greater Asia_Developed_wt10],c.[Greater Asia_Emerging_wt10],c.[Greater China_Developed_wt10]
,c.[Greater China_Emerging_wt10],c.[India SL & BL_Emerging_wt10],c.[ISE_Emerging_wt10],c.[Latin America_Emerging_wt10],c.[Northern Europe_Developed_wt10]
,c.[Southern Europe_Developed_wt10],c.[UK&I_Developed_wt10],c.[North America_Developed_wt11],c.[Central Europe_Developed_wt11],c.[Central Europe_Emerging_wt11]
,c.[Greater Asia_Developed_wt11],c.[Greater Asia_Emerging_wt11],c.[Greater China_Developed_wt11],c.[Greater China_Emerging_wt11],c.[India SL & BL_Emerging_wt11]
,c.[ISE_Emerging_wt11],c.[Latin America_Emerging_wt11],c.[Northern Europe_Developed_wt11],c.[Southern Europe_Developed_wt11],c.[UK&I_Developed_wt11]
,c.[North America_Developed_wt12],c.[Central Europe_Developed_wt12],c.[Central Europe_Emerging_wt12],c.[Greater Asia_Developed_wt12],c.[Greater Asia_Emerging_wt12]
,c.[Greater China_Developed_wt12],c.[Greater China_Emerging_wt12],c.[India SL & BL_Emerging_wt12],c.[ISE_Emerging_wt12],c.[Latin America_Emerging_wt12]
,c.[Northern Europe_Developed_wt12],c.[Southern Europe_Developed_wt12],c.[UK&I_Developed_wt12],c.[North America_Developed_wt13],c.[Central Europe_Developed_wt13]
,c.[Central Europe_Emerging_wt13],c.[Greater Asia_Developed_wt13],c.[Greater Asia_Emerging_wt13],c.[Greater China_Developed_wt13],c.[Greater China_Emerging_wt13]
,c.[India SL & BL_Emerging_wt13],c.[ISE_Emerging_wt13],c.[Latin America_Emerging_wt13],c.[Northern Europe_Developed_wt13],c.[Southern Europe_Developed_wt13]
,c.[UK&I_Developed_wt13]
                         from usageSummary2TEr5 a 
                         left join usageSummary2TEmkt10 b
                          on a.printer_platform_name=b.printer_platform_name and a.CM=b.CM and a.platform_market_code=b.platform_market_code
                         left join usageSummary2TEde c
                          on a.printer_platform_name=c.printer_platform_name and a.CM=c.CM and a.platform_market_code=c.platform_market_code
")

# COMMAND ----------

wtaverage <- sqldf('select cm, platform_market_code
                   , avg(EU_wt) as EUa
                   , avg([Central Europe_wt]) as Central_Europena
                   , avg([Central Europe_Developed_wt]) as Central_Europe_Dna
                   , avg([Central Europe_Emerging_wt]) as Central_Europe_Ena
                   , avg([Northern Europe_wt]) as Northern_Europena   --dont need to split as only developed
                   , avg([Southern Europe_wt]) as Southern_Europena
                   , avg([ISE_wt]) as ISEna
                   , avg([UK&I_wt]) as UKIna
                   , avg([Greater Asia_wt]) as Greater_Asiana
                   , avg([Greater Asia_Developed_wt]) as Greater_Asia_Dna
                   , avg([Greater Asia_Emerging_wt]) as Greater_Asia_Ena
                   , avg([Greater China_wt]) as Greater_Chinana
                   , avg([Greater China_Developed_wt]) as Greater_China_Dna
                   , avg([Greater China_Emerging_wt]) as Greater_China_Ena
                   , avg([India_wt]) as Indiana
                   , avg([Latin America_wt]) as Latin_Americana
                   --
                   , avg([North America_wt2]) as North_Americace
                   , avg([Central Europe_Emerging_wt2]) as Central_Europe_Eced
                   , avg([Central Europe_Developed_wt3]) as Central_Europe_Dcee
                   , avg([Northern Europe_wt2]) as Northern_Europece
                   , avg([Southern Europe_wt2]) as Southern_Europece
                   , avg([ISE_wt2]) as ISEce
                   , avg([UK&I_wt2]) as UKIce
                   , avg([Greater Asia_wt2]) as Greater_Asiace
                   , avg([Greater Asia_Developed_wt2]) as Greater_Asia_Dced
                   , avg([Greater Asia_Emerging_wt2]) as Greater_Asia_Eced
                   , avg([Greater Asia_Developed_wt3]) as Greater_Asia_Dcee
                   , avg([Greater Asia_Emerging_wt3]) as Greater_Asia_Ecee
                   , avg([Greater China_wt2]) as Greater_Chinace
                   , avg([Greater China_Developed_wt2]) as Greater_China_Dced
                   , avg([Greater China_Emerging_wt2]) as Greater_China_Eced
                   , avg([Greater China_Developed_wt3]) as Greater_China_Dcee
                   , avg([Greater China_Emerging_wt3]) as Greater_China_Ecee
                   , avg([India_wt2]) as Indiace
                   , avg([Latin America_wt2]) as Latin_Americace
                   --
                   , avg([Central Europe_wt3]) as Central_Europega
                   , avg([Central Europe_Emerging_wt4]) as Central_Europe_Egad
                   , avg([Central Europe_Developed_wt4]) as Central_Europe_Dgad
                   , avg([Central Europe_Emerging_wt5]) as Central_Europe_Egae
                   , avg([Central Europe_Developed_wt5]) as Central_Europe_Dgae
                   , avg([Northern Europe_wt3]) as Northern_Europega
                   , avg([Southern Europe_wt3]) as Southern_Europega
                   , avg([ISE_wt3]) as ISEga
                   , avg([UK&I_wt3]) as UKIga
                   , avg([North America_wt3]) as North_Americaga
                   , avg([Greater Asia_Developed_wt5]) as Greater_Asia_Dgae
                   , avg([Greater Asia_Emerging_wt4]) as Greater_Asia_Egad
                   , avg([Greater China_wt3]) as Greater_Chinaga
                   , avg([Greater China_Developed_wt4]) as Greater_China_Dgad
                   , avg([Greater China_Emerging_wt4]) as Greater_China_Egad
                   , avg([Greater China_Developed_wt5]) as Greater_China_Dgae
                   , avg([Greater China_Emerging_wt5]) as Greater_China_Egae
                   , avg([India_wt3]) as Indiaga
                   , avg([Latin America_wt3]) as Latin_Americaga
                   --
                   , avg([Central Europe_wt4]) as Central_Europegc
                   , avg([Central Europe_Emerging_wt6]) as Central_Europe_Egcd
                   , avg([Central Europe_Developed_wt6]) as Central_Europe_Dgcd
                   , avg([Central Europe_Emerging_wt7]) as Central_Europe_Egce
                   , avg([Central Europe_Developed_wt7]) as Central_Europe_Dgce
                   , avg([Northern Europe_wt4]) as Northern_Europegc
                   , avg([Southern Europe_wt4]) as Southern_Europegc
                   , avg([ISE_wt4]) as ISEgc
                   , avg([UK&I_wt4]) as UKIgc
                   , avg([Greater Asia_wt4]) as Greater_Asiagc
                   , avg([Greater Asia_Developed_wt6]) as Greater_Asia_Dgcd
                   , avg([Greater Asia_Emerging_wt6]) as Greater_Asia_Egcd
                   , avg([Greater Asia_Developed_wt7]) as Greater_Asia_Dgce
                   , avg([Greater Asia_Emerging_wt7]) as Greater_Asia_Egce
                   , avg([Greater China_Developed_wt7]) as Greater_China_Dgce
                   , avg([Greater China_Emerging_wt6]) as Greater_China_Egcd
                   , avg([North America_wt4]) as North_Americagc
                   , avg([India_wt4]) as Indiagc
                   , avg([Latin America_wt4]) as Latin_Americagc
                   --
                   , avg([Central Europe_wt5]) as Central_Europeia
                   , avg([Central Europe_Emerging_wt8]) as Central_Europe_Eia
                   , avg([Central Europe_Developed_wt8]) as Central_Europe_Dia
                   , avg([Northern Europe_wt5]) as Northern_Europeia
                   , avg([Southern Europe_wt5]) as Southern_Europeia
                   , avg([ISE_wt5]) as ISEia
                   , avg([UK&I_wt5]) as UKIia
                   , avg([Greater Asia_wt5]) as Greater_Asiaia
                   , avg([Greater Asia_Developed_wt8]) as Greater_Asia_Dia
                   , avg([Greater Asia_Emerging_wt8]) as Greater_Asia_Eia
                   , avg([Greater China_wt5]) as Greater_Chinaia
                   , avg([Greater China_Developed_wt8]) as Greater_China_Dia
                   , avg([Greater China_Emerging_wt8]) as Greater_China_Eia
                   , avg([North America_wt5]) as North_Americaia
                   , avg([Latin America_wt5]) as Latin_Americaia
                   --
                   , avg([Central Europe_wt6]) as Central_Europeis
                   , avg([Central Europe_Emerging_wt9]) as Central_Europe_Eis
                   , avg([Central Europe_Developed_wt9]) as Central_Europe_Dis
                   , avg([Northern Europe_wt6]) as Northern_Europeis
                   , avg([Southern Europe_wt6]) as Southern_Europeis
                   , avg([North America_wt6]) as North_Americais
                   , avg([UK&I_wt6]) as UKIis
                   , avg([Greater Asia_wt6]) as Greater_Asiais
                   , avg([Greater Asia_Developed_wt9]) as Greater_Asia_Dis
                   , avg([Greater Asia_Emerging_wt9]) as Greater_Asia_Eis
                   , avg([Greater China_wt6]) as Greater_Chinais
                   , avg([Greater China_Developed_wt9]) as Greater_China_Dis
                   , avg([Greater China_Emerging_wt9]) as Greater_China_Eis
                   , avg([India_wt6]) as Indiais
                   , avg([Latin America_wt6]) as Latin_Americais
                   --
                   , avg([Central Europe_wt7]) as Central_Europela
                   , avg([Central Europe_Emerging_wt10]) as Central_Europe_Ela
                   , avg([Central Europe_Developed_wt10]) as Central_Europe_Dla
                   , avg([Northern Europe_wt7]) as Northern_Europela
                   , avg([Southern Europe_wt7]) as Southern_Europela
                   , avg([ISE_wt7]) as ISEla
                   , avg([UK&I_wt7]) as UKIla
                   , avg([Greater Asia_wt7]) as Greater_Asiala
                   , avg([Greater Asia_Developed_wt10]) as Greater_Asia_Dla
                   , avg([Greater Asia_Emerging_wt10]) as Greater_Asia_Ela
                   , avg([Greater China_wt7]) as Greater_Chinala
                   , avg([Greater China_Developed_wt10]) as Greater_China_Dla
                   , avg([Greater China_Emerging_wt10]) as Greater_China_Ela
                   , avg([India_wt7]) as Indiala
                   , avg([North America_wt7]) as North_Americala
                   --
                   , avg([Central Europe_wt8]) as Central_Europene
                   , avg([Central Europe_Emerging_wt11]) as Central_Europe_Ene
                   , avg([Central Europe_Developed_wt11]) as Central_Europe_Dne
                   , avg([North America_wt8]) as Nort_Americane
                   , avg([Southern Europe_wt8]) as Southern_Europene
                   , avg([ISE_wt8]) as ISEne
                   , avg([UK&I_wt8]) as UKIne
                   , avg([Greater Asia_wt8]) as Greater_Asiane
                   , avg([Greater Asia_Developed_wt11]) as Greater_Asia_Dne
                   , avg([Greater Asia_Emerging_wt11]) as Greater_Asia_Ene
                   , avg([Greater China_wt8]) as Greater_Chinane
                   , avg([Greater China_Developed_wt11]) as Greater_China_Dne
                   , avg([Greater China_Emerging_wt11]) as Greater_China_Ene
                   , avg([India_wt8]) as Indiane
                   , avg([Latin America_wt8]) as Latin_Americane
                   --
                   , avg([Central Europe_wt9]) as Central_Europese
                   , avg([Central Europe_Emerging_wt12]) as Central_Europe_Ese
                   , avg([Central Europe_Developed_wt12]) as Central_Europe_Dse
                   , avg([Northern Europe_wt9]) as Northern_Europese
                   , avg([North America_wt9]) as North_Americase
                   , avg([ISE_wt9]) as ISEse
                   , avg([UK&I_wt9]) as UKIse
                   , avg([Greater Asia_wt9]) as Greater_Asiase
                   , avg([Greater Asia_Developed_wt12]) as Greater_Asia_Dse
                   , avg([Greater Asia_Emerging_wt12]) as Greater_Asia_Ese
                   , avg([Greater China_wt9]) as Greater_Chinase
                   , avg([Greater China_Developed_wt12]) as Greater_China_Dse
                   , avg([Greater China_Emerging_wt12]) as Greater_China_Ese
                   , avg([India_wt9]) as Indiase
                   , avg([Latin America_wt9]) as Latin_Americase
                   --
                   , avg([Central Europe_wt10]) as Central_Europeuk
                   , avg([Central Europe_Emerging_wt13]) as Central_Europe_Euk
                   , avg([Central Europe_Developed_wt13]) as Central_Europe_Duk
                   , avg([Northern Europe_wt10]) as Northern_Europeuk
                   , avg([Southern Europe_wt10]) as Southern_Europeuk
                   , avg([ISE_wt10]) as ISEuk
                   , avg([North America_wt10]) as North_Americauk
                   , avg([Greater Asia_wt10]) as Greater_Asiauk
                   , avg([Greater Asia_Developed_wt13]) as Greater_Asia_Duk
                   , avg([Greater Asia_Emerging_wt13]) as Greater_Asia_Euk
                   , avg([Greater China_wt10]) as Greater_Chinauk
                   , avg([Greater China_Developed_wt13]) as Greater_China_Duk
                   , avg([Greater China_Emerging_wt13]) as Greater_China_Euk
                   , avg([India_wt10]) as Indiauk
                   , avg([Latin America_wt10]) as Latin_Americauk
                   --
                   , avg(LA_wt) as LAa 
                   , avg(AP_wt) as APa 
                   , avg(AP_wt2) as APa2 
                   , avg(LA_wt2) as LAa2
                   , avg(LA_wt3) as LAa3
                   from usageSummary2TE 
                   where 
                   cm is not null 
                   group by cm, platform_market_code')
wtaverage[is.na(wtaverage)] <- 1

#  #---Write out weight file------------#
#  s3write_using(x=wtaverage,FUN = write.csv, object = paste0("s3://insights-environment-sandbox/BrentT/toner_weights_75_",outnm_dt,"_(",Sys.Date(),").csv"), row.names=FALSE, na="")
 
 if(lock_weights==1) {
   wtavgin <- s3read_using(FUN = read_csv, object = paste0("s3://", aws_bucket_name, lockwt_file,".csv"), col_names=TRUE, na="")
   wtaverage <- wtavgin
  }

# COMMAND ----------

usageSummary2TE_D2<-sqldf('select aa1.*,aa2.[EUa],aa2.[Central_Europena],aa2.[Central_Europe_Dna],aa2.[Central_Europe_Ena],aa2.[Northern_Europena]
,aa2.[Southern_Europena],aa2.[ISEna],aa2.[UKIna],aa2.[Greater_Asiana],aa2.[Greater_Asia_Dna],aa2.[Greater_Asia_Ena],aa2.[Greater_Chinana]
,aa2.[Greater_China_Dna],aa2.[Greater_China_Ena],aa2.[Indiana],aa2.[Latin_Americana],aa2.[North_Americace],aa2.[Central_Europe_Eced],aa2.[Central_Europe_Dcee]
,aa2.[Northern_Europece],aa2.[Southern_Europece],aa2.[ISEce],aa2.[UKIce],aa2.[Greater_Asiace],aa2.[Greater_Asia_Dced],aa2.[Greater_Asia_Eced]
,aa2.[Greater_Asia_Dcee],aa2.[Greater_Asia_Ecee],aa2.[Greater_Chinace],aa2.[Greater_China_Dced],aa2.[Greater_China_Eced],aa2.[Greater_China_Dcee]
,aa2.[Greater_China_Ecee],aa2.[Indiace],aa2.[Latin_Americace],aa2.[Central_Europega],aa2.[Central_Europe_Egad],aa2.[Central_Europe_Dgad],aa2.[Central_Europe_Egae]
,aa2.[Central_Europe_Dgae],aa2.[Northern_Europega],aa2.[Southern_Europega],aa2.[ISEga],aa2.[UKIga],aa2.[North_Americaga],aa2.[Greater_Asia_Dgae],aa2.[Greater_Asia_Egad],aa2.[Greater_Chinaga],aa2.[Greater_China_Dgad],aa2.[Greater_China_Egad],aa2.[Greater_China_Dgae],aa2.[Greater_China_Egae],aa2.[Indiaga],aa2.[Latin_Americaga]
,aa2.[Central_Europegc],aa2.[Central_Europe_Egcd],aa2.[Central_Europe_Dgcd],aa2.[Central_Europe_Egce],aa2.[Central_Europe_Dgce],aa2.[Northern_Europegc]
,aa2.[Southern_Europegc],aa2.[ISEgc],aa2.[UKIgc],aa2.[Greater_Asiagc],aa2.[Greater_Asia_Dgcd],aa2.[Greater_Asia_Egcd],aa2.[Greater_Asia_Dgce],aa2.[Greater_Asia_Egce]
,aa2.[Greater_China_Dgce],aa2.[Greater_China_Egcd],aa2.[North_Americagc],aa2.[Indiagc],aa2.[Latin_Americagc],aa2.[Central_Europeia],aa2.[Central_Europe_Eia]
,aa2.[Central_Europe_Dia],aa2.[Northern_Europeia],aa2.[Southern_Europeia],aa2.[ISEia],aa2.[UKIia],aa2.[Greater_Asiaia],aa2.[Greater_Asia_Dia]
,aa2.[Greater_Asia_Eia],aa2.[Greater_Chinaia],aa2.[Greater_China_Dia],aa2.[Greater_China_Eia],aa2.[North_Americaia],aa2.[Latin_Americaia],aa2.[Central_Europeis]
,aa2.[Central_Europe_Eis],aa2.[Central_Europe_Dis],aa2.[Northern_Europeis],aa2.[Southern_Europeis],aa2.[North_Americais],aa2.[UKIis],aa2.[Greater_Asiais]
,aa2.[Greater_Asia_Dis],aa2.[Greater_Asia_Eis],aa2.[Greater_Chinais],aa2.[Greater_China_Dis],aa2.[Greater_China_Eis],aa2.[Indiais],aa2.[Latin_Americais]
,aa2.[Central_Europela],aa2.[Central_Europe_Ela],aa2.[Central_Europe_Dla],aa2.[Northern_Europela],aa2.[Southern_Europela],aa2.[ISEla],aa2.[UKIla]
,aa2.[Greater_Asiala],aa2.[Greater_Asia_Dla],aa2.[Greater_Asia_Ela],aa2.[Greater_Chinala],aa2.[Greater_China_Dla],aa2.[Greater_China_Ela],aa2.[Indiala]
,aa2.[North_Americala],aa2.[Central_Europene],aa2.[Central_Europe_Ene],aa2.[Central_Europe_Dne],aa2.[Nort_Americane],aa2.[Southern_Europene],aa2.[ISEne]
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
                              (aa1.CM = aa2.CM
                              and 
                              aa1.platform_market_code = aa2.platform_market_code)
                              order by CM, platform_market_code')

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
                                WHEN [North America] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dna
                                WHEN [North America] is null and [UK&I] is not null then [UK&I]*1/UKIna
                                WHEN [North America] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiana
                                WHEN [North America] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Ena
                                WHEN [North America] is null and [ISE] is not null then [ISE]*1/ISEna
                                WHEN [North America] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dna
                                WHEN [North America] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Ena
                                WHEN [North America] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dna
                                WHEN [North America] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Ena
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
                                WHEN [UK&I] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Duk
                                WHEN [UK&I] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Duk
                                WHEN [UK&I] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Duk
                                WHEN [UK&I] is null and [ISE] is not null then [ISE]*1/ISEuk
                                WHEN [UK&I] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiauk
                                WHEN [UK&I] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Euk
                                WHEN [UK&I] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Euk
                                WHEN [UK&I] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Euk
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
                                WHEN [Northern Europe] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dne
                                WHEN [Northern Europe] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dne
                                WHEN [Northern Europe] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dne
                                WHEN [Northern Europe] is null and [ISE] is not null then [ISE]*1/ISEne
                                WHEN [Northern Europe] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiane
                                WHEN [Northern Europe] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Ene
                                WHEN [Northern Europe] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Ene
                                WHEN [Northern Europe] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Ene
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
                                WHEN [Southern Europe] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dse
                                WHEN [Southern Europe] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dse
                                WHEN [Southern Europe] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dse
                                WHEN [Southern Europe] is null and [ISE] is not null then [ISE]*1/ISEse
                                WHEN [Southern Europe] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiase
                                WHEN [Southern Europe] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Ese
                                WHEN [Southern Europe] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Ese
                                WHEN [Southern Europe] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Ese
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
                                WHEN [ISE] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dis
                                WHEN [ISE] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dis
                                WHEN [ISE] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dis
                                WHEN [ISE] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiais
                                WHEN [ISE] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Eis
                                WHEN [ISE] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Eis
                                WHEN [ISE] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Eis
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
                                WHEN [Central Europe_Developed] is not null then [Central Europe_Developed]*1
                                WHEN [Central Europe_Developed] is null and [North America] is not null then [North America]*Central_Europena
                                WHEN [Central Europe_Developed] is null and [UK&I] is not null then [UK&I]*1/UKIce
                                WHEN [Central Europe_Developed] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europece
                                WHEN [Central Europe_Developed] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europece
                                WHEN [Central Europe_Developed] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dcee
                                WHEN [Central Europe_Developed] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dced
                                WHEN [Central Europe_Developed] is null and [ISE] is not null then [ISE]*1/ISEce
                                WHEN [Central Europe_Developed] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiace
                                WHEN [Central Europe_Developed] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Dcee
                                WHEN [Central Europe_Developed] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Dcee
                                WHEN [Central Europe_Developed] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Dcee
                                WHEN [Central Europe_Developed] is null and [Latin America] is not null then [Latin America]*1/Latin_Americace
                                ELSE null
                                END as Central_Europe_D2
                                , CASE
                                WHEN [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1
                                WHEN [Central Europe_Emerging] is null and [North America] is not null then [North America]*Central_Europena
                                WHEN [Central Europe_Emerging] is null and [UK&I] is not null then [UK&I]*1/UKIce
                                WHEN [Central Europe_Emerging] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europece
                                WHEN [Central Europe_Emerging] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europece
                                WHEN [Central Europe_Emerging] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dcee
                                WHEN [Central Europe_Emerging] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dcee
                                WHEN [Central Europe_Emerging] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dcee
                                WHEN [Central Europe_Emerging] is null and [ISE] is not null then [ISE]*1/ISEce
                                WHEN [Central Europe_Emerging] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiace
                                WHEN [Central Europe_Emerging] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Ecee
                                WHEN [Central Europe_Emerging] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Ecee
                                WHEN [Central Europe_Emerging] is null and [Latin America] is not null then [Latin America]*1/Latin_Americace
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
                                WHEN [India SL & BL] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dia
                                WHEN [India SL & BL] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dia
                                WHEN [India SL & BL] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dia
                                WHEN [India SL & BL] is null and [ISE] is not null then [ISE]*1/ISEia
                                WHEN [India SL & BL] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Eia
                                WHEN [India SL & BL] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Eia
                                WHEN [India SL & BL] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Eia
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
                                WHEN [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1
                                WHEN [Greater Asia_Developed] is null and [North America] is not null then [North America]*Greater_Asiana
                                WHEN [Greater Asia_Developed] is null and [UK&I] is not null then [UK&I]*1/UKIga
                                WHEN [Greater Asia_Developed] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europega
                                WHEN [Greater Asia_Developed] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europega
                                WHEN [Greater Asia_Developed] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dgad
                                WHEN [Greater Asia_Developed] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dgad
                                WHEN [Greater Asia_Developed] is null and [ISE] is not null then [ISE]*1/ISEga
                                WHEN [Greater Asia_Developed] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiaga
                                WHEN [Greater Asia_Developed] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Egad
                                WHEN [Greater Asia_Developed] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Egad
                                WHEN [Greater Asia_Developed] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Egad
                                WHEN [Greater Asia_Developed] is null and [Latin America] is not null then [Latin America]*1/Latin_Americaga
                                ELSE null
                                END as Greater_Asia_D2
                                , CASE
                                WHEN [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1
                                WHEN [Greater Asia_Emerging] is null and [North America] is not null then [North America]*Greater_Asiana
                                WHEN [Greater Asia_Emerging] is null and [UK&I] is not null then [UK&I]*1/UKIga
                                WHEN [Greater Asia_Emerging] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europega
                                WHEN [Greater Asia_Emerging] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europega
                                WHEN [Greater Asia_Emerging] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dgae
                                WHEN [Greater Asia_Emerging] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dgae
                                WHEN [Greater Asia_Emerging] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dgae
                                WHEN [Greater Asia_Emerging] is null and [ISE] is not null then [ISE]*1/ISEga
                                WHEN [Greater Asia_Emerging] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiaga
                                WHEN [Greater Asia_Emerging] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Egae
                                WHEN [Greater Asia_Emerging] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Egae
                                WHEN [Greater Asia_Emerging] is null and [Latin America] is not null then [Latin America]*1/Latin_Americaga
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
                                WHEN [Greater China_Developed] is not null then [Greater China_Developed]*1
                                WHEN [Greater China_Developed] is null and [North America] is not null then [North America]*Greater_China_Dna
                                WHEN [Greater China_Developed] is null and [UK&I] is not null then [UK&I]*1/UKIgc
                                WHEN [Greater China_Developed] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europegc
                                WHEN [Greater China_Developed] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europegc
                                WHEN [Greater China_Developed] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dgcd
                                WHEN [Greater China_Developed] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dgcd
                                WHEN [Greater China_Developed] is null and [ISE] is not null then [ISE]*1/ISEgc
                                WHEN [Greater China_Developed] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiagc
                                WHEN [Greater China_Developed] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Egcd
                                WHEN [Greater China_Developed] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Egcd
                                WHEN [Greater China_Developed] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Egcd
                                WHEN [Greater China_Developed] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
                                ELSE null
                                END as Greater_China_D2
                                , CASE
                                WHEN [Greater China_Emerging] is not null then [Greater China_Emerging]*1
                                WHEN [Greater China_Emerging] is null and [North America] is not null then [North America]*Greater_China_Ena
                                WHEN [Greater China_Emerging] is null and [UK&I] is not null then [UK&I]*1/UKIgc
                                WHEN [Greater China_Emerging] is null and [Northern Europe] is not null then [Northern Europe]*1/Northern_Europegc
                                WHEN [Greater China_Emerging] is null and [Southern Europe] is not null then [Southern Europe]*1/Southern_Europegc
                                WHEN [Greater China_Emerging] is null and [Central Europe_Developed] is not null then [Central Europe_developed]*1/Central_Europe_Dgce
                                WHEN [Greater China_Emerging] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dgce
                                WHEN [Greater China_Emerging] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_Asia_Dgce
                                WHEN [Greater China_Emerging] is null and [ISE] is not null then [ISE]*1/ISEgc
                                WHEN [Greater China_Emerging] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiagc
                                WHEN [Greater China_Emerging] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Egce
                                WHEN [Greater China_Emerging] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Egce
                                WHEN [Greater China_Emerging] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
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
                                WHEN [Latin America] is null and [Central Europe_Developed] is not null then [Central Europe_Developed]*1/Central_Europe_Dla
                                WHEN [Latin America] is null and [Greater Asia_Developed] is not null then [Greater Asia_Developed]*1/Greater_Asia_Dla
                                WHEN [Latin America] is null and [Greater China_Developed] is not null then [Greater China_Developed]*1/Greater_China_Dla
                                WHEN [Latin America] is null and [ISE] is not null then [ISE]*1/ISEla
                                WHEN [Latin America] is null and [India SL & BL] is not null then [India SL & BL]*1/Indiala
                                WHEN [Latin America] is null and [Central Europe_Emerging] is not null then [Central Europe_Emerging]*1/Central_Europe_Ela
                                WHEN [Latin America] is null and [Greater Asia_Emerging] is not null then [Greater Asia_Emerging]*1/Greater_Asia_Ela
                                WHEN [Latin America] is null and [Greater China_Emerging] is not null then [Greater China_Emerging]*1/Greater_China_Ela
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
                                WHEN [North America_Developed] is not null then 'Self'
                                WHEN [North America_Developed] is null and [UK&I_Developed] is not null then 'UK'
                                WHEN [North America_Developed] is null and [Northern Europe_Developed] is not null then 'NE'
                                WHEN [North America_Developed] is null and [Southern Europe_Developed] is not null then 'SE'
                                WHEN [North America_Developed] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [North America_Developed] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [North America_Developed] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [North America_Developed] is null and [ISE_Emerging] is not null then 'IS'
                                WHEN [North America_Developed] is null and [India SL & BL_Emerging] is not null then 'IN'
                                WHEN [North America_Developed] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [North America_Developed] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [North America_Developed] is null and [Greater China_Emerging] is not null then 'GCE'
                                WHEN [North America_Developed] is null and [Latin America_Emerging] is not null then 'LA'
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
                                WHEN [UK&I] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [UK&I] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [UK&I] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [UK&I] is null and [ISE] is not null then 'IS'
                                WHEN [UK&I] is null and [India SL & BL] is not null then 'IN'
                                WHEN [UK&I] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [UK&I] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [UK&I] is null and [Greater China_Emerging] is not null then 'GCE'
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
                                WHEN [Northern Europe] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [Northern Europe] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [Northern Europe] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [Northern Europe] is null and [ISE] is not null then 'IS'
                                WHEN [Northern Europe] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Northern Europe] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [Northern Europe] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [Northern Europe] is null and [Greater China_Emerging] is not null then 'GCE'
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
                                WHEN [Southern Europe] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [Southern Europe] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [Southern Europe] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [Southern Europe] is null and [ISE] is not null then 'IS'
                                WHEN [Southern Europe] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Southern Europe] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [Southern Europe] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [Southern Europe] is null and [Greater China_Emerging] is not null then 'GCE'
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
                                WHEN [ISE] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [ISE] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [ISE] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [ISE] is null and [India SL & BL] is not null then 'IN'
                                WHEN [ISE] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [ISE] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [ISE] is null and [Greater China_Emerging] is not null then 'GCE'
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
                                WHEN [Central Europe_Developed] is not null then 'Self'
                                WHEN [Central Europe_Developed] is null and [North America] is not null then 'NA'
                                WHEN [Central Europe_Developed] is null and [UK&I] is not null then 'UK'
                                WHEN [Central Europe_Developed] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Central Europe_Developed] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Central Europe_Developed] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [Central Europe_Developed] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [Central Europe_Developed] is null and [ISE] is not null then 'IS'
                                WHEN [Central Europe_Developed] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Central Europe_Developed] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [Central Europe_Developed] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [Central Europe_Developed] is null and [Greater China_Emerging] is not null then 'GCE'
                                WHEN [Central Europe_Developed] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as Central_Europe_DRoute
                                , CASE
                                WHEN [Central Europe_Emerging] is not null then 'Self'
                                WHEN [Central Europe_Emerging] is null and [North America] is not null then 'NA'
                                WHEN [Central Europe_Emerging] is null and [UK&I] is not null then 'UK'
                                WHEN [Central Europe_Emerging] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Central Europe_Emerging] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Central Europe_Emerging] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [Central Europe_Emerging] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [Central Europe_Emerging] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [Central Europe_Emerging] is null and [ISE] is not null then 'IS'
                                WHEN [Central Europe_Emerging] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Central Europe_Emerging] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [Central Europe_Emerging] is null and [Greater China_Emerging] is not null then 'GCE'
                                WHEN [Central Europe_Emerging] is null and [Latin America] is not null then 'LA'
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
                                WHEN [India SL & BL] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [India SL & BL] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [India SL & BL] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [India SL & BL] is null and [ISE] is not null then 'IS'
                                WHEN [India SL & BL] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [India SL & BL] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [India SL & BL] is null and [Greater China_Emerging] is not null then 'GCE'
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
                                WHEN [Greater Asia_Developed] is not null then 'Self'
                                WHEN [Greater Asia_Developed] is null and [North America] is not null then 'NA'
                                WHEN [Greater Asia_Developed] is null and [UK&I] is not null then 'UK'
                                WHEN [Greater Asia_Developed] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Greater Asia_Developed] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Greater Asia_Developed] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [Greater Asia_Developed] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [Greater Asia_Developed] is null and [ISE] is not null then 'IS'
                                WHEN [Greater Asia_Developed] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Greater Asia_Developed] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [Greater Asia_Developed] is null and [Greater China_Emerging] is not null then 'GCE'
                                WHEN [Greater Asia_Developed] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [Greater Asia_Developed] is null and [Latin America] is not null then 'LA'
                                ELSE null
                                END as Greater_Asia_DRoute
                                , CASE
                                WHEN [Greater Asia_Emerging] is not null then 'Self'
                                WHEN [Greater Asia_Emerging] is null and [North America] is not null then 'NA'
                                WHEN [Greater Asia_Emerging] is null and [UK&I] is not null then 'UK'
                                WHEN [Greater Asia_Emerging] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Greater Asia_Emerging] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Greater Asia_Emerging] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [Greater Asia_Emerging] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [Greater Asia_Emerging] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [Greater Asia_Emerging] is null and [ISE] is not null then 'IS'
                                WHEN [Greater Asia_Emerging] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Greater Asia_Emerging] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [Greater Asia_Emerging] is null and [Greater China_Emerging] is not null then 'GCE'
                                WHEN [Greater Asia_Emerging] is null and [Latin America] is not null then 'LA'
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
                                WHEN [Greater China_Developed] is not null then 'Self'
                                WHEN [Greater China_Developed] is null and [North America] is not null then 'NA'
                                WHEN [Greater China_Developed] is null and [UK&I] is not null then 'UK'
                                WHEN [Greater China_Developed] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Greater China_Developed] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Greater China_Developed] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [Greater China_Developed] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [Greater China_Developed] is null and [ISE] is not null then 'IS'
                                WHEN [Greater China_Developed] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Greater China_Developed] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [Greater China_Developed] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [Greater China_Developed] is null and [Greater China_Emerging] is not null then 'GCE'
                                WHEN [Greater China_Developed] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
                                ELSE null
                                END as Greater_China_DRoute
                                , CASE
                                WHEN [Greater China_Emerging] is not null then 'Self'
                                WHEN [Greater China_Emerging] is null and [North America] is not null then 'NA'
                                WHEN [Greater China_Emerging] is null and [UK&I] is not null then 'UK'
                                WHEN [Greater China_Emerging] is null and [Northern Europe] is not null then 'NE'
                                WHEN [Greater China_Emerging] is null and [Southern Europe] is not null then 'SE'
                                WHEN [Greater China_Emerging] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [Greater China_Emerging] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [Greater China_Emerging] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [Greater China_Emerging] is null and [ISE] is not null then 'IS'
                                WHEN [Greater China_Emerging] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Greater China_Emerging] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [Greater China_Emerging] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [Greater China_Emerging] is null and [Latin America] is not null then [Latin America]*1/Latin_Americagc
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
                                WHEN [Latin America] is null and [Central Europe_Developed] is not null then 'CED'
                                WHEN [Latin America] is null and [Greater Asia_Developed] is not null then 'GAD'
                                WHEN [Latin America] is null and [Greater China_Developed] is not null then 'GCD'
                                WHEN [Latin America] is null and [ISE] is not null then 'IS'
                                WHEN [Latin America] is null and [India SL & BL] is not null then 'IN'
                                WHEN [Latin America] is null and [Central Europe_Emerging] is not null then 'CEE'
                                WHEN [Latin America] is null and [Greater Asia_Emerging] is not null then 'GAE'
                                WHEN [Latin America] is null and [Greater China_Emerging] is not null then 'GCE'
                                ELSE null
                                END as Latin_America_ERoute
                                
                              from usageSummary2TE_D2
                              order by CM, platform_market_code
                              ")
   
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
                       ,CASE 
                             WHEN a.Source_vlook = 'None' then 
                              CASE
                              WHEN a.src = 'reg5' THEN 'region5'
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
                        END as Route2
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

q4 <- (
    "
    SELECT DISTINCT platform_subset AS printer_platform_name
          , substring(mono_color,1,1) AS CM
          , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                        WHEN vc_category in ('SWT-H','SWT-H Pro','Dept','Dept-High','WG') then 'ENT'
                        ELSE NULL
                        END AS EP
          , CASE WHEN por_ampv > 0 THEN por_ampv 
            ELSE NULL
            END AS product_usage_por_pages
          , mono_ppm AS print_mono_speed_pages
          , color_ppm AS print_color_speed_pages
          , intro_price
          , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H') THEN 'DSK'
                 WHEN vc_category in ('SWT-L') THEN 'SWL'
                 WHEN vc_category in ('SWT-H','SWT-H Pro') THEN 'SWH'
                 WHEN vc_category in ('Dept','Dept-High') THEN 'DPT'
                 WHEN vc_category in ('WG') THEN 'WGP'
                 ELSE NULL
            END AS platform_finance_market_category_code
          , pl as product_line_code
          , format as platform_page_category
        FROM
          ie2_Prod.dbo.hardware_xref
        WHERE (upper(technology) ='LASER' or (technology='PWA' and (upper(hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3')) 
                            or platform_subset like 'PANTHER%' or platform_subset like 'JAGUAR%'))
    "
  )  
  
  #PoR <- dbGetQuery(ch,q4)
  PoR_1 <- dbGetQuery(cprod,q4)
  PoR_1$product_usage_por_pages <- as.numeric(PoR_1$product_usage_por_pages)
  PoR_1$IsDSK <- ifelse(PoR_1$platform_finance_market_category_code =="DSK",1,0)
  PoR_1$IsDPT <- ifelse(PoR_1$platform_finance_market_category_code =="DPT",1,0)
 
q4b <- (
    "with aa1 as (
        SELECT DISTINCT printer_platform_name
          , max(printer_intro_price) as product_intro_price
        FROM
          ie2_Landing.dbo.tri_printer_ref_landing
        WHERE printer_technology_type in ('LASER','PWA')
        GROUP BY printer_platform_name
    )
    , aa2 as (
    SELECT DISTINCT printer_platform_name
          , CASE WHEN printer_usage_por_pages > 0 THEN printer_usage_por_pages
            ELSE NULL END as product_usage_por_pages
        FROM
          ie2_Landing.dbo.tri_printer_ref_landing
        WHERE printer_technology_type in ('LASER','PWA')
        )
     
    SELECT a.*,min(b.product_usage_por_pages) as product_usage_por_pages 
        from aa1 a 
        left join aa2 b 
        on a.printer_platform_name=b.printer_platform_name
        group by a.printer_platform_name, a.product_intro_price
    "
  )  
  PoR_2 <- dbGetQuery(clanding,q4b)
  
    PoR <- sqldf("select a.printer_platform_name 
                ,a.cm
                ,a.ep
                ,a.platform_finance_market_category_code as platform_market_code
                ,a.product_usage_por_pages, CASE WHEN a.intro_price is null THEN b.product_intro_price ELSE a.intro_price END AS product_intro_price
                ,a.print_mono_speed_pages, a.print_color_speed_pages, a.product_line_code, a.platform_page_category
                ,a.isdsk, a.isdpt
                FROM PoR_1 a
                LEFT JOIN PoR_2 b
                ON a.printer_platform_name=b.printer_platform_name
        ")
  PoR <- subset(PoR, !is.na(CM))
  PoR$print_mono_speed_pages <- as.numeric(PoR$print_mono_speed_pages)
  PoR$print_color_speed_pages <- as.numeric(PoR$print_color_speed_pages)
  PoR_platformLIST <- sqldf('select distinct printer_platform_name, "product_ref" as Source from PoR')
    #missing PWA price
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="AUTOBAHN",ifelse(is.na(PoR$product_intro_price),1866,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="AUTOBAHN MANAGED",ifelse(is.na(PoR$product_intro_price),1999,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BAJA HI DW SFP I-INK",ifelse(is.na(PoR$product_intro_price),599,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BAJA HI DW SFP MONO",ifelse(is.na(PoR$product_intro_price),449,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BAJA LO DW SFP I-INK",ifelse(is.na(PoR$product_intro_price),399,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BAJA LO DW SFP MONO",ifelse(is.na(PoR$product_intro_price),449,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BLACKBIRD",ifelse(is.na(PoR$product_intro_price),1999,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BLACKBIRD CDK",ifelse(is.na(PoR$product_intro_price),1866,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BLACKBIRD MANAGED",ifelse(is.na(PoR$product_intro_price),1999,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BUGATTI 45",ifelse(is.na(PoR$product_intro_price),5874,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BUGATTI 50 MANAGED 9C",ifelse(is.na(PoR$product_intro_price),4762,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BUGATTI 55",ifelse(is.na(PoR$product_intro_price),8462,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BUGATTI 60 MANAGED 9C",ifelse(is.na(PoR$product_intro_price),4762,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BUGATTI MANAGED GENERIC",ifelse(is.na(PoR$product_intro_price),15254,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="EVERETT",ifelse(is.na(PoR$product_intro_price),229,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="FORRESTER",ifelse(is.na(PoR$product_intro_price),329,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="ICEMAN HI DW M SFP",ifelse(is.na(PoR$product_intro_price),1299,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="ICEMAN HI DW SFP",ifelse(is.na(PoR$product_intro_price),599,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="ICEMAN LO DN SFP",ifelse(is.na(PoR$product_intro_price),499,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="ICEMAN LO MINUS DW SFP",ifelse(is.na(PoR$product_intro_price),299,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="JAGUAR 40 MANAGED",ifelse(is.na(PoR$product_intro_price),8181,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="JAGUAR 60 MANAGED",ifelse(is.na(PoR$product_intro_price),4812,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="JAGUAR MANAGED GENERIC",ifelse(is.na(PoR$product_intro_price),13379,PoR$product_intro_price),PoR$product_intro_price)
  
  #missing laser price
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="AGATE 22 MANAGED",ifelse(is.na(PoR$product_intro_price),9178,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="AGATE 28 MANAGED",ifelse(is.na(PoR$product_intro_price),10524,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="AMBER 25 MANAGED",ifelse(is.na(PoR$product_intro_price),1886,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="AMBER 30 MANAGED",ifelse(is.na(PoR$product_intro_price),2215,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BANFF",ifelse(is.na(PoR$product_intro_price),2499,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="BLACKROCK",ifelse(is.na(PoR$product_intro_price),179,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="CHINA HORNET",ifelse(is.na(PoR$product_intro_price),129,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="CICADA PLUS ROW",ifelse(is.na(PoR$product_intro_price),149,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="CRICKET",ifelse(is.na(PoR$product_intro_price),179,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="DIAMOND LITE 40 MANAGED",ifelse(is.na(PoR$product_intro_price) ,16745,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="FAIRMONT MANAGED",ifelse(is.na(PoR$product_intro_price),3186,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="FAIRMONT",ifelse(is.na(PoR$product_intro_price),1799,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="FIREBIRD 60",ifelse(is.na(PoR$product_intro_price),6224,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="GECKO",ifelse(is.na(PoR$product_intro_price),179,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="HECTOR",ifelse(is.na(PoR$product_intro_price),249,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="HORNET",ifelse(is.na(PoR$product_intro_price),129,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="KOBE",ifelse(is.na(PoR$product_intro_price),649,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="ONYX 25 MANAGED",ifelse(is.na(PoR$product_intro_price),7836,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="QUARTZ LITE COPIER",ifelse(is.na(PoR$product_intro_price),429,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="SEAGULL 2 EM",ifelse(is.na(PoR$product_intro_price),179,PoR$product_intro_price),PoR$product_intro_price)  
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="SWAN LITE 3:1",ifelse(is.na(PoR$product_intro_price),159,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="SWAN LITE 4:1",ifelse(is.na(PoR$product_intro_price),179,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="TITANIUM CDK",ifelse(is.na(PoR$product_intro_price),1699,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="TITANIUM KIOSK",ifelse(is.na(PoR$product_intro_price),1340,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="TSUNAMI 3:1 ROW",ifelse(is.na(PoR$product_intro_price),149,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="TWEETIE",ifelse(is.na(PoR$product_intro_price),1279,PoR$product_intro_price),PoR$product_intro_price)
  PoR$product_intro_price <- ifelse(PoR$printer_platform_name=="WHITEHAWK",ifelse(is.na(PoR$product_intro_price),179,PoR$product_intro_price),PoR$product_intro_price)

# COMMAND ----------

# Step - 50 -- extracting platform specific general IntroDate and create Old-Future type

old <- dbGetQuery(cprod,paste("
   WITH ibset as (
        SELECT ib.platform_subset, cc.country_level_2 as region_code, cr.developed_emerging
        ,cal_date 
        ,CASE
          WHEN MONTH(cal_date) > 10 THEN  concat(YEAR(cal_date)+1,reverse(substring(reverse(concat('0000',MONTH(cal_date)-10)),1,2)))
          ELSE concat(YEAR(cal_date),reverse(substring(reverse(concat('0000',MONTH(cal_date)+2)),1,2)))
          END as fyearmo
        FROM IE2_Prod.dbo.ib ib
        LEFT JOIN IE2_Prod.dbo.iso_country_code_xref cr
        ON ib.country=cr.country_alpha2
        LEFT JOIN (select * from IE2_Prod.dbo.iso_cc_rollup_xref where country_scenario='Market10') cc
        ON ib.country=cc.country_alpha2
   )
   , aa0 AS (
      SELECT substring(ref.mono_color,1,1) AS cm
    , ib.region_code AS printer_region_code
    , ib.platform_subset AS printer_platform_name
    , ib.developed_emerging
    , CASE WHEN ref.vc_category in ('ULE','PLE-L','PLE-H') THEN 'DSK'
           WHEN ref.vc_category in ('SWT-L') THEN 'SWL'
           WHEN ref.vc_category in ('SWT-H','SWT-H Pro') THEN 'SWH'
           WHEN ref.vc_category in ('Dept','Dept-High') THEN 'DPT'
           WHEN ref.vc_category in ('WG') THEN 'WGP'
           ELSE NULL
           END AS platform_market_code
    , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                        WHEN vc_category in ('SWT-H','SWT-H Pro','Dept','Dept-High','WG') then 'ENT'
                        ELSE NULL
                        END AS ep
    , MIN(ib.fyearmo * 1) AS INTRODATE
    FROM IE2_Prod.dbo.hardware_xref ref
    INNER JOIN
      ibset ib
    ON (ref.platform_subset=ib.platform_subset)
    WHERE ref.platform_subset != '?' and (upper(ref.technology) ='LASER' or (ref.technology='PWA' and (upper(ref.hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3')) 
                            or ref.platform_subset like 'PANTHER%' or ref.platform_subset like 'JAGUAR%')) and ref.pl not in ('E0','E4','ED','GW')
    GROUP BY ref.mono_color
    , ib.platform_subset
    , ib.region_code
    , ib.developed_emerging
    , CASE WHEN ref.vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                        WHEN ref.vc_category in ('SWT-H','SWT-H Pro','Dept','Dept-High','WG') then 'ENT'
                        ELSE NULL
                        END
    , CASE WHEN ref.vc_category in ('ULE','PLE-L','PLE-H') THEN 'DSK'
           WHEN ref.vc_category in ('SWT-L') THEN 'SWL'
           WHEN ref.vc_category in ('SWT-H','SWT-H Pro') THEN 'SWH'
           WHEN ref.vc_category in ('Dept','Dept-High') THEN 'DPT'
           WHEN ref.vc_category in ('WG') THEN 'WGP'
           ELSE NULL END
  )

  , aa1b as (
    SELECT
    cm, ep, platform_market_code, printer_platform_name, printer_region_code, INTRODATE
    , CASE WHEN printer_region_code='North America' THEN  INTRODATE ELSE NULL END AS INTRODATE_NA
    , CASE WHEN printer_region_code='Northern Europe' THEN INTRODATE ELSE NULL END AS INTRODATE_NE
    , CASE WHEN printer_region_code='Southern Europe' THEN INTRODATE ELSE NULL END AS INTRODATE_SE
    , CASE WHEN printer_region_code='Central Europe' AND developed_emerging='Developed' THEN INTRODATE ELSE NULL END AS INTRODATE_CED
    , CASE WHEN printer_region_code='Central Europe' AND developed_emerging='Emerging' THEN INTRODATE ELSE NULL END AS INTRODATE_CEE
    , CASE WHEN printer_region_code='UK&I' THEN INTRODATE ELSE NULL END AS INTRODATE_UK
    , CASE WHEN printer_region_code='ISE' THEN INTRODATE ELSE NULL END AS INTRODATE_IS
    , CASE WHEN printer_region_code='India SL & BL' THEN  INTRODATE ELSE NULL END AS INTRODATE_IN
    , CASE WHEN printer_region_code='Greater Asia' AND developed_emerging='Developed' THEN INTRODATE ELSE NULL END AS INTRODATE_GAD
    , CASE WHEN printer_region_code='Greater Asia' AND developed_emerging='Emerging' THEN INTRODATE ELSE NULL END AS INTRODATE_GAE
    , CASE WHEN printer_region_code='Greater China' AND developed_emerging='Developed' THEN INTRODATE ELSE NULL END AS INTRODATE_GCD
    , CASE WHEN printer_region_code='Greater China' AND developed_emerging='Emerging' THEN INTRODATE ELSE NULL END AS INTRODATE_GCE
    , CASE WHEN printer_region_code='Latin America' THEN  INTRODATE ELSE NULL END AS INTRODATE_LA
    FROM
    aa0
    )
  , aa1 as (
  SELECT
    cm, ep, platform_market_code, printer_platform_name
    , MAX(INTRODATE_NA) AS INTRODATE_NA
    , MAX(INTRODATE_NE) AS INTRODATE_NE
    , MAX(INTRODATE_SE) AS INTRODATE_SE
    , MAX(INTRODATE_CED) AS INTRODATE_CED
    , MAX(INTRODATE_CEE) AS INTRODATE_CEE
    , MAX(INTRODATE_UK) AS INTRODATE_UK
    , MAX(INTRODATE_IS) AS INTRODATE_IS
    , MAX(INTRODATE_IN) AS INTRODATE_IN
    , MAX(INTRODATE_GAD) AS INTRODATE_GAD
    , MAX(INTRODATE_GAE) AS INTRODATE_GAE
    , MAX(INTRODATE_GCD) AS INTRODATE_GCD
    , MAX(INTRODATE_GCE) AS INTRODATE_GCE
    , MAX(INTRODATE_LA) AS INTRODATE_LA
    , MIN(INTRODATE) AS INTRODATE
  FROM aa1b
  GROUP BY cm, ep, platform_market_code, printer_platform_name
  )

    , bb2 AS (
    SELECT *
    , CASE
    WHEN ",end1,"-INTRODATE > 0 THEN 'OLD'
    ELSE 'FUTURE'
    END AS platform_type
    FROM
    aa1
    )
    
    
    SELECT *
    FROM
    bb2
    ORDER BY CM, EP, platform_market_code, printer_platform_name
    ",sep = " ", collapse = NULL
))

  head(old)
  colnames(old)
  dim(old)
  str(old)
  
  # "dropList" is the list of platforms which are recorded both as Old and Future
  
  dropList <- old[old$platform_type=="FUTURE",]$printer_platform_name[old[old$platform_type=="FUTURE",]$printer_platform_name %in% (usagesummaryNAEUAP$printer_platform_name)]; dropList

# COMMAND ----------

# Step - 51 attching introdate, platform type with PoR table

PoR2 <- sqldf('select aa1.*, aa2.INTRODATE_NA, aa2.INTRODATE_NE, aa2.INTRODATE_SE, aa2.INTRODATE_CED, aa2.INTRODATE_CEE, aa2.INTRODATE_UK, aa2.INTRODATE_IS, aa2.INTRODATE_IN, aa2.INTRODATE_GAD
              , aa2.INTRODATE_GAE, aa2.INTRODATE_GCD, aa2.INTRODATE_GCE, aa2.INTRODATE_LA, aa2.INTRODATE as Intro_FYearMo, aa2.platform_type
              from PoR aa1
              inner join
              old aa2
              on
              aa1.printer_platform_name=aa2.printer_platform_name
              ')

PoR2$J90Mo <- (as.numeric(substr(PoR2$Intro_FYearMo, 1,4)) - 1990)*12 + (as.numeric(substr(PoR2$Intro_FYearMo, 5,6))-1)
str(PoR2)

# COMMAND ----------

# Step 51A extracting platform-dim

q5B <- ("SELECT distinct platform_subset AS printer_platform_name 
                 , format AS platform_speed_segment_code
                 , sf_mf AS SM
                 , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                      WHEN vc_category in ('SWT-H','SWT-H Pro','Dept','Dept-High','WG') then 'ENT'
                      ELSE NULL
                      END AS EP
                 , substring(mono_color,1,1) AS CM
                 , CASE WHEN product_structure in ('Volume','Value High','Business','HPS')  THEN 'VHI'
                        WHEN product_structure in ('Value','Personal','OPS') THEN 'VAL'
                        ELSE NULL
                    END AS VV
                 FROM ie2_Prod.dbo.hardware_xref WHERE upper(technology) ='LASER' or (technology='PWA' and (upper(hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3')) 
                          or platform_subset like 'PANTHER%' or platform_subset like 'JAGUAR%')")

platDim <- dbGetQuery(cprod,q5B)
head(platDim)
colnames(platDim)
dim(platDim)
str(platDim)

PoR2B <- sqldf('select aa1.*, aa2.SM from PoR2 aa1 inner join platDim aa2 on aa1.printer_platform_name = aa2.printer_platform_name ')
str(PoR2B)

# COMMAND ----------

# Step - 52 Creating raw_iMPV for all old Platforms using model information

PoR2model <- PoR2B
  PoR2model$print_color_speed_pages <- as.numeric(PoR2model$print_color_speed_pages)
  PoR2model$print_mono_speed_pages <- as.numeric(PoR2model$print_mono_speed_pages)
  

  
  #PoR2model <- sqldf('select *
  #, case 
  #when PLATFORM_TYPE = "OLD" and CM = "M" and SM = "SF" then (-0.70292 + 0.8000206*log(INTRO_PRICE_NUMBER) + 1.315551*log(MONO_SPEED_NUMBER) -0.008051*J90Mo) 
  #when PLATFORM_TYPE = "OLD" and CM = "M" and SM = "MF" then (-2.376331 + 0.8661936*log(INTRO_PRICE_NUMBER) + 1.6562368*log(MONO_SPEED_NUMBER) -0.008247*J90Mo) 
  #when PLATFORM_TYPE = "OLD" and CM = "C" and SM = "SF" then (-0.01346 + 0.7295208*log(INTRO_PRICE_NUMBER) + 1.1682769*log(MONO_SPEED_NUMBER) -0.006288*J90Mo -0.206628*IsDPT + 0.017758*IsDSK ) 
  #when PLATFORM_TYPE = "OLD" and CM = "C" and SM = "MF" then (1.2004303 + 0.4817492*log(INTRO_PRICE_NUMBER) + 1.0721192*log(MONO_SPEED_NUMBER) -0.007462*J90Mo +0.3521691*log(COLOR_SPEED_NUMBER)) 
  #end as model
  #from PoR2B
  #ORDER BY 
  #Intro_FYearMo 
  #, PLATFORM_TYPE 
  #')
######NEED TO PULL IN NEW MODELS#######  
  PoR2model <- sqldf('select *
    , case 
    when product_intro_price = 0 then null
    when (PLATFORM_TYPE = "OLD" and CM = "M" and SM = "SF") then (-0.70292 + 0.8000206*log(product_intro_price) + 1.315551*log(print_mono_speed_pages) -0.008051*J90Mo) 
    when (PLATFORM_TYPE = "OLD" and CM = "M" and SM = "MF") then (-2.376331 + 0.8661936*log(product_intro_price) + 1.6562368*log(print_mono_speed_pages) -0.008247*J90Mo) 
    when (PLATFORM_TYPE = "OLD" and CM = "C" and SM = "SF") then (-0.01346 + 0.7295208*log(product_intro_price) + 1.1682769*log(print_mono_speed_pages) -0.006288*J90Mo -0.206628*IsDPT + 0.017758*IsDSK ) 
    when (PLATFORM_TYPE = "OLD" and CM = "C" and SM = "MF") then (1.2004303 + 0.4817492*log(product_intro_price) + 1.0721192*log(print_mono_speed_pages) -0.007462*J90Mo +0.3521691*log(print_color_speed_pages))
    else null
      end as model
    from PoR2model
    ORDER BY 
      Intro_FYearMo 
      , PLATFORM_TYPE 
    ')
  
  PoR2model$rawMPV <- exp(as.numeric(PoR2model$model))
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
                        order by platform_market_code
                        ')

# COMMAND ----------

# Step - 54 Calculating the ratio of calculated_iMPV and raw_iMPV for NA 

PoR2model_iMPV$ratio1 <- PoR2model_iMPV$North_America2/PoR2model_iMPV$rawMPV
PoR2model_iMPV$ratio2 <- PoR2model_iMPV$NA2/PoR2model_iMPV$rawMPV
PoR2model_iMPV$ratio3 <- PoR2model_iMPV$North_America_D2/PoR2model_iMPV$rawMPV

# COMMAND ----------

# Step - 55 Calculating the average of iMPV ratio specific to groups defined by SM, CM

avgRatio1 <- sqldf('select sm, platform_market_code, cm, avg(ratio1) as avgratio1 from PoR2model_iMPV group by platform_market_code, sm, cm')
avgRatio2 <- sqldf('select sm, platform_market_code, cm, avg(ratio2) as avgratio2 from PoR2model_iMPV group by platform_market_code, sm, cm')
avgRatio3 <- sqldf('select sm, platform_market_code, cm, avg(ratio3) as avgratio3 from PoR2model_iMPV group by platform_market_code, sm, cm')

# COMMAND ----------

# Step - 56 attaching the average iMPV ratio

PoR2model_iMPV2 <- sqldf(' select aa1.*, aa2.avgratio1, aa3.avgratio2, aa4.avgratio3,
                              CASE WHEN aa2.avgratio1 < aa3.avgratio2 and aa2.avgratio1 < aa4.avgratio3 THEN avgratio1
                                   WHEN aa3.avgratio2 < aa4.avgratio3 THEN avgratio2
                                   ELSE avgratio3
                                   END as minavgratio
                         from 
                         PoR2model_iMPV aa1
                         inner join 
                         avgRatio1 aa2
                         on 
                         aa1.SM = aa2.SM
                         and
                         aa1.CM = aa2.CM
                         and
                         aa1.platform_market_code = aa2.platform_market_code
                         inner join 
                         avgRatio2 aa3
                         on 
                         aa1.SM = aa3.SM
                         and
                         aa1.CM = aa3.CM
                         and
                         aa1.platform_market_code = aa3.platform_market_code
                         inner join 
                         avgRatio3 aa4
                         on 
                         aa1.SM = aa4.SM
                         and
                         aa1.CM = aa4.CM
                         and
                         aa1.platform_market_code = aa4.platform_market_code
                         order by platform_market_code, SM, platform_market_code
                         ')

PoR2model_iMPV3 <- sqldf('select *
                         , case
                         when NA2 is not null then NA2
                         else rawMPV*minavgratio
                         end as NA3
                          , case
                         when North_America2 is not null then North_America2
                         else rawMPV*minavgratio 
                         end as North_America3
                         from
                         PoR2model_iMPV2
                         order by platform_market_code
                         ')

# COMMAND ----------

# ---- Step - 58 attaching regional coefficients -------------------------------------------#

PoR2model_iMPV4 <- sqldf('select aa1.*, 1 as NAa, aa2.EUa, aa2.APa, aa2.LAa, 1 as North_Americana, aa2.UKIna, aa2.Northern_Europena, aa2.Southern_Europena, aa2.ISEna, aa2.Central_Europena, aa2.Indiana, aa2.Greater_Asiana, aa2.Greater_Chinana,aa2.Latin_Americana, aa2.Greater_Asia_Dna, aa2.Greater_Asia_Ena, aa2.Greater_China_Dna, aa2.Greater_China_Ena, aa2.Central_Europe_Dna, aa2.Central_Europe_Ena
                           from PoR2model_iMPV3 aa1
                           inner join 
                           wtaverage aa2
                           on 
                           aa1.cm =aa2.cm
                           and
                           aa1.platform_market_code = aa2.platform_market_code
                           order by printer_platform_name, platform_market_code')

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
                         order by platform_market_code, CM
                         ')

#write.csv('C:/UPM/iMPV_OLD_complete.csv', x=PoR2model_iMPV5,row.names=FALSE, na="")

# COMMAND ----------

# Step 60 - Populating the route matrix for iMPV creation based on modeled values

route5 <- sqldf("select platform_market_code as platform_market_code, CM as CM,printer_platform_name
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

route5B <- reshape2::melt(route5 , id.vars = c("platform_market_code", "CM","printer_platform_name"),variable.name = "printer_region_code", value.name = "Route")

route5B <- route5B[which((route5B$Route=="Modeled")|is.na(route5B$Route)),]
route5B$printer_platform_name <- factor(route5B$printer_platform_name)

# COMMAND ----------

# Step 61 - Calculating percentage of platform installed base across regions

q6 <- ("SELECT distinct platform_subset AS printer_platform_name 
                 , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                      WHEN vc_category in ('SWT-H','SWT-H Pro','Dept','Dept-High','WG') then 'ENT'
                      ELSE NULL
                      END AS EP
                 , substring(mono_color,1,1) AS CM
                 , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H') THEN 'DSK'
                    WHEN vc_category in ('SWT-L') THEN 'SWL'
                    WHEN vc_category in ('SWT-H','SWT-H Pro') THEN 'SWH'
                    WHEN vc_category in ('Dept','Dept-High') THEN 'DPT'
                    WHEN vc_category in ('WG') THEN 'WGP'
                    ELSE NULL
                    END AS platform_market_code
                 FROM ie2_Prod.dbo.hardware_xref WHERE upper(technology) ='LASER' or (technology='PWA' and (upper(hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3')) 
                          or platform_subset like 'PANTHER%' or platform_subset like 'JAGUAR%')
                  --AND subbrand is not null
       ")

new1 <- dbGetQuery(cprod,q6)

new1b <- sqldf("SELECT a.*
            , c.market10
            , c.developed_emerging
            , b.month_begin as fiscal_year_month 
            , SUM(b.ib) AS printer_installed_base_month 
            FROM new1 a 
            INNER JOIN (select * from ibtable where ib IS NOT NULL) b 
              ON a.printer_platform_name=b.Platform_Subset
            LEFT JOIN country_info c
              ON b.country=c.country_alpha2
              GROUP BY
               a.printer_platform_name, a.CM, a.EP, a.platform_market_code
             , c.market10, c.developed_emerging
             , b.month_begin")
new1b <- subset(new1b,!is.na(platform_market_code))
new1c <- sqldf("SELECT platform_market_code, CM, printer_platform_name, market10, SUBSTR(developed_emerging,1,1) as developed_emerging
                , sum(printer_installed_base_month) as sumINSTALLED_BASE_COUNT
             FROM
             new1b
              where printer_installed_base_month IS NOT NULL
             group by platform_market_code, CM, printer_platform_name, market10, developed_emerging
             order by platform_market_code, CM, printer_platform_name, market10")
new1c$sumINSTALLED_BASE_COUNT <- as.numeric(new1c$sumINSTALLED_BASE_COUNT)
  new1cc <- sqldf("SELECT platform_market_code, CM, printer_platform_name
                , sum(printer_installed_base_month) as sumINSTALLED_BASE_COUNT
             FROM
             new1b
              where printer_installed_base_month IS NOT NULL
             group by platform_market_code, CM, printer_platform_name
             order by platform_market_code, CM, printer_platform_name")
new1cc$sumINSTALLED_BASE_COUNT <- as.numeric(new1cc$sumINSTALLED_BASE_COUNT)
new1d <- reshape2::dcast(new1c, platform_market_code+CM+printer_platform_name ~ market10+developed_emerging, value.var = "sumINSTALLED_BASE_COUNT", fun.aggregate=sum)

#new1d$totalIBC <-as.numeric(new1d$AP1)+as.numeric(new1d$EU1)+as.numeric(new1d$LA1)+as.numeric(new1d$NA1) 
new <- sqldf("
             SELECT a.* ,b.sumINSTALLED_BASE_COUNT 
             , COALESCE(a.[North America_D]/b.sumINSTALLED_BASE_COUNT,0) as pctIBNorth_America
             , COALESCE(a.[Latin America_E]/b.sumINSTALLED_BASE_COUNT,0) as pctIBLatin_America
             , COALESCE(a.[Northern Europe_D]/b.sumINSTALLED_BASE_COUNT,0) as pctIBNorthern_Europe
             , COALESCE(a.[UK&I_D]/b.sumINSTALLED_BASE_COUNT,0) as pctIBUKI
             , COALESCE(a.[Southern Europe_D]/b.sumINSTALLED_BASE_COUNT,0) as pctIBSouthern_Europe
             , COALESCE(a.[Central Europe_D]/b.sumINSTALLED_BASE_COUNT,0) as pctIBCentral_EuropeD
             , COALESCE(a.[Central Europe_E]/b.sumINSTALLED_BASE_COUNT,0) as pctIBCentral_EuropeE
             , COALESCE(a.[ISE_E]/b.sumINSTALLED_BASE_COUNT,0) as pctIBISE
             , COALESCE(a.[India SL & BL_E]/b.sumINSTALLED_BASE_COUNT,0) as pctIBIndia
             , COALESCE(a.[Greater China_D]/b.sumINSTALLED_BASE_COUNT,0) as pctIBGreater_ChinaD
             , COALESCE(a.[Greater China_E]/b.sumINSTALLED_BASE_COUNT,0) as pctIBGreater_ChinaE
             , COALESCE(a.[Greater Asia_D]/b.sumINSTALLED_BASE_COUNT,0) as pctIBGreater_AsiaD
             , COALESCE(a.[Greater Asia_E]/b.sumINSTALLED_BASE_COUNT,0) as pctIBGreater_AsiaE
             FROM
            new1d a
            left join
            new1cc b
            on a.platform_market_code=b.platform_market_code and a.CM=b.CM and a.printer_platform_name=b.printer_platform_name


              ")

head(new)
colnames(new)
dim(new)
str(new)

# COMMAND ----------

# Step 62 - Attaching PoR with the percent installed base for OLD platforms

new2 <- sqldf('select aa1.*, aa2.product_usage_por_pages, aa2.platform_type
              from new aa1
              inner join 
              PoR2model aa2
              on
              aa1.printer_platform_name =aa2.printer_platform_name 
              where aa2.platform_type = "FUTURE" and aa2.product_usage_por_pages is not null
              ')

# COMMAND ----------

# Step 63 - Attaching the Regional Coeffieicnt (RC) to the respective platform

new3 <- sqldf('select aa1.*, aa2.[Central_Europena],aa2.[Central_Europe_Dna],aa2.[Central_Europe_Ena],aa2.[Northern_Europena],aa2.[Southern_Europena],aa2.[ISEna]
  ,aa2.[UKIna],aa2.[Greater_Asiana],aa2.[Greater_Asia_Dna],aa2.[Greater_Asia_Ena],aa2.[Greater_Chinana],aa2.[Greater_China_Dna],aa2.[Greater_China_Ena]
  ,aa2.[Indiana],aa2.[Latin_Americana]
              from new2 aa1
              inner join 
              wtaverage aa2
              on 
              aa1.platform_market_code = aa2.platform_market_code
              and
              aa1.CM = aa2.CM
              order by platform_market_code, CM')

# COMMAND ----------

# Step 64 - Calculating MPV for future NA platfomrs

new3$NA2 <- new3$product_usage_por_pages/(new3$pctIBNorth_America + 
                                                new3$pctIBLatin_America*new3$Latin_Americana + 
                                                new3$pctIBNorthern_Europe*new3$Northern_Europena +
                                                new3$pctIBUKI*new3$UKIna +
                                                new3$pctIBSouthern_Europe*new3$Southern_Europena +
                                                new3$pctIBCentral_EuropeD*new3$Central_Europe_Dna +
                                                new3$pctIBCentral_EuropeE*new3$Central_Europe_Ena +
                                                new3$pctIBISE*new3$ISEna +
                                                new3$pctIBIndia*new3$Indiana +
                                                new3$pctIBGreater_AsiaD*new3$Greater_Asia_Dna +
                                                new3$pctIBGreater_AsiaE*new3$Greater_Asia_Ena +
                                                new3$pctIBGreater_ChinaD*new3$Greater_China_Dna +
                                                new3$pctIBGreater_ChinaE*new3$Greater_Asia_Ena

)

# COMMAND ----------

# Step 65 - Calculating MPV for other future platfomrs 

new3$LA2 <- new3$NA2*new3$Latin_Americana  
new3$NE2 <- new3$NA2*new3$Northern_Europena 
new3$UK2 <- new3$NA2*new3$UKIna
new3$SE2 <- new3$NA2*new3$Southern_Europena
new3$CED2 <-new3$NA2*new3$Central_Europe_Dna
new3$CEE2 <-new3$NA2*new3$Central_Europe_Ena
new3$IS2 <- new3$NA2*new3$ISEna
new3$IN2 <- new3$NA2*new3$Indiana
new3$GAD2 <-new3$NA2*new3$Greater_Asia_Dna
new3$GAE2 <-new3$NA2*new3$Greater_Asia_Ena
new3$GCD2 <-new3$NA2*new3$Greater_China_Dna 
new3$GCE2 <-new3$NA2*new3$Greater_Asia_Ena

# COMMAND ----------

# Step 66 - Attaching decay matrix with MPV table

decayT <- reshape2::dcast(decay, CM+platform_market_code~market10+developed_emerging, value.var="b1")

new4 <- sqldf('select aa1.*
              , aa2.`North America_D`/12 as NAd
              , aa2.`Central Europe_D`/12 as CEDd
              , aa2.`Central Europe_E`/12 as CEEd
              , aa2.`Greater Asia_D`/12 as GADd
              , aa2.`Greater Asia_E`/12 as GAEd
              , aa2.`Greater China_D`/12 as GCDd
              , aa2.`Greater China_E`/12 as GCEd
              , aa2.`India SL & BL_E`/12 as INd
              , aa2.`ISE_E`/12 as ISd
              , aa2.`Latin America_E`/12 as LAd
              , aa2.`Northern Europe_D`/12 as NEd
              , aa2.`Southern Europe_D`/12 as SEd
              , aa2.`UK&I_D`/12 as UKd
              from new3 aa1
              inner join 
              decayT aa2
              on 
              aa1.platform_market_code = aa2.platform_market_code
              and
              aa1.CM = aa2.CM
              order by platform_market_code, CM')

# COMMAND ----------

# Step 67 - Calculating iMPV using decay rate for all future platforms

new4$NA3 <- new4$NA2/((1+new4$NAd)^30)
new4$CED3 <- new4$CED2/((1+new4$CEDd)^30)
new4$CEE3 <- new4$CEE2/((1+new4$CEEd)^30)
new4$GAD3 <- new4$GAD2/((1+new4$GADd)^30)
new4$GAE3 <- new4$GAE2/((1+new4$GAEd)^30)
new4$GCD3 <- new4$GCD2/((1+new4$GCDd)^30)
new4$GCE3 <- new4$GCE2/((1+new4$GCEd)^30)
new4$IN3 <- new4$IN2/((1+new4$INd)^30)
new4$IS3 <- new4$IS2/((1+new4$ISd)^30)
new4$LA3 <- new4$LA2/((1+new4$LAd)^30)
new4$NE3 <- new4$NE2/((1+new4$NEd)^30)
new4$SE3 <- new4$SE2/((1+new4$SEd)^30)
new4$UK3 <- new4$UK2/((1+new4$UKd)^30)
#   rm(new3)
#   rm(new2)
#   rm(new1d)
#   rm(new1cc)
#   rm(new1c)
#   rm(new1b)

# COMMAND ----------

# Step 68 - Populating the route matrix of iMPV creations for Futre products

route6 <- sqldf('select platform_market_code as platform_market_code, CM as CM, printer_platform_name
                , case when NA3 is not null then "Future" else null end as NAs
                , case when CED3 is not null then "Future" else null end as CEDs                
                , case when CEE3 is not null then "Future" else null end as CEEs               
                , case when GAD3 is not null then "Future" else null end as GADs               
                , case when GAE3 is not null then "Future" else null end as GAEs  
                , case when GCD3 is not null then "Future" else null end as GCDs  
                , case when GCE3 is not null then "Future" else null end as GCEs
                , case when IN3 is not null then "Future" else null end as INs
                , case when IS3 is not null then "Future" else null end as ISs
                , case when LA3 is not null then "Future" else null end as LAs
                , case when NE3 is not null then "Future" else null end as NEs
                , case when SE3 is not null then "Future" else null end as SEs
                , case when UK3 is not null then "Future" else null end as UKs
                from new4

                ')

route6B <- reshape2::melt(route6 , id.vars = c("platform_market_code", "CM","printer_platform_name"),variable.name = "printer_region_code", value.name = "Route")

route1 <- usagesummaryNAEUAP[c(2,3,1, 509, 512, 514, 516, 518, 520, 521, 524, 526, 527, 529, 530, 533)] #need route columns
route1B <- reshape2::melt(route1 , id.vars = c("platform_market_code", "CM","printer_platform_name"),variable.name = "printer_region_code", value.name = "Route")
route1B <- sqldf("SELECT platform_market_code, CM, printer_platform_name
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

route <- rbind(route1B, route5B, route6B)
#route$printer_region_code <- substr(route$printer_region_code, 1, 2)

routeT <- reshape2::dcast(route, platform_market_code + CM +printer_platform_name ~printer_region_code, value.var="Route")

  route <- sqldf("SELECT *,
                  CASE WHEN Route = 'Self' THEN 'modelled: Self'
                       WHEN Route = 'NA' THEN 'proxied: NA ;'||CM||platform_market_code
                       WHEN Route = 'UK' THEN 'proxied: UK ;'||CM||platform_market_code
                       WHEN Route = 'IS' THEN 'proxied: IS ;'||CM||platform_market_code
                       WHEN Route = 'IN' THEN 'proxied: IN ;'||CM||platform_market_code
                       WHEN Route = 'GAD' THEN 'proxied: GAD ;'||CM||platform_market_code
                       WHEN Route = 'GAE' THEN 'proxied: GAE ;'||CM||platform_market_code
                       WHEN Route = 'GCD' THEN 'proxied: GCD ;'||CM||platform_market_code
                       WHEN Route = 'GCE' THEN 'proxied: GCE ;'||CM||platform_market_code
                       WHEN Route = 'CED' THEN 'proxied: CED ;'||CM||platform_market_code
                       WHEN Route = 'CEE' THEN 'proxied: CEE ;'||CM||platform_market_code
                       WHEN Route = 'LA' THEN 'proxied: LA ;'||CM||platform_market_code
                       WHEN Route = 'NE' THEN 'proxied: NE ;'||CM||platform_market_code
                       WHEN Route = 'SE' THEN 'proxied: SE ;'||CM||platform_market_code
                       WHEN Route = 'Modeled' THEN 'ML'
                       WHEN Route = 'Future' THEN 'HW POR'
                  END AS label
                  FROM route 
                  
                 
                 ")
   
  routeT <- reshape2::dcast(route, platform_market_code + CM +printer_platform_name ~printer_region_code, value.var="label")

# rm(route1B)
# rm(route5B)
# rm(route6B)

# COMMAND ----------

# Step - 69 - combining old and future iMPV matrix to create complete iMPV matrix

normdataOLD <- sqldf('select platform_market_code, CM 
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
                     ')

normdataNew <- sqldf('select platform_market_code, CM
                     , printer_platform_name
                     , NA3 AS NA
                     , NE3 AS NE
                     , SE3 AS SE
                     , CED3 AS CED
                     , CEE3 AS CEE
                     , IS3 AS ISE
                     , UK3 AS UK
                     , IN3 AS INE
                     , GAD3 AS GAD
                     , GAE3 AS GAE
                     , GCD3 AS GCD
                     , GCE3 AS GCE
                     , LA3 AS LA
                     FROM new4
                     ')

normdata <- rbind(normdataOLD, normdataNew)

# COMMAND ----------

# Step - 70 Normalize complete iMPV matrix with respect to VV, CM, SM and Plat_Nm

normdata2 <- reshape2::melt(normdata, id.vars = c("platform_market_code", "CM", "printer_platform_name"),
                  variable.name = "printer_region_code", 
                  value.name = "iMPV")

# COMMAND ----------

# Step - 71 Extracting region specific introDate for each of the platforms

normdatadate <- sqldf('select platform_market_code, CM
                      , printer_platform_name
                      , INTRODATE_NA AS NA, INTRODATE_NE AS NE, INTRODATE_SE AS SE, INTRODATE_CED AS CED, INTRODATE_CEE AS CEE, INTRODATE_UK AS UK, INTRODATE_IS AS [IS], INTRODATE_IN AS [IN]
                      , INTRODATE_GAD AS GAD, INTRODATE_GAE AS GAE, INTRODATE_GCD AS GCD, INTRODATE_GCE AS GCE, INTRODATE_LA AS LA
                      FROM PoR2model
                      ')

# COMMAND ----------

# Step - 72 creating detailed iMPV matrix for all platforms

combined <- sqldf('select aa1.PLATFORM_TYPE
                  , aa2.platform_market_code
                  , aa2.CM
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
                  , aa1.INTRODATE_NA
                  , aa1.INTRODATE_CED
                  , aa1.INTRODATE_CEE
                  , aa1.INTRODATE_GAD
                  , aa1.INTRODATE_GAE
                  , aa1.INTRODATE_GCD
                  , aa1.INTRODATE_GCE
                  , aa1.INTRODATE_IN
                  , aa1.INTRODATE_IS
                  , aa1.INTRODATE_LA
                  , aa1.INTRODATE_NE
                  , aa1.INTRODATE_SE
                  , aa1.INTRODATE_UK
                  from
                  routeT aa2
                  inner join
                  old aa1
                  on
                  aa1.platform_market_code = aa2.platform_market_code
                  and
                  aa1.CM = aa2.CM
                  and
                  aa1.printer_platform_name = aa2.printer_platform_name
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
                   on
                   aa1.platform_market_code = aa2.platform_market_code
                   and
                   aa1.CM = aa2.CM
                   and
                   aa1.printer_platform_name = aa2.printer_platform_name
                   ')

# COMMAND ----------

# Step - 73 Normalize introDate data with respect to VV, CM, SM and Plat_Nm
 
normdatadate2 <- reshape2::melt(normdatadate, id.vars = c("platform_market_code", "CM", "printer_platform_name"),
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
                   , CASE WHEN src.Route IS NOT NULL THEN src.Route
                      ELSE src.mde
                      END AS Route
                   , por.CM
                   , por.platform_market_code
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
                          WHEN por.product_usage_por_pages IS NOT NULL THEN por.product_usage_por_pages
                          WHEN por.rawMPV is not NULL THEN por.rawMPV
                          --WHEN src.Route IS NULL THEN ?
                          ELSE null

                      END AS iMPV
                      ,CASE WHEN src.Route='country' THEN 'modelled: Self' 
                            WHEN src.Route='dev/em' THEN 'modelled: Dev/EM'
                            WHEN src.Route='market10' THEN 'modelled: Market10'
                            WHEN src.Route='region5' THEN 'modelled: Region5'
                            WHEN src.Route='Self' THEN 'modelled: Self'
                            WHEN src.Route2='region5' THEN 'modelled: Region5'
                            WHEN src.mde='LA' AND por.Latin_America3 IS NOT NULL THEN 'proxied: LA ;'||por.CM||por.platform_market_code
                            WHEN src.mde='CEE' AND por.Central_Europe_E3 IS NOT NULL THEN 'proxied: CED ;'||por.CM||por.platform_market_code
                            WHEN src.mde='CED' AND por.Central_Europe_D3 IS NOT NULL THEN 'proxied: CEE ;'||por.CM||por.platform_market_code
                            WHEN src.mde='GAD' AND por.Greater_Asia_D3 IS NOT NULL THEN'proxied: GAD ;'||por.CM||por.platform_market_code
                            WHEN src.mde='GAE' AND por.Greater_Asia_E3 IS NOT NULL THEN 'proxied: GAE ;'||por.CM||por.platform_market_code
                            WHEN src.mde='GCD' AND por.Greater_China_D3 IS NOT NULL THEN 'proxied: GCD ;'||por.CM||por.platform_market_code
                            WHEN src.mde='GCE' AND por.Greater_China_E3 IS NOT NULL THEN 'proxied: GCE ;'||por.CM||por.platform_market_code
                            WHEN src.mde='IN' AND por.India3 IS NOT NULL THEN 'proxied: ISB ;'||por.CM||por.platform_market_code
                            WHEN src.mde='IS' AND por.ISE3 IS NOT NULL THEN 'proxied: ISE ;'||por.CM||por.platform_market_code
                            WHEN src.mde='NA' AND por.North_America3 IS NOT NULL THEN 'proxied: NA ;'||por.CM||por.platform_market_code
                            WHEN src.mde='NE' AND por.Northern_Europe3 IS NOT NULL THEN 'proxied: NE ;'||por.CM||por.platform_market_code
                            WHEN src.mde='SE' AND por.Southern_Europe3 IS NOT NULL THEN 'proxied: SE ;'||por.CM||por.platform_market_code
                            WHEN src.mde='UK' AND por.UKI3 IS NOT NULL THEN 'proxied: UK ;'||por.CM||por.platform_market_code
                            WHEN por.product_usage_por_pages IS NOT NULL THEN 'HW POR'
                            WHEN por.rawMPV is not NULL THEN 'ML'
                          END as label
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
                  --LEFT JOIN route rt
                    --ON src.printer_platform_name=rt.printer_platform_name AND src.market10=rt.market10
                   ")

# normdataFinal0 <- sqldf('select aa1.*, aa2.introdate as introdate0
#                         from normdata2 aa1
#                         inner join
#                         normdatadate2 aa2
#                         on
#                         aa1.platform_market_code = aa2.platform_market_code
#                         and
#                         aa1.CM = aa2.CM
#                         and
#                         aa1.printer_platform_name = aa2.printer_platform_name
#                         and
#                         aa1.printer_region_code = aa2.printer_region_code
#                         order by platform_market_code, CM, printer_region_code, introdate0
#                         ')

normdataFinal <- sqldf('select distinct aa1.*, aa2.minYear as introdate from normdataFinal0 aa1
                       inner join
                       introYear2a aa2
                       on
                       aa1.printer_platform_name = aa2.printer_platform_name')
#normdataFinal$ep <- ifelse(normdataFinal$ep=="E","ENT","PRO")

cols <- c( 'CM', 'platform_market_code','country_alpha2' )
cols2 <- c('CM', 'platform_market_code','market10','developed_emerging')
normdataFinal$strata1 <- apply( normdataFinal[ , cols2 ] , 1 , paste , collapse = "_" )
normdataFinal$strata2 <- apply( normdataFinal[ , cols ] , 1 , paste , collapse = "_" )

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

dsea1 <- crossJoin(SparkR::sql("SELECT DISTINCT strata, b1, b1check FROM d"), s2_spark)
createOrReplaceTempView(dsea1, "dsea1")

dsea <- SparkR::sql("select distinct m.strata, m.b1, m.b1check, m.month, d.seasonality
              from dsea1 m
              left join d
                on m.month=d.month
                  and m.strata=d.strata
                  and m.b1=d.b1
              --group by m.strata, m.month, d.seasonality 
              ")

createOrReplaceTempView(dsea, "dsea")

dseason <- SparkR::sql('select distinct strata, b1, b1check, month, seasonality
                     from dsea')

createOrReplaceTempView(dseason, "dseason")

d3 <- SparkR::sql('select aa1.*, aa2.b1, aa2.b1check, CASE WHEN aa2.seasonality is null then 0 ELSE aa2.seasonality END as seasonality
            from d2 aa1
            inner join 
            dseason aa2
            on
            aa1.strata1 = aa2.strata
            and 
            aa1.month = aa2. month
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
            aa1.yyyymm = aa2. FYearMo
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
createOrReplaceTempView(as.DataFrame(outcome), "outcome")

final4 <- SparkR::sql('select distinct aa1.*
                , aa2.mpva
                , aa2.na
                , aa3.MUT
                from final1 aa1
                left outer join 
                usage aa2
                on 
                aa1.printer_platform_name = aa2.printer_platform_name and aa1.country_alpha2 = aa2.country_alpha2
                and aa1.yyyymm = aa2.FYearMo and aa1.platform_market_code = aa2.platform_market_code and aa1.CM = aa2.CM
                left outer join 
                outcome aa3
                on aa1.market10 = aa3.market10 and aa1.developed_emerging=aa3.developed_emerging and
                aa1.yyyymm = aa3.FYearMo and aa1.platform_market_code = aa3.platform_market_code and aa1.CM = aa3.CM
                ')

createOrReplaceTempView(final4, "final4")

# COMMAND ----------

# Step 83 - Exatracting and Attaching Installed Base infomration

######Not keeping all IB######

caldts <- dbGetQuery(cprod,"SELECT distinct Calendar_Yr_Mo, Fiscal_Year_Qtr FROM IE2_Prod.dbo.calendar WHERE Day_of_Month=1")

createOrReplaceTempView(as.DataFrame(caldts), "caldts")

createOrReplaceTempView(as.DataFrame(ibtable), "ibtable")

final4 <- SparkR::sql('select aa1.*
                , aa2.ib as IB
                , cd.Fiscal_Year_Qtr as FYearQtr
                from final4 aa1
                left outer join 
                ibtable aa2
                on 
                aa1.printer_platform_name = aa2.platform_subset and aa1.country_alpha2 = aa2.country and aa1.yyyymm = aa2.month_begin
                left join caldts cd
                on aa1.yyyymm=cd.Calendar_Yr_Mo
                ')


final4$rFYearQtr <- cast(substr(final4$FYearQtr,1,4), "integer")*1 +  ((cast(substr(final4$FYearQtr,6,6), "integer") - 1)/4) + (1/8)

final4$rFYearMo_Ad <- final4$rFYearMo + (1/24)

createOrReplaceTempView(final4, "final4")

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
                , TD_MPV AS MPV_TD
                , MPVA as MPV_Raw
                , NA as MPV_N
                , IB
                , introdate as FIntroDt
                --, EP as EP
                , CM as CM
                --, CASE
                  --WHEN sm IS NOT NULL THEN sm
                  --ELSE 
                    --CASE
                    --WHEN SUBSTR(FMC,2,1) = "M" THEN "MF"
                    --WHEN SUBSTR(FMC,2,1) = "S" THEN "SF"
                    --END
                  --END
                  --AS SM
                , platform_market_code AS Mkt
                --, FMC
                --, K_PPM
                --, Clr_PPM
                --, product_intro_price as Intro_Price
                , iMPV as MPV_Init
                , Decay
                , b1check
                , Seasonality
                , Cyclical
                , MUT
                , Trend
                , Route as IMPV_Route
                , label
                from final4  
                where IB is not null --and intro_price != 0
                ')

final9$FYearMo <- cast(final9$FYearMo, "string")

createOrReplaceTempView(final9, "final9")

# COMMAND ----------

# Step 84B - creating ther POR_MPV column

# newTable <- sqlQuery(ch,paste(
#   "								
#   select DISTINCT product_ref.platform_lab_name AS printer_platform_name
#   , product_ref.product_chrome_code as CM
#   , product_ref.product_business_code AS EP
#   , product_ref.product_usage_por_pages AS PLAN_OF_RECORD
#   from
#   ref_enrich.product_ref
#   
#   "
#   ,sep = "", collapse = NULL),na.strings = "")
# newTable <- sqldf('
#                   select distinct printer_platform_name as printer_platform_name
#                   , CM
#                   , EP
#                   , product_usage_por_pages as PLAN_OF_RECORD
#                   from PoR
#                   ')
# 
# final10 <- sqldf('select distinct aa1.*
#                   --, aa2.PLAN_OF_RECORD 
#                  from final9 aa1
#                  left join
#                  newTable aa2 
#                  on
#                  aa1.Platform_Subset_Nm = aa2.printer_platform_name')

#final10$POR_MPV <- final10$PLAN_OF_RECORD *((1+(final10$Decay/12))^(final10$MoSI - 30))

#head(final10)
# To check the outcome with Van's table. The calculation is correct. 
# However, the decay rate used in Van's table is different than what is being created in UPM code

#final10[which(final10$printer_platform_name == "ARGON MANAGED" & final10$printer_region_code == "EU"),c("printer_platform_name", "printer_region_code","FYearMo",	"MoSI",	"Decay", "POR_MPV")]

#close(ch)
decay_rates <- SparkR::sql("select distinct Platform_Subset_Nm, Market10, Region, Region_DE, avg(MPV_Init) as MPV_Init, avg(Decay) as Decay, avg(b1check) as b1check, '2021-11-02' as rundate  from final9
                 Group by Platform_Subset_Nm, Market10, Region, Region_DE
                 ")
# s3write_using(x=decay_rates,FUN = write.csv, object = paste0("s3://insights-environment-sandbox/BrentT/toner_decay_75_",outnm_dt,"_(",Sys.Date(),").csv"), row.names=FALSE, na="")

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
SparkR::write.parquet(x=final9, path=paste0("s3://", aws_bucket_name, "UPM_ctry(",Sys.Date(),").parquet"), mode="overwrite")

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


