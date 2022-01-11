# Databricks notebook source
notebook_start_time <- Sys.time()

# COMMAND ----------

dbutils.widgets.text("ib_version", "")
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", list("dev", "itg", "prd"))
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("bdtbl", "dashboard.print_share_usage_agg_stg")
dbutils.widgets.text("upm_date", "")

# COMMAND ----------

# MAGIC %run ../../scala/common/Constants.scala

# COMMAND ----------

# MAGIC %run ../../scala/common/DatabaseUtils.scala

# COMMAND ----------

# MAGIC %run ../../python/common/secrets_manager_utils.py

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

# ---
# #Version 2021.11.29.1#
# title: "UPM with IE2.0 IB Color Pct"
# output:
#   word_document: default
#   html_notebook: default
# ---

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /usr/bin/java
# MAGIC ls -l /etc/alternatives/java
# MAGIC ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/default-java
# MAGIC R CMD javareconf

# COMMAND ----------

packages <- c("rJava", "RJDBC", "DBI", "sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR", "spatstat")

# COMMAND ----------

# MAGIC %run ../common/package_check.r

# COMMAND ----------

options(java.parameters = "-Xmx40g")

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

UPMDate <- Sys.Date() #Change to date if not running on same date as UPM "2019-12-19" #

sqlserver_driver <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/dbfs/FileStore/jars/801b0636_e136_471a_8bb4_498dc1f9c99b-mssql_jdbc_9_4_0_jre8-13bd8.jar")

cprod <- dbConnect(sqlserver_driver, paste0("jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database=IE2_Prod;user=", sparkR.conf("sfai_username"), ";password=", sparkR.conf("sfai_password")))

clanding <- dbConnect(sqlserver_driver, paste0("jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database=IE2_Landing;user=", sparkR.conf("sfai_username"), ";password=", sparkR.conf("sfai_password")))

# COMMAND ----------

todaysDate <- Sys.Date()
nyear <- .5  #number of years
start1b <- add_with_rollback(todaysDate,months(-6)) 
start1 <- as.numeric(paste(lubridate::year(start1b), 0, lubridate::month(start1b), sep=""))

end1 <- as.numeric(ifelse(lubridate::month(todaysDate)<10, (paste(lubridate::year(todaysDate), 0, lubridate::month(todaysDate), sep="")), (paste(lubridate::year(todaysDate), lubridate::month(todaysDate), sep=""))))
# start1 <- 201307  #to set a specific start date instead of n years ago
# end1 <- 201808     #to set a specific end date instead of todays date

# ---- Specify the minimum sample size required while calculating MUT (Step 2) -------------------#

MUTminSize <- 200

# ---- Specify the minimum sample size required while calculating MUT (Step 5) -------------------#

minSize <- 200

# ---- Specify the time window that will be considered while calculating MUT (Step 7) ------------#

startMUT <- start1 #or set to 201101
endMUT  <- end1

# ---- Specify the last year avaialble in FIJI IB ------------------------------------------------#

#lastAvaiableYear <- 2048

# ---- Specify whether to replace the data of PRO by ENT after the end of MUT creation (Step 7) --#

replace <- 0 # Value 1 will replace the values of PRO by the respective values from ENT set

# ---- Specify time (YYYYMM) that works as a demarcation b/n Old and New Platforms (Step 50) -----#

oldNewDemarcation <- end1

options(stringsAsFactors= FALSE)
options(scipen=999)

# COMMAND ----------

# MAGIC %scala
# MAGIC val zeroiQuery = s""" SELECT  tpmib.printer_platform_name as platform_name  
# MAGIC                , tpmib.platform_std_name as platform_standard_name  
# MAGIC                , tpmib.printer_country_iso_code as iso_country_code  
# MAGIC                , tpmib.fiscal_year_quarter 
# MAGIC                , tpmib.date_month_dim_ky as calendar_year_month 
# MAGIC                , tpmib.share_region_incl_flag AS share_region_incl_flag   
# MAGIC                , tpmib.ps_region_incl_flag AS page_share_region_incl_flag 
# MAGIC                , tpmib.usage_region_incl_flag AS usage_region_incl_flag 
# MAGIC                --Get numerator and denominator, already ib weighted at printer level
# MAGIC                , SUM(COALESCE(tpmib.print_pages_total_ib_ext_sum,0)) as usage_numerator
# MAGIC                , SUM(COALESCE(tpmib.print_pages_color_ib_ext_sum,0)) as color_numerator
# MAGIC                , SUM(COALESCE(tpmib.print_months_ib_ext_sum,0)) as usage_denominator
# MAGIC                , SUM(COALESCE(tpmib.printer_count_month_usage_flag_sum,0)) AS printer_count_month_use
# MAGIC                , SUM(COALESCE(tpmib.printer_count_fyqtr_usage_flag_sum,0)) AS printer_count_fiscal_quarter_use
# MAGIC                 FROM  
# MAGIC                   ${dbutils.widgets.get("bdtbl")} tpmib
# MAGIC                 WHERE 1=1 
# MAGIC                   AND printer_route_to_market_ib='Aftermarket'
# MAGIC                 GROUP BY tpmib.printer_platform_name  
# MAGIC                , tpmib.platform_std_name  
# MAGIC                , tpmib.printer_country_iso_code  
# MAGIC                , tpmib.fiscal_year_quarter 
# MAGIC                , tpmib.date_month_dim_ky 
# MAGIC                , tpmib.share_region_incl_flag   
# MAGIC                , tpmib.ps_region_incl_flag 
# MAGIC                , tpmib.usage_region_incl_flag"""
# MAGIC 
# MAGIC val zeroi = readRedshiftToDF(configs)
# MAGIC   .option("query", zeroiQuery)
# MAGIC   .load()
# MAGIC 
# MAGIC zeroi.createOrReplaceTempView("zeroi")

# COMMAND ----------

zeroi <- SparkR::collect(SparkR::sql("SELECT * FROM zeroi"))

# COMMAND ----------

# Step 1 - query for Normalized extract specific to PE and RM

# zeroi <- dbGetQuery(cprod,paste(						
#   " 
#   SELECT  tpmib.printer_platform_name as platform_name  
#                , tpmib.platform_std_name as platform_standard_name  
#                , tpmib.printer_country_iso_code as iso_country_code  
#                , tpmib.fiscal_year_quarter 
#                , tpmib.date_month_dim_ky as calendar_year_month 
#                , tpmib.share_region_incl_flag AS share_region_incl_flag   
#                , tpmib.ps_region_incl_flag AS page_share_region_incl_flag 
#                , tpmib.usage_region_incl_flag AS usage_region_incl_flag 
#                --Get numerator and denominator, already ib weighted at printer level
#                , SUM(COALESCE(tpmib.print_pages_total_ib_ext_sum,0)) as usage_numerator
#                , SUM(COALESCE(tpmib.print_pages_color_ib_ext_sum,0)) as color_numerator
#                , SUM(COALESCE(tpmib.print_months_ib_ext_sum,0)) as usage_denominator
#                , SUM(COALESCE(tpmib.printer_count_month_usage_flag_sum,0)) AS printer_count_month_use
#                , SUM(COALESCE(tpmib.printer_count_fyqtr_usage_flag_sum,0)) AS printer_count_fiscal_quarter_use
#                 FROM  
#                   dashboard.print_share_usage_agg_stg tpmib with (NOLOCK)
#                 WHERE 1=1 
#                   AND printer_route_to_market_ib='Aftermarket'
#                 GROUP BY tpmib.printer_platform_name  
#                , tpmib.platform_std_name  
#                , tpmib.printer_country_iso_code  
#                , tpmib.fiscal_year_quarter 
#                , tpmib.date_month_dim_ky 
#                , tpmib.share_region_incl_flag   
#                , tpmib.ps_region_incl_flag 
#                , tpmib.usage_region_incl_flag
#   ",sep = " ", collapse = NULL))

#######SELECT IB VERSION#####
#ib_version <- as.character(dbGetQuery(ch,"select max(ib_version) as ib_version from biz_trans.tri_platform_measures_ib"))  #FROM BD Dashboard
ib_version <- dbutils.widgets.get("ib_version") #SELECT SPECIFIC VERSION
#ib_version <- as.character(dbGetQuery(cprod,"select max(version) as ib_version from IE2_Prod.dbo.ib with (NOLOCK)"))  #Phoenix

ibtable <- dbGetQuery(cprod,paste0("
                   select  a.platform_subset
                          ,YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          ,d.technology AS hw_type
                          ,b.region_5
                          ,a.country
                          ,substring(b.developed_emerging,1,1) as de
                          ,sum(a.units) as ib
                          ,a.version
                          ,d.mono_color
                          ,d.vc_category
                          ,d.business_feature
                    from IE2_Prod.dbo.ib a with (NOLOCK)
                    left join IE2_Prod.dbo.iso_country_code_xref b with (NOLOCK)
                      on (a.country=b.country_alpha2)
                    left join IE2_Prod.dbo.hardware_xref d with (NOLOCK)
                       on (a.platform_subset=d.platform_subset)
                    where a.measure = 'ib'
                      and a.version = '",ib_version,"'
                      and (upper(d.technology)='LASER' or (d.technology='PWA' and (upper(d.hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3') 
                          or a.platform_subset like 'PANTHER%' or a.platform_subset like 'JAGUAR%')))
                    group by a.platform_subset
                          ,a.cal_date
                          ,d.technology
                          ,b.region_5
                          ,a.country
                          ,b.developed_emerging
                          ,a.version
                          ,d.mono_color
                          ,d.vc_category
                          ,d.business_feature
                   "))


hwval <- dbGetQuery(cprod,"
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
                    WHERE (upper(technology)='LASER' or (technology='PWA' and (upper(hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3') 
                          or platform_subset like 'PANTHER%' or platform_subset like 'JAGUAR%')))
                    ")


intro_dt <- dbGetQuery(cprod,"
                     SELECT platform_subset, min(cal_date) as platform_intro_month
                     FROM ie2_Prod.dbo.ib
                     GROUP BY platform_subset
                     ")

#Get Market10 Information
country_info <- dbGetQuery(cprod,"
                      WITH mkt10 AS (
                           SELECT country_alpha2, country_level_2 as market10, country_level_4 as emdm
                           FROM IE2_Prod.dbo.iso_cc_rollup_xref
                           WHERE country_scenario='Market10'
                      ),
                      rgn5 AS (
                            SELECT country_alpha2, CASE WHEN region_5='JP' THEN 'AP' ELSE region_5 END AS region_5, developed_emerging, country 
                            FROM IE2_Prod.dbo.iso_country_code_xref
                      )
                      SELECT a.country_alpha2, a.region_5, b.market10, a.developed_emerging, country
                            FROM rgn5 a
                            LEFT JOIN mkt10 b
                            ON a.country_alpha2=b.country_alpha2
                           ")

zero <- sqldf("with sub0 as (select a.platform_name, c.region_5 as printer_region_code, a.calendar_year_month as FYearMo
                , upper(SUBSTR(hw.mono_color,1,1)) as CM
                , hw.pro_vs_ent AS EP
                , hw.sf_mf
                , hw.platform_market_code
                , c.market10
                , c.country_alpha2
                , c.developed_emerging as de
                , SUM(a.usage_numerator) AS usage_numerator, sum(a.usage_denominator) as usage_denominator, sum(a.printer_count_month_use) as SumN
                , SUM(a.color_numerator) AS color_numerator
                , SUM(ib.IB) as sumIB
            FROM zeroi a
            LEFT JOIN ibtable ib
              ON (a.iso_country_code=ib.country and a.calendar_year_month=ib.month_begin and a.platform_name=ib.platform_subset)
            LEFT JOIN country_info c
              ON a.iso_country_code=c.country_alpha2 
            LEFT JOIN hwval hw
              ON a.platform_name=hw.platform_subset
            group by  1,2,3,4,5,6,7,8,9,10)
            , sub_1 AS (
            SELECT platform_name as printer_platform_name
                    , printer_region_code 
                    , FYearMo
                    , CM
                    , EP
                    , sf_mf
                    --, platform_function_code
                    , platform_market_code
                    , market10
                    , country_alpha2
                    , de
                    , SUM(usage_numerator) AS SumMPVwtN
                    , SUM(usage_denominator) AS SumibWt
                    , SUM(color_numerator) AS SumCMPVwtN
                    , SUM(color_numerator)/SUM(usage_numerator) AS meansummpvwtn
                    , SUM(SumN) AS SumN
                    , SUM(sumIB) AS sumIB
                    , SUM(sumIB*usage_numerator) AS pgs
                   FROM sub0
                    GROUP BY
                      platform_name  
                    , printer_region_code 
                    , FYearMo
                    , CM
                    , EP
                    , sf_mf
                    --, platform_function_code
                    , platform_market_code
                    , market10
                    , country_alpha2
                    , de
                    ORDER BY
                      platform_name  
                    , printer_region_code 
                    , FYearMo
                    , CM
                    , EP)
                    select * from sub_1
            ")

zero <- subset(zero, !is.na(CM))

zero$platform_market_code <- ifelse(zero$platform_market_code=="WFP","WGP",zero$platform_market_code)

#zero$EP <- ifelse(substring(zero$printer_platform_name,1,10)=="CLEARWATER",'PRO',zero$EP)
#zero$EP <- ifelse(zero$printer_platform_name=="CLEARWATER MANAGED",'ENT',zero$EP)
#zero$platform_market_code <- ifelse(substring(zero$printer_platform_name,1,10)=="CLEARWATER",'SWL',zero$platform_market_code)

zero$pMPV <- ifelse(((zero$FYearMo) >= start1 & (zero$FYearMo <=end1)), zero$meansummpvwtn, NA)
zero$pN <- ifelse(((zero$FYearMo) >= start1 & (zero$FYearMo <=end1)), zero$SumN, NA)
zero$pMPVN <- zero$pMPV*zero$pN

zero_platform <- sqldf('select distinct printer_platform_name, printer_region_code from zero order by printer_platform_name, printer_region_code')
zero_platform$source1 <-"tri_printer_usage_sn"
zero_platformList <- reshape2::dcast(zero_platform, printer_platform_name ~printer_region_code, value.var="source1")

countFACT_LP_MONTH <- sum(!is.na(zero_platformList[, c("AP","EU", "LA","NA")])) 
paste("The total numbers of platform - region combinations that could be retrieved from the FACT PRINTER LASER MONTH table =", countFACT_LP_MONTH)

head(zero)

#not_in_use <- sqldf("select platform_subset,min(month_begin) as mindate,max(month_begin) as maxdate, sf_mf,mono_color,vc_category,subbrand,pro_vs_ent,business_feature from ibtable where platform_subset not in (select platform_name from zeroi) group by platform_subset,sf_mf,mono_color,vc_category,subbrand,pro_vs_ent,business_feature  ") 
#not_in_use2 <- sqldf("select platform_subset,min(month_begin) as mindate,max(month_begin) as maxdate, sf_mf,mono_color,vc_category,subbrand,pro_vs_ent,business_feature from ibtable where platform_subset not in (select printer_platform_name from zero) and platform_subset not in (select platform_subset from not_in_use) group by platform_subset,sf_mf,mono_color,vc_category,subbrand,pro_vs_ent,business_feature  ") 
#miss_val <- sqldf("select printer_platform_name, printer_region_code , CM, EP, sf_mf, platform_market_code
                  #from zero
                    #where (CM is null or EP is null or sf_mf is null or platform_market_code is null) and SumibWt is not null
                   #")

# COMMAND ----------

for (ppn in unique(zero$printer_platform_name))
{
grph1 <- subset(zero, printer_platform_name==ppn)
if (grph1$CM[1] %in% "M") next
grph1 <- subset(grph1,!is.na(meansummpvwtn))
if (nrow(grph1) < 2) next
grph1$dt <- as.Date(paste0(grph1$FYearMo,"01"),format="%Y%m%d")
grph1$colgp <- paste0(grph1$market10,"_",grph1$de)

grph1b <- grph1 %>% dplyr::group_by(printer_region_code) %>% dplyr::slice(tail(dplyr::row_number(), 6))
meanppn <- aggregate(grph1b$meansummpvwtn, by=list(grph1b$market10, grph1b$de),FUN=median)

colnames(meanppn) <- c("market10","devem","meanpct")

meanppn$colgp <- paste0(meanppn$market10,"_",meanppn$devem)

grph2 <- ggplot(data=grph1, aes(x=dt, y=meansummpvwtn, color=colgp)) +
  geom_line() +
  ggtitle(ppn) +
  geom_hline(data=meanppn,aes(yintercept=meanpct,color=colgp))
plot(grph2)
}

# COMMAND ----------

# Step 2 - extract Normalized value for RM and PE based on Plat_Nm, Region_Cd and Src_Cd

# Based on the extract from Step 1, for a given group defined by Plat_Nm, Region_Cd and Src_Cd, create the 
# following variables; Sum of pMPVN as SUMpMPVN, Sum of pN as SUMpN	and SUMpMPVN/SUMpN as NormMPV

one <- sqldf(paste("								
                   SELECT *, SUMpMPVN AS NormMPV
                   , 'avaiable' as dummy
                   FROM										
                   (										
                   SELECT printer_platform_name, printer_region_code,  market10, de, platform_market_code, sf_mf
                   , MEDIAN(pMPV) AS SUMpMPVN										
                   , SUM(pN) AS SUMpN										
                   FROM										
                   zero	
                   WHERE CM='C'
                   GROUP BY printer_platform_name, printer_region_code, market10, de, platform_market_code, sf_mf

                   )AA0	
                   where SUMpN >=", MUTminSize, " and SUMpN is not null ORDER BY printer_platform_name, printer_region_code, sf_mf
                   ", sep = " "))

head(one)
colnames(one)
dim(one)
str(one)

oneT <- reshape2::dcast(one, printer_platform_name + market10 + de ~ .	, value.var="dummy")

# COMMAND ----------

# Step 3 - combine zero and one using left outer join

# Combine outcome of Step 2 to the outcome of Step 1 using left outer join and 
  # further create the following variables; MPV_Pct = meanSumMPVWtN/ NormMPV and MPVPN =  MPV_Pct* SumN
 
two <- sqldf('select aa1.*, aa2.NormMPV, aa2.SUMpN
               from 
               zero aa1 
               left outer join 
               one aa2
               on 
               aa1.printer_platform_name = aa2.printer_platform_name
               and 
               aa1.market10 = aa2.market10
               and 
               aa1.de=aa2.de
               and aa1.sf_mf = aa2.sf_mf
               where 
               (aa2.NormMPV is NOT NULL
               or 
               aa2.NormMPV != 0)
               order by 
               printer_platform_name ASC, 								
               printer_region_code ASC, 								
               fyearmo ASC, 								
               CM ASC, 								
               platform_market_code ASC')
  
str(two)

# COMMAND ----------

# Step 4 - summary stat for a group  defined by EP, CM, printer_region_code and FYearMo

# Using the extract from Step 3, create the following summary stat for a group defined by EP,CM, printer_region_code 
# and FYearMo (notice, Plat_Nm is no more a part of a group); Sum of MPVPN as SumMPVPN and Sum of SumN as SumN

three <- sqldf('select CM, platform_market_code, printer_region_code,market10, de, sf_mf
               , MEDIAN(NormMPV) as medNMPV
               , AVG(NormMPV) as mnMPV
               , SUM(SUMpN) as SumN 
               FROM two
               WHERE CM ="C" and NormMPV > 0
               GROUP by CM, platform_market_code, printer_region_code,market10, de, sf_mf
               ORDER by CM, platform_market_code, printer_region_code,market10, de, sf_mf
               ')
threeb <- sqldf('SELECT CM, platform_market_code, printer_region_code,market10, de, printer_platform_name, sf_mf
              , max(NormMPV) as NormMPV, SUM(SUMpN) as SUMpN
              FROM two WHERE NormMPV > 0
              GROUP BY CM, platform_market_code, printer_region_code,market10, de, printer_platform_name, sf_mf
              ')
threeb <- threeb %>% dplyr::group_by(CM, platform_market_code, printer_region_code, market10, de, sf_mf) %>% plyr::mutate(wmedNMPV = weighted.median(x=NormMPV, w=SUMpN, na.rm=TRUE)) %>% 
    plyr::mutate(wmnNMPV = weighted.mean(x=NormMPV, w=SUMpN, na.rm=TRUE))
threec <- sqldf('SELECT CM, platform_market_code, printer_region_code ,market10, de, sf_mf, max(wmedNMPV) as wmedNMPV, max(wmnNMPV) as wmnNMPV from threeb group by CM, platform_market_code
          , printer_region_code,market10, de, sf_mf 
                ')
threed <- sqldf('SELECT CM, platform_market_code, printer_region_code,market10, de, printer_platform_name, sf_mf
              , max(NormMPV) as NormMPV, SUM(sumIB) as sumIB
              FROM two WHERE NormMPV > 0
              GROUP BY CM, platform_market_code, printer_region_code,market10, de, printer_platform_name, sf_mf
              ')
threed <- threed %>% dplyr::group_by(CM, platform_market_code, printer_region_code,market10, de, sf_mf) %>% plyr::mutate(wmedNMPVib = weighted.median(x=NormMPV, w=sumIB, na.rm=TRUE)) %>% 
        plyr::mutate(wmnNMPVib = weighted.mean(x=NormMPV, w=sumIB, na.rm=TRUE))
threee <- sqldf('SELECT CM, platform_market_code, printer_region_code,market10, de, sf_mf, max(wmedNMPVib) as wmedNMPVib, max(wmnNMPVib) as wmnNMPVib
                from threed group by CM, platform_market_code,market10, de, printer_region_code, sf_mf 
                ')
threef <- sqldf('SELECT CM, platform_market_code, printer_region_code,market10, de, printer_platform_name, sf_mf
              , max(NormMPV) as NormMPV, SUM(pgs) as sumPgs
              FROM two WHERE NormMPV > 0 AND pgs > 0
              GROUP BY CM, platform_market_code, printer_region_code,market10, de, printer_platform_name, sf_mf
              ')
threef <- threef %>% dplyr::group_by(CM, platform_market_code, printer_region_code,market10, de, sf_mf) %>% plyr::mutate(wmedNMPVpg = weighted.median(x=NormMPV, w=sumPgs, na.rm=TRUE)) %>% 
    plyr::mutate(wmnNMPVpg = weighted.mean(x=NormMPV, w=sumPgs, na.rm=TRUE))
threeg <- sqldf('SELECT CM, platform_market_code,market10, de, printer_region_code, sf_mf, max(wmedNMPVpg) as wmedNMPVpg, max(wmnNMPVpg) as wmnNMPVpg from threef group by CM, platform_market_code, printer_region_code,market10, de, sf_mf 
                ')
threegdpt <- subset(threeg,platform_market_code=="WGP" & printer_region_code %in% c("AP","LA") & sf_mf=="SF")
threegdpt$platform_market_code <- 'DPT'
threeg <- rbind(threeg,threegdpt)
threegj <- subset(threeg,printer_region_code=="AP")
threegj$printer_region_code <- 'JP'
threeg <- rbind(threeg,threegj)

# COMMAND ----------

# # Step 5 - drop groups if respective number < 200 and create MUT

# four <- sqldf(paste("select three.CM, one.platform_market_code, one.printer_region_code, one.market10, one.de, one.printer_platform_name, one.sf_mf
#                     , one.NormMPV
#                     , three.medNMPV
#                     , threec.wmedNMPV
#                     , threee.wmedNMPVib
#                     , threeg.wmedNMPVpg
#                     , three.mnMPV
#                     , threec.wmnNMPV
#                     , threee.wmnNMPVib
#                     , threeg.wmnNMPVpg
#                     from one
#                     left join three 
#                     on one.platform_market_code=three.platform_market_code and one.printer_region_code=three.printer_region_code 
#                       and one.sf_mf=three.sf_mf and one.market10=three.market10 and one.de=three.de
#                     left join threec 
#                     on one.platform_market_code=threec.platform_market_code and one.printer_region_code=threec.printer_region_code 
#                       and one.sf_mf=threec.sf_mf and one.market10=threec.market10 and one.de=threec.de
#                     left join threee 
#                     on one.platform_market_code=threee.platform_market_code and one.printer_region_code=threee.printer_region_code 
#                       and one.sf_mf=threee.sf_mf and one.market10=threee.market10 and one.de=threee.de
#                     left join threeg 
#                     on one.platform_market_code=threeg.platform_market_code and one.printer_region_code=threeg.printer_region_code 
#                       and one.sf_mf=threeg.sf_mf and one.market10=threeg.market10 and one.de=threeg.de
#                     where SumN >=", minSize, " and medNMPV is not null 
#                     order by three.CM, one.platform_market_code, one.printer_region_code", sep = " ")
# )

# COMMAND ----------

#UPM <- s3read_using(FUN = read.csv, object = paste0("s3://insights-environment-sandbox/BrentT/UPM_ctry(",UPMDate,").csv"),  header=TRUE, sep=",", na="")
UPMDate <- dbutils.widgets.get("upm_date")
UPM0 <- SparkR::read.parquet(path=paste0("s3://", aws_bucket_name, "UPM_ctry(",UPMDate,").parquet"))

# UPM <- s3read_using(FUN = read_parquet, object = paste0("s3://insights-environment-sandbox/BrentT/UPM_ctry(",UPMDate,").parquet"))

UPM1 <- SparkR::filter(UPM0, "CM == 'C'")

createOrReplaceTempView(UPM1, "UPM")
createOrReplaceTempView(as.DataFrame(hwval), "hwval")

UPM <- SparkR::sql("select upm.*, hw.sf_mf as SM, hw.pro_vs_ent as EP
              from UPM upm left join hwval hw on upm.Platform_Subset_Nm =hw.platform_subset")

createOrReplaceTempView(UPM, "UPM")

# UPMc <- sqldf("SELECT upm.*, b.wmedNMPVpg
#                FROM UPM upm
#                LEFT JOIN threeg b
#                 ON upm.Mkt=b.platform_market_code and upm.Region=b.printer_region_code and upm.SM=b.sf_mf
              
#               ")

# COMMAND ----------

# Step 84 - Renaming and aligning Variables

createOrReplaceTempView(as.DataFrame(one), "one")
createOrReplaceTempView(as.DataFrame(threef), "threef")
createOrReplaceTempView(as.DataFrame(threeg), "threeg")

final9 <- SparkR::sql('
                select distinct
                upm.Platform_Subset_Nm
                , upm.Region_3
                , upm.Region
                , upm.Region_DE
                , upm.Country_Cd
                , upm.Country_Nm
                , upm.FYearMo
                , upm.rFYearMo
                , upm.FYearQtr
                , upm.rFYearQtr
                , upm.FYear
                , upm.FMo
                , upm.MoSI
                , CASE WHEN c.NormMPV > 0 THEN c.NormMPV
                       WHEN b.wmedNMPVpg > 0 THEN b.wmedNMPVpg
                       WHEN d.wmedNMPVpg > 0 THEN d.wmedNMPVpg
                       ELSE NULL
                       END
                  AS color_pct
                , upm.MPV_N
                , upm.IB
                , upm.FIntroDt
                , upm.EP
                , upm.CM
                , upm.SM
                , upm.Mkt
                --, upm.FMC
                --, upm.K_PPM
                --, upm.Clr_PPM
                --, upm.Intro_Price
                , upm.MPV_Init
                , upm.Decay
                , upm.Seasonality
                , upm.Cyclical
                , upm.MUT
                , upm.Trend
                , upm.IMPV_Route
                from UPM upm
                LEFT JOIN threeg b
                    ON upm.Mkt=b.platform_market_code and upm.market10=b.market10 and upm.Region_DE=substr(b.de,1,1) and upm.SM=b.sf_mf
                left join one c
                    ON upm.Mkt=c.platform_market_code and upm.market10=c.market10 and upm.Region_DE=substr(c.de,1,1)  and upm.SM=c.sf_mf and upm.Platform_Subset_Nm=c.printer_platform_name
                left join threef d
                    ON upm.Mkt=d.platform_market_code and upm.Region_DE=substr(d.de,1,1) and upm.SM=d.sf_mf
                ')

printSchema(final9)
# final9$FYearMo <- as.character(final9$FYearMo)

# head(final9)

# COMMAND ----------

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


# s3write_using(x=final9,FUN = write.csv, object = paste0("s3://insights-environment-sandbox/BrentT/UPMColor_ctry(",Sys.Date(),").csv"), row.names=FALSE, na="")
# s3write_using(x=final9,FUN = write_parquet, object = paste0("s3://insights-environment-sandbox/BrentT/UPMColor_ctry(",Sys.Date(),").parquet"))
SparkR::write.parquet(x=final9, path=paste0("s3://", aws_bucket_name, "UPMColor_ctry(",Sys.Date(),").parquet"), mode="overwrite")


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


