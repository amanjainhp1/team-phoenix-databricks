# Databricks notebook source
# ---
# #Version 2021.01.19.1#
# title: "100% IB Toner Share (country level)"
# output:
#   html_notebook: default
#   pdf_document: default
# ---

# COMMAND ----------

notebook_start_time <- Sys.time()

# COMMAND ----------

dbutils.widgets.text("ib_version", "")
dbutils.widgets.text("redshift_secrets_name", "")
dbutils.widgets.text("sqlserver_secrets_name", "")
dbutils.widgets.dropdown("stack", "dev", list("dev", "itg", "prd"))
dbutils.widgets.text("aws_iam_role", "")
dbutils.widgets.text("bdtbl", "cumulus_prod04_dashboard.dashboard.print_share_usage_agg_stg")
dbutils.widgets.text("upm_date", "")
dbutils.widgets.text("writeout", "")

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

# MAGIC %sh
# MAGIC ls -l /usr/bin/java
# MAGIC ls -l /etc/alternatives/java
# MAGIC ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/default-java
# MAGIC R CMD javareconf

# COMMAND ----------

packages <- c("rJava", "RJDBC", "DBI", "sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR", "spatstat", "nls2")

# COMMAND ----------

# MAGIC %run ../common/package_check.r

# COMMAND ----------

options(java.parameters = "-Xmx30g" ) 
options(stringsAsFactors = FALSE)

tempdir(check=TRUE)
# writeout <- 'NO' #change to YES to write to MDM
UPMDate <- dbutils.widgets.get("upm_date") #Sys.Date() #Change to date if not running on same date as UPM "2021-07-19" #  '2021-09-10' #
UPMColorDate <- dbutils.widgets.get("upm_color_date") #Sys.Date() #Change to date if not running on same date as UPM "2021-07-19" # '2021-09-10' #

#--------Ouput Qtr Pulse or Quarter End-----------------------------------------------------------#
outnm_dt <- dbutils.widgets.get("outnm_dt")

# COMMAND ----------

# mount s3 bucket to cluster
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

###Data from Cumulus

sqlserver_driver <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/dbfs/FileStore/jars/801b0636_e136_471a_8bb4_498dc1f9c99b-mssql_jdbc_9_4_0_jre8-13bd8.jar")

cprod <- dbConnect(sqlserver_driver, paste0("jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database=IE2_Prod;user=", sparkR.conf("sfai_username"), ";password=", sparkR.conf("sfai_password")))

clanding <- dbConnect(sqlserver_driver, paste0("jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database=IE2_Landing;user=", sparkR.conf("sfai_username"), ";password=", sparkR.conf("sfai_password")))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val cutoffDate = dbutils.widgets.get("cutoff_dt")
# MAGIC val tableMonth0Query = s"""
# MAGIC            SELECT  tpmib.printer_group  
# MAGIC            , tpmib.printer_platform_name as platform_name 
# MAGIC            , tpmib.platform_std_name 
# MAGIC            , tpmib.printer_country_iso_code as iso_country_code
# MAGIC            , tpmib.fiscal_year_quarter 
# MAGIC            , tpmib.date_month_dim_ky as calendar_year_month
# MAGIC            --Get BD Flags 
# MAGIC            , CASE WHEN tpmib.ps_region_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' THEN 1 ELSE 0 END AS page_share_region_incl_flag 
# MAGIC            , CASE WHEN tpmib.ps_country_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' THEN 1 ELSE 0 END AS page_share_country_incl_flag
# MAGIC            , CASE WHEN tpmib.ps_market_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' THEN 1 ELSE 0 END as page_share_market_incl_flag
# MAGIC            , CASE WHEN tpmib.usage_region_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' THEN 1 ELSE 0 END AS usage_region_incl_flag 
# MAGIC            , CASE WHEN tpmib.usage_country_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' THEN 1 ELSE 0 END AS usage_country_incl_flag
# MAGIC            , CASE WHEN tpmib.usage_market_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' THEN 1 ELSE 0 END AS usage_market_incl_flag
# MAGIC            , CASE WHEN tpmib.share_region_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' then 1 else 0 END AS share_region_incl_flag   
# MAGIC            , CASE WHEN tpmib.share_country_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' THEN 1 ELSE 0 END AS share_country_incl_flag
# MAGIC            , CASE WHEN tpmib.share_market_incl_flag=1 AND tpmib.date_month_dim_ky < '${cutoffDate}' THEN 1 ELSE 0 END AS share_market_incl_flag
# MAGIC            --Get numerator and denominator, already ib weighted at printer level
# MAGIC 
# MAGIC            --ci share
# MAGIC            , SUM(COALESCE(tpmib.supply_installs_cmyk_hp_trade_ib_ext_sum,0)) AS ci_numerator 
# MAGIC            , SUM(COALESCE(tpmib.supply_installs_cmyk_share_ib_ext_sum,0)) AS ci_denominator
# MAGIC            --pageshare
# MAGIC            , SUM(COALESCE(tpmib.supply_pages_cmyk_hp_ib_ext_sum,0)) AS ps_numerator
# MAGIC            , SUM(COALESCE(tpmib.supply_pages_cmyk_share_ib_ext_sum,0)) AS ps_denominator
# MAGIC            --usage
# MAGIC            , SUM(COALESCE(tpmib.print_pages_total_ib_ext_sum,0)) as usage_numerator
# MAGIC            , SUM(COALESCE(tpmib.print_pages_color_ib_ext_sum,0)) as color_numerator
# MAGIC            , SUM(COALESCE(tpmib.print_months_ib_ext_sum,0)) as usage_denominator
# MAGIC            --counts
# MAGIC            , SUM(COALESCE(tpmib.printer_count_month_page_share_flag_sum,0)) AS printer_count_month_ps
# MAGIC            , SUM(COALESCE(tpmib.printer_count_month_supply_install_flag_sum,0)) AS printer_count_month_ci
# MAGIC            , SUM(COALESCE(tpmib.printer_count_month_usage_flag_sum,0)) AS printer_count_month_use
# MAGIC 
# MAGIC            , SUM(COALESCE(tpmib.printer_count_fyqtr_page_share_flag_sum,0)) AS printer_count_fiscal_quarter_ci   
# MAGIC            , SUM(COALESCE(tpmib.printer_count_fyqtr_supply_install_flag_sum,0)) AS printer_count_fiscal_quarter_ps   
# MAGIC            , SUM(COALESCE(tpmib.printer_count_fyqtr_usage_flag_sum,0)) AS printer_count_fiscal_quarter_use
# MAGIC 
# MAGIC             FROM 
# MAGIC               ${dbutils.widgets.get("bdtbl")} tpmib with (NOLOCK)
# MAGIC             WHERE 1=1 
# MAGIC             --AND tpmib.date_month_dim_ky < '${cutoffDate}'
# MAGIC             AND printer_route_to_market_ib='Aftermarket'
# MAGIC             AND printer_platform_name not in ('CICADA PLUS ROW',
# MAGIC                                       'TSUNAMI 4:1 ROW',
# MAGIC                                       'CRICKET',
# MAGIC                                       'LONE PINE',
# MAGIC                                       'MANTIS',
# MAGIC                                       'CARACAL',
# MAGIC                                       'EAGLE EYE',
# MAGIC                                       'SID',
# MAGIC                                       'TSUNAMI 4:1 CH/IND')
# MAGIC             GROUP BY tpmib.printer_group  
# MAGIC            , tpmib.printer_platform_name
# MAGIC            , tpmib.platform_std_name  
# MAGIC            , tpmib.printer_country_iso_code
# MAGIC            , tpmib.fiscal_year_quarter 
# MAGIC            , tpmib.date_month_dim_ky
# MAGIC            , tpmib.share_region_incl_flag 
# MAGIC            , tpmib.share_country_incl_flag
# MAGIC            , tpmib.share_market_incl_flag
# MAGIC            , tpmib.ps_region_incl_flag
# MAGIC            , tpmib.ps_country_incl_flag
# MAGIC            , tpmib.ps_market_incl_flag
# MAGIC            , tpmib.usage_region_incl_flag
# MAGIC            , tpmib.usage_country_incl_flag
# MAGIC            , tpmib.usage_market_incl_flag
# MAGIC """
# MAGIC 
# MAGIC val tableMonth0 = readRedshiftToDF(configs)
# MAGIC   .option("query", tableMonth0Query)
# MAGIC   .load()
# MAGIC 
# MAGIC tableMonth0.createOrReplaceTempView("table_month0")

# COMMAND ----------

table_month0 <- SparkR::sql("SELECT * FROM table_month0")

SparkR::write.parquet(x=table_month0, path=paste0("s3://", aws_bucket_name, "BD_data_" ,outnm_dt, "(", Sys.Date(), ").parquet"), mode="overwrite")

table_month0 <- SparkR::collect(table_month0)

# COMMAND ----------

# MAGIC %scala
# MAGIC // ######Big Data Version#########
# MAGIC val dmVersionQuery = s"""select distinct insert_ts from ${dbutils.widgets.get("bdtbl")} tri_printer_usage_sn with (NOLOCK)"""
# MAGIC val dmVersion: String = readRedshiftToDF(configs)
# MAGIC   .option("query", dmVersionQuery)
# MAGIC   .load().collect().map(_.getTimestamp(0)).mkString("")
# MAGIC 
# MAGIC spark.conf.set("dm_version", dmVersion)

# COMMAND ----------

#######SELECT IB VERSION#####
#ib_version <- as.character(dbGetQuery(ch,"select max(ib_version) as ib_version from biz_trans.tri_platform_measures_ib"))  #FROM BD Dashboard
ib_version <- dbutils.widgets.get("ib_version") #SELECT SPECIFIC VERSION
#ib_version <- as.character(dbGetQuery(cprod,"select max(version) as ib_version from IE2_Prod.dbo.ib with (NOLOCK)"))  #Phoenix

dm_version <- as.character(sparkR.conf("dm_version"))
#be sure ib versions match in these next two tables
ibtable <- dbGetQuery(cprod,paste("
                   select  a.platform_subset
                          , YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          , e.Fiscal_Year_Qtr as fiscal_year_quarter
                          , d.technology AS hw_type
                          , b.region_5
                          , a.country
                          , substring(b.developed_emerging,1,1) as de
                          , SUM(COALESCE(a.units,0)) as ib
                          , a.version
                    from IE2_Prod.dbo.ib a with (NOLOCK)
                    left join IE2_Prod.dbo.iso_country_code_xref b with (NOLOCK)
                      on (a.country=b.country_alpha2)
                    left join IE2_Prod.dbo.hardware_xref d with (NOLOCK)
                       on (a.platform_subset=d.platform_subset)
                    left join IE2_Prod.dbo.calendar e with (NOLOCK)
                        on (a.cal_date=e.date)
                    where a.measure='ib'
                      and (upper(d.technology)='LASER' or (d.technology='PWA' and (upper(d.hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3')) 
                          or a.platform_subset like 'PANTHER%' or a.platform_subset like 'JAGUAR%'))
                      --and YEAR(a.cal_date) < 2026
                      and a.version='",ib_version,"'
                    group by a.platform_subset, a.cal_date 
                            , e.Fiscal_Year_Qtr, d.technology
                            , a.version
                            , b.developed_emerging
                            , b.region_5, a.country
                   ",sep="", collapse = NULL))

product_ib2a <- dbGetQuery(cprod,paste("
                   select  a.platform_subset
                          , a.cal_date
                          , YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          , YEAR(DATEADD(month,+2,a.cal_date))*100+MONTH(DATEADD(month,+2,a.cal_date)) as fyearmo
                          , d.technology AS hw_type
                          , a.customer_engagement  AS RTM
                          , b.region_5
                          , b.country_alpha2
                          , sum(a.units) as ib
                          , a.version
                    from IE2_Prod.dbo.ib a with (NOLOCK)
                    left join IE2_Prod.dbo.iso_country_code_xref b with (NOLOCK)
                      on (a.country=b.country_alpha2)
                    left join IE2_Prod.dbo.hardware_xref d with (NOLOCK)
                       on (a.platform_subset=d.platform_subset)
                    where a.measure='ib'
                      and (upper(d.technology)='LASER' or (d.technology='PWA' and (upper(d.hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3')) 
                          or a.platform_subset like 'PANTHER%' or a.platform_subset like 'JAGUAR%'))
                      and a.version='",ib_version,"'
                      --and YEAR(a.cal_date) < 2026
                    group by a.platform_subset, a.cal_date, d.technology, a.customer_engagement,a.version
                      , b.region_5, b.country_alpha2
                   ",sep="", collapse = NULL))

hwval <- dbGetQuery(cprod,"
                  SELECT distinct platform_subset
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
                  where technology in ('Laser','PWA')")


#Get Intro Dates
intro_dt <- dbGetQuery(cprod, paste0("
                     with minyr as (
                     SELECT platform_subset, min(YEAR(cal_date)*100+MONTH(cal_date)) AS intro_month, min(cal_date) as printer_intro_month
                     FROM ie2_Prod.dbo.ib with (NOLOCK)
                     WHERE version='",ib_version,"'
                     AND measure='ib'
                     GROUP BY platform_subset
                     ),
                     max_units_prep as
                     (
                      SELECT platform_subset
                      , cal_date
                      , version
                      , max(units) as maxunits
                      FROM ie2_Prod.dbo.ib with (NOLOCK)
                      WHERE 1=1
                      AND version = '",ib_version,"'
                      AND measure = 'ib'
                      GROUP BY platform_subset, cal_date, version
                      ),
                      max_units as
                      (
                      SELECT platform_subset
                      , cal_date
                      , maxunits
                      , row_number() over (partition by platform_subset order by maxunits desc) as rn
                      FROM max_units_prep
                      )

                      SELECT a.platform_subset, a.intro_month, a.printer_intro_month, b.cal_date as printer_high_month
                      FROM minyr a LEFT JOIN (SELECT * FROM max_units WHERE rn=1) b ON a.platform_subset=b.platform_subset
                     "))

product_info2 <- sqldf("SELECT a.*,b.printer_intro_month, b.printer_high_month
                       from hwval a 
                       left join intro_dt b 
                       on a.platform_subset=b.platform_subset")

#Get Market10 Information
country_info <- dbGetQuery(cprod,"
                      WITH mkt10 AS (
                           SELECT country_alpha2, country_level_2 as market10, country_level_4 as emdm
                           FROM IE2_Prod.dbo.iso_cc_rollup_xref with (NOLOCK)
                           WHERE country_scenario='Market10'
                      ),
                      rgn5 AS (
                            SELECT country_alpha2, CASE WHEN region_5='JP' THEN 'AP' ELSE region_5 END AS region_5, developed_emerging, country 
                            FROM IE2_Prod.dbo.iso_country_code_xref with (NOLOCK)
                      )
                      SELECT a.country_alpha2, a.region_5, b.market10, a.developed_emerging, country
                            FROM rgn5 a
                            LEFT JOIN mkt10 b
                            ON a.country_alpha2=b.country_alpha2
                           ")
country_info <- sqldf("SELECT * from country_info where country_alpha2 in (select distinct country from ibtable)")
  country_info$region_5 <- ifelse(country_info$market10=='Latin America','LA',country_info$region_5)

table_month <- sqldf("
                    with sub0 as (select a.platform_name, c.region_5 as printer_region_code, c.market10, c.developed_emerging, c.country_alpha2, a.calendar_year_month as FYearMo
                    , a.fiscal_year_quarter
                , hw.platform_chrome_code AS CM
                , hw.platform_business_code AS EP
                , hw.platform_function_code AS platform_function_code
                , hw.platform_market_code
                , max(share_region_incl_flag) AS share_region_incl_flag 
                , max(share_country_incl_flag) AS share_country_incl_flag
                , max(share_market_incl_flag) AS share_market_incl_flag
                , max(page_share_region_incl_flag) AS page_share_region_incl_flag
                , max(page_share_country_incl_flag) AS page_share_country_incl_flag
                , max(page_share_market_incl_flag) AS page_share_market_incl_flag
                , max(usage_region_incl_flag) AS usage_region_incl_flag
                --, c.developed_emerging as de
                , SUM(a.ci_numerator) as ci_numerator
                , SUM(a.ci_denominator) as ci_denominator
                , SUM(a.ps_numerator) as ps_numerator
                , SUM(a.ps_denominator) as ps_denominator
                , SUM(a.usage_numerator) as usage_numerator
                , SUM(a.usage_denominator) as usage_denominator
                , SUM(a.printer_count_month_ps) as printer_count_month_ps
                , SUM(a.printer_count_month_ci) as printer_count_month_ci
                , SUM(a.printer_count_month_use) as printer_count_month_use
            FROM table_month0 a
            LEFT JOIN ibtable ib
              ON (a.iso_country_code=ib.country and a.calendar_year_month=ib.month_begin and a.platform_name=ib.platform_subset)
            LEFT JOIN country_info c
              ON a.iso_country_code=c.country_alpha2 
            LEFT JOIN hwval hw
              ON a.platform_name=hw.platform_subset
              group by  1,2,3,4,5,6,7,8,9,10, 11)
            , sub_1 AS (
            SELECT platform_name as printer_platform_name
                    , printer_region_code 
                    , market10
                    , developed_emerging
                    , country_alpha2
                    , FYearMo
                    , fiscal_year_quarter
                    , CM
                    , EP
                    , platform_function_code
                    , platform_market_code
                    , share_region_incl_flag
                    , share_country_incl_flag
                    , share_market_incl_flag
                    , page_share_region_incl_flag 
                    , page_share_country_incl_flag 
                    , page_share_market_incl_flag 
                    , usage_region_incl_flag
                    --, de
                    , SUM(ci_numerator) as ci_numerator
                    , SUM(ci_denominator) as ci_denominator
                    , SUM(ps_numerator) as ps_numerator
                    , SUM(ps_denominator) as ps_denominator
                    , SUM(usage_numerator) as usage_numerator
                    , SUM(usage_denominator) as usage_denominator
                    , SUM(printer_count_month_ps) as printer_count_month_ps
                    , SUM(printer_count_month_ci) as printer_count_month_ci
                    , SUM(printer_count_month_use) as printer_count_month_use
                   FROM sub0
                    GROUP BY
                      platform_name  
                    , printer_region_code 
                    , market10
                    , developed_emerging
                    , country_alpha2
                    , FYearMo
                    , CM
                    , EP
                    , share_region_incl_flag
                    , share_country_incl_flag
                    , share_market_incl_flag
                    , page_share_region_incl_flag 
                    , page_share_country_incl_flag 
                    , page_share_market_incl_flag 
                    , usage_region_incl_flag
                    --, de
                    ORDER BY
                      platform_name  
                    , printer_region_code 
                    , FYearMo
                    , CM
                    , EP)
                    select * from sub_1
                          ")

# COMMAND ----------

# MAGIC %scala
# MAGIC val predRawFile = spark.read
# MAGIC   .option("delimiter", ",")
# MAGIC   .option("quote", "\"")
# MAGIC   .option("header", "true")
# MAGIC   .csv("s3a://insights-environment-sandbox/BrentT/Predecessor_List_5Nov19.csv")
# MAGIC 
# MAGIC predRawFile.createOrReplaceTempView("pred_raw_file")

# COMMAND ----------

###Background Data not on Cumulus

#TODO: move to S3?
#printer_list <- s3read_using(FUN = read_xlsx, object = "s3://insights-environment-sandbox/BrentT/IE Proxy Map Toner(2019.03.05).xlsx", sheet = "Data", col_names=TRUE)
# Pred_Raw_file <-   s3read_using(FUN = read.csv, object = paste0("s3://insights-environment-sandbox/BrentT/Predecessor_List_5Nov19.csv"), na="")
Pred_Raw_file <- SparkR::collect(SparkR::sql("SELECT * FROM pred_raw_file"))

# Pred_Raw_file_out <- sqldf("select a.*,b.product_previous_model
#                            from Pred_Raw_file a
#                            left join product_info2 b  on a.Platform_Subset=b.platform_subset_name")
# s3write_using(FUN = write.csv, x=Pred_Raw_file_out,object = paste0("s3://insights-environment-sandbox/BrentT/Predecessor_List_5Nov19.csv"), na="")

printer_listf <- sqldf("select distinct Platform_Subset as platform_subset_name
                  ,CASE 
                    WHEN mdm_predecessor is not null THEN mdm_predecessor
                    WHEN Van_Predecessor is not null THEN Van_Predecessor
                    WHEN Toner_Predecessor is not null THEN Toner_Predecessor
                    WHEN Trevor_Predecessor is not null THEN Trevor_Predecessor
                    WHEN Mike_Predecessor is not null THEN Mike_Predecessor
                    WHEN Rainbow_Predecessor is not null THEN Rainbow_Predecessor
                    WHEN [HW.Predecessor] is not null THEN [HW.Predecessor]
                    WHEN [Emma.Predecessor] is not null THEN [Emma.Predecessor]
                    WHEN product_previous_model is not null THEN product_previous_model
                    ELSE null 
                    END AS Predecessor
                  ,CASE
                    WHEN mdm_predecessor is not null THEN 'mdm'
                    WHEN Van_Predecessor is not null THEN 'Van'
                    WHEN Toner_Predecessor is not null THEN 'Toner'
                    WHEN Trevor_Predecessor is not null THEN 'Trevor'
                    WHEN Mike_Predecessor is not null THEN 'Mike'
                    WHEN Rainbow_Predecessor is not null THEN 'Rainbow'
                    WHEN [HW.Predecessor] is not null THEN 'HW'
                    WHEN [Emma.Predecessor] is not null THEN 'Emma'
                    WHEN product_previous_model is not null THEN 'BD info table'
                    ELSE null 
                    END AS Predecessor_src
                  from Pred_Raw_file")

printer_list <- sqldf("SELECT pi.platform_subset as platform_subset_name
                      , CASE WHEN pi.predecessor is not null then pi.predecessor
                             WHEN pl.Predecessor='0' THEN NULL
                             ELSE pl.Predecessor
                             END AS Predecessor
                      , CASE WHEN pi.predecessor is not null then 'mdm'
                            WHEN pl.Predecessor='0' THEN NULL
                            ELSE pl.Predecessor_src
                            END AS Predecessor_src
                      , pi.product_line_code, pi.platform_market_code, pi.platform_function_code, pi.platform_business_code, pi.platform_chrome_code, pi.print_mono_speed_pages
                      , pi.print_color_speed_pages, pi.por_ampv, pi.printer_technology_type, pi.product_group, pi.platform_speed_segment_code, pi.platform_division_code, pi.printer_intro_month
                      from product_info2 pi
                      left join printer_listf pl
                      on pi.platform_subset=pl.platform_subset_name
                      ")
head(printer_list)




product_ib2 <- sqldf("
              SELECT pib.platform_subset as platform_subset_name
              , ci.region_5 as region_code 
              , ci.market10
              , ci.developed_emerging
              , ci.country_alpha2
              , pib.cal_date
              , pib.month_begin
              , pib.fyearmo
              , CASE WHEN SUBSTR(pib.fyearmo,5,2) IN ('01','02','03') THEN SUBSTR(pib.fyearmo,1,4)||'Q1'
                      WHEN SUBSTR(pib.fyearmo,5,2) IN ('04','05','06') THEN SUBSTR(pib.fyearmo,1,4)||'Q2'
                      WHEN SUBSTR(pib.fyearmo,5,2) IN ('07','08','09') THEN SUBSTR(pib.fyearmo,1,4)||'Q3'
                      WHEN SUBSTR(pib.fyearmo,5,2) IN ('10','11','12') THEN SUBSTR(pib.fyearmo,1,4)||'Q4'
                      ELSE NULL END AS fiscal_year_quarter
              , sum(pib.ib) AS tot_ib
               FROM product_ib2a pib
               LEFT JOIN country_info ci
                ON pib.country_alpha2=ci.country_alpha2
                where month_begin IS NOT NULL
               group by pib.platform_subset, ci.region_5, ci.market10, ci.developed_emerging
              , ci.country_alpha2, pib.cal_date, pib.month_begin, pib.fyearmo
  
")

# COMMAND ----------

###Page Share

page_share_1c <- subset(table_month,page_share_country_incl_flag==1)
page_share_1 <- subset(table_month,page_share_region_incl_flag==1)
page_share_1m <- subset(table_month,page_share_market_incl_flag==1)
page_share_2c <- subset(page_share_1c,!is.na(country_alpha2) & !is.na(ps_numerator) & ps_numerator/ps_denominator>0.01)
page_share_2 <- subset(page_share_1,!is.na(country_alpha2) & !is.na(ps_numerator) & ps_numerator/ps_denominator>0.01)
page_share_2m <- subset(page_share_1m,!is.na(country_alpha2) & !is.na(ps_numerator) & ps_numerator/ps_denominator>0.01)

#check1 <- subset(page_share_2,platform_name=="MOON AIO" & fiscal_year_quarter=="2017Q3" & iso_country_code=="BG")
#check2 <- subset(page_share,page_share$`Platform Name`=="MOON AIO" & page_share$`Report Period`=="2017Q3" & page_share$Country=="BULGARIA")

page_share_reg <- sqldf("
                       SELECT printer_platform_name as platform_name, fiscal_year_quarter
                       --, product_type
                       , printer_region_code as region_code 
                       --, de                 
                        , SUM(ci_numerator) as ci_numerator
                        , SUM(ci_denominator) as ci_denominator
                        , SUM(ps_numerator) as ps_numerator
                        , SUM(ps_denominator) as ps_denominator
                        , SUM(usage_numerator) as usage_numerator
                        , SUM(usage_denominator) as usage_denominator
                        , SUM(printer_count_month_ps) as printer_count_month_ps
                        , SUM(printer_count_month_ci) as printer_count_month_ci
                        , SUM(printer_count_month_use) as printer_count_month_use
                       , page_share_region_incl_flag
                      from  page_share_2
                      group by platform_name,fiscal_year_quarter
                       --,product_type
                       ,region_code 
                       , page_share_region_incl_flag
                      order by platform_name,fiscal_year_quarter
                       --,product_type
                       ,region_code 
                       , page_share_region_incl_flag
                       ")
page_share_m10 <- sqldf("
                       SELECT printer_platform_name as platform_name, fiscal_year_quarter
                       --, product_type
                       , printer_region_code as region_code 
                       , market10
                       --, de                 
                        , SUM(ci_numerator) as ci_numerator
                        , SUM(ci_denominator) as ci_denominator
                        , SUM(ps_numerator) as ps_numerator
                        , SUM(ps_denominator) as ps_denominator
                        , SUM(usage_numerator) as usage_numerator
                        , SUM(usage_denominator) as usage_denominator
                        , SUM(printer_count_month_ps) as printer_count_month_ps
                        , SUM(printer_count_month_ci) as printer_count_month_ci
                        , SUM(printer_count_month_use) as printer_count_month_use
                       , page_share_region_incl_flag
                      from  page_share_2m
                      group by platform_name,fiscal_year_quarter
                       --,product_type
                       ,region_code, market10
                       , page_share_region_incl_flag
                      order by platform_name,fiscal_year_quarter
                       --,product_type
                       ,region_code 
                       , page_share_region_incl_flag
                       ")
page_share_mde <- sqldf("
                       SELECT printer_platform_name as platform_name, fiscal_year_quarter
                       --, product_type
                       , printer_region_code as region_code 
                       , market10
                       , developed_emerging                 
                        , SUM(ci_numerator) as ci_numerator
                        , SUM(ci_denominator) as ci_denominator
                        , SUM(ps_numerator) as ps_numerator
                        , SUM(ps_denominator) as ps_denominator
                        , SUM(usage_numerator) as usage_numerator
                        , SUM(usage_denominator) as usage_denominator
                        , SUM(printer_count_month_ps) as printer_count_month_ps
                        , SUM(printer_count_month_ci) as printer_count_month_ci
                        , SUM(printer_count_month_use) as printer_count_month_use
                       , page_share_region_incl_flag
                      from  page_share_2m
                      group by platform_name,fiscal_year_quarter
                       --,product_type
                       ,region_code, market10, developed_emerging
                       , page_share_region_incl_flag
                      order by platform_name,fiscal_year_quarter
                       --,product_type
                       ,region_code 
                       , page_share_region_incl_flag
                       ")
page_share_ctr <- sqldf("
                       SELECT printer_platform_name as platform_name, fiscal_year_quarter
                       --, product_type
                       , printer_region_code as region_code 
                       , market10
                       , developed_emerging  
                       , country_alpha2
                        , SUM(ci_numerator) as ci_numerator
                        , SUM(ci_denominator) as ci_denominator
                        , SUM(ps_numerator) as ps_numerator
                        , SUM(ps_denominator) as ps_denominator
                        , SUM(usage_numerator) as usage_numerator
                        , SUM(usage_denominator) as usage_denominator
                        , SUM(printer_count_month_ps) as printer_count_month_ps
                        , SUM(printer_count_month_ci) as printer_count_month_ci
                        , SUM(printer_count_month_use) as printer_count_month_use
                       , page_share_region_incl_flag
                      from  page_share_2c
                      group by platform_name,fiscal_year_quarter
                       --,product_type
                       ,region_code, market10, developed_emerging, country_alpha2
                       , page_share_region_incl_flag
                      order by platform_name,fiscal_year_quarter
                       --,product_type
                       ,region_code 
                       , page_share_region_incl_flag
                       ")

combcolr <- c('platform_name','region_code')
combcolm <- c('platform_name','region_code','market10')
combcold <- c('platform_name','region_code','market10','developed_emerging')
combcolc <- c('platform_name','region_code','market10','developed_emerging','country_alpha2')

page_share_reg$grp <- apply(page_share_reg[,combcolr],1, paste,collapse ="_")
page_share_reg$value<- page_share_reg$ps_numerator/(page_share_reg$ps_denominator)
page_share_m10$grp <- apply(page_share_m10[,combcolm],1, paste,collapse ="_")
page_share_m10$value<- page_share_m10$ps_numerator/(page_share_m10$ps_denominator)
page_share_mde$grp <- apply(page_share_mde[,combcold],1, paste,collapse ="_")
page_share_mde$value<- page_share_mde$ps_numerator/(page_share_mde$ps_denominator)
page_share_ctr$grp <- apply(page_share_ctr[,combcolc],1, paste,collapse ="_")
page_share_ctr$value<- page_share_ctr$ps_numerator/(page_share_ctr$ps_denominator)
#page_share_reg$region_code<-ifelse(page_share_reg$region_code=="AJ","AP",page_share_reg$region_code)

#str(page_share_reg)

page_share_reg <- merge(page_share_reg,intro_dt, by.x="platform_name", by.y="platform_subset", all.x = TRUE)
page_share_m10 <- merge(page_share_m10,intro_dt, by.x="platform_name", by.y="platform_subset", all.x = TRUE)
page_share_mde <- merge(page_share_mde,intro_dt, by.x="platform_name", by.y="platform_subset", all.x = TRUE)
page_share_ctr <- merge(page_share_ctr,intro_dt, by.x="platform_name", by.y="platform_subset", all.x = TRUE)

page_share_niq2r <- subset(page_share_reg,is.na(printer_intro_month))
page_share_reg <- subset(page_share_reg,!is.na(printer_intro_month))
page_share_niq2m <- subset(page_share_m10,is.na(printer_intro_month))
page_share_m10 <- subset(page_share_m10,!is.na(printer_intro_month))
page_share_niq2d <- subset(page_share_mde,is.na(printer_intro_month))
page_share_mde <- subset(page_share_mde,!is.na(printer_intro_month))
page_share_niq2c <- subset(page_share_ctr,is.na(printer_intro_month))
page_share_ctr <- subset(page_share_ctr,!is.na(printer_intro_month))

page_share_reg <- subset(page_share_reg, !is.na(value))
page_share_m10 <- subset(page_share_m10, !is.na(value))
page_share_mde <- subset(page_share_mde, !is.na(value))
page_share_ctr <- subset(page_share_ctr, !is.na(value))

#page_share_reg <- subset(page_share_reg, page_share_reg$supply_count_share>200)
page_share_reg$fiscal_year_quarter<-as.character(page_share_reg$fiscal_year_quarter)
page_share_m10$fiscal_year_quarter<-as.character(page_share_m10$fiscal_year_quarter)
page_share_mde$fiscal_year_quarter<-as.character(page_share_mde$fiscal_year_quarter)
page_share_ctr$fiscal_year_quarter<-as.character(page_share_ctr$fiscal_year_quarter)
str(page_share_ctr)

#Remove A3
#page_share_reg <- subset(page_share_reg,product_type=="A4")

# COMMAND ----------

##Region 5

page_share_reg$printer_intro_month <- as.Date(page_share_reg$printer_intro_month)
page_share_reg$printer_high_month <- as.Date(page_share_reg$printer_high_month)

data_coefr <- list()

for (printer in unique(page_share_reg$grp)){
  
  dat1 <- subset(page_share_reg,grp==printer & fiscal_year_quarter != '2020Q1' & fiscal_year_quarter != '2020Q2')
  #dat1 <- subset(page_share_reg,grp=='CICADA PLUS ROW_NA' & fiscal_year_quarter != '2020Q1' & fiscal_year_quarter != '2020Q2')
  if (nrow(dat1) <4 ) {next}
  
  #mindt <- min(dat1$fiscal_year_quarter)
  mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
  dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  dat1$timediff2 <- round(as.numeric(difftime(as.Date(dat1$printer_high_month,format="%Y-%m-%d"),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  dat1$timediff3 <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(dat1$printer_intro_month))*4)
  mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
  mintimediff2 <- min(dat1$timediff3) #number of quarters between printer introduction and start of data
  #values for sigmoid curve start
  midtime <- (min(dat1$timediff)+max(dat1$timediff))/2 #median(dat1$timediff)
  midtime2 <- min(dat1$timediff) #median(dat1$timediff)
  midtime3 <- min(dat1$timediff3) #median(dat1$timediff)
  maxtimediff <- max(90-max(dat1$timediff),0) #number of months between end of data and 7.5 years (90 months)
  #maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
  maxtimediff2 <- max(dat1$timediff)
  maxtimediff3 <- max(2*midtime3-max(dat1$timediff3),0) #number of quarters between end of data and 30 quarters
  
  dat1$value <- as.numeric(dat1$value)
  maxv=min(max(dat1$value)+max(dat1$value)*(0.002*mintimediff),1.5)
  minv=max(min(dat1$value)-min(dat1$value)*(0.002*maxtimediff),0.05)
  maxv2=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff),1.5)
  minv2=max(min(dat1$value)-min(dat1$value)*(0.02*maxtimediff),0.05)
  maxv3=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff2),1.5)
  minv3=max(min(dat1$value)-min(dat1$value)*(0.02*maxtimediff2),0.05)
  spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2,.1)
  spreadv2 <- min(1-(maxv + minv)/2,(maxv+minv)/2)
  spreadv3 <- min(1-(maxv + minv)/2,(maxv+minv)/2)
  frstshr <- dat1[1,10]
 
  #Sigmoid Model  
  sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
 
  fitmodel <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                    lower=c(min=0.05,max=minv,med=0, spread=0.01),
                    upper=c(min=maxv,max=2,med=1000, spread=.1),
                    algorithm = "grid-search", 
                    #weights=supply_count_share,
                    #weights = timediff,
                    control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
  #linear model
  dat1b <- sqldf("select * from dat1 order by timediff desc limit 8")
  fitmodel2 <- lm(value ~ timediff, data=dat1b)
  fitmodel3 <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv2, max=maxv2, med=midtime2, spread=spreadv2),
                    #lower=c(min=0.05,max=minv,med=0, spread=0.05),
                    #upper=c(min=maxv,max=2,med=1000, spread=1),
                    algorithm = "grid-search", 
                    #weights=supply_count_share,
                    #weights = timediff2,
                    control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
  
  fitmodel4 <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv3, max=maxv3, med=midtime3, spread=spreadv3),
                    #lower=c(min=0.05,max=minv,med=0, spread=0.05),
                    #upper=c(min=maxv,max=2,med=1000, spread=1),
                    algorithm = "grid-search", 
                    #weights=supply_count_share,
                    #weights = timediff3,
                    control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
  
  timediff <- c(0:150)
  preddata <- as.data.frame(timediff)

  #dat2<- dat1[,c("timediff")]
  preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)

  fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
  fit2 <- as.data.frame(predict(object=fitmodel3,newdata=preddata, type="response", se.fit=F))
  fit3 <- as.data.frame(predict(object=fitmodel4,newdata=preddata, type="response", se.fit=F))
  colnames(fit1) <- "proposed"
  colnames(fit2) <- "QE"
  colnames(fit3) <- "pulse"
  preddata <- cbind(preddata,fit1,fit2,fit3)
  preddata <- sqldf(paste("select a.*, b.value as obs
                            --, b.supply_count_share
                          from preddata a left join dat1 b on a.timediff=b.timediff"))
  #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
                                  #ifelse(preddata$supply_count_share<10000,3,4)))

   plot(y=preddata$value,x=preddata$timediff, type="n"
        ,main=paste("Share for ",printer)
         ,xlab="Date", ylab="HP Share", ylim=c(0,2))
   #lines(y=preddata$value,x=preddata$timediff, col='black')
   lines(y=preddata$pulse,x=preddata$timediff,col="blue")
   lines(y=preddata$QE,x=preddata$timediff,col="red")
   lines(y=preddata$proposed,x=preddata$timediff,col="grey")
   #legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
  
    coeffo <- as.data.frame(coef(fitmodel))
    coeffo2 <- as.data.frame(coef(fitmodel2))
    coeffo3 <- as.data.frame(coef(fitmodel3))
    coeffo4 <- as.data.frame(coef(fitmodel4))
    
    coeffout<-  as.data.frame(cbind(coeffo[1,],coeffo[2,],coeffo[3,]
                        ,coeffo[4,],coeffo2[1,],coeffo2[2,]
                        #,unique(as.character(dat1$Platform.Subset))
                        #,unique(as.character(dat1$Region_5))
                        #,unique(as.character(dat1$RTM))
                        ,unique(as.character(dat1$grp))
                        ,coeffo3[1,],coeffo3[2,],coeffo3[3,]
                        ,coeffo3[4,]
                        ,coeffo4[1,],coeffo4[2,],coeffo4[3,]
                        ,coeffo4[4,]
    ))
    colnames(coeffout)[1] <- "min"
    colnames(coeffout)[2] <- "max"
    colnames(coeffout)[3] <- "med"
    colnames(coeffout)[4] <- "spread"
    colnames(coeffout)[5] <- "a"
    colnames(coeffout)[6] <- "b"
    colnames(coeffout)[7] <- "grp"
    colnames(coeffout)[8] <- "min2"
    colnames(coeffout)[9] <- "max2"
    colnames(coeffout)[10] <- "med2"
    colnames(coeffout)[11] <- "spread2"
    colnames(coeffout)[12] <- "min3"
    colnames(coeffout)[13] <- "max3"
    colnames(coeffout)[14] <- "med3"
    colnames(coeffout)[15] <- "spread3"

  
  data_coefr[[printer]] <- coeffout

}


#unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

##mkt 10

data_coefm <- list()

for (printer in unique(page_share_m10$grp)){
  
  dat1 <- subset(page_share_m10,grp==printer & fiscal_year_quarter != '2020Q1' & fiscal_year_quarter != '2020Q2')
  if (nrow(dat1) <4 ) {next}
  
  #mindt <- min(dat1$fiscal_year_quarter)
  mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
  dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
  mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
  
  #values for sigmoid curve start
  midtime <- (min(dat1$timediff)+min(dat1$timediff))/2 #median(dat1$timediff)
  maxtimediff <- max(90-max(dat1$timediff),0) #number of months between end of data and 7.5 years (90 months)
  #maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
  
  dat1$value <- as.numeric(dat1$value)
  maxv=min(max(dat1$value)+(max(dat1$value)*(0.002*mintimediff)),1.5)
  minv=max(min(dat1$value)-(min(dat1$value)*(0.002*maxtimediff)),0.05)
  spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2,.1)
  frstshr <- dat1[1,10]
  
  sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
 
  fitmodel <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                    lower=c(min=0.05,max=maxv,med=0, spread=0.01),
                    upper=c(min=minv,max=2,med=1000, spread=.1),
                    algorithm = "grid-search", 
                    #weights=supply_count_share,
                    weights = timediff,
                    control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
  
  dat1b <- sqldf("select * from dat1 order by timediff desc limit 8")
  fitmodel2 <- lm(value ~ timediff, data=dat1b)
  #fitmodel3 <- lm((value) ~ timediff, data=dat1)
    
  # timediff <- c(0:59)
  # preddata <- as.data.frame(timediff)
  # 
  # #dat2<- dat1[,c("timediff")]
  # preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)
  # 
  # fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
  # colnames(fit1) <- "fit1"
  # preddata <- cbind(preddata,fit1)
  # preddata <- sqldf(paste("select a.*, b.value as obs
  #                           --, b.supply_count_share
  #                         from preddata a left join dat1 b on a.timediff=b.timediff"))
  #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
  #                                ifelse(preddata$supply_count_share<10000,3,4)))

  # plot(y=preddata$value,x=preddata$timediff, type="p", col=preddata$rpgp
  #      ,main=paste("Share for ",printer)
  #       ,xlab="Date", ylab="HP Share", ylim=c(0,1))
  # lines(y=preddata$value,x=preddata$timediff, col='black')
  # lines(y=preddata$fit1,x=preddata$timediff,col="blue")
  # legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
  
    ceoffo <- as.data.frame(coef(fitmodel))
    coeffo2 <- as.data.frame(coef(fitmodel2))
  
    coeffout<-  as.data.frame(cbind(ceoffo[1,],ceoffo[2,],ceoffo[3,]
                        ,ceoffo[4,],coeffo2[1,],coeffo2[2,]
                        #,unique(as.character(dat1$Platform.Subset))
                        #,unique(as.character(dat1$Region_5))
                        #,unique(as.character(dat1$RTM))
                        ,unique(as.character(dat1$grp))
    ))
    colnames(coeffout)[1] <- "min"
    colnames(coeffout)[2] <- "max"
    colnames(coeffout)[3] <- "med"
    colnames(coeffout)[4] <- "spread"
    colnames(coeffout)[5] <- "a"
    colnames(coeffout)[6] <- "b"
    colnames(coeffout)[7] <- "grp"
  
  data_coefm[[printer]] <- coeffout

}


#unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

##developed_emerging

data_coefd <- list()

for (printer in unique(page_share_mde$grp)){
  
  dat1 <- subset(page_share_mde,grp==printer & fiscal_year_quarter != '2020Q1' & fiscal_year_quarter != '2020Q2')
  if (nrow(dat1) <4 ) {next}
  
  #mindt <- min(dat1$fiscal_year_quarter)
  mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
  dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
  mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
  
  #values for sigmoid curve start
  midtime <- (min(dat1$timediff)+min(dat1$timediff))/2 #median(dat1$timediff)
  maxtimediff <- max(90-max(dat1$timediff),0) #number of months between end of data and 7.5 years (90 months)
  #maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
  
  dat1$value <- as.numeric(dat1$value)
  maxv=min(max(dat1$value)+(max(dat1$value)*(0.002*mintimediff)),1.5)
  minv=max(min(dat1$value)-(min(dat1$value)*(0.002*maxtimediff)),0.05)
  spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2,.1)
  frstshr <- dat1[1,10]
  
  sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
 
  fitmodel <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                    lower=c(min=0.05,max=maxv,med=0, spread=0.01),
                    upper=c(min=minv,max=2,med=1000, spread=.1),
                    algorithm = "grid-search", 
                    #weights=supply_count_share,
                    weights = timediff,
                    control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
  
  dat1b <- sqldf("select * from dat1 order by timediff desc limit 8")
  fitmodel2 <- lm(value ~ timediff, data=dat1b)
  #fitmodel3 <- lm((value) ~ timediff, data=dat1)
    
  # timediff <- c(0:59)
  # preddata <- as.data.frame(timediff)
  # 
  # #dat2<- dat1[,c("timediff")]
  # preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)
  # 
  # fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
  # colnames(fit1) <- "fit1"
  # preddata <- cbind(preddata,fit1)
  # preddata <- sqldf(paste("select a.*, b.value as obs
  #                           --, b.supply_count_share
  #                         from preddata a left join dat1 b on a.timediff=b.timediff"))
  #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
  #                                ifelse(preddata$supply_count_share<10000,3,4)))

  # plot(y=preddata$value,x=preddata$timediff, type="p", col=preddata$rpgp
  #      ,main=paste("Share for ",printer)
  #       ,xlab="Date", ylab="HP Share", ylim=c(0,1))
  # lines(y=preddata$value,x=preddata$timediff, col='black')
  # lines(y=preddata$fit1,x=preddata$timediff,col="blue")
  # legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
  
    ceoffo <- as.data.frame(coef(fitmodel))
    coeffo2 <- as.data.frame(coef(fitmodel2))
  
    coeffout<-  as.data.frame(cbind(ceoffo[1,],ceoffo[2,],ceoffo[3,]
                        ,ceoffo[4,],coeffo2[1,],coeffo2[2,]
                        #,unique(as.character(dat1$Platform.Subset))
                        #,unique(as.character(dat1$Region_5))
                        #,unique(as.character(dat1$RTM))
                        ,unique(as.character(dat1$grp))
    ))
    colnames(coeffout)[1] <- "min"
    colnames(coeffout)[2] <- "max"
    colnames(coeffout)[3] <- "med"
    colnames(coeffout)[4] <- "spread"
    colnames(coeffout)[5] <- "a"
    colnames(coeffout)[6] <- "b"
    colnames(coeffout)[7] <- "grp"
  
  data_coefd[[printer]] <- coeffout

}


#unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

##country

data_coefc <- list()

for (printer in unique(page_share_ctr$grp)){
  
  dat1 <- subset(page_share_ctr,grp==printer & fiscal_year_quarter != '2020Q1' & fiscal_year_quarter != '2020Q2')
  if (nrow(dat1) <4 ) {next}
  
  #mindt <- min(dat1$fiscal_year_quarter)
  mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
  dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
  mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
  
  #values for sigmoid curve start
  midtime <- (min(dat1$timediff)+min(dat1$timediff))/2 #median(dat1$timediff)
  maxtimediff <- max(90-max(dat1$timediff),0) #number of months between end of data and 7.5 years (90 months)
  #maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
  
  dat1$value <- as.numeric(dat1$value)
  maxv=min(max(dat1$value)+(max(dat1$value)*(0.002*mintimediff)),1.5)
  minv=max(min(dat1$value)-(min(dat1$value)*(0.002*maxtimediff)),0.05)
  spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2,.1)
  frstshr <- dat1[1,10]
  
  sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
 
  fitmodel <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                    lower=c(min=0.05,max=maxv,med=0, spread=0.01),
                    upper=c(min=minv,max=2,med=1000, spread=.1),
                    algorithm = "grid-search", 
                    #weights=supply_count_share,
                    weights = timediff,
                    control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
  
  dat1b <- sqldf("select * from dat1 order by timediff desc limit 8")
  fitmodel2 <- lm(value ~ timediff, data=dat1b)
  #fitmodel3 <- lm((value) ~ timediff, data=dat1)
    
  # timediff <- c(0:59)
  # preddata <- as.data.frame(timediff)
  # 
  # #dat2<- dat1[,c("timediff")]
  # preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)
  # 
  # fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
  # colnames(fit1) <- "fit1"
  # preddata <- cbind(preddata,fit1)
  # preddata <- sqldf(paste("select a.*, b.value as obs
  #                           --, b.supply_count_share
  #                         from preddata a left join dat1 b on a.timediff=b.timediff"))
  #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
  #                                ifelse(preddata$supply_count_share<10000,3,4)))

  # plot(y=preddata$value,x=preddata$timediff, type="p", col=preddata$rpgp
  #      ,main=paste("Share for ",printer)
  #       ,xlab="Date", ylab="HP Share", ylim=c(0,1))
  # lines(y=preddata$value,x=preddata$timediff, col='black')
  # lines(y=preddata$fit1,x=preddata$timediff,col="blue")
  # legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
  
    ceoffo <- as.data.frame(coef(fitmodel))
    coeffo2 <- as.data.frame(coef(fitmodel2))
  
    coeffout<-  as.data.frame(cbind(ceoffo[1,],ceoffo[2,],ceoffo[3,]
                        ,ceoffo[4,],coeffo2[1,],coeffo2[2,]
                        #,unique(as.character(dat1$Platform.Subset))
                        #,unique(as.character(dat1$Region_5))
                        #,unique(as.character(dat1$RTM))
                        ,unique(as.character(dat1$grp))
    ))
    colnames(coeffout)[1] <- "min"
    colnames(coeffout)[2] <- "max"
    colnames(coeffout)[3] <- "med"
    colnames(coeffout)[4] <- "spread"
    colnames(coeffout)[5] <- "a"
    colnames(coeffout)[6] <- "b"
    colnames(coeffout)[7] <- "grp"
  
  data_coefc[[printer]] <- coeffout

}


#unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

#country <- c( "NA_CA","NA_US","EU_ODE","EU_FR","EU_DE","EU_IT","EU_ES","EU_GB","EU_OEE","EU_PL","EU_RU","EU_SA","EU_AT","EU_AE","EU_TR","LA_OLA","LA_MX","AP_ODA","AP_OEA","AP_TH","LA_AR","LA_BR","AP_CN","AP_IN")
#regions <- c("AP","EU","LA","NA")
#mkt10 <- unique(country_info$market10)
#devem <- c("Developed","Emerging")
cntry <- unique(country_info$country_alpha2)
#devem <- c("D", "E")
printer_list_ctr <- printer_list[rep(seq_len(nrow(printer_list)),each=nrow(country_info)),]
printer_list_ctr <- cbind(printer_list_ctr,cntry)
printer_list_ctr <- sqldf("select a.*,CASE WHEN c.region_5 is NULL THEN 'null' ELSE c.region_5 END as region_5, c.market10, c.developed_emerging
                          from printer_list_ctr a
                          left join country_info c
                          on a.cntry=c.country_alpha2")
#printer_list_reg <- printer_list_reg[rep(seq_len(nrow(printer_list_reg)),each=2),]
#printer_list_reg <- cbind(printer_list_reg, devem)
printer_list_ctr$FMC <- paste0(printer_list_ctr$platform_chrome_code,substring(printer_list_ctr$platform_function_code,1,1),printer_list_ctr$platform_market_code)

printer_list_ctr$grpr <-paste0(printer_list_ctr$platform_subset_name,"_",printer_list_ctr$region_5)
printer_list_ctr$grpm <-paste0(printer_list_ctr$platform_subset_name,"_",printer_list_ctr$region_5,"_",printer_list_ctr$market10)
printer_list_ctr$grpd <-paste0(printer_list_ctr$platform_subset_name,"_",printer_list_ctr$region_5,"_",printer_list_ctr$market10,"_",printer_list_ctr$developed_emerging)
printer_list_ctr$grpc <-paste0(printer_list_ctr$platform_subset_name,"_",printer_list_ctr$region_5,"_",printer_list_ctr$market10,"_",printer_list_ctr$developed_emerging,"_",printer_list_ctr$cntry)

#printer_list_reg$grp2 <-paste0(printer_list_reg$regions,"_",devem)
rownames(printer_list_ctr) <- NULL

#printer_list_reg <- subset(printer_list_reg,grp2 !="NA_E")
#printer_list_reg <- subset(printer_list_reg,grp2 !="LA_D")

data_coeffr <- as.data.frame(do.call("rbind", data_coefr))
data_coeffr <- subset(data_coeffr,as.numeric(as.character(max))>0)
data_coeffm <- as.data.frame(do.call("rbind", data_coefm))
data_coeffm <- subset(data_coeffm,as.numeric(as.character(max))>0)
data_coeffd <- as.data.frame(do.call("rbind", data_coefd))
data_coeffd <- subset(data_coeffd,as.numeric(as.character(max))>0)
data_coeffc <- as.data.frame(do.call("rbind", data_coefc))
data_coeffc <- subset(data_coeffc,as.numeric(as.character(max))>0)


printer_list_ctr$intro_date <- ymd(printer_list_ctr$printer_intro_month) %m+% months(2)

#library(RH2)
combined1 <- sqldf("select a.platform_subset_name
                            , a.Predecessor
                            , a.region_5 as regions
                            , SUBSTR(a.developed_emerging,1,1) as emdm
                            , a.cntry as country_alpha2
                            , CASE  
                                WHEN c.min is NOT NULL THEN c.min
                                WHEN d.min is NOT NULL THEN d.min
                                WHEN m.min is NOT NULL THEN m.min
                                WHEN r.min is NOT NULL THEN r.min
                                ELSE NULL
                                END as min
                            , CASE  
                                WHEN c.max is NOT NULL THEN c.max
                                WHEN d.max is NOT NULL THEN d.max
                                WHEN m.max is NOT NULL THEN m.max
                                WHEN r.max is NOT NULL THEN r.max
                                ELSE NULL
                                END as max
                            , CASE  
                                WHEN c.med is NOT NULL THEN c.med
                                WHEN d.med is NOT NULL THEN d.med
                                WHEN m.med is NOT NULL THEN m.med
                                WHEN r.med is NOT NULL THEN r.med
                                ELSE NULL
                                END as med
                            , CASE  
                                WHEN c.spread is NOT NULL THEN c.spread
                                WHEN d.spread is NOT NULL THEN d.spread
                                WHEN m.spread is NOT NULL THEN m.spread
                                WHEN r.spread is NOT NULL THEN r.spread
                                ELSE NULL
                                END as spread
                            , CASE  
                                WHEN c.a is NOT NULL THEN c.a
                                WHEN d.a is NOT NULL THEN d.a
                                WHEN m.a is NOT NULL THEN m.a
                                WHEN r.a is NOT NULL THEN r.a
                                ELSE NULL
                                END as a
                              , CASE  
                                WHEN c.b is NOT NULL THEN c.b
                                WHEN d.b is NOT NULL THEN d.b
                                WHEN m.b is NOT NULL THEN m.b
                                WHEN r.b is NOT NULL THEN r.b
                                ELSE NULL
                                END as b
                            , CASE  
                                WHEN c.min is NOT NULL THEN 'country'
                                WHEN d.min is NOT NULL THEN 'Dev/Em'
                                WHEN m.min is NOT NULL THEN 'Market10'
                                WHEN r.min is NOT NULL THEN 'Region'
                                ELSE NULL
                                END as source
                            , a.FMC
                            , a.printer_intro_month
                           
                    FROM printer_list_ctr a 
                    left join data_coeffr r
                      on (a.grpr=r.grp)
                    left join data_coeffm m
                      on (a.grpm=m.grp)
                    left join data_coeffd d
                      on (a.grpd=d.grp)
                    left join data_coeffc c
                      on (a.grpc=c.grp)
                   
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
head(output1)

# COMMAND ----------

# Find Proxy

predlist <- combined1b[c(1,3,5,2,12,15)]  #printer_platform_name, regions, country, Predecessor, source, curve_exists#
#predlist <- combined1b[c(1,3,2,10)]  #printer_platform_name, regions, Predecessor, curve_exists#
proxylist1 <- subset(predlist,curve_exists=="Y")
proxylist1$proxy <- proxylist1$printer_platform_name
proxylist1$gen   <- 0



####Managed to non-Managed####
predlist_use <- anti_join(predlist,proxylist1,by=c("printer_platform_name","country_alpha2"))
predlist$curve_exists <- ifelse(predlist$printer_platform_name=='CICADA PLUS ROW',"N",predlist$curve_exists)

predlist_use$pred_iter <- predlist_use$Predecessor


predlist_iter3 <- sqldf("
                  select distinct
                  a.printer_platform_name
                  , a.regions
                  , a.country_alpha2
                  , a.Predecessor
                  , a.Source
                  , CASE 
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.printer_platform_name as pred_iter
                  from 
                      (select a1.*, substr(a1.printer_platform_name,1,charindex(' MANAGED',a1.printer_platform_name)-1) as manname
                        from predlist_use a1 
                        where charindex('MANAGED',a1.printer_platform_name)>0) a
                  left join 
                     (select b1.*, 
                        CASE WHEN charindex(' ',b1.printer_platform_name)>0
                          THEN substr(b1.printer_platform_name,1,charindex(' ',b1.printer_platform_name)-1)
                        ELSE printer_platform_name
                        END AS manname
                      from predlist b1
                      where charindex('MANAGED',b1.printer_platform_name)=0) b
                    on (a.manname=b.manname and a.country_alpha2=b.country_alpha2)
                  ")


predlist_iter <- predlist_iter3
proxylist_iter <- subset(predlist_iter,curve_exists=="Y")

if(dim(proxylist_iter)[1] != 0){

proxylist_iter$proxy <- proxylist_iter$pred_iter
proxylist_iter <- proxylist_iter[c(1,2,3,4,5,6,8)]
proxylist_iter$gen   <- 0.3
proxylist1 <-rbind(proxylist1,proxylist_iter)
}
#predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","regions","emdm"))



####Same printer group####
predlist_use <- anti_join(predlist,proxylist1,by=c("printer_platform_name","regions"))
predlist_use$pred_iter <- predlist_use$Predecessor


predlist_iter3 <- sqldf("
                  select distinct
                  a.printer_platform_name
                  , a.regions
                  , a.country_alpha2
                  , a.Predecessor
                  , a.Source
                  , CASE
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.printer_platform_name as pred_iter
                  from
                      (select a1.*, a2.product_group as printer_group
                        from predlist_use a1
                        left join product_info2 a2
                        on (a1.printer_platform_name=a2.platform_subset)) a
                  left join
                     (select b1.*, b2.product_group as printer_group
                      from predlist b1
                      left join product_info2 b2
                      on (b1.printer_platform_name=b2.platform_subset)) b
                    on (a.printer_platform_name != b.printer_platform_name and a.printer_group=b.printer_group and a.country_alpha2=b.country_alpha2)
                    where a.printer_group is not null
                  ")
predlist_iter <- predlist_iter3
proxylist_iter <- subset(predlist_iter,curve_exists=="Y")

if(dim(proxylist_iter)[1] != 0){

proxylist_iter$proxy <- proxylist_iter$pred_iter
proxylist_iter <- proxylist_iter[c(1,2,3,4,5,6,8)]
proxylist_iter$gen   <- 0.5
proxylist1 <-rbind(proxylist1,proxylist_iter)
}
predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","country_alpha2"))

predlist_use$pred_iter <- predlist_use$Predecessor
predlist_use <- anti_join(predlist,proxylist1,by=c("printer_platform_name","country_alpha2"))
predlist_use$pred_iter <- predlist_use$Predecessor
####Predecessors####
y <- 0
repeat{
predlist_iter2 <- sqldf("
                  select a.printer_platform_name
                  , a.regions
                  , a.country_alpha2
                  , a.Predecessor
                  , a.source
                  , CASE 
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.Predecessor as pred_iter
                  from predlist_use a
                  left join predlist b
                    on (a.pred_iter=b.printer_platform_name and a.country_alpha2=b.country_alpha2)
                  ")
predlist_iter <- predlist_iter2
proxylist_iter <- subset(predlist_iter,curve_exists=="Y")
y<- y-1

#print(y)

if(dim(proxylist_iter)[1] != 0){

proxylist_iter$proxy <- proxylist_iter$Predecessor
proxylist_iter <- proxylist_iter[c(1,2,3,4,5,6,8)]
proxylist_iter$gen   <- y
proxylist1 <-rbind(proxylist1,proxylist_iter)
}
predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","regions"))

if(all(is.na(predlist_use$pred_iter))){break}
if(y<-15){break}
}

predlist_use$pred_iter <- predlist_use$printer_platform_name
#####Successors#####
y=0
repeat{
predlist_iter2 <- sqldf("
                  select a.printer_platform_name
                  , a.regions
                  , a.country_alpha2
                  , a.Predecessor
                  , a.source
                  , CASE 
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.printer_platform_name as pred_iter
                  from predlist_use a
                  left join predlist b
                    on (a.pred_iter=b.Predecessor and a.country_alpha2=b.country_alpha2)
                  ")

predlist_iter <- predlist_iter2
proxylist_iter <- subset(predlist_iter,curve_exists=="Y")

y<- y+1

#print(y)

if(dim(proxylist_iter)[1] != 0){

proxylist_iter$proxy <- proxylist_iter$pred_iter
proxylist_iter <- proxylist_iter[c(1,2,3,4,5,6,8)]
proxylist_iter$gen   <- y
proxylist1 <-rbind(proxylist1,proxylist_iter)
}

predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","country_alpha2"))

if(all(is.na(predlist_use$pred_iter))){break}
if(y>15){break}
}
#successor list empty

proxylist_temp <- proxylist1

leftlist <- anti_join(predlist,proxylist_temp,by=c("printer_platform_name","country_alpha2"))

# COMMAND ----------

# manual proxy assignments

leftlist$proxy <- ifelse(leftlist$printer_platform_name=="ATHENA MID","TAISHAN",
                  ifelse(leftlist$printer_platform_name=="ANTELOPE","STARS 3:1 ROW",
                  ifelse(leftlist$printer_platform_name=="ARGON", "VEGA PQ",
                  ifelse(leftlist$printer_platform_name=="CASCABEL CNTRCTL", "MAMBA",
                  ifelse(leftlist$printer_platform_name=="CICADA PLUS ROW","ASTEROID",
                  ifelse((leftlist$printer_platform_name=="CORDOBA MANAGED" & leftlist$regions=="AP"),"MERFCURY MFP",
                  ifelse(leftlist$printer_platform_name=="CORAL CNTRCTL","STARS 3:1",
                  ifelse(leftlist$printer_platform_name=="CRANE","CORAL C5",                         
                  ifelse((leftlist$printer_platform_name=="DENALI MANAGED" ),"DENALI MFP",
                  ifelse(leftlist$printer_platform_name=="HAWAII MFP","EVEREST MFP",
                  ifelse(leftlist$printer_platform_name=="HUMMINGBIRD PLUS" & (leftlist$regions=="NA" |(leftlist$regions=="AP")),"JAYBIRD",
                  ifelse(leftlist$printer_platform_name=="HUMMINGBIRD PLUS","BUCK",
                  ifelse(leftlist$printer_platform_name=="KONA","EVEREST MFP",
                  ifelse(leftlist$printer_platform_name=="OUTLAW","STARS 3:1",
                  ifelse(leftlist$printer_platform_name=="RAINIER MFP","DENALI MFP",
                  ifelse((leftlist$printer_platform_name=="SAPPHIRE MANAGED" & leftlist$regions=="EU"),"SAPPHIRE MFP",    
                  ifelse((leftlist$printer_platform_name=="SERRANO LITE" & leftlist$regions=="NA"),"FIJIMFP",
                  ifelse(leftlist$printer_platform_name=="ANTARES PQ","DIAMOND 40 MANAGED",
                  ifelse(leftlist$printer_platform_name=="ELECTRA PQ","DIAMOND 40 MANAGED",
                  ifelse(leftlist$printer_platform_name=="MAPLE","DIAMOND 40 MANAGED",
                  ifelse(leftlist$printer_platform_name=="MAPLE MANAGED","DIAMOND 40 MANAGED",
                  ifelse(leftlist$printer_platform_name=="VEGA MANAGED","DIAMOND 40 MANAGED",
                  ifelse(leftlist$printer_platform_name=="VEGA PQ","DIAMOND 40 MANAGED",
                         NA
                         )))))))))))))))))))))))
# proxylist_temp$proxy <- ifelse(proxylist_temp$printer_platform_name=="ATHENA MID","TAISHAN",
#                   ifelse(proxylist_temp$printer_platform_name=="ANTELOPE","STARS 3:1 ROW",
#                   #ifelse(proxylist_temp$printer_platform_name=="ARGON", "VEGA PQ",
#                   ifelse(proxylist_temp$printer_platform_name=="CASCABEL CNTRCTL", "MAMBA",
#                   ifelse(proxylist_temp$printer_platform_name=="CICADA PLUS ROW","ASTEROID",
#                   ifelse((proxylist_temp$printer_platform_name=="CORDOBA MANAGED" & proxylist_temp$regions=="AP"),"MERFCURY MFP",
#                   ifelse(proxylist_temp$printer_platform_name=="CORAL CNTRCTL","STARS 3:1",
#                   ifelse(proxylist_temp$printer_platform_name=="CRANE","CORAL C5",                         
#                   ifelse((proxylist_temp$printer_platform_name=="DENALI MANAGED" ),"DENALI MFP",
#                   ifelse(proxylist_temp$printer_platform_name=="HAWAII MFP","EVEREST MFP",
#                   ifelse(proxylist_temp$printer_platform_name=="HUMMINGBIRD PLUS" & (proxylist_temp$regions=="NA" |(proxylist_temp$regions=="AP")),"JAYBIRD",
#                   ifelse(proxylist_temp$printer_platform_name=="HUMMINGBIRD PLUS","BUCK",
#                   ifelse(proxylist_temp$printer_platform_name=="KONA","EVEREST MFP",
#                   ifelse(proxylist_temp$printer_platform_name=="OUTLAW","STARS 3:1",
#                   ifelse(proxylist_temp$printer_platform_name=="RAINIER MFP","DENALI MFP",
#                   ifelse((proxylist_temp$printer_platform_name=="SAPPHIRE MANAGED" & proxylist_temp$regions=="EU"),"SAPPHIRE MFP",    
#                   ifelse((proxylist_temp$printer_platform_name=="SERRANO LITE" & proxylist_temp$regions=="NA"),"FIJIMFP",
#                   ifelse((proxylist_temp$printer_platform_name=="ANTARES PQ" & proxylist_temp$proxy != "ANTARES PQ"),"ANTARES PQ REGION",
#                   ifelse((proxylist_temp$printer_platform_name=="ELECTRA PQ" & proxylist_temp$proxy != "ELECCTRA PQ"),"ELECTRA PQ REGION",
#                   ifelse((proxylist_temp$printer_platform_name=="MAPLE" & proxylist_temp$proxy != "MAPLE"),"MAPLE REGION",
#                   ifelse((proxylist_temp$printer_platform_name=="MAPLE MANAGED" & proxylist_temp$proxy != "MAPLE MANAGED"),"MAPLE MANAGED REGION",
#                   ifelse((proxylist_temp$printer_platform_name=="VEGA MANAGED" & proxylist_temp$proxy != "VEGA MANAGED"),"VEGA MANAGED REGION",
#                   ifelse((proxylist_temp$printer_platform_name=="VEGA PQ" & proxylist_temp$proxy != "VEGA PQ"),"VEGA PQ REGION",
#                          proxylist_temp$proxy
#                          )))))))))))))))))))))) #)
proxylist_left <- subset(leftlist, !is.na(proxy))
proxylist_temp <- bind_rows(proxylist_temp,proxylist_left)

#manual change of proxy

# COMMAND ----------

proxylist <- proxylist_temp[c(1,2,3,4,5,7)]
combined1na <- subset(combined1,regions=="NA")

output2<- sqldf("select a.printer_platform_name
                        , a.regions
                        , a.country_alpha2
                        , a.Predecessor
                        , c.proxy
                        , CASE WHEN c.proxy like '%REGION' THEN d.min ELSE b.min END as min
                        , CASE WHEN c.proxy like '%REGION' THEN d.max ELSE b.max END as max
                        , CASE WHEN c.proxy like '%REGION' THEN d.med ELSE b.med END as med
                        , CASE WHEN c.proxy like '%REGION' THEN d.spread ELSE b.spread END as spread
                        , CASE WHEN c.proxy like '%REGION' THEN d.a ELSE b.a END as a
                        , CASE WHEN c.proxy like '%REGION' THEN d.b ELSE b.b END as b
                        , a.source
                        , a.FMC
                        , a.printer_intro_month
                        from combined1 a
                        left join proxylist c
                        on (a.printer_platform_name=c.printer_platform_name and a.country_alpha2=c.country_alpha2)
                        left join combined1 b
                        on (c.proxy=b.printer_platform_name and c.country_alpha2=b.country_alpha2)
                        left join combined1na d
                        on (c.printer_platform_name=d.printer_platform_name)
              ")

head(output2)

# COMMAND ----------

# need to compare duplicates and get values for missing

missing <- subset(output2,is.na(min))

plotlook <- sqldf("select a.*, b.printer_intro_month as PIM
                  from output2 a
                  left join output2 b
                  on (a.proxy=b.printer_platform_name and a.country_alpha2=b.country_alpha2)
                  ")

  plotlook$min <- as.numeric(plotlook$min)
  plotlook$max <- as.numeric(plotlook$max)
  plotlook$med <- as.numeric(plotlook$med)
  plotlook$spread <- as.numeric(plotlook$spread)
  plotlook$a <- as.numeric(plotlook$a)
  plotlook$b <- as.numeric(plotlook$b)
  
plotlook_srt <- plotlook[order(plotlook$printer_platform_name,plotlook$country_alpha2,plotlook$PIM),]
plotlook_dual<- plotlook[which(duplicated(plotlook[,c('printer_platform_name','country_alpha2')])==T),]
plotlook_out <- plotlook[which(duplicated(plotlook[,c('printer_platform_name','country_alpha2')])==F),]

proxylist_out <- plotlook_out

proxylist_out$FMC <- ifelse(proxylist_out$printer_platform_name=="AMBER 25 MANAGED" | proxylist_out$printer_platform_name=="AMBER 30 MANAGED", "MMSWL",proxylist_out$FMC)

plotlook_out$grp <- paste0(plotlook_out$printer_platform_name,"_",plotlook_out$country_alpha2)

#sigmoid_pred <- function(min,max,med,spread,timediff){max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))}

# for (printer in unique(plotlook_out$grp)){
#   dat1 <- subset(plotlook_out,grp==printer)
#   #dat1 <- subset(plotlook_out,grp=="ALPINE PQ_AP")
#     if (is.na(dat1$proxy)) {next}
#   proxy <- dat1$proxy
#   platform <- dat1$printer_platform_name
#   region <- dat1$regions
#   #emdm <- dat1$emdm
#   timediff <- c(0:59)
#   preddata <- as.data.frame(timediff)
#   dat1 %>% uncount(60)
#   preddata <- cbind(preddata,dat1, row.names=NULL) 
#   preddata$value <- sigmoid_pred(preddata$min,preddata$max,preddata$med,preddata$spread,preddata$timediff)
#   
#   #plot(y=preddata$value,x=preddata$timediff, type="l",col="blue",main=paste("Share for ",platform,",",region,",",emdm,"\nUsing proxy: ",proxy),
#   #      xlab="Date", ylab="HP Share", ylim=c(0,1))
# 
# }

# COMMAND ----------

# fill in missing

modellist <- subset(proxylist_out,!is.na(min))
aggr_models <- do.call(data.frame,aggregate(cbind(min, max, med, spread, a, b) ~ country_alpha2 + FMC, data=modellist, FUN=function(x) c(mn = mean(x), md = median(x), mx=max(x))))
aggr_models$grp <- paste0(aggr_models$FMC,"_",aggr_models$country_alpha2)
# for (group in unique(aggr_models$grp)){
#   dat1 <- subset(aggr_models,grp==group)
#   timediff <- c(0:24)
#   preddata <- as.data.frame(timediff)
#   dat1 %>% uncount(25)
#   preddata <- cbind(preddata,dat1, row.names=NULL) 
#   preddata$value_mn <- sigmoid_pred(preddata$min.mn,preddata$max.mn,preddata$med.mn,preddata$spread.mn,preddata$timediff)
#   preddata$value_md <- sigmoid_pred(preddata$min.md,preddata$max.md,preddata$med.md,preddata$spread.md,preddata$timediff)
#   #plot(y=preddata$value_mn,x=preddata$timediff, type="l",col="blue",main=paste("Share for ",group),
#   #      xlab="Date", ylab="HP Share", ylim=c(0,1))
#   #lines(y=preddata$value_md,x=preddata$timediff, col='red')
# 
# }

cm <- c("C","M")
sm <- c("S", "S","M", "M")
bmlst <- as.data.frame(as.character(c("DPT", "DSK", "SWH", "SWL", "WGP")))
bmlst <- as.data.frame(bmlst[rep(seq_len(nrow(bmlst)),each=4),] )
colnames(bmlst)<-"BMC"
bmlst <- as.data.frame(cbind(bmlst,cm,sm))
bmlst <- as.data.frame(bmlst[rep(seq_len(nrow(bmlst)),each=nrow(country_info)),] )
agg_lst <- as.data.frame(cbind(bmlst,cntry))
#agg_lst <- as.data.frame(agg_lst[rep(seq_len(nrow(agg_lst)),each=2),] )
#agg_lst <- as.data.frame(cbind(agg_lst,devem))
colnames(agg_lst)[4] <- "country_alpha2"
agg_lst$FMC <- paste0(agg_lst$cm,agg_lst$sm,agg_lst$BMC)
aggr_models_f <- merge(agg_lst,aggr_models,by=c("FMC","country_alpha2"),all.x=TRUE)
aggr_models_f$country_alpha2 <- as.character(aggr_models_f$country_alpha2)

# COMMAND ----------

proxylist_b <- sqldf("
                         SELECT a.printer_platform_name, a.regions, a.country_alpha2, a.Predecessor, a.source 
                           ,CASE 
                            WHEN a.min IS NULL THEN b.FMC
                            ELSE a.proxy 
                          END as proxy 
                          ,CASE 
                            WHEN a.min IS NULL THEN b.[min.mn]
                            ELSE a.min 
                          END as min
                          ,CASE 
                            WHEN a.max IS NULL THEN b.[max.mn]
                            ELSE a.max 
                          END as max
                          ,CASE 
                            WHEN a.med IS NULL THEN b.[med.mn]
                            ELSE a.med 
                          END as med
                          ,CASE 
                            WHEN a.spread IS NULL THEN b.[spread.mn]
                            ELSE a.spread 
                          END as spread
                          ,CASE 
                            WHEN a.a IS NULL THEN b.[a.mx]
                            ELSE a.a
                          END as a
                          ,CASE 
                            WHEN a.b IS NULL THEN b.[b.mx]
                            ELSE a.b
                          END as b
                          , a.FMC
                        FROM proxylist_out a
                        LEFT JOIN  aggr_models b
                          ON (a.FMC=b.FMC AND a.country_alpha2=b.country_alpha2)
                         ")
proxylist_b$proxy <- ifelse(is.na(proxylist_b$proxy),"FMC",proxylist_b$proxy)

remaining <- subset(proxylist_b,is.na(min))

remaining2 <- subset(remaining,substring(FMC,nchar(FMC)-2,nchar(FMC))=='WGP' | substring(FMC,nchar(FMC)-2,nchar(FMC))=='DSK')

# remaining_LA <- subset(remaining,!is.na(FMC))
# remaining_LA$regions <- as.character(remaining_LA$regions)
# proxylist_la <- sqldf("
#                          SELECT a.printer_platform_name, a.regions, a.Predecessor 
#                           ,CASE 
#                             WHEN a.min IS NULL THEN 
#                               CASE
#                                 WHEN b.[min.mn] IS NULL THEN 
#                                   CASE 
#                                     WHEN c.[min.mn] IS NULL THEN 
#                                       CASE WHEN d.[min.mn] IS NULL THEN e.FMC
#                                       ELSE d.FMC
#                                       END
#                                     ELSE c.FMC
#                                   END
#                                 ELSE b.FMC
#                               END
#                             ELSE a.FMC 
#                           END as proxy
#                           ,CASE 
#                             WHEN a.min IS NULL THEN 
#                               CASE
#                                 WHEN b.[min.mn] IS NULL THEN 
#                                   CASE 
#                                     WHEN c.[min.mn] IS NULL THEN 
#                                       CASE WHEN d.[min.mn] IS NULL THEN e.[min.mn]
#                                       ELSE d.[min.mn]
#                                       END
#                                     ELSE c.[min.mn]
#                                   END
#                                 ELSE b.[min.mn]
#                               END
#                             ELSE a.min 
#                           END as min
#                           ,CASE 
#                             WHEN a.max IS NULL THEN 
#                               CASE
#                                 WHEN b.[max.mn] IS NULL THEN 
#                               CASE 
#                                 WHEN c.[max.mn] IS NULL THEN
#                                   CASE WHEN d.[max.mn] IS NULL THEN e.[max.mn]
#                                   ELsE d.[max.mn]
#                                   END
#                                 ELSE c.[max.mn]
#                                 END
#                                 ELSE b.[max.mn]
#                               END
#                             ELSE a.min
#                           END as max
#                           ,CASE 
#                               WHEN a.med IS NULL THEN 
#                               CASE
#                                 WHEN b.[med.mn] IS NULL THEN 
#                                   CASE 
#                                     WHEN c.[med.mn] IS NULL THEN
#                                       CASE WHEN d.[med.mn] IS NULL THEN e.[med.mn]
#                                       ELSE d.[med.mn]
#                                       END
#                                     ELSE c.[med.mn]
#                                   END
#                                 ELSE b.[med.mn]
#                               END
#                             ELSE a.min
#                           END as med
#                           ,CASE 
#                             WHEN a.spread IS NULL THEN 
#                               CASE
#                                 WHEN b.[spread.mn] IS NULL THEN 
#                                   CASE 
#                                     WHEN c.[spread.mn] IS NULL THEN
#                                       CASE WHEN d.[spread.mn] IS NULL THEN e.[spread.mn]
#                                       ELSE d.[spread.mn]
#                                       END
#                                     ELSE c.[spread.mn]
#                                   END
#                                 ELSE b.[spread.mn]
#                               END
#                             ELSE a.spread
#                           END as spread
#                           , a.FMC
#                         FROM remaining_LA a
#                         LEFT JOIN  aggr_models_f b
#                           ON (SUBSTR(a.FMC,1,2)=SUBSTR(b.FMC,1,2) AND SUBSTR(b.FMC,3,5)='WGP' AND a.regions=b.regions)
#                         LEFT JOIN  aggr_models_f c
#                           ON (SUBSTR(a.FMC,1,2)=SUBSTR(c.FMC,1,2) AND SUBSTR(c.FMC,3,5)='SWH' AND a.regions=c.regions)
#                         LEFT JOIN  aggr_models_f d
#                           ON (SUBSTR(a.FMC,1,2)=SUBSTR(d.FMC,1,2) AND SUBSTR(d.FMC,3,5)='SWL' AND a.regions=d.regions)
#                         LEFT JOIN  aggr_models_f e
#                           ON (SUBSTR(a.FMC,1,2)=SUBSTR(e.FMC,1,2) AND SUBSTR(e.FMC,3,5)='DSK' AND a.regions=e.regions)
#                          ")
# proxylist_la2 <- subset(proxylist_la,is.na(min))
# proxylist_la3 <- subset(proxylist_la,!is.na(min))
# #remaining_FMC <- subset(remaining,is.na(FMC))
# proxylist_c <- subset(proxylist_b,!is.na(min))
# proxylist_f1 <- rbind(proxylist_c,proxylist_la3)
proxylist_f1 <- subset(proxylist_b,!is.na(min))
#write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","Page Share for 100 percent IB DE ",".csv", sep=''), x=proxylist_final,row.names=FALSE, na="")

# COMMAND ----------

###Cartridge Unit Share

# 
# crg_share_1c <- subset(table_month,share_country_incl_flag==1)
# crg_share_1 <- subset(table_month,share_region_incl_flag==1)
# crg_share_1m <- subset(table_month,share_market_incl_flag==1)
# crg_share_2c <- subset(crg_share_1c,!is.na(country_alpha2) & !is.na(ci_numerator) & ci_numerator/ci_denominator>0.01)
# crg_share_2 <- subset(crg_share_1,!is.na(country_alpha2) & !is.na(ci_numerator) & ci_numerator/ci_denominator>0.01)
# crg_share_2m <- subset(crg_share_1m,!is.na(country_alpha2) & !is.na(ci_numerator) & ci_numerator/ci_denominator>0.01)
# 
# #check1 <- subset(page_share_2,platform_name=="MOON AIO" & fiscal_year_quarter=="2017Q3" & iso_country_code=="BG")
# #check2 <- subset(page_share,page_share$`Platform Name`=="MOON AIO" & page_share$`Report Period`=="2017Q3" & page_share$Country=="BULGARIA")
# 
# crg_share_reg <- sqldf("
#                        SELECT printer_platform_name as platform_name, fiscal_year_quarter
#                        --, product_type
#                        , printer_region_code as region_code 
#                        --, de                 
#                         , SUM(ci_numerator) as ci_numerator
#                         , SUM(ci_denominator) as ci_denominator
#                         , SUM(ps_numerator) as ps_numerator
#                         , SUM(ps_denominator) as ps_denominator
#                         , SUM(usage_numerator) as usage_numerator
#                         , SUM(usage_denominator) as usage_denominator
#                         , SUM(printer_count_month_ps) as printer_count_month_ps
#                         , SUM(printer_count_month_ci) as printer_count_month_ci
#                         , SUM(printer_count_month_use) as printer_count_month_use
#                        , share_region_incl_flag
#                       from  crg_share_2
#                       group by platform_name,fiscal_year_quarter
#                        --,product_type
#                        ,region_code 
#                        , share_region_incl_flag
#                       order by platform_name,fiscal_year_quarter
#                        --,product_type
#                        ,region_code 
#                        , share_region_incl_flag
#                        ")
# crg_share_m10 <- sqldf("
#                        SELECT printer_platform_name as platform_name, fiscal_year_quarter
#                        --, product_type
#                        , printer_region_code as region_code 
#                        , market10
#                        --, de                 
#                         , SUM(ci_numerator) as ci_numerator
#                         , SUM(ci_denominator) as ci_denominator
#                         , SUM(ps_numerator) as ps_numerator
#                         , SUM(ps_denominator) as ps_denominator
#                         , SUM(usage_numerator) as usage_numerator
#                         , SUM(usage_denominator) as usage_denominator
#                         , SUM(printer_count_month_ps) as printer_count_month_ps
#                         , SUM(printer_count_month_ci) as printer_count_month_ci
#                         , SUM(printer_count_month_use) as printer_count_month_use
#                        , share_region_incl_flag
#                       from crg_share_2m
#                       group by platform_name,fiscal_year_quarter
#                        --,product_type
#                        ,region_code, market10
#                        , share_region_incl_flag
#                       order by platform_name,fiscal_year_quarter
#                        --,product_type
#                        ,region_code 
#                        , share_region_incl_flag
#                        ")
# crg_share_mde <- sqldf("
#                        SELECT printer_platform_name as platform_name, fiscal_year_quarter
#                        --, product_type
#                        , printer_region_code as region_code 
#                        , market10
#                        , developed_emerging                 
#                         , SUM(ci_numerator) as ci_numerator
#                         , SUM(ci_denominator) as ci_denominator
#                         , SUM(ps_numerator) as ps_numerator
#                         , SUM(ps_denominator) as ps_denominator
#                         , SUM(usage_numerator) as usage_numerator
#                         , SUM(usage_denominator) as usage_denominator
#                         , SUM(printer_count_month_ps) as printer_count_month_ps
#                         , SUM(printer_count_month_ci) as printer_count_month_ci
#                         , SUM(printer_count_month_use) as printer_count_month_use
#                        , share_region_incl_flag
#                       from  crg_share_2m
#                       group by platform_name,fiscal_year_quarter
#                        --,product_type
#                        ,region_code, market10, developed_emerging
#                        , share_region_incl_flag
#                       order by platform_name,fiscal_year_quarter
#                        --,product_type
#                        ,region_code 
#                        , share_region_incl_flag
#                        ")
# crg_share_ctr <- sqldf("
#                        SELECT printer_platform_name as platform_name, fiscal_year_quarter
#                        --, product_type
#                        , printer_region_code as region_code 
#                        , market10
#                        , developed_emerging  
#                        , country_alpha2
#                         , SUM(ci_numerator) as ci_numerator
#                         , SUM(ci_denominator) as ci_denominator
#                         , SUM(ps_numerator) as ps_numerator
#                         , SUM(ps_denominator) as ps_denominator
#                         , SUM(usage_numerator) as usage_numerator
#                         , SUM(usage_denominator) as usage_denominator
#                         , SUM(printer_count_month_ps) as printer_count_month_ps
#                         , SUM(printer_count_month_ci) as printer_count_month_ci
#                         , SUM(printer_count_month_use) as printer_count_month_use
#                        , share_region_incl_flag
#                       from  crg_share_2c
#                       group by platform_name,fiscal_year_quarter
#                        --,product_type
#                        ,region_code, market10, developed_emerging, country_alpha2
#                        , share_region_incl_flag
#                       order by platform_name,fiscal_year_quarter
#                        --,product_type
#                        ,region_code 
#                        , share_region_incl_flag
#                        ")
# 
# crg_share_reg$grp <- apply(crg_share_reg[,combcolr],1, paste,collapse ="_")
# crg_share_reg$value<- crg_share_reg$ci_numerator/(crg_share_reg$ci_denominator)
# crg_share_m10$grp <- apply(crg_share_m10[,combcolm],1, paste,collapse ="_")
# crg_share_m10$value<- crg_share_m10$ci_numerator/(crg_share_m10$ci_denominator)
# crg_share_mde$grp <- apply(crg_share_mde[,combcold],1, paste,collapse ="_")
# crg_share_mde$value<- crg_share_mde$ci_numerator/(crg_share_mde$ci_denominator)
# crg_share_ctr$grp <- apply(crg_share_ctr[,combcolc],1, paste,collapse ="_")
# crg_share_ctr$value<- crg_share_ctr$ci_numerator/(crg_share_ctr$ci_denominator)
# #page_share_reg$region_code<-ifelse(page_share_reg$region_code=="AJ","AP",page_share_reg$region_code)
# 
# #str(page_share_reg)
# 
# crg_share_reg <- merge(crg_share_reg,intro_dt, by.x="platform_name", by.y="platform_subset", all.x = TRUE)
# crg_share_m10 <- merge(crg_share_m10,intro_dt, by.x="platform_name", by.y="platform_subset", all.x = TRUE)
# crg_share_mde <- merge(crg_share_mde,intro_dt, by.x="platform_name", by.y="platform_subset", all.x = TRUE)
# crg_share_ctr <- merge(crg_share_ctr,intro_dt, by.x="platform_name", by.y="platform_subset", all.x = TRUE)
# 
# crg_share_niq2r <- subset(crg_share_reg,is.na(printer_intro_month))
# crg_share_reg <- subset(crg_share_reg,!is.na(printer_intro_month))
# crg_share_niq2m <- subset(crg_share_m10,is.na(printer_intro_month))
# crg_share_m10 <- subset(crg_share_m10,!is.na(printer_intro_month))
# crg_share_niq2d <- subset(crg_share_mde,is.na(printer_intro_month))
# crg_share_mde <- subset(crg_share_mde,!is.na(printer_intro_month))
# crg_share_niq2c <- subset(crg_share_ctr,is.na(printer_intro_month))
# crg_share_ctr <- subset(crg_share_ctr,!is.na(printer_intro_month))
# 
# crg_share_reg <- subset(crg_share_reg, !is.na(value))
# crg_share_m10 <- subset(crg_share_m10, !is.na(value))
# crg_share_mde <- subset(crg_share_mde, !is.na(value))
# crg_share_ctr <- subset(crg_share_ctr, !is.na(value))
# 
# #crg_share_reg <- subset(crg_share_reg, crg_share_reg$supply_count_share>200)
# crg_share_reg$fiscal_year_quarter<-as.character(crg_share_reg$fiscal_year_quarter)
# crg_share_m10$fiscal_year_quarter<-as.character(crg_share_m10$fiscal_year_quarter)
# crg_share_mde$fiscal_year_quarter<-as.character(crg_share_mde$fiscal_year_quarter)
# crg_share_ctr$fiscal_year_quarter<-as.character(crg_share_ctr$fiscal_year_quarter)
# str(crg_share_ctr)

# COMMAND ----------

# data_coefrc <- list()
# 
# for (printer in unique(crg_share_reg$grp)){
#   
#   dat1 <- subset(crg_share_reg,grp==printer)
#   if (nrow(dat1) <4 ) {next}
#   
#   #mindt <- min(dat1$fiscal_year_quarter)
#   mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
#   #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
#   dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
#   mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
#   
#   #values for sigmoid curve start
#   midtime <- median(dat1$timediff)
#   maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
#   
#   dat1$value <- as.numeric(dat1$value)
#   maxv=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff),1.5)
#   minv=max(min(dat1$value)-min(dat1$value)*(0.02*maxtimediff),0.05)
#   spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2)
#   frstshr <- dat1[1,10]
#   
#   sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
#  
#   fitmodel <- nls2(formula=sigmoid, 
#                     data=dat1,
#                     start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
#                     algorithm = "grid-search", 
#                     #weights=supply_count_share,
#                     weights = timediff,
#                     control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
#   
#    timediff <- c(0:39)
#   preddata <- as.data.frame(timediff)
#   
#   #dat2<- dat1[,c("timediff")]
#   preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)
#   
#   fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
#   colnames(fit1) <- "fit1"
#   preddata <- cbind(preddata,fit1)
#   preddata <- sqldf(paste("select a.*, b.value as obs
#                             --, b.supply_count_share
#                           from preddata a left join dat1 b on a.timediff=b.timediff"))
#   #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
#   #                                ifelse(preddata$supply_count_share<10000,3,4)))
# 
#   # plot(y=preddata$value,x=preddata$timediff, type="p", col=preddata$rpgp
#   #      ,main=paste("Share for ",printer)
#   #       ,xlab="Date", ylab="HP Share", ylim=c(0,1))
#   # lines(y=preddata$value,x=preddata$timediff, col='black')
#   # lines(y=preddata$fit1,x=preddata$timediff,col="blue")
#   # legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
#   
#     ceoffo <- as.data.frame(coef(fitmodel))
#   
#     coeffout<-  as.data.frame(cbind(ceoffo[1,],ceoffo[2,],ceoffo[3,]
#                         ,ceoffo[4,]
#                         #,unique(as.character(dat1$Platform.Subset))
#                         #,unique(as.character(dat1$Region_5))
#                         #,unique(as.character(dat1$RTM))
#                         ,unique(as.character(dat1$grp))
#     ))
#     colnames(coeffout)[1] <- "min"
#     colnames(coeffout)[2] <- "max"
#     colnames(coeffout)[3] <- "med"
#     colnames(coeffout)[4] <- "spread"
#     colnames(coeffout)[5] <- "grp"
#   
#   data_coefrc[[printer]] <- coeffout
# 
# }
# 
# 
# #unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

# ##mkt 10
# 
# 
# data_coefmc <- list()
# 
# for (printer in unique(crg_share_m10$grp)){
#   
#   dat1 <- subset(page_share_m10,grp==printer)
#   if (nrow(dat1) <4 ) {next}
#   
#   #mindt <- min(dat1$fiscal_year_quarter)
#   mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
#   #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
#   dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
#   mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
#   
#   #values for sigmoid curve start
#   midtime <- median(dat1$timediff)
#   maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
#   
#   dat1$value <- as.numeric(dat1$value)
#   maxv=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff),1.5)
#   minv=max(min(dat1$value)-min(dat1$value)*(0.02*maxtimediff),0.05)
#   spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2)
#   frstshr <- dat1[1,10]
#   
#   sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
#  
#   fitmodel <- nls2(formula=sigmoid, 
#                     data=dat1,
#                     start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
#                     algorithm = "grid-search", 
#                     #weights=supply_count_share,
#                     weights = timediff,
#                     control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
#   
#    timediff <- c(0:39)
#   preddata <- as.data.frame(timediff)
#   
#   #dat2<- dat1[,c("timediff")]
#   preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)
#   
#   fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
#   colnames(fit1) <- "fit1"
#   preddata <- cbind(preddata,fit1)
#   preddata <- sqldf(paste("select a.*, b.value as obs
#                             --, b.supply_count_share
#                           from preddata a left join dat1 b on a.timediff=b.timediff"))
#   #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
#   #                                ifelse(preddata$supply_count_share<10000,3,4)))
# 
#   # plot(y=preddata$value,x=preddata$timediff, type="p", col=preddata$rpgp
#   #      ,main=paste("Share for ",printer)
#   #       ,xlab="Date", ylab="HP Share", ylim=c(0,1))
#   # lines(y=preddata$value,x=preddata$timediff, col='black')
#   # lines(y=preddata$fit1,x=preddata$timediff,col="blue")
#   # legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
#   
#     ceoffo <- as.data.frame(coef(fitmodel))
#   
#     coeffout<-  as.data.frame(cbind(ceoffo[1,],ceoffo[2,],ceoffo[3,]
#                         ,ceoffo[4,]
#                         #,unique(as.character(dat1$Platform.Subset))
#                         #,unique(as.character(dat1$Region_5))
#                         #,unique(as.character(dat1$RTM))
#                         ,unique(as.character(dat1$grp))
#     ))
#     colnames(coeffout)[1] <- "min"
#     colnames(coeffout)[2] <- "max"
#     colnames(coeffout)[3] <- "med"
#     colnames(coeffout)[4] <- "spread"
#     colnames(coeffout)[5] <- "grp"
#   
#   data_coefmc[[printer]] <- coeffout
# 
# }
# 
# 
# #unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

# ##developed_emerging
# 
# 
# data_coefdc <- list()
# 
# for (printer in unique(crg_share_mde$grp)){
#   
#   dat1 <- subset(page_share_mde,grp==printer)
#   if (nrow(dat1) <4 ) {next}
#   
#   #mindt <- min(dat1$fiscal_year_quarter)
#   mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
#   #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
#   dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
#   mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
#   
#   #values for sigmoid curve start
#   midtime <- median(dat1$timediff)
#   maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
#   
#   dat1$value <- as.numeric(dat1$value)
#   maxv=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff),1.5)
#   minv=max(min(dat1$value)-min(dat1$value)*(0.02*maxtimediff),0.05)
#   spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2)
#   frstshr <- dat1[1,10]
#   
#   sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
#  
#   fitmodel <- nls2(formula=sigmoid, 
#                     data=dat1,
#                     start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
#                     algorithm = "grid-search", 
#                     #weights=supply_count_share,
#                     weights = timediff,
#                     control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
#   
#    timediff <- c(0:39)
#   preddata <- as.data.frame(timediff)
#   
#   #dat2<- dat1[,c("timediff")]
#   preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)
#   
#   fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
#   colnames(fit1) <- "fit1"
#   preddata <- cbind(preddata,fit1)
#   preddata <- sqldf(paste("select a.*, b.value as obs
#                             --, b.supply_count_share
#                           from preddata a left join dat1 b on a.timediff=b.timediff"))
#   #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
#   #                                ifelse(preddata$supply_count_share<10000,3,4)))
# 
#   # plot(y=preddata$value,x=preddata$timediff, type="p", col=preddata$rpgp
#   #      ,main=paste("Share for ",printer)
#   #       ,xlab="Date", ylab="HP Share", ylim=c(0,1))
#   # lines(y=preddata$value,x=preddata$timediff, col='black')
#   # lines(y=preddata$fit1,x=preddata$timediff,col="blue")
#   # legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
#   
#     ceoffo <- as.data.frame(coef(fitmodel))
#   
#     coeffout<-  as.data.frame(cbind(ceoffo[1,],ceoffo[2,],ceoffo[3,]
#                         ,ceoffo[4,]
#                         #,unique(as.character(dat1$Platform.Subset))
#                         #,unique(as.character(dat1$Region_5))
#                         #,unique(as.character(dat1$RTM))
#                         ,unique(as.character(dat1$grp))
#     ))
#     colnames(coeffout)[1] <- "min"
#     colnames(coeffout)[2] <- "max"
#     colnames(coeffout)[3] <- "med"
#     colnames(coeffout)[4] <- "spread"
#     colnames(coeffout)[5] <- "grp"
#   
#   data_coefdc[[printer]] <- coeffout
# 
# }
# 
# 
# #unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

# ##country
# 
# 
# data_coefcc <- list()
# 
# for (printer in unique(crg_share_ctr$grp)){
#   
#   dat1 <- subset(page_share_ctr,grp==printer)
#   if (nrow(dat1) <4 ) {next}
#   
#   #mindt <- min(dat1$fiscal_year_quarter)
#   mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
#   #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
#   dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
#   mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
#   
#   #values for sigmoid curve start
#   midtime <- median(dat1$timediff)
#   maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
#   
#   dat1$value <- as.numeric(dat1$value)
#   maxv=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff),1.5)
#   minv=max(min(dat1$value)-min(dat1$value)*(0.02*maxtimediff),0.05)
#   spreadv <- min(1-(maxv + minv)/2,(maxv+minv)/2)
#   frstshr <- dat1[1,10]
#   
#   sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
#  
#   fitmodel <- nls2(formula=sigmoid, 
#                     data=dat1,
#                     start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
#                     algorithm = "grid-search", 
#                     #weights=supply_count_share,
#                     weights = timediff,
#                     control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)
#   
#    timediff <- c(0:39)
#   preddata <- as.data.frame(timediff)
#   
#   #dat2<- dat1[,c("timediff")]
#   preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)
#   
#   fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
#   colnames(fit1) <- "fit1"
#   preddata <- cbind(preddata,fit1)
#   preddata <- sqldf(paste("select a.*, b.value as obs
#                             --, b.supply_count_share
#                           from preddata a left join dat1 b on a.timediff=b.timediff"))
#   #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
#   #                                ifelse(preddata$supply_count_share<10000,3,4)))
# 
#   # plot(y=preddata$value,x=preddata$timediff, type="p", col=preddata$rpgp
#   #      ,main=paste("Share for ",printer)
#   #       ,xlab="Date", ylab="HP Share", ylim=c(0,1))
#   # lines(y=preddata$value,x=preddata$timediff, col='black')
#   # lines(y=preddata$fit1,x=preddata$timediff,col="blue")
#   # legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)
#   
#     ceoffo <- as.data.frame(coef(fitmodel))
#   
#     coeffout<-  as.data.frame(cbind(ceoffo[1,],ceoffo[2,],ceoffo[3,]
#                         ,ceoffo[4,]
#                         #,unique(as.character(dat1$Platform.Subset))
#                         #,unique(as.character(dat1$Region_5))
#                         #,unique(as.character(dat1$RTM))
#                         ,unique(as.character(dat1$grp))
#     ))
#     colnames(coeffout)[1] <- "min"
#     colnames(coeffout)[2] <- "max"
#     colnames(coeffout)[3] <- "med"
#     colnames(coeffout)[4] <- "spread"
#     colnames(coeffout)[5] <- "grp"
#   
#   data_coefcc[[printer]] <- coeffout
# 
# }
# 
# 
# #unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

# data_coeffrc <- as.data.frame(do.call("rbind", data_coefrc))
# data_coeffrc <- subset(data_coeffrc,as.numeric(as.character(max))>0)
# data_coeffmc <- as.data.frame(do.call("rbind", data_coefmc))
# data_coeffmc <- subset(data_coeffmc,as.numeric(as.character(max))>0)
# data_coeffdc <- as.data.frame(do.call("rbind", data_coefdc))
# data_coeffdc <- subset(data_coeffdc,as.numeric(as.character(max))>0)
# data_coeffcc <- as.data.frame(do.call("rbind", data_coefcc))
# data_coeffcc <- subset(data_coeffcc,as.numeric(as.character(max))>0)
# 
# #library(RH2)
# combined2 <- sqldf("select a.platform_subset_name
#                             , a.Predecessor
#                             , a.region_5 as regions
#                             , SUBSTR(a.developed_emerging,1,1) as emdm
#                             , a.cntry as country_alpha2
#                             , CASE  
#                                 WHEN c.min is NOT NULL THEN c.min
#                                 WHEN d.min is NOT NULL THEN d.min
#                                 WHEN m.min is NOT NULL THEN m.min
#                                 WHEN r.min is NOT NULL THEN r.min
#                                 ELSE NULL
#                                 END as min
#                             , CASE  
#                                 WHEN c.max is NOT NULL THEN c.max
#                                 WHEN d.max is NOT NULL THEN d.max
#                                 WHEN m.max is NOT NULL THEN m.max
#                                 WHEN r.max is NOT NULL THEN r.max
#                                 ELSE NULL
#                                 END as max
#                             , CASE  
#                                 WHEN c.med is NOT NULL THEN c.med
#                                 WHEN d.med is NOT NULL THEN d.med
#                                 WHEN m.med is NOT NULL THEN m.med
#                                 WHEN r.med is NOT NULL THEN r.med
#                                 ELSE NULL
#                                 END as med
#                             , CASE  
#                                 WHEN c.spread is NOT NULL THEN c.spread
#                                 WHEN d.spread is NOT NULL THEN d.spread
#                                 WHEN m.spread is NOT NULL THEN m.spread
#                                 WHEN r.spread is NOT NULL THEN r.spread
#                                 ELSE NULL
#                                 END as spread
#                             , CASE  
#                                 WHEN c.min is NOT NULL THEN 'country'
#                                 WHEN d.min is NOT NULL THEN 'Dev/Em'
#                                 WHEN m.min is NOT NULL THEN 'Market10'
#                                 WHEN r.min is NOT NULL THEN 'Region'
#                                 ELSE NULL
#                                 END as source
#                             , a.FMC
#                             , a.printer_intro_month
#                            
#                     FROM printer_list_ctr a 
#                     left join data_coeffrc r
#                       on (a.grpr=r.grp)
#                     left join data_coeffmc m
#                       on (a.grpm=m.grp)
#                     left join data_coeffdc d
#                       on (a.grpd=d.grp)
#                     left join data_coeffcc c
#                       on (a.grpc=c.grp)
#                    
#                    "
#                     )
# #detach(package:RH2)
# 
# colnames(combined2)[names(combined2)=="platform_subset_name"] <- "printer_platform_name"
# 
# output3 <- subset(combined2,!is.na(min))
# 
# combined2b <- sqldf("select
#                       a.*,
#                       CASE WHEN
#                         a.min IS NULL THEN 'N'
#                       ELSE 'Y'
#                       END AS curve_exists
#                       from combined2 a
#                   ")
# head(output3)

# COMMAND ----------

# Find Proxy

# predlistc <- combined2b[c(1,3,5,2,13)]  #printer_platform_name, regions, country, Predecessor, curve_exists#
# #predlist <- combined1b[c(1,3,2,10)]  #printer_platform_name, regions, Predecessor, curve_exists#
# proxylist1c <- subset(predlistc,curve_exists=="Y")
# proxylist1c$proxy <- proxylist1c$printer_platform_name
# proxylist1c$gen   <- 0
# 
# ####Managed to non-Managed####
# predlist_usec <- anti_join(predlistc,proxylist1c,by=c("printer_platform_name","country_alpha2"))
# predlist_usec$pred_iter <- predlist_usec$Predecessor
# 
# 
# predlist_iter3c <- sqldf("
#                   select distinct
#                   a.printer_platform_name
#                   , a.regions
#                   , a.country_alpha2
#                   , a.Predecessor
#                   , CASE 
#                       WHEN b.curve_exists='Y' THEN 'Y'
#                     ELSE 'N'
#                     END AS curve_exists
#                   , b.printer_platform_name as pred_iter
#                   from 
#                       (select a1.*, substr(a1.printer_platform_name,1,charindex(' MANAGED',a1.printer_platform_name)-1) as manname
#                         from predlist_usec a1 
#                         where charindex('MANAGED',a1.printer_platform_name)>0) a
#                   left join 
#                      (select b1.*, 
#                         CASE WHEN charindex(' ',b1.printer_platform_name)>0
#                           THEN substr(b1.printer_platform_name,1,charindex(' ',b1.printer_platform_name)-1)
#                         ELSE printer_platform_name
#                         END AS manname
#                       from predlistc b1
#                       where charindex('MANAGED',b1.printer_platform_name)=0) b
#                     on (a.manname=b.manname and a.country_alpha2=b.country_alpha2)
#                   ")
# 
# 
# predlist_iterc <- predlist_iter3c
# proxylist_iterc <- subset(predlist_iterc,curve_exists=="Y")
# 
# if(dim(proxylist_iterc)[1] != 0){
# 
# proxylist_iterc$proxy <- proxylist_iterc$pred_iter
# proxylist_iterc <- proxylist_iterc[c(1,2,3,5)]
# proxylist_iterc$gen   <- 0.3
# proxylist1c <-rbind(proxylist1c,proxylist_iter)
# }
# predlist_usec <- anti_join(predlist_iterc,proxylist_iterc,by=c("printer_platform_name","country_alpha2"))
# 
# 
# 
# ####Same printer group####
# predlist_usec <- anti_join(predlistc,proxylist1c,by=c("printer_platform_name","country_alpha2"))
# predlist_usec$pred_iter <- predlist_usec$Predecessor
# 
# 
# predlist_iter3c <- sqldf("
#                   select distinct
#                   a.printer_platform_name
#                   , a.regions
#                   , a.country_alpha2
#                   , a.Predecessor
#                   , CASE 
#                       WHEN b.curve_exists='Y' THEN 'Y'
#                     ELSE 'N'
#                     END AS curve_exists
#                   , b.printer_platform_name as pred_iter
#                   from 
#                       (select a1.*, a2.product_group as printer_group
#                         from predlist_usec a1 
#                         left join product_info2 a2
#                         on (a1.printer_platform_name=a2.platform_subset)) a
#                   left join 
#                      (select b1.*, b2.product_group as printer_group
#                       from predlistc b1
#                       left join product_info2 b2
#                       on (b1.printer_platform_name=b2.platform_subset)) b
#                     on (a.printer_platform_name != b.printer_platform_name and a.printer_group=b.printer_group and a.country_alpha2=b.country_alpha2)
#                     where a.printer_group is not null
#                   ")
# predlist_iterc <- predlist_iter3c
# proxylist_iterc <- subset(predlist_iterc,curve_exists=="Y")
# 
# if(dim(proxylist_iterc)[1] != 0){
# 
# proxylist_iterc$proxy <- proxylist_iterc$pred_iter
# proxylist_iterc <- proxylist_iterc[c(1,2,3,4,5,7)]
# proxylist_iterc$gen   <- 0.5
# proxylist1c <-rbind(proxylist1c,proxylist_iterc)
# }
# predlist_usec <- anti_join(predlist_iterc,proxylist_iterc,by=c("printer_platform_name","country_alpha2"))
# 
# predlist_usec$pred_iter <- predlist_usec$Predecessor
# ####Predecessors####
# y <- 0
# repeat{
# predlist_iter2c <- sqldf("
#                   select a.printer_platform_name
#                   , a.regions
#                   , a.country_alpha2
#                   , a.Predecessor
#                   , CASE 
#                       WHEN b.curve_exists='Y' THEN 'Y'
#                     ELSE 'N'
#                     END AS curve_exists
#                   , b.Predecessor as pred_iter
#                   from predlist_usec a
#                   left join predlistc b
#                     on (a.pred_iter=b.printer_platform_name and a.country_alpha2=b.country_alpha2)
#                   ")
# predlist_iterc <- predlist_iter2c
# proxylist_iterc <- subset(predlist_iterc,curve_exists=="Y")
# y<- y-1
# 
# #print(y)
# 
# if(dim(proxylist_iterc)[1] != 0){
# 
# proxylist_iterc$proxy <- proxylist_iterc$Predecessor
# proxylist_iterc <- proxylist_iterc[c(1,2,3,4,5,7)]
# proxylist_iterc$gen   <- y
# proxylist1c <-rbind(proxylist1c,proxylist_iter)
# }
# predlist_usec <- anti_join(predlist_iterc,proxylist_iterc,by=c("printer_platform_name","country_alpha2"))
# 
# if(all(is.na(predlist_usec$pred_iter))){break}
# if(y<-15){break}
# }
# 
# predlist_usec$pred_iter <- predlist_usec$printer_platform_name
# #####Successors#####
# y=0
# repeat{
# predlist_iter2c <- sqldf("
#                   select a.printer_platform_name
#                   , a.regions
#                   , a.country_alpha2
#                   , a.Predecessor
#                   , CASE 
#                       WHEN b.curve_exists='Y' THEN 'Y'
#                     ELSE 'N'
#                     END AS curve_exists
#                   , b.printer_platform_name as pred_iter
#                   from predlist_usec a
#                   left join predlistc b
#                     on (a.pred_iter=b.Predecessor and a.country_alpha2=b.country_alpha2)
#                   ")
# 
# predlist_iterc <- predlist_iter2c
# proxylist_iterc <- subset(predlist_iterc,curve_exists=="Y")
# 
# y<- y+1
# 
# #print(y)
# 
# if(dim(proxylist_iterc)[1] != 0){
# 
# proxylist_iterc$proxy <- proxylist_iterc$pred_iter
# proxylist_iterc <- proxylist_iterc[c(1,2,3,4,5,7)]
# proxylist_iterc$gen   <- y
# proxylist1c <-rbind(proxylist1c,proxylist_iterc)
# }
# 
# predlist_usec <- anti_join(predlist_iterc,proxylist_iterc,by=c("printer_platform_name","country_alpha2"))
# 
# if(all(is.na(predlist_usec$pred_iter))){break}
# if(y>15){break}
# }
# #successor list empty
# 
# proxylist_tempc <- proxylist1c
# 
# leftlistc <- anti_join(predlistc,proxylist_tempc,by=c("printer_platform_name","country_alpha2"))

# COMMAND ----------

# manual proxy assignments

# leftlistc$proxy <- ifelse(leftlistc$printer_platform_name=="ATHENA MID","TAISHAN",
#                   ifelse(leftlistc$printer_platform_name=="ANTELOPE","STARS 3:1 ROW",
#                   ifelse(leftlistc$printer_platform_name=="ATHENA BASE","TAISHAN",
#                   ifelse(leftlistc$printer_platform_name=="CASCABEL CNTRCTL", "MAMBA",
#                   ifelse(leftlistc$printer_platform_name=="CICADA PLUS ROW","ASTEROID",
#                   ifelse((leftlistc$printer_platform_name=="CORDOBA MANAGED" & leftlistc$regions=="AP"),"MERFCURY MFP",
#                   ifelse(leftlistc$printer_platform_name=="CORAL CNTRCTL","STARS 3:1",
#                   ifelse(leftlistc$printer_platform_name=="CRANE","CORAL C5",                         
#                   ifelse((leftlistc$printer_platform_name=="DENALI MANAGED" ),"DENALI MFP",
#                   ifelse(leftlistc$printer_platform_name=="HAWAII MFP","EVEREST MFP",
#                   ifelse(leftlistc$printer_platform_name=="HUMMINGBIRD PLUS" & (leftlistc$regions=="NA" |leftlistc$regions=="AP"),"JAYBIRD",
#                   ifelse(leftlistc$printer_platform_name=="HUMMINGBIRD PLUS","BUCK",
#                   ifelse(leftlistc$printer_platform_name=="KONA","EVEREST MFP",
#                   ifelse(leftlistc$printer_platform_name=="OUTLAW","STARS 3:1",
#                   ifelse(leftlistc$printer_platform_name=="RAINIER MFP","DENALI MFP",
#                   ifelse((leftlistc$printer_platform_name=="SAPPHIRE MANAGED" & leftlistc$regions=="EU"),"SAPPHIRE MFP",    
#                   ifelse((leftlistc$printer_platform_name=="SERRANO LITE" & leftlistc$regions=="NA"),"FIJIMFP",
#                   ifelse(leftlistc$printer_platform_name=="ANTARES PQ","DIAMOND 40 MANAGED",
#                   ifelse(leftlistc$printer_platform_name=="ELECTRA PQ","DIAMOND 40 MANAGED",
#                   ifelse(leftlistc$printer_platform_name=="MAPLE","DIAMOND 40 MANAGED",
#                   ifelse(leftlistc$printer_platform_name=="MAPLE MANAGED","DIAMOND 40 MANAGED",
#                   ifelse(leftlistc$printer_platform_name=="VEGA MANAGED","DIAMOND 40 MANAGED",
#                   ifelse(leftlistc$printer_platform_name=="VEGA PQ","DIAMOND 40 MANAGED",
#                          NA
#                          )))))))))))))))))))))))
# # proxylist_tempc$proxy <- ifelse(proxylist_tempc$printer_platform_name=="ATHENA MID","TAISHAN",
# #                   ifelse(proxylist_tempc$printer_platform_name=="ANTELOPE","STARS 3:1 ROW",
# #                   #ifelse(proxylist_tempc$printer_platform_name=="ARGON", "VEGA PQ",
# #                   ifelse(proxylist_tempc$printer_platform_name=="CASCABEL CNTRCTL", "MAMBA",
# #                   ifelse(proxylist_tempc$printer_platform_name=="CICADA PLUS ROW","ASTEROID",
# #                   ifelse((proxylist_tempc$printer_platform_name=="CORDOBA MANAGED" & proxylist_tempc$regions=="AP"),"MERFCURY MFP",
# #                   ifelse(proxylist_tempc$printer_platform_name=="CORAL CNTRCTL","STARS 3:1",
# #                   ifelse(proxylist_tempc$printer_platform_name=="CRANE","CORAL C5",                         
# #                   ifelse((proxylist_tempc$printer_platform_name=="DENALI MANAGED" ),"DENALI MFP",
# #                   ifelse(proxylist_tempc$printer_platform_name=="HAWAII MFP","EVEREST MFP",
# #                   ifelse(proxylist_tempc$printer_platform_name=="HUMMINGBIRD PLUS" & (proxylist_tempc$regions=="NA" |(proxylist_tempc$regions=="AP")),"JAYBIRD",
# #                   ifelse(proxylist_tempc$printer_platform_name=="HUMMINGBIRD PLUS","BUCK",
# #                   ifelse(proxylist_tempc$printer_platform_name=="KONA","EVEREST MFP",
# #                   ifelse(proxylist_tempc$printer_platform_name=="OUTLAW","STARS 3:1",
# #                   ifelse(proxylist_tempc$printer_platform_name=="RAINIER MFP","DENALI MFP",
# #                   ifelse((proxylist_tempc$printer_platform_name=="SAPPHIRE MANAGED" & proxylist_tempc$regions=="EU"),"SAPPHIRE MFP",    
# #                   ifelse((proxylist_tempc$printer_platform_name=="SERRANO LITE" & proxylist_tempc$regions=="NA"),"FIJIMFP",
# #                   ifelse(proxylist_tempc$printer_platform_name=="ANTARES PQ","DIAMOND 40 MANAGED",
# #                   ifelse(proxylist_tempc$printer_platform_name=="ELECTRA PQ","DIAMOND 40 MANAGED",
# #                   ifelse(proxylist_tempc$printer_platform_name=="MAPLE","DIAMOND 40 MANAGED",
# #                   ifelse(proxylist_tempc$printer_platform_name=="MAPLE MANAGED","DIAMOND 40 MANAGED",
# #                   ifelse(proxylist_tempc$printer_platform_name=="VEGA MANAGED","DIAMOND 40 MANAGED",
# #                   ifelse(proxylist_tempc$printer_platform_name=="VEGA PQ","DIAMOND 40 MANAGED",
# #                          proxylist_tempc$proxy
# #                          )))))))))))))))))))))) #)
# 
# proxylist_leftc <- subset(leftlistc, !is.na(proxy))
# proxylist_tempc <- bind_rows(proxylist_tempc,proxylist_leftc)
# 
# #manual change of proxy

# COMMAND ----------

# proxylistc <- proxylist_tempc[c(1,2,3,4,6)]
# 
# output4<- sqldf("select a.printer_platform_name
#                         , a.regions
#                         , a.country_alpha2
#                         , a.Predecessor
#                         , c.proxy
#                         , b.min
#                         , b.max
#                         , b.med
#                         , b.spread
#                         , a.FMC
#                         , a.printer_intro_month
#                         from combined2 a
#                         left join proxylistc c
#                         on (a.printer_platform_name=c.printer_platform_name and a.country_alpha2=c.country_alpha2 )
#                         left join combined2 b
#                         on (c.proxy=b.printer_platform_name and c.country_alpha2=b.country_alpha2)
#               ")
# 
# head(output4)

# COMMAND ----------

# missingc <- subset(output4,is.na(min))
# 
# plotlookc <- sqldf("select a.*, b.printer_intro_month as PIM
#                   from output4 a
#                   left join output4 b
#                   on (a.proxy=b.printer_platform_name and a.country_alpha2=b.country_alpha2)
#                   ")
# 
#   plotlookc$min <- as.numeric(plotlookc$min)
#   plotlookc$max <- as.numeric(plotlookc$max)
#   plotlookc$med <- as.numeric(plotlookc$med)
#   plotlookc$spread <- as.numeric(plotlookc$spread)
#   
# plotlook_srtc <- plotlookc[order(plotlookc$printer_platform_name,plotlookc$country_alpha2,plotlookc$PIM),]
# plotlook_dualc<- plotlookc[which(duplicated(plotlookc[,c('printer_platform_name','country_alpha2')])==T),]
# plotlook_outc <- plotlookc[which(duplicated(plotlookc[,c('printer_platform_name','country_alpha2')])==F),]
# 
# proxylist_outc <- plotlook_outc
# 
# plotlook_outc$grp <- paste0(plotlook_outc$printer_platform_name,"_",plotlook_outc$country_alpha2)
# 
# # for (printer in unique(plotlook_outc$grp)){
# #   dat1 <- subset(plotlook_out,grp==printer)
# #   #dat1 <- subset(plotlook_out,grp=="ALPINE PQ_AP")
# #     if (is.na(dat1$proxy)) {next}
# #   proxy <- dat1$proxy
# #   platform <- dat1$printer_platform_name
# #   region <- dat1$regions
# #   #emdm <- dat1$emdm
# #   timediff <- c(0:59)
# #   preddatac <- as.data.frame(timediff)
# #   dat1 %>% uncount(60)
# #   preddatac <- cbind(preddatac,dat1, row.names=NULL) 
# #   preddatac$value <- sigmoid_pred(preddatac$min,preddatac$max,preddatac$med,preddatac$spread,preddatac$timediff)
# #   
#   #plot(y=preddata$value,x=preddata$timediff, type="l",col="blue",main=paste("Share for ",platform,",",region,",",emdm,"\nUsing proxy: ",proxy),
#   #      xlab="Date", ylab="HP Share", ylim=c(0,1))
# 
# #}

# COMMAND ----------

# fill in missing
# 
# modellistc <- subset(proxylist_outc,!is.na(min))
# aggr_modelsc <- do.call(data.frame,aggregate(cbind(min, max, med, spread) ~ country_alpha2 + FMC, data=modellistc, FUN=function(x) c(mn = mean(x), md = median(x))))
# aggr_modelsc$grp <- paste0(aggr_modelsc$FMC,"_",aggr_modelsc$country_alpha2)
# # for (group in unique(aggr_modelsc$grp)){
# #   dat1 <- subset(aggr_modelsc,grp==group)
# #   timediff <- c(0:24)
# #   preddata <- as.data.frame(timediff)
# #   dat1 %>% uncount(25)
# #   preddatac <- cbind(preddatac,dat1, row.names=NULL) 
# #   preddatac$value_mn <- sigmoid_pred(preddatac$min.mn,preddatac$max.mn,preddatac$med.mn,preddatac$spread.mn,preddatac$timediff)
# #   preddatac$value_md <- sigmoid_pred(preddatac$min.md,preddatac$max.md,preddatac$med.md,preddatac$spread.md,preddatac$timediff)
# #   #plot(y=preddata$value_mn,x=preddata$timediff, type="l",col="blue",main=paste("Share for ",group),
# #   #      xlab="Date", ylab="HP Share", ylim=c(0,1))
# #   #lines(y=preddata$value_md,x=preddata$timediff, col='red')
# # 
# # }
# 
# aggr_models_fc <- merge(agg_lst,aggr_modelsc,by=c("FMC","country_alpha2"),all.x=TRUE)
# aggr_models_fc$country_alpha2 <- as.character(aggr_models_fc$country_alpha2)

# COMMAND ----------

# proxylist_bc <- sqldf("
#                          SELECT a.printer_platform_name, a.regions, a.country_alpha2, a.Predecessor 
#                            ,CASE 
#                             WHEN a.min IS NULL THEN b.FMC
#                             ELSE a.proxy 
#                           END as proxy 
#                           ,CASE 
#                             WHEN a.min IS NULL THEN b.[min.mn]
#                             ELSE a.min 
#                           END as min
#                           ,CASE 
#                             WHEN a.max IS NULL THEN b.[max.mn]
#                             ELSE a.max 
#                           END as max
#                           ,CASE 
#                             WHEN a.med IS NULL THEN b.[med.mn]
#                             ELSE a.med 
#                           END as med
#                           ,CASE 
#                             WHEN a.spread IS NULL THEN b.[spread.mn]
#                             ELSE a.spread 
#                           END as spread
#                           , a.FMC
#                         FROM proxylist_outc a
#                         LEFT JOIN  aggr_modelsc b
#                           ON (a.FMC=b.FMC AND a.country_alpha2=b.country_alpha2)
#                          ")
# proxylist_bc$proxy <- ifelse(is.na(proxylist_bc$proxy),"FMC",proxylist_bc$proxy)
# 
# remainingc <- subset(proxylist_bc,is.na(min))
# # remaining_LAc <- subset(remainingc,!is.na(FMC))
# # remaining_LAc$regions <- as.character(remaining_LAc$regions)
# # proxylist_lac <- sqldf("
# #                          SELECT a.printer_platform_name, a.regions, a.Predecessor 
# #                           ,CASE 
# #                             WHEN a.min IS NULL THEN 
# #                               CASE
# #                                 WHEN b.[min.mn] IS NULL THEN 
# #                                   CASE 
# #                                     WHEN c.[min.mn] IS NULL THEN 
# #                                       CASE WHEN d.[min.mn] IS NULL THEN e.FMC
# #                                       ELSE d.FMC
# #                                       END
# #                                     ELSE c.FMC
# #                                   END
# #                                 ELSE b.FMC
# #                               END
# #                             ELSE a.FMC 
# #                           END as proxy
# #                           ,CASE 
# #                             WHEN a.min IS NULL THEN 
# #                               CASE
# #                                 WHEN b.[min.mn] IS NULL THEN 
# #                                   CASE 
# #                                     WHEN c.[min.mn] IS NULL THEN 
# #                                       CASE WHEN d.[min.mn] IS NULL THEN e.[min.mn]
# #                                       ELSE d.[min.mn]
# #                                       END
# #                                     ELSE c.[min.mn]
# #                                   END
# #                                 ELSE b.[min.mn]
# #                               END
# #                             ELSE a.min 
# #                           END as min
# #                           ,CASE 
# #                             WHEN a.max IS NULL THEN 
# #                               CASE
# #                                 WHEN b.[max.mn] IS NULL THEN 
# #                               CASE 
# #                                 WHEN c.[max.mn] IS NULL THEN
# #                                   CASE WHEN d.[max.mn] IS NULL THEN e.[max.mn]
# #                                   ELsE d.[max.mn]
# #                                   END
# #                                 ELSE c.[max.mn]
# #                                 END
# #                                 ELSE b.[max.mn]
# #                               END
# #                             ELSE a.min
# #                           END as max
# #                           ,CASE 
# #                               WHEN a.med IS NULL THEN 
# #                               CASE
# #                                 WHEN b.[med.mn] IS NULL THEN 
# #                                   CASE 
# #                                     WHEN c.[med.mn] IS NULL THEN
# #                                       CASE WHEN d.[med.mn] IS NULL THEN e.[med.mn]
# #                                       ELSE d.[med.mn]
# #                                       END
# #                                     ELSE c.[med.mn]
# #                                   END
# #                                 ELSE b.[med.mn]
# #                               END
# #                             ELSE a.min
# #                           END as med
# #                           ,CASE 
# #                             WHEN a.spread IS NULL THEN 
# #                               CASE
# #                                 WHEN b.[spread.mn] IS NULL THEN 
# #                                   CASE 
# #                                     WHEN c.[spread.mn] IS NULL THEN
# #                                       CASE WHEN d.[spread.mn] IS NULL THEN e.[spread.mn]
# #                                       ELSE d.[spread.mn]
# #                                       END
# #                                     ELSE c.[spread.mn]
# #                                   END
# #                                 ELSE b.[spread.mn]
# #                               END
# #                             ELSE a.spread
# #                           END as spread
# #                           , a.FMC
# #                         FROM remaining_LAc a
# #                         LEFT JOIN  aggr_models_fc b
# #                           ON (SUBSTR(a.FMC,1,2)=SUBSTR(b.FMC,1,2) AND SUBSTR(b.FMC,3,5)='WGP' AND a.regions=b.regions)
# #                         LEFT JOIN  aggr_models_fc c
# #                           ON (SUBSTR(a.FMC,1,2)=SUBSTR(c.FMC,1,2) AND SUBSTR(c.FMC,3,5)='SWH' AND a.regions=c.regions)
# #                         LEFT JOIN  aggr_models_fc d
# #                           ON (SUBSTR(a.FMC,1,2)=SUBSTR(d.FMC,1,2) AND SUBSTR(d.FMC,3,5)='SWL' AND a.regions=d.regions)
# #                         LEFT JOIN  aggr_models_fc e
# #                           ON (SUBSTR(a.FMC,1,2)=SUBSTR(e.FMC,1,2) AND SUBSTR(e.FMC,3,5)='DSK' AND a.regions=e.regions)
# #                          ")
# # proxylist_la2c <- subset(proxylist_lac,is.na(min))
# # proxylist_la3c <- subset(proxylist_lac,!is.na(min))
# #remaining_FMC <- subset(remaining,is.na(FMC))
# #proxylist_cc <- subset(proxylist_bc,!is.na(min))
# #proxylist_f1c <- rbind(proxylist_cc,proxylist_la3c)
# proxylist_f1c <- proxylist_bc

#write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","Page Share for 100 percent IB DE ",".csv", sep=''), x=proxylist_final,row.names=FALSE, na="")

# COMMAND ----------

###HERE--need to run usage to continue
###Need to remove files to clear up memory
ibversion <- unique(product_ib2a$version)
rm(product_ib2a)
rm(ibtable)
rm(table_month)
# rm(list= ls()[!(ls() %in% c('ibversion','proxylist_f1','product_info2','proxylist_f1c','UPMDate','UPMColorDate','clanding','ch','cprod','server',
#                            'table_month0'))])  ###Clear up memory- gets rid of all but these tables

# COMMAND ----------

###Usage

proxylist_final2 <- sqldf("
                          SELECT distinct
                              a.printer_platform_name
                              --, c.Supplies_Product_Family
                              --, d.selectability
                              --, e.Crg_PL_Name
                              --, a.printer_platform_name
                              , a.regions
                              , a.country_alpha2
                              , a.Predecessor
                              , a.proxy as ps_proxy
                              , a.min as ps_min
                              , a.max as ps_max
                              , a.med as ps_med
                              , a.spread as ps_spread
                              , a.a as ps_a
                              , a.b as ps_b
                              , a.FMC
                              , a.source
                              --, f.proxy as cu_proxy
                              --, f.min as cu_min
                              --, f.max as cu_max
                              --, f.med as cu_med
                              --, f.spread as cu_spread
                              , b.printer_intro_month
                            FROM proxylist_f1 a
                            LEFT JOIN product_info2 b
                              ON a.printer_platform_name = b.platform_subset
                            --LEFT JOIN (SELECT Supplies_Product_Family, Platform_Subset FROM SupplyFam GROUP BY Platform_Subset) c
                              --ON (a.printer_platform_name = c. Platform_Subset)
                            --LEFT JOIN selectability d
                              --ON a.printer_platform_name = d.Platform_Subset_Nm
                            --LEFT JOIN pcplnm e
                              --ON a.printer_platform_name = e.Platform_Subset
                            --LEFT JOIN proxylist_f1c f
                              --ON (a.printer_platform_name = f.printer_platform_name AND a.country_alpha2=f.country_alpha2)
                          ")
#proxylist_final2$Supplies_Product_Family <- ifelse(is.na(proxylist_final2$Supplies_Product_Family),proxylist_final2$printer_platform_name,proxylist_final2$Supplies_Product_Family)
#Uncomment when ready to run
UPM <- SparkR::read.parquet(path=paste0("s3://", aws_bucket_name, "UPM_ctry(",UPMDate,").parquet"))
UPMC <- SparkR::read.parquet(path=paste0("s3://", aws_bucket_name, "UPMColor_ctry(",UPMDate,").parquet"))
# UPM <- s3read_using(FUN = read.csv, object = paste0("s3://insights-environment-sandbox/BrentT/UPM_ctry(",UPMDate,").csv"),  header=TRUE, sep=",", na="")
# UPMC <- s3read_using(FUN = read.csv, object = paste0("s3://insights-environment-sandbox/BrentT/UPMColor_ctry(",UPMColorDate,").csv"),  header=TRUE, sep=",", na="")

createOrReplaceTempView(UPM, "UPM")
createOrReplaceTempView(UPMC, "UPMC")

# tmpdir <- dir(tempdir())
# tmpdir <- tmpdir[grep(".csv", tmpdir)]
# unlink(paste0(normalizePath(tempdir()),"/",tmpdir),recursive=T) #clean S3 copied files from tmp folder

# UPM$MPV_N <- as.numeric(as.character(UPM$MPV_N))
# UPM$MPV_Raw <- as.numeric(as.character(UPM$MPV_Raw))

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
              , sum(a.MPV_TD*a.IB) as MPV_TDIB
              , sum(a.MPV_Raw*a.IB) as MPV_RawIB
              , sum(a.MPV_N) as MPV_n
              , sum(a.IB) as IB
              , sum(a.MPV_Init) as Init_MPV
              --, avg(b.MUT) as MUT
              , avg(b.Trend) as Trend
              , avg(b.Seasonality) as Seasonality
              --, avg(b.Cyclical) as Cyclical
              , avg(b.Decay) as Decay
              FROM UPM a 
              LEFT JOIN 
                (SELECT Mkt, CM, FYearQtr --, avg(MUT) as MUT
                , avg(Trend) as Trend, avg(Seasonality) as Seasonality
                --, avg(Cyclical) as Cyclical
                , avg(Decay) as Decay
                FROM
                  (SELECT Mkt, CM, FYearMo, FYearQtr
                  --, max(MUT) as MUT
                  , max(Trend) as Trend,
                  max(Seasonality) as Seasonality
                     -- , max(Cyclical) as Cyclical
                    , max(Decay) as Decay
                  FROM UPM 
                    GROUP BY Mkt, CM, FYearMo, FYearQtr)
                    GROUP BY Mkt, CM, FYearQtr) b
                ON (a.Mkt=b.Mkt AND a.CM=b.CM AND a.FYearQtr=b.FYearQtr)
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
              , FQtr
              ')

UPM2C <- SparkR::sql('
              SELECT distinct
                a.Platform_Subset_Nm as printer_platform_name
              , a.Region_3
              , a.Region
              , a.Region_DE
              , a.Country_Cd
              --, a.market10
              , a.FYearMo
              , a.FYearQtr
              , a.rFYearQtr
              , a.FYear
              , substr(a.FYearQtr,6,6) as FQtr
              , CEIL(avg(a.MoSI)/3) as QSI
              , avg(a.color_pct) as color_pct
              , sum(a.MPV_N) as MPV_n
              , sum(a.IB) as IB
              , sum(a.MPV_Init) as Init_MPV
              --, avg(b.MUT) as MUT
              , avg(b.Trend) as Trend
              , avg(b.Seasonality) as Seasonality
              --, avg(b.Cyclical) as Cyclical
              , avg(b.Decay) as Decay
              FROM UPMC a 
              LEFT JOIN 
                (SELECT Mkt, CM, FYearQtr --, avg(MUT) as MUT
                , avg(Trend) as Trend, avg(Seasonality) as Seasonality
                    --, avg(Cyclical) as Cyclical
                    , avg(Decay) as Decay
                FROM
                  (SELECT Mkt, CM, FYearMo, FYearQtr--, max(MUT) as MUT
                  , max(Trend) as Trend, max(Seasonality) as Seasonality
                      --, max(Cyclical) as Cyclical
                      , max(Decay) as Decay
                  FROM UPM 
                    GROUP BY Mkt, CM, FYearMo, FYearQtr)
                    GROUP BY Mkt, CM, FYearQtr) b
                ON (a.Mkt=b.Mkt AND a.CM=b.CM AND a.FYearQtr=b.FYearQtr)
              GROUP BY 
                a.Platform_Subset_Nm
              , a.Region_3
              , a.Region
              , a.Region_DE
              , a.Country_Cd
              --, a.market10
              , a.FYearMo
              , a.FYearQtr
              , a.rFYearQtr
              , a.FYear
              , FQtr
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
              --, EP
              , CM
              --, SM
              , Mkt
              --, FMC
              --, K_PPM
              /*, CASE
                WHEN CM="M" THEN NULL
                WHEN Clr_PPM="NA" THEN K_PPM
                ELSE Clr_PPM
                END AS Clr_PPM*/
              --, Intro_Price

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
              , CM
              --, SM
              , Mkt
              --, FMC
              --, K_PPM
              --, Clr_PPM
              --, Intro_Price
              , Decay
              , Seasonality
              --, Cyclical
              , IMPV_Route
              ')
#UPM2 <- aggregate(cbind(UPM$MPV_TS*UPM$IB,UPM$MPV_Raw*UPM$IB,UPM$IB,UPM$MPV_N),
#                  by=list(UPM$Platform_Subset_Nm,UPM$Region,UPM$Country_Cd,UPM$FYearQtr),FUN=sum)
#UPM2 <- aggregate(cbind(UPM$IPM_MPV*UPM$IB,UPM$mpva*UPM$IB,UPM$IB,UPM$na),
#                  by=list(UPM$printer_platform_name,UPM$printer_region_code,UPM$FYearQtr),FUN=sum)

#colnames(UPM2)<- c("printer_platform_name","printer_region_code","country","FYQ","UPM_MPVIB","MMPVIB","IB","MPV_sample")
UPM2$MPV_UPM <- (UPM2$MPV_UPMIB/UPM2$IB)
UPM2$MPV_Raw <- (UPM2$MPV_RawIB/UPM2$IB)
UPM2$MPV_TS  <- (UPM2$MPV_TSIB/UPM2$IB)
UPM2$MPV_TD  <- (UPM2$MPV_TDIB/UPM2$IB)

#quarters <- as.data.frame(c("2016Q1","2016Q2","2016Q3","2016Q4","2017Q1","2017Q2","2017Q3","2017Q4","2018Q1","2018Q2","2018Q3","2018Q4","2019Q1","2019Q2","2019Q3","2019Q4","2020Q1","2020Q2","2020Q3","2020Q4","2021Q1","2021Q2","2021Q3","2021Q4","2022Q1","2022Q2","2022Q3","2022Q4","2023Q1","2023Q2","2023Q3","2023Q4","2024Q1","2024Q2","2024Q3","2024Q4"))
###19891101-20500101
#months <- as.data.frame(c(201601:201612,201701:201712,201801:201812,201901:201912,202001:202012,202101:202112,202201:202212,202301:202312,202401:202412))
months <- as.data.frame(seq(as.Date("1989101",format="%Y%m%d"),as.Date("20500101",format="%Y%m%d"),"month"))
colnames(months) <- "CalYrDate"
quarters2 <- as.data.frame(months[rep(seq_len(nrow(months)),nrow(proxylist_final2)),])
colnames(quarters2) <- "CalDate"

rm(UPM) #remove for space
rm(UPMC)#remove for space

#dashampv <- subset(table_month0, usage_region_incl_flag==1)
dashampv <- subset(table_month0, usage_country_incl_flag==1)
dashampv2 <- sqldf("
                       SELECT distinct platform_name
                        , fiscal_year_quarter
                        , calendar_year_month
                        --, product_type
                        --, region_code
                        , iso_country_code
                        --, usage_region_incl_flag
                        , usage_country_incl_flag
                        , sum(usage_numerator) as ampv_n
                        , sum(color_numerator) as ampv_c
                        , sum(usage_denominator) as ampv_d
                        , sum(printer_count_month_use) as usage_n
                      from dashampv a
                      group by platform_name,fiscal_year_quarter, calendar_year_month
                       --,product_type
                       --, usage_region_incl_flag
                       , usage_country_incl_flag
                       ,iso_country_code
                      order by platform_name,fiscal_year_quarter, calendar_year_month
                       --,product_type
                       ,iso_country_code
                       ")
dashampv2$ampv<- dashampv2$ampv_n/dashampv2$ampv_d
dashampv2$campv<- dashampv2$ampv_c/dashampv2$ampv_d

# rm(table_month0)
# #rm(table_month)
# rm(data_coefm)
# rm(data_coefmc)
# rm(data_coefr)
# rm(data_coefrc)
# rm(data_coefc)
# rm(data_coefcc)
# rm(data_coefd)
# rm(data_coefdc)
# gc(reset = TRUE)


# final_list <- proxylist_final2[rep(seq_len(nrow(proxylist_final2)),each=nrow(months)),] 
# final_list <- cbind(final_list,quarters2)
# 
# final_list$printer_intro_month <- as.Date(final_list$printer_intro_month,format="%Y-%m-%d")
# final_list$timediff <- as.numeric(((as.Date(final_list$CalDate, format="%Y-%m-%d")-as.Date(final_list$printer_intro_month,format="%Y-%m-%d"))/365)*4)
# final_list$FYearMo <- format(final_list$CalDate,"%Y%m")
# final_list$FYQtr <- lubridate::quarter(as.Date(final_list$'CalDate', format="%Y-%m-%d"),with_year=TRUE, fiscal_start=11)
# final_list$FYQtr <- gsub("[.]","Q",final_list$FYQtr)
# 
# final_list$model_share_ps <- ifelse(final_list$timediff<0,NA,sigmoid_pred(final_list$ps_min,final_list$ps_max,final_list$ps_med,final_list$ps_spread,final_list$timediff))
# final_list$model_share_cu <- ifelse(final_list$timediff<0,NA,sigmoid_pred(final_list$cu_min,final_list$cu_max,final_list$cu_med,final_list$cu_spread,final_list$timediff))

# COMMAND ----------

###Combine Results

hwlookup <- as.DataFrame(dbGetQuery(cprod,"
                       SELECT distinct platform_subset, technology, pl, sf_mf AS SM
                       FROM IE2_Prod.dbo.hardware_xref"))

createOrReplaceTempView(UPM2, "UPM2")
createOrReplaceTempView(as.DataFrame(proxylist_final2), "proxylist_final2")
createOrReplaceTempView(UPM2C, "UPM2C")
createOrReplaceTempView(as.DataFrame(product_ib2), "product_ib2")
createOrReplaceTempView(as.DataFrame(page_share_ctr), "page_share_ctr")
createOrReplaceTempView(as.DataFrame(dashampv2), "dashampv2")
# createOrReplaceTempView(as.DataFrame(crg_share_ctr), "crg_share_ctr")
createOrReplaceTempView(UPM3, "UPM3")
createOrReplaceTempView(hwlookup, "hwlookup")
createOrReplaceTempView(as.DataFrame(intro_dt), "intro_dt")

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
                      , c.tot_ib AS IB
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
                      , idt.intro_month as FIntroDt
                      --, g.EP
                      , g.CM
                      , hw.SM
                      , g.Mkt
                      --, g.K_PPM
                      --, g.Clr_PPM
                      --, g.Intro_Price
                      , b.Init_MPV
                      , CASE WHEN g.CM='C' THEN b.MPV_UPM*bb.color_pct ELSE 0 END AS MPV_UPMc
                      , CASE WHEN g.CM='C' THEN b.MPV_TS*bb.color_pct ELSE 0 END AS MPV_TSc
                      , CASE WHEN g.CM='C' THEN b.MPV_TD*bb.color_pct ELSE 0 END AS MPV_TDc
                      , CASE WHEN g.CM='C' THEN b.MPV_Raw*bb.color_pct ELSE 0 END AS MPV_Rawc
                      , CASE WHEN g.CM='C' THEN bb.MPV_N ELSE 0 END AS MPV_Nc
                      , g.IMPV_Route
                      , a.Predecessor
                      --, e.usage_region_incl_flag AS BD_Usage_Flag
                      , e.usage_country_incl_flag AS BD_Usage_Flag
                      , e.ampv AS MPV_Dash
                      , e.campv AS MPV_DashC
                      , a.ps_proxy AS Proxy_PS
                      , d.page_share_region_incl_flag AS BD_Share_Flag_PS
                      , a.source
                      , a.ps_min AS Model_Min_PS
                      , a.ps_max AS Model_Max_PS
                      , a.ps_med AS Model_Median_PS
                      , a.ps_spread AS Model_Spread_PS
                      , a.ps_a AS Model_a_PS
                      , a.ps_b AS Model_b_PS
                      , d.value AS Share_Raw_PS
                      , d.printer_count_month_ps AS Share_Raw_N_PS
                      , e.usage_n
                      --, a.cu_proxy AS Proxy_CU
                      --, f.share_region_incl_flag AS BD_Share_Flag_CU
                      --, a.cu_min AS Model_Min_CU
                      --, a.cu_max AS Model_Max_CU
                      --, a.cu_med AS Model_Median_CU
                      --, a.cu_spread AS Model_Spread_CU
                      --, f.value AS Share_Raw_CU
                      --, f.printer_count_month_ci AS Share_Raw_N_CU
                      , bb.color_pct

                      FROM UPM2 b
                      LEFT JOIN proxylist_final2 a
                        ON (a.printer_platform_name=b.printer_platform_name and a.country_alpha2=b.Country_cd)
                      LEFT JOIN UPM2C bb
                        ON (b.printer_platform_name=bb.printer_platform_name and b.Country_cd=bb.Country_cd
                            and b.FYearMo=bb.FYearMo)
                      LEFT JOIN product_ib2 c
                        ON (b.printer_platform_name=c.platform_subset_name and b.Country_cd=c.country_alpha2
                              and b.FYearMo=c.month_begin)
                      LEFT JOIN page_share_ctr d
                        ON (b.printer_platform_name=d.platform_name AND b.Country_cd=d.country_alpha2 
                            AND b.FYearQtr=d.fiscal_year_quarter)
                      LEFT JOIN dashampv2 e
                        ON (b.printer_platform_name=e.platform_name AND b.Country_cd=e.iso_country_code  
                          AND b.FYearMo=e.calendar_year_month)
                      --LEFT JOIN crg_share_ctr f
                        --ON (b.printer_platform_name=f.platform_name AND b.Country_cd=f.country_alpha2  
                            --AND b.FYearQtr=f.fiscal_year_quarter)
                      LEFT JOIN UPM3 g
                        ON (b.printer_platform_name=g.printer_platform_name and b.Country_cd=g.Country_cd)
                      LEFT JOIN hwlookup hw
                        ON (b.printer_platform_name=hw.platform_subset)
                      LEFT JOIN intro_dt idt
                        ON (b.printer_platform_name=idt.platform_subset)
                     ")

sigmoid_pred <- function(input_min, input_max, input_med, input_spread, input_timediff) {
  input_max-(exp((input_timediff-input_med)*input_spread)/(exp((input_timediff-input_med)*input_spread)+lit(1))*(lit(1)-(input_min+(lit(1)-input_max))))
}
#sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))

final_list2$timediff <- round(datediff(concat_ws(sep="-", substr(final_list2$FYearMo, 1, 4), substr(final_list2$FYearMo, 5, 6), lit("01")), concat_ws(sep="-", substr(final_list2$FIntroDt, 1, 4), substr(final_list2$FIntroDt, 5, 6), lit("01")))/(365.25/12))
#dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)


final_list2$Share_Model_PS <- sigmoid_pred(final_list2$Model_Min_PS, final_list2$Model_Max_PS, final_list2$Model_Median_PS, final_list2$Model_Spread_PS, final_list2$timediff)
final_list2$Share_Model_PS <- ifelse(final_list2$Share_Model_PS > 1,1,ifelse(final_list2$Share_Model_PS<0.05,0.05,final_list2$Share_Model_PS))
final_list2$Share_Model_PSlin <- (final_list2$Model_a_PS+final_list2$Model_b_PS*final_list2$timediff)
#final_list2$Share_Model_CU <- sigmoid_pred(final_list2$Model_Min_CU,final_list2$Model_Max_CU,final_list2$Model_Median_CU,final_list2$Model_Spread_CU,final_list2$timediff)
#final_list2$Share_Model_CU <- ifelse(final_list2$Share_Model_CU > 1,1,ifelse(final_list2$Share_Model_PS<0.05,0.05,final_list2$Share_Model_CU))

createOrReplaceTempView(final_list2, "final_list2")

final_list2 <- SparkR::sql("
                      select a.*
                      , case
                        when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.BD_Share_Flag_PS is NULL then 
                            case when a.source='country' then 'Modelled at Country Level'
                                 when a.source='Dev/Em' then 'Modelled at Market13 Level'
                                 when a.source='Market10' then 'Modelled at Market10 Level'
                                 else 'Modelled at Region5 Level'
                                 end
                          when a.BD_Share_Flag_PS = 0 then 
                            case when a.source='country' then 'Modelled at Country Level'
                                 when a.source='Dev/Em' then 'Modelled at Market13 Level'
                                 when a.source='Market10' then 'Modelled at Market10 Level'
                                 else 'Modelled at Region5 Level'
                                 end
                          when a.Share_Raw_PS is NULL then 
                              case when a.source='country' then 'Modelled at Country Level'
                                 when a.source='Dev/Em' then 'Modelled at Market13 Level'
                                 when a.source='Market10' then 'Modelled at Market10 Level'
                                 else 'Modelled at Region5 Level'
                                 end
                          when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then 
                              case when a.source='country' then 'Modelled at Country Level'
                                 when a.source='Dev/Em' then 'Modelled at Market13 Level'
                                 when a.source='Market10' then 'Modelled at Market10 Level'
                                 else 'Modelled at Region5 Level'
                                 end
                          else 'Have Data'
                          end
                        else 'Modelled by Proxy'
                        end as Share_Source_PS
                      , case
                        when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.BD_Share_Flag_PS = 0 then a.Share_Model_PS
                          when a.BD_Share_Flag_PS is NULL then a.Share_Model_PS
                          when a.Share_Raw_PS IN (NULL, 0) then a.Share_Model_PS
                          when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then a.Share_Model_PS
                          else a.Share_Raw_PS
                          end
                          else a.Share_Model_PS
                        end as Page_Share_sig
                        , case
                        when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.BD_Share_Flag_PS = 0 then a.Share_Model_PSlin
                          when a.Share_Raw_PS IN (NULL, 0) then a.Share_Model_PSlin
                          when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then a.Share_Model_PSlin
                          else a.Share_Raw_PS
                          end
                          else a.Share_Model_PSlin
                        end as Page_Share_lin
                      --, case
                      --  when a.Platform_Subset_Nm=a.Proxy_CU then
                      --    case when a.BD_Share_Flag_CU = 0 then 'Modeled'
                      --    when a.BD_Share_Flag_CU is NULL then 'Modeled'
                      --   when a.Share_Raw_CU IN (NULL, 0) then 'Modeled'
                      --    when a.Share_Raw_N_CU is NULL or a.Share_Raw_N_CU < 20 then 'Modeled'
                      --    else 'Have Data'
                      --    end
                      --  else 'Modeled by Proxy'
                      --  end as Share_Source_CU
                      --, case
                      --  when a.Platform_Subset_Nm=a.Proxy_CU then
                      --    case when a.BD_Share_Flag_CU = 0 then a.Share_Model_CU
                      --    when a.BD_Share_Flag_CU is NULL then a.Share_Model_CU
                      --    when a.Share_Raw_CU IN (NULL, 0) then a.Share_Model_CU
                      --    when a.Share_Raw_N_CU is NULL or a.Share_Raw_N_CU < 20 then a.Share_Model_CU
                      --    else a.Share_Raw_CU
                      --    end
                      --    else a.Share_Model_CU
                      --  end as Crg_Unit_Share
                      , case
                        when a.BD_Usage_Flag is NULL then 'UPM'
                        when a.BD_Share_Flag_PS = 0 then 'UPM'
                        when a.MPV_DASH is NULL then 'UPM'
                        when a.usage_n < 75 then 'UPM Sample Size'
                          else 'Dashboard'
                          end
                           as Usage_Source
                      , case
                        when a.technology='LASER' then
                        case
                          when a.BD_Usage_Flag is NULL then a.MPV_TD
                          when a.BD_Share_Flag_PS = 0 then a.MPV_TD
                          when a.MPV_DASH is NULL then a.MPV_TD
                          when a.usage_n < 75 then a.MPV_TD
                            else a.MPV_DASH
                            end
                        when a.technology='PWA' then
                          case
                          when a.BD_Usage_Flag is NULL then a.MPV_TS
                          when a.BD_Share_Flag_PS = 0 then a.MPV_TS
                          when a.MPV_Dash is NULL then a.MPV_TS
                          when a.usage_n < 75 then a.MPV_TS
                            else a.MPV_Dash
                            end
                          end as Usage
                      , case
                          when a.technology='LASER' then
                          case
                          when a.BD_Usage_Flag is NULL then a.MPV_TD*a.color_pct
                          when a.BD_Share_Flag_PS = 0 then a.MPV_TD*a.color_pct
                          when (a.MPV_DashC is NULL or a.MPV_DashC < 0 or a.MPV_DashC >1) then a.MPV_TD*a.color_pct
                          when a.usage_n < 75 then a.MPV_TD*a.color_pct
                            else a.MPV_DashC
                            end
                            when a.technology='PWA' then
                          case
                          when a.BD_Usage_Flag is NULL then a.MPV_TS*a.color_pct
                          when a.BD_Share_Flag_PS = 0 then a.MPV_TS*a.color_pct
                          when (a.MPV_DashC is NULL or a.MPV_DashC < 0 or a.MPV_DashC >1) then a.MPV_TS*a.color_pct
                          when a.usage_n < 75 then a.MPV_TS*a.color_pct
                            else a.MPV_DashC
                            end
                          end as Usage_c
                     from final_list2 a
                     ")

final_list2$Pages_Device_UPM <- final_list2$MPV_TS*final_list2$IB
final_list2$Pages_Device_Raw <- final_list2$MPV_Raw*final_list2$IB
final_list2$Pages_Device_Dash <- final_list2$MPV_Dash*final_list2$IB
final_list2$Pages_Device_Use <- final_list2$Usage*final_list2$IB
final_list2$Pages_Share_Model_PS <- final_list2$MPV_TS*final_list2$IB*final_list2$Share_Model_PS
#final_list2$Pages_Share_Model_CU <- final_list2$MPV_TS*final_list2$IB*final_list2$Share_Model_CU
final_list2$Pages_Share_Raw_PS <- final_list2$MPV_Raw*final_list2$IB*cast(final_list2$Share_Raw_PS, "double")
#final_list2$Pages_Share_Raw_CU <- final_list2$MPV_Raw*final_list2$IB*as.numeric(final_list2$Share_Raw_CU)
final_list2$Pages_Share_Dash_PS <- final_list2$MPV_Dash*final_list2$IB*cast(final_list2$Share_Raw_PS, "double")
#final_list2$Pages_Share_Dash_CU <- final_list2$MPV_Dash*final_list2$IB*as.numeric(final_list2$Share_Raw_CU)
final_list2$Pages_PS <- final_list2$Pages_Device_Use*cast(final_list2$Page_Share_sig, "double")
#final_list2$Pages_CU <- final_list2$Pages_Device_Use*as.numeric(final_list2$Crg_Unit_Share)


#final_list3 <- subset(final_list2,IB>0)
#final_list4 <- subset(final_list3,!is.na(MPV_UPM))


#final_list6 <- final_list2
final_list2$BD_Usage_Flag <- ifelse(isNull(final_list2$BD_Usage_Flag),0,final_list2$BD_Usage_Flag)
final_list2$BD_Share_Flag_PS <- ifelse(isNull(final_list2$BD_Share_Flag_PS),0,final_list2$BD_Share_Flag_PS)
#final_list2$BD_Share_Flag_CU <- ifelse(is.na(final_list2$BD_Share_Flag_CU),0,final_list2$BD_Share_Flag_CU)
#change FIntroDt from YYYYMM to YYYYQQ
# final_list2$FIntroDt <- gsub(" ","",as.character(as.yearqtr(as.Date(final_list2$FIntroDt,format="%Y-%m-%d")+3/4)))

#colnames(final_list5)[6] <- "Emerging/Developed"

#filenm <- paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer (",Sys.Date(),").xlsx")
#write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer (",Sys.Date(),").csv", sep=''), x=final_list6,row.names=FALSE, na="")
#test1 <- subset(final_list2, Platform_Subset_Nm=="SID" & Region=="NA" & Fiscal_Quarter > '2016Q1' & Fiscal_Quarter < '2025Q1')

# COMMAND ----------

# Adjust curve at join

# tempdir(check=TRUE)
# rm(list= ls()[!(ls() %in% c('ibversion','final_list2','writeout','clanding','dm_version'))])  ###Clear up memory- gets rid of all but these tables
# gc(reset = TRUE)

createOrReplaceTempView(final_list2, "final_list2")

# final_list7 <- SparkR::sql(
#   "SELECT *, lagShare_Source_PS AS LAG(Share_Source_PS) FROM final_list2 GROUP BY Platform_Subset_Nm, Country_Cd"
# )

ws <- orderBy(windowPartitionBy("Platform_Subset_Nm", "Country_Cd"), "FYearMo")
final_list7 <- mutate(final_list2
                     ,lagShare_Source_PS = over(lag("Share_Source_PS"), ws)
                     #,lagShare_Source_CU = lag(Share_Source_CU)
                     ,lagUsage_Source = over(lag("Usage_Source"), ws)
                     ,lagShare_PS = over(lag("Page_Share_sig"), ws)
                     #,lagShare_CU = lag(Crg_Unit_Share)
                     ,lagShare_Usage = over(lag("Usage"), ws)
                     ,lagShare_Usagec = over(lag("Usage_c"), ws)
                     ,index1 = over(dense_rank(), ws)
                     )

final_list7$hd_mchange_ps <- ifelse(substr(final_list7$Share_Source_PS,1,8)=="Modelled",ifelse(final_list7$lagShare_Source_PS=="Have Data",final_list7$Page_Share_sig-final_list7$lagShare_PS, NA ), NA)
final_list7$hd_mchange_ps_i <- ifelse(!isNull(final_list7$hd_mchange_ps),final_list7$index1,NA)
final_list7$hd_mchange_psb <- ifelse(final_list7$Share_Source_PS=="Have Data",ifelse(substr(final_list7$Share_Source_PS,1,8)=="Modelled",final_list7$Page_Share_sig, NA ),NA)
final_list7$hd_mchange_ps_j <- ifelse(!isNull(final_list7$hd_mchange_psb),final_list7$index1,NA)
#final_list7$hd_mchange_cu <- ifelse(final_list7$Share_Source_CU=="Modeled",ifelse(final_list7$lagShare_Source_CU=="Have Data",final_list7$Crg_Unit_Share-final_list7$lagShare_CU, NA ),NA)
#final_list7$hd_mchange_cu_i <- ifelse(!is.na(final_list7$hd_mchange_cu),final_list7$index1,NA)
final_list7$hd_mchange_use <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage-final_list7$lagShare_Usage, NA ),NA)
#final_list7$hd_mchange_usec <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage_c-final_list7$lagShare_Usagec, NA ),NA)
final_list7$hd_mchange_used <- ifelse(final_list7$Usage_Source=="Dashboard",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage-final_list7$lagShare_Usage, NA ),NA)
final_list7$hd_mchange_use_i <- ifelse(!isNull(final_list7$hd_mchange_use),final_list7$index1,NA)


createOrReplaceTempView(final_list7, "final_list7")

final_list7 <- SparkR::sql("
                with sub0 as (
                    SELECT Platform_Subset_Nm,Country_Cd
                        ,max(hd_mchange_ps_i) as hd_mchange_ps_i
                        ,min(hd_mchange_ps_j) as hd_mchange_ps_j
                        --,max(hd_mchange_cu_i) as hd_mchange_cu_i
                        ,max(hd_mchange_use_i) as hd_mchange_use_i
                        FROM final_list7
                        GROUP BY Platform_Subset_Nm,Country_Cd
                )
                , subusev as (
                    SELECT Platform_Subset_Nm,Country_Cd, FYearMo, Usage, IB
                    FROM final_list7
                    WHERE Usage_Source='Dashboard'
                  )
                , subusev2 as (
                    SELECT Platform_Subset_Nm,Country_Cd, FYearMo, Usage*IB as UIB, IB, ROW_NUMBER() 
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
                  SELECT Platform_Subset_Nm,Country_Cd, sumUIB/IB as avgUsage
                  FROM subusev3
                )
                , sub1ps as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,sub0.hd_mchange_ps_i
                        ,final_list7.Page_Share_sig-final_list7.lagShare_PS AS hd_mchange_ps
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.hd_mchange_ps_i=sub0.hd_mchange_ps_i
                  )
                  , sub1psb as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,sub0.hd_mchange_ps_j
                        ,final_list7.Page_Share_sig AS hd_mchange_psb
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.hd_mchange_ps_j=sub0.hd_mchange_ps_j
                  )
                 --, sub1cu as( 
                    --SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,sub0.hd_mchange_cu_i
                        --,final_list7.Page_Share-final_list7.lagShare_CU AS hd_mchange_cu
                        --FROM final_list7
                        --INNER JOIN 
                        --sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          --and final_list7.hd_mchange_cu_i=sub0.hd_mchange_cu_i
                  --)
                  , sub1use as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,sub0.hd_mchange_use_i,final_list7.Usage
                        ,final_list7.Usage-final_list7.lagShare_Usage AS hd_mchange_use
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.index1 = sub0.hd_mchange_use_i 
                  )
                  , sub1usec as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,sub0.hd_mchange_use_i
                        ,final_list7.Usage_c-final_list7.lagShare_Usagec AS hd_mchange_usec
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.index1 = sub0.hd_mchange_use_i 
                  )
                  , sub1used as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd, final_list7.FYearMo,sub0.hd_mchange_use_i
                        ,final_list7.Usage-final_list7.lagShare_Usage AS hd_mchange_used
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.index1=sub0.hd_mchange_use_i-1
                  )
                  , sub1used2 as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd, final_list7.FYearMo, subusev4.avgUsage, sub1use.hd_mchange_use_i,final_list7.Usage
                        ,sub1use.Usage-subusev4.avgUsage AS hd_mchange_useavg, sub1use.hd_mchange_use
                        FROM final_list7
                        LEFT JOIN 
                        subusev4 ON final_list7.Platform_Subset_Nm=subusev4.Platform_Subset_Nm and final_list7.Country_Cd=subusev4.Country_Cd
                        INNER JOIN 
                        sub1use ON final_list7.Platform_Subset_Nm=sub1use.Platform_Subset_Nm and final_list7.Country_Cd=sub1use.Country_Cd  
                          and final_list7.index1=sub1use.hd_mchange_use_i-1
                  )
                  
                  , sub2 as (
                     SELECT a.*
                      ,sub1ps.hd_mchange_ps as adjust_ps
                      ,sub1ps.hd_mchange_ps_i as adjust_ps_i
                      ,sub1psb.hd_mchange_psb as adjust_psb
                      ,sub1psb.hd_mchange_ps_j as adjust_ps_j
                      ,sub1use.hd_mchange_use as adjust_use
                      ,sub1usec.hd_mchange_usec as adjust_usec
                      ,sub1used.hd_mchange_used as adjust_used
                      ,sub1use.hd_mchange_use_i as adjust_use_i
                      --,sub1cu.hd_mchange_cu as adjust_cu
                      --,sub1cu.hd_mchange_cu_i as adjust_cu_i
                      ,subusev4.avgUsage as avgUsage
                      ,sub1used2.hd_mchange_useavg as adjust_useav
                        
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
                        sub1usec
                        ON a.Platform_Subset_Nm=sub1usec.Platform_Subset_Nm and a.Country_Cd=sub1usec.Country_Cd --and a.FYearMo=sub1usec.FYearMo
                      LEFT JOIN 
                        sub1used
                        ON a.Platform_Subset_Nm=sub1used.Platform_Subset_Nm and a.Country_Cd=sub1used.Country_Cd --and a.FYearMo=sub1used.FYearMo
                      LEFT JOIN 
                        subusev4
                        ON a.Platform_Subset_Nm=subusev4.Platform_Subset_Nm and a.Country_Cd=subusev4.Country_Cd --and a.FYearMo=subusev4.FYearMo
                      LEFT JOIN 
                        sub1used2
                        ON a.Platform_Subset_Nm=sub1used2.Platform_Subset_Nm and a.Country_Cd=sub1used2.Country_Cd --and a.FYearMo=sub1used2.FYearMo
                      --LEFT JOIN 
                        --sub1cu
                       -- ON a.Platform_Subset_Nm=sub1cu.Platform_Subset_Nm and a.Country_Cd=sub1cu.Country_Cd --and a.FYearMo=sub1cu.FYearMo
                  )
                  SELECT *
                  FROM sub2
                     ")

#test1s <- subset(final_list7,Platform_Subset_Nm=="WEBER BASE I-INK" & Region=="NA")
final_list7$adjust_ps <- ifelse(isNull(final_list7$adjust_ps), 0, final_list7$adjust_ps)
final_list7$adjust_psb <- ifelse(isNull(final_list7$adjust_psb), 0, final_list7$adjust_psb)
final_list7$adjust_ps_i <- ifelse(isNull(final_list7$adjust_ps_i), 10000, final_list7$adjust_ps_i)
final_list7$adjust_ps_j <- ifelse(isNull(final_list7$adjust_ps_j), 0, final_list7$adjust_ps_j)
#final_list7$adjust_cu <- ifelse(is.na(final_list7$adjust_cu), 0, final_list7$adjust_cu)
final_list7$adjust_ps <- ifelse(final_list7$adjust_ps == -Inf, 0, final_list7$adjust_ps)
#final_list7$adjust_cu <- ifelse(final_list7$adjust_cu == -Inf, 0, final_list7$adjust_cu)
final_list7$adjust_used <- ifelse(isNull(final_list7$adjust_used),0,final_list7$adjust_used)
final_list7$adjust_use <- ifelse(isNull(final_list7$adjust_use),0,final_list7$adjust_use)
final_list7$adjust_usec <- ifelse(isNull(final_list7$adjust_usec),0,final_list7$adjust_usec)
final_list7$adjust_useav <- ifelse(isNull(final_list7$adjust_useav),0,final_list7$adjust_useav)

# final_list7$Page_Share_Adj <- ifelse(final_list7$Share_Source_PS=="Modeled",
#                                      ifelse((final_list7$adjust_ps>0) & final_list7$adjust_ps_i<= final_list7$index1,
#                                             pmax(final_list7$Page_Share -1.95*(final_list7$adjust_ps),0.05),
#                                             ifelse(final_list7$Page_Share < 0.12,pmax(final_list7$Page_Share -.95*(final_list7$adjust_ps),0.05),
#                                             ifelse((abs(final_list7$adjust_ps)< 0.05) & final_list7$adjust_ps_i<= final_list7$index1,pmax(final_list7$Page_Share-.95*(final_list7$adjust_ps),0.05),
#                                             ifelse(final_list7$adjust_ps_j>final_list7$index1,pmax(final_list7$Page_Share,final_list7$adjust_psb),final_list7$Page_Share))))
#                                      ,final_list7$Page_Share)
# final_list7$Page_Share_Adj <- ifelse(final_list7$Page_Share_Adj>1,1,ifelse(final_list7$Page_Share_Adj<0,0,final_list7$Page_Share_Adj))

adjval <- 0.99

final_list7$Page_Share_Adj <- ifelse(substr(final_list7$Share_Source_PS,1,8)=="Modelled"
                                     ,ifelse(final_list7$adjust_ps_i <= final_list7$index1, 
                                             lit(adjval)^(final_list7$index1-final_list7$adjust_ps_i+1)*final_list7$Page_Share_lin +(lit(1)-(lit(adjval)^(final_list7$index1-final_list7$adjust_ps_i+1)))*final_list7$Page_Share_sig 
                                             ,ifelse(final_list7$adjust_ps_j>final_list7$index1
                                             ,ifelse(final_list7$Page_Share_sig > final_list7$adjust_psb, final_list7$Page_Share_sig, final_list7$adjust_psb)
                                             #,final_list7$Page_Share_sig)
                                             ,final_list7$Page_Share_sig))
                                     ,final_list7$Share_Raw_PS)


final_list7$Page_Share_Adj <- ifelse(final_list7$Page_Share_Adj>1,1,ifelse(final_list7$Page_Share_Adj<0.05,0.05,final_list7$Page_Share_Adj))

#final_list7$CU_Share_Adj <- ifelse(final_list7$Share_Source_CU=="Modeled",ifelse((final_list7$adjust_cu>0) & final_list7$adjust_cu_i<= final_list7$index1,pmax(final_list7$Crg_Unit_Share -2*(final_list7$adjust_  cu),0.05),ifelse((final_list7$adjust_cu< -0.05) & final_list7$adjust_cu_i<= final_list7$index1,pmax(final_list7$Crg_Unit_Share -.95*(final_list7$adjust_cu),0.05),final_list7$Crg_Unit_Share)),final_list7$Crg_Unit_Share)
#final_list7$CU_Share_Adj <- ifelse(final_list7$CU_Share_Adj>1,1,ifelse(final_list7$CU_Share_Adj<0,0,final_list7$CU_Share_Adj))

###ADJUST USAGE
final_list7$adjust_use_i <- ifelse(isNull(final_list7$adjust_use_i),0,final_list7$adjust_use_i)
#final_list7$Usage_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse((abs(final_list7$adjust_use/final_list7$adjust_used)>1.5) & final_list7$adjust_use_i<= final_list7$index1,pmax(final_list7$Usage -(final_list7$adjust_use+0.95*final_list7$adjust_used),0.05),final_list7$Usage),final_list7$Usage)
final_list7$Usage_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$adjust_use_i <= final_list7$index,pmax((final_list7$Usage-final_list7$adjust_useav),0.05),final_list7$Usage),final_list7$Usage)

final_list7$Usagec_Adj <- ifelse(final_list7$Usage_Adj!=final_list7$Usage,final_list7$Usage_Adj*final_list7$color_pct,final_list7$Usage_c)

final_list7$Page_Share_old <- cast(final_list7$Page_Share_sig, "double")
final_list7$Page_Share <- cast(final_list7$Page_Share_Adj, "double")
#final_list7$Crg_Unit_Share <- as.numeric(final_list7$CU_Share_Adj)
final_list7$Usage <- cast(final_list7$Usage_Adj, "double")
final_list7$Usage_c <- cast(final_list7$Usagec_Adj, "double")
final_list7$Usage_k <- cast(final_list7$Usage_Adj, "double")

final_list7$Usage_c <- ifelse(final_list7$Usage_c>final_list7$Usage,final_list7$Usage*.985,final_list7$Usage_c)
final_list7$Usage_k <- final_list7$Usage
final_list7$color_pct2 <- final_list7$Usage_c/final_list7$Usage

#Change share for CTSS platform subsets (PL IR)
#final_list7$Page_Share <- ifelse(final_list7$pl=='IR',1,final_list7$Page_Share)


final_list7$Pages_Device_UPM <- final_list7$MPV_TS*final_list7$IB
final_list7$Pages_Device_Raw <- final_list7$MPV_Raw*final_list7$IB
final_list7$Pages_Device_Dash <- final_list7$MPV_Dash*final_list7$IB
final_list7$Pages_Device_Use <- final_list7$Usage*final_list7$IB
final_list7$Pages_Share_Model_PS <- final_list7$MPV_TS*final_list7$IB*final_list7$Share_Model_PS
final_list7$Pages_Share_Raw_PS <- final_list7$MPV_Raw*final_list7$IB*cast(final_list7$Share_Raw_PS, "double")
final_list7$Pages_Share_Dash_PS <- final_list7$MPV_Dash*final_list7$IB*cast(final_list7$Share_Raw_PS, "double")
final_list7$Pages_PS <- final_list7$Pages_Device_Use*cast(final_list7$Page_Share, "double")

#final_list7$Pages_Share_Model_CU <- final_list7$MPV_TS*final_list7$IB*final_list7$Share_Model_CU
#final_list7$Pages_Share_Raw_CU <- final_list7$MPV_Raw*final_list7$IB*as.numeric(final_list7$Share_Raw_CU)
#final_list7$Pages_Share_Dash_CU <- final_list7$MPV_Dash*final_list7$IB*as.numeric(final_list7$Share_Raw_CU)
#final_list7$Pages_CU <- final_list7$Pages_Device_Use*as.numeric(final_list7$Crg_Unit_Share)
#final_list7 <- final_list7[c(1:79)]
# final_list7 <- as.data.frame(final_list7)

# #Change PWA to CCs

final_list7$Usage <- ifelse(final_list7$technology!='PWA',final_list7$Usage,
                            ifelse(final_list7$Region=="AP",final_list7$Usage*0.040,
                            ifelse(final_list7$Region=="EU",final_list7$Usage*0.036,
                            ifelse(final_list7$Region=="JP",final_list7$Usage*0.040,
                            ifelse(final_list7$Region=="LA",final_list7$Usage*0.038,
                                   final_list7$Usage*0.037)))))

final_list7$color_pct <- cast(final_list7$color_pct, "double")
final_list7$Usage_c <- ifelse(final_list7$CM != "C",NA,ifelse(final_list7$technology!='PWA',final_list7$Usage_c,
                            ifelse(final_list7$Region=="AP",final_list7$Usage_c*0.040,
                            ifelse(final_list7$Region=="EU",final_list7$Usage_c*0.036,
                            ifelse(final_list7$Region=="JP",final_list7$Usage_c*0.040,
                            ifelse(final_list7$Region=="LA",final_list7$Usage_c*0.038,
                                   final_list7$Usage_c*0.037))))))



# #write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer Adj (",Sys.Date(),").csv", sep=''), x=final_list8,row.names=FALSE, na="")

# COMMAND ----------

# Change to match MDM format

final_list8 <- filter(final_list7, !isNull(final_list7$Page_Share))  #missing intro date
final_list8$fiscal_date <- concat_ws(sep = "-", substr(final_list8$FYearMo, 1, 4), substr(final_list8$FYearMo, 5, 6), lit("01"))
final_list8$model_group <- concat(final_list8$CM,final_list8$SM,final_list8$Mkt,"_",final_list8$market10,"_",final_list8$Region_DE)

#final_list8jp <- subset(final_list8,Region=="AP" & Platform_Subset_Nm %in% c('ANNAPURNA','ANTARES PQ','AZALEA','BLUEFIN','CORDOBA','CORDOBA MANAGED','CYPRESS'
                            #,'DENALI MFP','EVEREST MFP','FIJIMFP','GARNETAK','MADRID','MADRID LITE','MADRID MANAGED','MAPLE','NOVA PQ','REDWOOD','SAPPHIRE MFP'
                            #,'TAHITI PQ','VEGA PQ','YELLOWTAIL','BIGEYE','OPALAK'))
#final_list8jp$Region <- 'JP'final_list8$year_month_float <- as.Date(final_list8$fiscal_date)
#final_list8 <- rbind(final_list8,final_list8jp)

#Change from Fiscal Date to Calendar Date
final_list8$year_month_float <- to_date(final_list8$fiscal_date, "yyyy-MM-dd")
#month(final_list8$year_month_float) <- month(final_list8$year_month_float)-2

today <- Sys.Date()
vsn <- '2022.01.19.1  #for DUPSM
#vsn <- 'New Version'
rec1 <- 'usage_share'
geog1 <- 'country'
tempdir(check=TRUE)
# rm(final_list2)
#rm(final_list6)
# rm(final_list7)
#rm(UPM2)
#rm(UPM2C)
gc()
#s3write_using(x=final_list8,FUN = write.csv, object = paste0("s3://insights-environment-sandbox/BrentT/toner_100pct_",Sys.Date(),".csv"), row.names=FALSE, na="")

createOrReplaceTempView(final_list8, "final_list8")
cacheTable("final_list8")

# COMMAND ----------

# createOrReplaceTempView(mdm_tbl_share, "mdm_tbl_share")
# createOrReplaceTempView(mdm_tbl_usage, "mdm_tbl_usage")
# createOrReplaceTempView(mdm_tbl_sharen, "mdm_tbl_sharen")
# createOrReplaceTempView(mdm_tbl_usagen, "mdm_tbl_usagen")

# COMMAND ----------

# %scala
# if (dbutils.widgets.get("writeout") == "YES") {
  
#   val mdmTblShare = spark.sql("SELECT * FROM mdm_tbl_share")
#   val mdmTblUsage = spark.sql("SELECT * FROM mdm_tbl_usage")
#   val mdmTblShareN = spark.sql("SELECT * FROM mdm_tbl_sharen")
#   val mdmTblUsageN = spark.sql("SELECT * FROM mdm_tbl_usagen")
  
#   writeDFToRedshift(configs, mdmTblShare, "stage.mdm_tbl_share", mode = "overwrite")
#   writeDFToRedshift(configs, mdmTblUsage, "stage.mdm_tbl_usage", mode = "overwrite")
#   writeDFToRedshift(configs, mdmTblShareN, "stage.mdm_tbl_sharen", mode = "overwrite")
#   writeDFToRedshift(configs, mdmTblUsageN, "stage.mdm_tbl_usagen", mode = "overwrite")
# }

# COMMAND ----------

mdm_tbl_share <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Share_Source_PS as data_source
                , '",vsn,"' as version
                , 'hp_share' as measure
                , Page_Share as units
                , Proxy_PS as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                , model_group
                from final_list8
                 
                 "))

mdm_tbl_usage <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'usage' as measure
                , Usage as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                , model_group
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
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'Usage_n' as measure
                , MPV_n as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                , model_group
                from final_list8
                WHERE MPV_n is not null AND MPV_n >0
                 
                 "))
                                            
mdm_tbl_sharen <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'Share_n' as measure
                , Share_Raw_N_PS as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                , model_group
                from final_list8
                WHERE Share_Raw_N_PS is not null AND Share_Raw_N_PS >0
                 
                 "))

mdm_tbl_kusage <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'k_usage' as measure
                , Usage as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                , model_group
                from final_list8
                WHERE Usage is not null
                 
                 "))

mdm_tbl_cusage <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'color_usage' as measure
                , Usage_c as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                , model_group
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))
                                            
mdm_tbl <- rbind(mdm_tbl_share, mdm_tbl_usage, mdm_tbl_usagen, mdm_tbl_sharen, mdm_tbl_kusage, mdm_tbl_cusage)

mdm_tbl$cal_date <- to_date(mdm_tbl$cal_date,format="yyyy-MM-dd")
mdm_tbl$forecast_created_date <- to_date(mdm_tbl$forecast_created_date,format="yyyy-MM-dd")
mdm_tbl$load_date <- to_date(mdm_tbl$load_date,format="yyyy-MM-dd")

mdm_tbl$dm_version <- lit(dm_version)

createOrReplaceTempView(mdm_tbl, "mdm_tbl")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val mdmTbl = spark.sql("SELECT * FROM mdm_tbl")
# MAGIC 
# MAGIC  mdmTbl.write
# MAGIC   .format("parquet")
# MAGIC   .mode("overwrite")
# MAGIC   .partitionBy("measure")
# MAGIC   .save(s"""s3://${spark.conf.get("aws_bucket_name")}toner_usage_share_75_Q4_qe.parquet""")
# MAGIC 
# MAGIC if (dbutils.widgets.get("writeout") == "YES") {
# MAGIC 
# MAGIC   submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), "TRUNCATE stage.usage_share_country_landing_G2")
# MAGIC   
# MAGIC   mdmTbl.write
# MAGIC     .format("com.databricks.spark.redshift")
# MAGIC     .option("url", configs("redshiftUrl"))
# MAGIC     .option("tempdir", configs("redshiftTempBucket"))
# MAGIC     .option("aws_iam_role", configs("redshiftAwsRole"))
# MAGIC     .option("user", configs("redshiftUsername"))
# MAGIC     .option("password", configs("redshiftPassword"))
# MAGIC     .option("dbtable", "stage.usage_share_country_landing_G2")
# MAGIC     .partitionBy("measure")
# MAGIC     .mode("append")
# MAGIC     .save()
# MAGIC   
# MAGIC   submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), "GRANT ALL ON ALL TABLES IN schema stage TO group dev_arch_eng")
# MAGIC }

# COMMAND ----------

notebook_end_time <- Sys.time()
notebook_total_time <- notebook_end_time - notebook_start_time;notebook_total_time
