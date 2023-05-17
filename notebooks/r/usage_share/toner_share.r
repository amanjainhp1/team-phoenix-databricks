# Databricks notebook source
# ---
# #Version 2021.05.01.1#
# title: "100% IB Toner Share (country level)"
# output:
#   html_notebook: default
#   pdf_document: default
# ---

# COMMAND ----------

notebook_start_time <- Sys.time()

# COMMAND ----------

dbutils.widgets.text("cutoff_dt", "")
dbutils.widgets.text("outnm_dt", "")
dbutils.widgets.text("datestamp","")
dbutils.widgets.text("timestamp","")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # retrieve tasks from widgets/parameters
# MAGIC tasks = dbutils.widgets.get("tasks") if dbutils.widgets.get("tasks") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["tasks"]
# MAGIC tasks = tasks.split(";")
# MAGIC 
# MAGIC # define all relevenat task parameters to this notebook
# MAGIC relevant_tasks = ["all", "share"]
# MAGIC 
# MAGIC # exit if tasks list does not contain a relevant task i.e. "all" or "share"
# MAGIC for task in tasks:
# MAGIC     if task not in relevant_tasks:
# MAGIC         dbutils.notebook.exit("EXIT: Tasks list does not contain a relevant value i.e. {}.".format(", ".join(relevant_tasks)))

# COMMAND ----------

# MAGIC %python
# MAGIC # set vars equal to widget vals for interactive sessions, else retrieve task values 
# MAGIC cutoff_dt = dbutils.widgets.get("cutoff_dt") if dbutils.widgets.get("cutoff_dt") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["cutoff_dt"]
# MAGIC outnm_dt = dbutils.widgets.get("outnm_dt") if dbutils.widgets.get("outnm_dt") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["outnm_dt"]
# MAGIC datestamp = dbutils.widgets.get("datestamp") if dbutils.widgets.get("datestamp") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["datestamp"]
# MAGIC timestamp = dbutils.widgets.get("timestamp") if dbutils.widgets.get("timestamp") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["timestamp"]

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # retrieve configs and export to spark.confs for usage across languages
# MAGIC for key, val in configs.items():
# MAGIC     spark.conf.set(key, val)
# MAGIC 
# MAGIC spark.conf.set('cutoff_dt', cutoff_dt)
# MAGIC spark.conf.set('datestamp', datestamp)
# MAGIC spark.conf.set('timestamp', timestamp)
# MAGIC spark.conf.set('aws_bucket_name', constants['S3_BASE_BUCKET'][stack])

# COMMAND ----------

packages <- c("sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR", "spatstat", "nls2")

# COMMAND ----------

# MAGIC %run ../common/package_check

# COMMAND ----------

options(java.parameters = "-Xmx30g" ) 
options(stringsAsFactors = FALSE)

tempdir(check=TRUE)

# COMMAND ----------

# define s3 bucket
aws_bucket_name <- sparkR.conf('aws_bucket_name')

# COMMAND ----------

# MAGIC %python
# MAGIC # load parquet data and register views
# MAGIC 
# MAGIC tables = ['bdtbl', 'calendar', 'hardware_xref', 'ib', 'iso_cc_rollup_xref', 'iso_country_code_xref']
# MAGIC for table in tables:
# MAGIC     spark.read.parquet(f'{constants["S3_BASE_BUCKET"][stack]}/cupsm_inputs/toner/{datestamp}/{timestamp}/{table}/').createOrReplaceTempView(f'{table}')

# COMMAND ----------

datestamp <- sparkR.conf("datestamp")
timestamp <- sparkR.conf("timestamp")
cutoff_date <- sparkR.conf("cutoff_dt")

table_month0 <- SparkR::collect(SparkR::sql(paste0("
           SELECT  tpmib.printer_group  
           , tpmib.printer_platform_name as platform_name 
           , tpmib.platform_std_name 
           , tpmib.printer_country_iso_code as iso_country_code
           , tpmib.fiscal_year_quarter 
           , tpmib.date_month_dim_ky as calendar_year_month
           --Get BD Flags 
           , CASE WHEN tpmib.ps_region_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' THEN 1 ELSE 0 END AS page_share_region_incl_flag 
           , CASE WHEN tpmib.ps_country_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' THEN 1 ELSE 0 END AS page_share_country_incl_flag
           , CASE WHEN tpmib.ps_market_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' THEN 1 ELSE 0 END as page_share_market_incl_flag
           , CASE WHEN tpmib.usage_region_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' THEN 1 ELSE 0 END AS usage_region_incl_flag 
           , CASE WHEN tpmib.usage_country_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' THEN 1 ELSE 0 END AS usage_country_incl_flag
           , CASE WHEN tpmib.usage_market_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' THEN 1 ELSE 0 END AS usage_market_incl_flag
           , CASE WHEN tpmib.share_region_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' then 1 else 0 END AS share_region_incl_flag   
           , CASE WHEN tpmib.share_country_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' THEN 1 ELSE 0 END AS share_country_incl_flag
           , CASE WHEN tpmib.share_market_incl_flag=1 AND tpmib.date_month_dim_ky < '",cutoff_date,"' THEN 1 ELSE 0 END AS share_market_incl_flag
           --Get numerator and denominator, already ib weighted at printer level

           --ci share
           , SUM(COALESCE(tpmib.supply_installs_cmyk_hp_trade_ib_ext_sum,0)) AS ci_numerator 
           , SUM(COALESCE(tpmib.supply_installs_cmyk_share_ib_ext_sum,0)) AS ci_denominator
           --pageshare
           , SUM(COALESCE(tpmib.supply_pages_cmyk_hp_ib_ext_sum,0)) AS ps_numerator
           , SUM(COALESCE(tpmib.supply_pages_cmyk_share_ib_ext_sum,0)) AS ps_denominator
           --usage
           , SUM(COALESCE(tpmib.print_pages_total_ib_ext_sum,0)) as usage_numerator
           , SUM(COALESCE(tpmib.print_pages_color_ib_ext_sum,0)) as color_numerator
           , SUM(COALESCE(tpmib.print_months_ib_ext_sum,0)) as usage_denominator
           --counts
           , SUM(COALESCE(tpmib.printer_count_month_page_share_flag_sum,0)) AS printer_count_month_ps
           , SUM(COALESCE(tpmib.printer_count_month_supply_install_flag_sum,0)) AS printer_count_month_ci
           , SUM(COALESCE(tpmib.printer_count_month_usage_flag_sum,0)) AS printer_count_month_use

           , SUM(COALESCE(tpmib.printer_count_fyqtr_page_share_flag_sum,0)) AS printer_count_fiscal_quarter_ci   
           , SUM(COALESCE(tpmib.printer_count_fyqtr_supply_install_flag_sum,0)) AS printer_count_fiscal_quarter_ps   
           , SUM(COALESCE(tpmib.printer_count_fyqtr_usage_flag_sum,0)) AS printer_count_fiscal_quarter_use

            FROM 
             bdtbl tpmib
            WHERE 1=1 
            AND printer_route_to_market_ib='AFTERMARKET'
            GROUP BY tpmib.printer_group  
           , tpmib.printer_platform_name
           , tpmib.platform_std_name  
           , tpmib.printer_country_iso_code
           , tpmib.fiscal_year_quarter 
           , tpmib.date_month_dim_ky
           , tpmib.share_region_incl_flag 
           , tpmib.share_country_incl_flag
           , tpmib.share_market_incl_flag
           , tpmib.ps_region_incl_flag
           , tpmib.ps_country_incl_flag
           , tpmib.ps_market_incl_flag
           , tpmib.usage_region_incl_flag
           , tpmib.usage_country_incl_flag
           , tpmib.usage_market_incl_flag
")))

# COMMAND ----------

######Big Data Version#########
dm_version <- SparkR::collect(SparkR::sql("select distinct insert_ts from bdtbl tri_printer_usage_sn"))[1,1]

# COMMAND ----------

#######SELECT IB VERSION#####
#be sure ib versions match in these next two tables
ibtable <- SparkR::collect(SparkR::sql("
                   select  a.platform_subset
                          , YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          , e.Fiscal_Year_Qtr as fiscal_year_quarter
                          , d.technology AS hw_type
                          , b.region_5
                          , a.country_alpha2
                          , substring(b.developed_emerging,1,1) as de
                          , SUM(COALESCE(a.units,0)) as ib
                          , a.version
                    from ib a
                    left join iso_country_code_xref b
                      on (a.country_alpha2=b.country_alpha2)
                    left join hardware_xref d
                       on (a.platform_subset=d.platform_subset)
                    left join calendar e
                        on (a.cal_date=e.date)
                    where a.measure='IB'
                       and (upper(d.technology)='LASER' or (d.technology='PWA' and (upper(d.hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3', 'TIJ_4.XG2 MORNESTA'))
                         ))
                    group by a.platform_subset, a.cal_date 
                            , e.Fiscal_Year_Qtr, d.technology
                            , a.version
                            , b.developed_emerging
                            , b.region_5, a.country_alpha2
                   "))

product_ib2a <- SparkR::collect(SparkR::sql("
                   select  a.platform_subset
                          , a.cal_date
                          , YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          , YEAR(ADD_MONTHS(a.cal_date, +2))*100+MONTH(ADD_MONTHS(a.cal_date, +2)) as fyearmo
                          , d.technology AS hw_type
                          , a.customer_engagement  AS RTM
                          , b.region_5
                          , b.country_alpha2
                          , sum(a.units) as ib
                          , a.version
                    from ib a
                    left join iso_country_code_xref b
                      on (a.country_alpha2=b.country_alpha2)
                    left join hardware_xref d
                       on (a.platform_subset=d.platform_subset)
                    where a.measure='IB'
                      and (upper(d.technology)='LASER' or (d.technology='PWA' and (upper(d.hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3', 'TIJ_4.XG2 MORNESTA')) ))
                    group by a.platform_subset, a.cal_date, d.technology, a.customer_engagement,a.version
                      , b.region_5, b.country_alpha2
                   "))

# COMMAND ----------

hwval <- SparkR::collect(SparkR::sql("
                  SELECT distinct platform_subset
                    , predecessor
                    , pl as product_line_code
                    , CASE WHEN vc_category in ('DEPT') THEN 'DPT'
                          WHEN vc_category in ('PLE-H','PLE-L','ULE') THEN 'DSK'
                          WHEN vc_category in ('SWT-H','SWT-H PRO') THEN 'SWH'
                          WHEN vc_category = 'SWT-L' THEN 'SWL'
                          WHEN vc_category = 'WG' THEN 'WGP'
                    END AS platform_market_code
                    , sf_mf AS platform_function_code
                     , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                      WHEN vc_category in ('SWT-H','SWT-H PRO','DEPT','DEPT-HIGH','WG') then 'ENT'
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
                  FROM hardware_xref
                  where technology in ('LASER','PWA')"))


#Get Intro Dates
intro_dt <- SparkR::collect(SparkR::sql(paste0("
                     with minyr as (
                     SELECT platform_subset, min(YEAR(cal_date)*100+MONTH(cal_date)) AS intro_month, min(cal_date) as printer_intro_month
                     FROM ib
                     WHERE 1=1
                     AND measure='IB'
                     GROUP BY platform_subset
                     ),
                     max_units_prep as
                     (
                      SELECT platform_subset
                      , cal_date
                      , version
                      , max(units) as maxunits
                      FROM ib
                      WHERE 1=1
                      AND measure = 'IB'
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
                     ")))

product_info2 <- sqldf("SELECT a.*,b.printer_intro_month, b.printer_high_month
                       from hwval a 
                       left join intro_dt b 
                       on a.platform_subset=b.platform_subset")

#Get Market10 Information
country_info <- SparkR::collect(SparkR::sql("
                      WITH mkt10 AS (
                           SELECT country_alpha2, country_level_2 as market10, country_level_4 as emdm
                           FROM iso_cc_rollup_xref
                           WHERE country_scenario='Market10'
                      ),
                      rgn5 AS (
                            SELECT country_alpha2, CASE WHEN region_5='JP' THEN 'AP' ELSE region_5 END AS region_5, developed_emerging, country 
                            FROM iso_country_code_xref
                      )
                      SELECT a.country_alpha2, a.region_5, b.market10, a.developed_emerging, country
                            FROM rgn5 a
                            LEFT JOIN mkt10 b
                            ON a.country_alpha2=b.country_alpha2
                           "))
country_info <- sqldf("SELECT * from country_info where country_alpha2 in (select distinct country_alpha2 from ibtable)")
country_info$region_5 <- ifelse(country_info$market10=='Latin America','LA',country_info$region_5)

table_month <- sqldf("
                    with sub0 as (select a.platform_name, c.region_5 as printer_region_code, c.market10, c.developed_emerging, c.country_alpha2
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
              ON (a.iso_country_code=ib.country_alpha2 and a.calendar_year_month=ib.month_begin and a.platform_name=ib.platform_subset)
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
                    , fiscal_year_quarter        
                    , developed_emerging
                    , country_alpha2
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
                    , fiscal_year_quarter
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

Pred_Raw_file <- SparkR::collect(SparkR::sql("SELECT * FROM pred_raw_file"))

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
                    WHEN mdm_predecessor is not null THEN 'MDM'
                    WHEN Van_Predecessor is not null THEN 'VAN'
                    WHEN Toner_Predecessor is not null THEN 'TONER'
                    WHEN Trevor_Predecessor is not null THEN 'TREVOR'
                    WHEN Mike_Predecessor is not null THEN 'MIKE'
                    WHEN Rainbow_Predecessor is not null THEN 'RAINBOW'
                    WHEN [HW.Predecessor] is not null THEN 'HW'
                    WHEN [Emma.Predecessor] is not null THEN 'EMMA'
                    WHEN product_previous_model is not null THEN 'BD INFO TABLE'
                    ELSE null 
                    END AS Predecessor_src
                  from Pred_Raw_file")

printer_list <- sqldf("SELECT pi.platform_subset as platform_subset_name
                      , CASE WHEN pi.predecessor is not null then pi.predecessor
                             WHEN pl.Predecessor='0' THEN NULL
                             ELSE pl.Predecessor
                             END AS Predecessor
                      , CASE WHEN pi.predecessor is not null then 'MDM'
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
              , pib.RTM
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
printer_list_ctr <- sqldf("select a.*,CASE WHEN c.region_5 is NULL THEN 'NULL' ELSE c.region_5 END as region_5, c.market10, c.developed_emerging
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
                                WHEN c.min is NOT NULL THEN 'COUNTRY'
                                WHEN d.min is NOT NULL THEN 'DEV/EM'
                                WHEN m.min is NOT NULL THEN 'MARKET10'
                                WHEN r.min is NOT NULL THEN 'REGION'
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
                      WHEN a.printer_platform_name like '%MANAGED' and b.printer_platform_name like '%MANAGED' and b.curve_exists='Y' THEN 'Y'
                      WHEN a.printer_platform_name not like '%MANAGED' and b.printer_platform_name like '%MANAGED' THEN 'N'
                      WHEN a.printer_platform_name not like '%MANAGED' and b.printer_platform_name  not like '%MANAGED' and b.curve_exists='Y' THEN 'Y'
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
                      WHEN a.printer_platform_name like '%MANAGED' and b.printer_platform_name like '%MANAGED' and b.curve_exists='Y' THEN 'Y'
                      WHEN a.printer_platform_name not like '%MANAGED' and b.printer_platform_name like '%MANAGED' THEN 'N'
                      WHEN a.printer_platform_name not like '%MANAGED' and b.printer_platform_name  not like '%MANAGED' and b.curve_exists='Y' THEN 'Y'
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
                      WHEN a.printer_platform_name like '%MANAGED' and b.printer_platform_name like '%MANAGED' and b.curve_exists='Y' THEN 'Y'
                      WHEN a.printer_platform_name not like '%MANAGED' and b.printer_platform_name like '%MANAGED' THEN 'N'
                      WHEN a.printer_platform_name not like '%MANAGED' and b.printer_platform_name  not like '%MANAGED' and b.curve_exists='Y' THEN 'Y'
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

proxylist_f1 <- subset(proxylist_b,!is.na(min))
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
UPM <- SparkR::read.parquet(path=paste(aws_bucket_name, "cupsm_outputs", "toner", datestamp, timestamp, "usage_total", sep="/"))
UPMC <- SparkR::read.parquet(path=paste(aws_bucket_name, "cupsm_outputs", "toner", datestamp, timestamp, "usage_color", sep="/"))

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
              , a.label
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
              , a.label
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

# COMMAND ----------

###Combine Results

hwlookup <- SparkR::sql("
                       SELECT distinct platform_subset, technology, pl, sf_mf AS SM
                       FROM hardware_xref")

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
                      , c.RTM as customer_engagement
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
                      , b.label
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
                            case when a.source='COUNTRY' then 'MODELLED AT COUNTRY LEVEL'
                                 when a.source='DEV/EM' then 'MODELLED AT MARKET13 LEVEL'
                                 when a.source='MARKET10' then 'MODELLED AT MARKET10 LEVEL'
                                 else 'MODELLED AT REGION5 MODELLED'
                                 end
                          when a.BD_Share_Flag_PS = 0 then 
                            case when a.source='COUNTRY' then 'MODELLED AT COUNTRY LEVEL'
                                 when a.source='DEV/EM' then 'MODELLED AT MARKET13 LEVEL'
                                 when a.source='MARKET10' then 'MODELLED AT MARKET10 LEVEL'
                                 else 'MODELLED AT REGION5 MODELLED'
                                 end
                          when a.Share_Raw_PS is NULL then 
                              case when a.source='COUNTRY' then 'MODELLED AT COUNTRY LEVEL'
                                 when a.source='DEV/EM' then 'MODELLED AT MARKET13 LEVEL'
                                 when a.source='MARKET10' then 'MODELLED AT MARKET10 LEVEL'
                                 else 'MODELLED AT REGION5 MODELLED'
                                 end
                          when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then 
                              case when a.source='COUNTRY' then 'MODELLED AT COUNTRY LEVEL'
                                 when a.source='DEV/EM' then 'MODELLED AT MARKET13 LEVEL'
                                 when a.source='MARKET10' then 'MODELLED AT MARKET10 LEVEL'
                                 else 'MODELLED AT REGION5 LEVEL'
                                 end
                          else 'HAVE DATA'
                          end
                        else 'MODELLED BY PROXY'
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
                      --    case when a.BD_Share_Flag_CU = 0 then 'MODELED'
                      --    when a.BD_Share_Flag_CU is NULL then 'MODELED'
                      --   when a.Share_Raw_CU IN (NULL, 0) then 'MODELED'
                      --    when a.Share_Raw_N_CU is NULL or a.Share_Raw_N_CU < 20 then 'MODELED'
                      --    else 'HAVE DATA'
                      --    end
                      --  else 'MODELED BY PROXY'
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
                          else 'DASHBOARD'
                          end
                           as Usage_Source
                      , case
                        when a.technology='LASER' then
                        case
                          when a.BD_Usage_Flag is NULL then a.MPV_TD
                          when a.BD_Share_Flag_PS = 0 then a.MPV_TD
                          when a.MPV_DASH is NULL then a.MPV_TD
                          --when a.usage_n < 75 then a.MPV_TD
                            else a.MPV_DASH
                            end
                        when a.technology='PWA' then
                          case
                          when a.BD_Usage_Flag is NULL then a.MPV_TS
                          when a.BD_Share_Flag_PS = 0 then a.MPV_TS
                          when a.MPV_Dash is NULL then a.MPV_TS
                          --when a.usage_n < 75 then a.MPV_TS
                            else a.MPV_Dash
                            end
                          end as Usage
                      , case
                          when a.technology='LASER' then
                          case
                          when a.BD_Usage_Flag is NULL then a.MPV_TD*a.color_pct
                          when a.BD_Share_Flag_PS = 0 then a.MPV_TD*a.color_pct
                          when (a.MPV_DashC is NULL or a.MPV_DashC < 0 or a.color_pct >1) then a.MPV_TD*a.color_pct
                          --when a.usage_n < 75 then a.MPV_TD*a.color_pct
                            else a.MPV_DashC
                            end
                            when a.technology='PWA' then
                          case
                          when a.BD_Usage_Flag is NULL then a.MPV_TS*a.color_pct
                          when a.BD_Share_Flag_PS = 0 then a.MPV_TS*a.color_pct
                          when (a.MPV_DashC is NULL or a.MPV_DashC < 0 or a.color_pct >1) then a.MPV_TS*a.color_pct
                          --when a.usage_n < 75 then a.MPV_TS*a.color_pct
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

final_list7$hd_mchange_ps <- ifelse(substr(final_list7$Share_Source_PS,1,8)=="MODELLED",ifelse(final_list7$lagShare_Source_PS=="HAVE DATA",final_list7$Page_Share_sig-final_list7$lagShare_PS, NA ), NA)
final_list7$hd_mchange_ps_i <- ifelse(!isNull(final_list7$hd_mchange_ps),final_list7$index1,NA)
final_list7$hd_mchange_psb <- ifelse(final_list7$Share_Source_PS=="HAVE DATA",ifelse(substr(final_list7$lagShare_Source_PS,1,8)=="MODELLED",final_list7$Page_Share_sig, NA ),NA)
final_list7$hd_mchange_ps_j <- ifelse(!isNull(final_list7$hd_mchange_psb),final_list7$index1,NA)
#final_list7$hd_mchange_cu <- ifelse(final_list7$Share_Source_CU=="Modeled",ifelse(final_list7$lagShare_Source_CU=="Have Data",final_list7$Crg_Unit_Share-final_list7$lagShare_CU, NA ),NA)
#final_list7$hd_mchange_cu_i <- ifelse(!is.na(final_list7$hd_mchange_cu),final_list7$index1,NA)
final_list7$hd_mchange_use <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$lagUsage_Source=="DASHBOARD" | upper(final_list7$lagUsage_Source)=="UPM SAMPLE SIZE",final_list7$Usage-final_list7$lagShare_Usage, NA ),NA)
#final_list7$hd_mchange_usec <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage_c-final_list7$lagShare_Usagec, NA ),NA)
final_list7$hd_mchange_used <- ifelse(final_list7$Usage_Source=="DASHBOARD"| upper(final_list7$Usage_Source)=="UPM SAMPLE SIZE",ifelse(final_list7$lagUsage_Source=="UPM",final_list7$Usage-final_list7$lagShare_Usage, NA ),NA)
final_list7$hd_mchange_use_i <- ifelse(!isNull(final_list7$hd_mchange_use),final_list7$index1,NA)
final_list7$hd_mchange_use_j <- ifelse(!isNull(final_list7$hd_mchange_used),final_list7$index1,NA)


createOrReplaceTempView(final_list7, "final_list7")

final_list7 <- SparkR::sql("
                with sub0 as (
                    SELECT Platform_Subset_Nm,Country_Cd
                        ,max(hd_mchange_ps_i) as hd_mchange_ps_i
                        ,min(hd_mchange_ps_j) as hd_mchange_ps_j
                        --,max(hd_mchange_cu_i) as hd_mchange_cu_i
                        ,max(hd_mchange_use_i) as hd_mchange_use_i
                        ,min(hd_mchange_use_j) as hd_mchange_use_j
                        FROM final_list7
                        GROUP BY Platform_Subset_Nm,Country_Cd
                )
                , subusev as (
                    SELECT Platform_Subset_Nm,Country_Cd, FYearMo, Usage, IB
                    FROM final_list7
                    WHERE Usage_Source='DASHBOARD'
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
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd, final_list7.FYearMo,sub0.hd_mchange_use_j
                        ,final_list7.MPV_Raw-final_list7.MPV_TD AS hd_mchange_used
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd  
                          and final_list7.index1=sub0.hd_mchange_use_j
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
                      ,sub1used.hd_mchange_use_j as adjust_use_j
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

final_list7$Page_Share_Adj <- ifelse(substr(final_list7$Share_Source_PS,1,8)=="MODELLED"
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
final_list7$adjust_use_j <- ifelse(isNull(final_list7$adjust_use_j),0,final_list7$adjust_use_j)
#final_list7$Usage_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse((abs(final_list7$adjust_use/final_list7$adjust_used)>1.5) & final_list7$adjust_use_i<= final_list7$index1,pmax(final_list7$Usage -(final_list7$adjust_use+0.95*final_list7$adjust_used),0.05),final_list7$Usage),final_list7$Usage)
final_list7$Usage_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$adjust_use_i <= final_list7$index1, ifelse((final_list7$Usage-final_list7$adjust_useav) > 0.05, (final_list7$Usage-final_list7$adjust_useav), 0.05), ifelse(final_list7$adjust_use_j >= final_list7$index1,final_list7$Usage+final_list7$adjust_used, final_list7$Usage)), final_list7$Usage)

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

 final_list7$total_pages <- final_list7$Usage*final_list7$ib
 final_list7$hp_pages <- final_list7$Usage*final_list7$ib*final_list7$Page_Share     
 final_list7$total_kpages <- final_list7$Usage_k*final_list7$ib 
 final_list7$total_cpages <- final_list7$Usage_c*final_list7$ib
 final_list7$hp_kpages <- final_list7$Usage_k*final_list7$ib*final_list7$Page_Share 
 final_list7$hp_cpages <- final_list7$Usage_c*final_list7$ib*final_list7$Page_Share
 final_list7$nonhp_kpages <- final_list7$Usage_k*final_list7$ib*(lit(1)-final_list7$Page_Share) 
 final_list7$nonhp_cpages <- final_list7$Usage_c*final_list7$ib*(lit(1)-final_list7$Page_Share)

# COMMAND ----------

# Change to match MDM format

final_list8 <- filter(final_list7, !isNull(final_list7$Page_Share))  #missing intro date
final_list8$fiscal_date <- concat_ws(sep = "-", substr(final_list8$FYearMo, 1, 4), substr(final_list8$FYearMo, 5, 6), lit("01"))
final_list8$model_group <- concat(final_list8$CM, final_list8$SM ,final_list8$Mkt, lit("_"), final_list8$market10, lit("_"), final_list8$Region_DE)

#Change from Fiscal Date to Calendar Date
final_list8$year_month_float <- to_date(final_list8$fiscal_date, "yyyy-MM-dd")
final_list8$dm_version <- as.character(dm_version)
final_list8$Usage_Source <- ifelse(upper(final_list8$Usage_Source)=="UPM SAMPLE SIZE","DASHBOARD",final_list8$Usage_Source)
today <- datestamp
vsn <- '2023.05.01.1'  #for DUPSM
rec1 <- 'USAGE_SHARE'
geog1 <- 'COUNTRY'
tempdir(check=TRUE)

gc()

createOrReplaceTempView(final_list8, "final_list8")
cacheTable("final_list8")

# COMMAND ----------

mdm_tbl_share <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Share_Source_PS as data_source
                , '",vsn,"' as version
                , 'hp_share' as measure
                , Page_Share as units
                , CONCAT(Proxy_PS,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                 
                 "))

mdm_tbl_usage <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'usage' as measure
                , Usage as units
                , CONCAT(IMPV_Route,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage is not null
                 
                 "))

mdm_tbl_usagen <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , 'n' as data_source
                , '",vsn,"' as version
                , 'Usage_n' as measure
                , MPV_n as units
                , CONCAT(IMPV_Route,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE MPV_n is not null AND MPV_n >0
                 
                 "))
                                            
mdm_tbl_sharen <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , 'n' as data_source
                , '",vsn,"' as version
                , 'Share_n' as measure
                , Share_Raw_N_PS as units
                , CONCAT(IMPV_Route,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Share_Raw_N_PS is not null AND Share_Raw_N_PS >0
                 
                 "))

mdm_tbl_kusage <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'k_usage' as measure
                , Usage as units
                , CONCAT(IMPV_Route,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage is not null
                 
                 "))

mdm_tbl_cusage <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'color_usage' as measure
                , Usage_c as units
                , CONCAT(IMPV_Route,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))
                       
mdm_tbl_pages <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'total_pages' as measure
                , total_pages as units
                , CONCAT(IMPV_Route,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))

 mdm_tbl_kpages <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'total_k_pages' as measure
                , total_kpages as units
                , CONCAT(IMPV_Route,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))
                      
 mdm_tbl_cpages <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'total_c_pages' as measure
                , total_cpages as units
                , CONCAT(IMPV_Route,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))
                       
mdm_tbl_hppages <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'hp_pages' as measure
                , hp_pages as units
                , CONCAT(Proxy_PS,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))

mdm_tbl_khppages <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'hp_k_pages' as measure
                , hp_kpages as units
                , CONCAT(Proxy_PS,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))   
                                            
mdm_tbl_chppages <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'hp_c_pages' as measure
                , hp_cpages as units
                , CONCAT(Proxy_PS,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))  

mdm_tbl_knhppages <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'non_hp_k_pages' as measure
                , nonhp_kpages as units
                , CONCAT(Proxy_PS,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 ")) 
                       
mdm_tbl_cnhppages <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'non_hp_c_pages' as measure
                , nonhp_cpages as units
                , CONCAT(Proxy_PS,';',model_group,';',label,';',dm_version) as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 ")) 
                                         
mdm_tbl_ib <- SparkR::sql(paste0("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , customer_engagement
                , '' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'ib' as measure
                , ib as units
                , NULL as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))
                                            
mdm_tbl <- rbind(mdm_tbl_share, mdm_tbl_usage, mdm_tbl_usagen, mdm_tbl_sharen, mdm_tbl_kusage, mdm_tbl_cusage, mdm_tbl_pages, mdm_tbl_hppages, 
                 mdm_tbl_kpages, mdm_tbl_cpages, mdm_tbl_khppages, mdm_tbl_chppages, mdm_tbl_knhppages, mdm_tbl_cnhppages, mdm_tbl_ib)

mdm_tbl$cal_date <- to_date(mdm_tbl$cal_date,format="yyyy-MM-dd")
mdm_tbl$forecast_created_date <- to_date(mdm_tbl$forecast_created_date,format="yyyy-MM-dd")
mdm_tbl$load_date <- to_date(mdm_tbl$load_date,format="yyyy-MM-dd")

createOrReplaceTempView(mdm_tbl, "mdm_tbl")

# COMMAND ----------

# MAGIC %python
# MAGIC import re
# MAGIC from pyspark.sql.functions import lit
# MAGIC 
# MAGIC forecast_process_note ="TONER {} {}.1".format(outnm_dt.upper(), datestamp[0:4])
# MAGIC 
# MAGIC version = call_redshift_addversion_sproc(configs=configs, record=forecast_process_note, source_name="CUPSM")
# MAGIC 
# MAGIC mdm_tbl = spark.sql("SELECT * FROM mdm_tbl") \
# MAGIC     .withColumn("version", lit(version[0])) \
# MAGIC     .withColumn("forecast_process_note", lit(forecast_process_note))
# MAGIC 
# MAGIC write_df_to_s3(df=mdm_tbl, destination=f"{constants['S3_BASE_BUCKET'][stack]}spectrum/cupsm/{version[0]}/{re.sub(' ','_',forecast_process_note.lower())}", format="parquet", mode="overwrite", upper_strings=True)

# COMMAND ----------

# MAGIC %python
# MAGIC # for proxy_locking pass usage_share_current_version
# MAGIC dbutils.jobs.taskValues.set(key = "usage_share_current_version", value = version[0])

# COMMAND ----------

notebook_end_time <- Sys.time()
notebook_total_time <- notebook_end_time - notebook_start_time;notebook_total_time
