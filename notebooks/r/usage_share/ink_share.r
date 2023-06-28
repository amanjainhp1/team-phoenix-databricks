# Databricks notebook source
# ---
# Version 2022.04.05.1
# title: "100% IB Ink Share DE with IE 2.0 IB"
# output:
#   html_notebook: default
#   pdf_document: default
# ---

# COMMAND ----------

notebook_start_time <- Sys.time()

# COMMAND ----------

dbutils.widgets.text("writeout", "")
dbutils.widgets.text("outnm_dt", "")
dbutils.widgets.text("ib_version", "")
dbutils.widgets.text("datestamp", "")
dbutils.widgets.text("timestamp", "")

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
# MAGIC writeout = dbutils.widgets.get("writeout") if dbutils.widgets.get("writeout") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["writeout"]
# MAGIC outnm_dt = dbutils.widgets.get("outnm_dt") if dbutils.widgets.get("outnm_dt") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["outnm_dt"]
# MAGIC ib_version = dbutils.widgets.get("ib_version") if dbutils.widgets.get("ib_version") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["ib_version"]
# MAGIC datestamp = dbutils.widgets.get("datestamp") if dbutils.widgets.get("datestamp") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["datestamp"]
# MAGIC timestamp = dbutils.widgets.get("timestamp") if dbutils.widgets.get("timestamp") != "" else dbutils.jobs.taskValues.get(taskKey = "cupsm_execute", key = "args")["timestamp"]

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # retrieve configs and export to spark.confs for usage across languages
# MAGIC for key, val in configs.items():
# MAGIC     spark.conf.set(key, val)
# MAGIC 
# MAGIC spark.conf.set('datestamp', datestamp)
# MAGIC spark.conf.set('timestamp', timestamp)
# MAGIC 
# MAGIC spark.conf.set('aws_bucket_name', constants['S3_BASE_BUCKET'][stack])

# COMMAND ----------

packages <- c("sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR", "spatstat", "nls2")

# COMMAND ----------

# MAGIC %run ../common/package_check

# COMMAND ----------

options(scipen=999)
sessionInfo()

# COMMAND ----------

# define s3 bucket
aws_bucket_name <- sparkR.conf('aws_bucket_name')

# COMMAND ----------

# MAGIC %python
# MAGIC # load parquet data and register views
# MAGIC 
# MAGIC tables = ['bdtbl', 'hardware_xref', 'ib', 'iso_cc_rollup_xref', 'iso_country_code_xref']
# MAGIC for table in tables:
# MAGIC     spark.read.parquet(f'{constants["S3_BASE_BUCKET"][stack]}/cupsm_inputs/ink/{datestamp}/{timestamp}/{table}/').createOrReplaceTempView(f'{table}')

# COMMAND ----------

table_month = SparkR::collect(SparkR::sql("
--Share and Usage Splits (Trad)
SELECT 
	printer_platform_name
	, printer_region_code
	, country_alpha2
	, hp_country_common_name
	, year
	, quarter
	, date_month_dim_ky
	, printer_managed
	, hp_share
	, black_cc_ib_wtd_avg
	, color_cc_ib_wtd_avg
	, cyan_cc_ib_wtd_avg
	, magenta_cc_ib_wtd_avg
	, yellow_cc_ib_wtd_avg
	, tot_cc_ib_wtd_avg
	, total_pages_ib_wtd_avg AS ps_den
    , total_pages_ib_wtd_avg * hp_share AS ps_num
	, pct_color	 
	, reporting_printers
    , connected_ib
FROM bdtbl
"))

# COMMAND ----------

table_month$rtm <- table_month$printer_managed

#IB Table from MDM  
  ibtable <- SparkR::collect(SparkR::sql("
                   select  a.platform_subset
                          ,YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          ,d.technology AS hw_type
                          ,a.customer_engagement  AS RTM
                          ,b.region_5
                          ,a.country_alpha2
                          ,sum(a.units) as ib
                          ,a.version
                          ,d.hw_product_family as platform_division_code
                    from ib a
                    left join iso_country_code_xref b
                      on (a.country_alpha2=b.country_alpha2)
                    left join hardware_xref d
                       on (a.platform_subset=d.platform_subset)
                    where a.measure='IB'
                      and (upper(d.technology)='INK' or (d.technology='PWA' and (upper(d.hw_product_family) not in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3', 'TIJ_4.XG2 MORNESTA'))
                         ))
                      and product_lifecycle_status not in ('E','M')
                    group by a.platform_subset, a.cal_date, d.technology, a.customer_engagement,a.version
                            , b.region_5, a.country_alpha2,d.hw_product_family
"))

# COMMAND ----------

#Get Market10 Information
country_info <- SparkR::collect(SparkR::sql("
                      WITH mkt10 AS (
                           SELECT country_alpha2, country_level_2 as market10, country_level_4 as emdm
                           FROM iso_cc_rollup_xref
                           WHERE country_scenario='MARKET10'
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

# COMMAND ----------

country_info <- sqldf("SELECT * from country_info where country_alpha2 in (select distinct country_alpha2 from ibtable)")
hw_info <- SparkR::collect(SparkR::sql(
                    "SELECT distinct platform_subset, pl as  product_line_code, epa_family as platform_division_code, sf_mf as platform_function_code
                    , SUBSTRING(mono_color,1,1) as cm, UPPER(brand) as product_brand
                    , mono_ppm as print_mono_speed_pages, color_ppm as print_color_speed_pages
                    , intro_price, intro_date, vc_category as market_group
                    , predecessor
                    , hw_product_family as supplies_family
                    from hardware_xref
                    "
                    ))

# COMMAND ----------

#Get Intro Dates
ibintrodt <- SparkR::collect(SparkR::sql("
            SELECT  a.platform_subset, a.customer_engagement
                    ,min(cal_date) AS intro_date
                    FROM ib a
                    LEFT JOIN hardware_xref d
                      ON (a.platform_subset=d.platform_subset)
                    WHERE a.measure='IB'
                      AND d.technology in ('INK','PWA')
                    GROUP BY a.platform_subset, customer_engagement
                   "))
ibintrodt$intro_yyyymm <- paste0(substr(ibintrodt$intro_date,1,4),substr(ibintrodt$intro_date,6,7))

# COMMAND ----------

table_month <- sqldf("
                     with sub1 as (
              select a.printer_platform_name
              , a.printer_region_code
              , a.country_alpha2
              , ci.market10
              , ci.region_5
              , ci.developed_emerging
              , a.rtm
              , hw.platform_function_code
              , hw.cm
              , a.year
              , a.quarter
              , a.date_month_dim_ky as fyearmo
              , a.printer_managed
              , hw.platform_division_code
              , hw.product_brand
              , hw.intro_price as product_intro_price
              , id.intro_yyyymm as product_introduction_date
              , hw.print_mono_speed_pages
              , hw.print_color_speed_pages
              , hw.product_line_code
              , a.tot_cc_ib_wtd_avg as total_cc                                
              , a.tot_cc_ib_wtd_avg as usage      --Consumed Ink
              , a.color_cc_ib_wtd_avg as c_usage  --Consumed color Ink
              , a.hp_share  --Ink Share
              , a.ps_num
              , a.ps_den
              , a.reporting_printers as sumn
              , a.pct_color
             from table_month a
             LEFT JOIN hw_info hw
              on a.printer_platform_name=hw.platform_subset
             LEFT JOIN country_info ci 
              ON a.country_alpha2=ci.country_alpha2 
             LEFT JOIN ibintrodt id
              ON a.printer_platform_name=id.platform_subset and a.rtm=id.customer_engagement
            )
            select * from sub1")


#product_info2$product_previous_model <- ifelse(product_info2$product_previous_model=="NONE",NA,product_info2$product_previous_model)


product_sum_ib <- sqldf(paste("
                              select platform_subset as platform_subset_name, rtm as rtm, region_5 as region_code, SUM(ib) as tot_ib
                              from ibtable
                              group by platform_subset_name, rtm, region_5
                              "))

product_ib2 <- sqldf(paste("
                              select platform_subset as platform_subset_name, rtm as rtm, region_5 as region_code, month_begin
                              ,SUM(ib) as tot_ib
                              FROM ibtable
                              group by platform_subset_name, rtm, region_5 ,month_begin
                              "))

 table_quarter <- sqldf("
                        SELECT printer_platform_name
                        , country_alpha2
                        , platform_function_code
                        , cm 
                        , market10
                        , region_5
                        , developed_emerging
                        , year
                        , quarter 
                        , quarter as  fiscal_year_quarter
                        , rtm
                        , platform_division_code
                        , product_brand
                        , min(product_intro_price) as product_intro_price
                        , product_introduction_date
                        , print_mono_speed_pages
                        , print_color_speed_pages
                        , product_line_code
                        , avg(pct_color) as pct_color
                        , sum(sumn) as sumn
                        , avg(usage) as usage
                        , avg(hp_share) as hp_share_avg        --straight average hp_share
                        , sum(ps_num)/sum(ps_den) AS hp_share  --weighted average hp_share
                        FROM table_month
                        group by
                        printer_platform_name, country_alpha2, platform_function_code, cm
                        , market10, region_5, developed_emerging
                        , year, quarter 
                        , rtm
                        , platform_division_code, product_brand
                        , product_introduction_date 
                        , print_mono_speed_pages, print_color_speed_pages
                        , product_line_code
                       ")
#close(ch)

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

# Pred_Raw_file_out <- sqldf("select a.*,b.product_previous_model
#                            from Pred_Raw_file a
#                            left join product_info2 b  on a.Platform_Subset=b.platform_subset_name")
# s3write_using(FUN = write.csv, x=Pred_Raw_file_out,object = paste0("s3://insights-environment-sandbox/BrentT/Predecessor_List_5Nov19.csv"), na="")

Pred_Raw_in <- sqldf("select distinct Platform_Subset as platform_subset_name
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

Pred_Raw <- sqldf("select trim(platform_subset_name) as platform_subset_name,trim(Predecessor) as Predecessor,trim(Predecessor_src) as Predecessor_src, count(*) as count
                  from Pred_Raw_in
                  where Predecessor !='0' and Predecessor is not null and Predecessor !='N/A' and Predecessor !='NONE'
                  group by trim(platform_subset_name),trim(Predecessor),trim(Predecessor_src)")

printer_list <- sqldf("
                      select distinct d.platform_subset as printer_platform_name
                            , d.product_brand
                            , d.platform_division_code
                            , b.intro_date as printer_intro_month
                            , case when d.predecessor is null then c.Predecessor
                              else d.predecessor
                              end as Predecessor
                            , b.customer_engagement as rtm
                            from hw_info d 
                            left join Pred_Raw c on d.platform_subset=c.platform_subset_name
                            left join ibintrodt b on b.platform_subset=d.platform_subset
                            --left join product_info2alt e on e.platform_subset_name=d.printer_platform_name
                      ")

product_ib2 <- sqldf("
              SELECT platform_subset_name
              , region_code 
              , rtm
              , fyearmo
              , CASE WHEN SUBSTR(fyearmo,5,2) IN ('01','02','03') THEN SUBSTR(fyearmo,1,4)||'Q1'
                      WHEN SUBSTR(fyearmo,5,2) IN ('04','05','06') THEN SUBSTR(fyearmo,1,4)||'Q2'
                      WHEN SUBSTR(fyearmo,5,2) IN ('07','08','09') THEN SUBSTR(fyearmo,1,4)||'Q3'
                      WHEN SUBSTR(fyearmo,5,2) IN ('10','11','12') THEN SUBSTR(fyearmo,1,4)||'Q4'
                      ELSE NULL END AS fiscal_year_quarter
              , sum(tot_ib) AS tot_ib
               FROM
               (select *
                  ,CASE WHEN SUBSTR(month_begin,5,6) IN ('11','12') THEN (month_begin + 100 -10)
                    WHEN SUBSTR(month_begin,5,6) BETWEEN '01' AND'10' THEN (month_begin + 2)
                    ELSE month_begin 
                    END as fyearmo
                  From product_ib2)
                where month_begin IS NOT NULL
               group by platform_subset_name, region_code, rtm, fyearmo, fiscal_year_quarter
               ORDER BY platform_subset_name, region_code, rtm, fyearmo, fiscal_year_quarter
  
")
product_ib2q <- sqldf("
              SELECT platform_subset_name
              , region_code 
              , rtm
              , fiscal_year_quarter
              , sum(tot_ib) AS tot_ib
               FROM product_ib2
               group by platform_subset_name, region_code, rtm, fiscal_year_quarter
               ORDER BY platform_subset_name, region_code, rtm, fiscal_year_quarter
  
")

# COMMAND ----------

###Page Share

#page_share_1 <- subset(table_quarter,share_country_incl_flag==1)
page_share_1 <- subset(table_quarter,sumn>20)
#page_share_2 <- subset(page_share_1,!is.na(iso_country_code) & !is.na(hp_share_ps_kcmy_times_ib))
page_share_2 <- subset(page_share_1,!is.na(platform_division_code) & !is.na(product_brand) & hp_share>0.05)

page_share_2$rtm=toupper(page_share_2$rtm)
#check1 <- subset(page_share_2,platform_name=="MOON AIO" & fiscal_year_quarter=="2017Q3" & iso_country_code=="BG")
#check2 <- subset(page_share,page_share$`Platform Name`=="MOON AIO" & page_share$`Report Period`=="2017Q3" & page_share$Country=="BULGARIA")

page_share_reg <- sqldf("
                       SELECT printer_platform_name as platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5 as region_code 
                       --, de                 
                        , avg(hp_share) as hp_share
                        , SUM(sumn) as printer_count_month_ps
                      from  page_share_2
                      group by printer_platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5
                       --, de                 
                      order by platform_name,fiscal_year_quarter
                       , product_brand, rtm
                       ,region_code 
                       
                       ")
page_share_m10 <- sqldf("
                       SELECT printer_platform_name as platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5 as region_code 
                       , market10
                       --, de                 
                        , avg(hp_share) as hp_share
                        , SUM(sumn) as printer_count_month_ps
                      from  page_share_2
                      group by printer_platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5
                       , market10
                      order by platform_name,fiscal_year_quarter
                       , product_brand, rtm
                       ,region_code 

                       ")
page_share_mde <- sqldf("
                       SELECT printer_platform_name as platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5 as region_code 
                       , market10
                       , developed_emerging                 
                        , avg(hp_share) as hp_share
                        , SUM(sumn) as printer_count_month_ps
                      from  page_share_2
                      group by printer_platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5
                       , market10
                       , developed_emerging
                      order by platform_name,fiscal_year_quarter
                       , product_brand, rtm
                       ,region_code 
                       ")
page_share_ctr <- sqldf("
                       SELECT printer_platform_name as platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5 as region_code 
                       , market10
                       , developed_emerging  
                       , country_alpha2
                        , avg(hp_share) as hp_share
                        , SUM(sumn) as printer_count_month_ps
                      from  page_share_2
                      group by printer_platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5
                       , market10
                       , developed_emerging  
                       , country_alpha2
                      order by platform_name,fiscal_year_quarter
                       , product_brand, rtm
                       ,region_code 
                       ")

combcolr <- c('platform_name','product_brand','region_code','rtm')
combcolm <- c('platform_name','product_brand','region_code','market10','rtm')
combcold <- c('platform_name','product_brand','region_code','market10','developed_emerging','rtm')
combcolc <- c('platform_name','product_brand','region_code','market10','developed_emerging','country_alpha2','rtm')

page_share_reg$grp <- apply(page_share_reg[,combcolr],1, paste,collapse ="_")
page_share_reg$value<- page_share_reg$hp_share
page_share_m10$grp <- apply(page_share_m10[,combcolm],1, paste,collapse ="_")
page_share_m10$value<- page_share_m10$hp_share
page_share_mde$grp <- apply(page_share_mde[,combcold],1, paste,collapse ="_")
page_share_mde$value<- page_share_mde$hp_share
page_share_ctr$grp <- apply(page_share_ctr[,combcolc],1, paste,collapse ="_")
page_share_ctr$value<- page_share_ctr$hp_share

page_share_reg <- sqldf("
                        SELECT a.*, b.intro_date as printer_intro_month
                        from page_share_reg a
                        left join ibintrodt b
                        on a.platform_name=b.platform_subset and a.rtm=b.customer_engagement")
page_share_m10 <- sqldf("
                        SELECT a.*, b.intro_date as printer_intro_month
                        from page_share_m10 a
                        left join ibintrodt b
                        on a.platform_name=b.platform_subset and a.rtm=b.customer_engagement")
page_share_mde <- sqldf("
                        SELECT a.*, b.intro_date as printer_intro_month
                        from page_share_mde a
                        left join ibintrodt b
                        on a.platform_name=b.platform_subset and a.rtm=b.customer_engagement")
page_share_ctr <- sqldf("
                        SELECT a.*, b.intro_date as printer_intro_month
                        from page_share_ctr a
                        left join ibintrodt b
                        on a.platform_name=b.platform_subset and a.rtm=b.customer_engagement")

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

page_share_reg$fiscal_year_quarter<-as.character(page_share_reg$fiscal_year_quarter)
page_share_m10$fiscal_year_quarter<-as.character(page_share_m10$fiscal_year_quarter)
page_share_mde$fiscal_year_quarter<-as.character(page_share_mde$fiscal_year_quarter)
page_share_ctr$fiscal_year_quarter<-as.character(page_share_ctr$fiscal_year_quarter)
page_share_reg$fiscal_year_quarter<-as.character(page_share_reg$fiscal_year_quarter)
str(page_share_reg)

#Remove A3
#page_share_reg <- subset(page_share_reg,product_type=="A4")

# COMMAND ----------

##region 5

data_coefr <- list()

for (printer in unique(page_share_reg$grp)){
  
  dat1 <- subset(page_share_reg,grp==printer)
  if (nrow(dat1) <4 ) {next}
  
    #mindt <- min(dat1$fiscal_year_quarter)
    mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
    #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
    dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
    dat1<- subset(dat1,timediff>0)
    #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
    mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data

    #values for sigmoid curve start
    midtime <- median(dat1$timediff)
    maxtimediff <- max(90-max(dat1$timediff),0) #number of months between end of data and 7.5 years (90 months)
    #maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
    maxtimediff2 <- max(dat1$timediff)

    dat1$value <- as.numeric(dat1$value)
    maxv=min(max(dat1$value)+max(dat1$value)*(0.002*mintimediff),1.5)
    minv=max(min(dat1$value)*0.85,0.05)
    spreadv <- min(1-(min(maxv,1) + minv)/2,(max(maxv,1)+minv)/2)
    
    frstshr <- dat1[1,10]


    #Sigmoid Model 
    sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))

    fitmodel <- nls2(formula=sigmoid, 
                      data=dat1,
                      start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                      lower=c(min=0.05,max=0.5,med=0, spread=0.05),
                      upper=c(min=1,max=2,med=1000, spread=1),
                      algorithm = "grid-search", 
                      #weights=supply_count_share,
                      weights = timediff,
                      control=list(maxiter=10000 ,tol=1e-05 ,minFactor=1/1024 , warnOnly=TRUE), all=FALSE)

    #linear model
    dat1b <- sqldf("select * from dat1 order by timediff desc limit 8")
    fitmodel2 <- lm(value ~ timediff, data=dat1b)

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

    data_coefr[[printer]] <- coeffout

}


#unlink(paste0(normalizePath(tempdir()),"/",dir(tempdir())),recursive=T) #clean S3 copied files from tmp folder

# COMMAND ----------

##mkt 10

data_coefm <- list()

for (printer in unique(page_share_m10$grp)){
  
  dat1 <- subset(page_share_m10,grp==printer)
  if (nrow(dat1) <4 ) {next}
  
  #mindt <- min(dat1$fiscal_year_quarter)
  mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
  dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  dat1<- subset(dat1,timediff>0)
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
  mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
  
  #values for sigmoid curve start
  midtime <- median(dat1$timediff)
  maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
  
  dat1$value <- as.numeric(dat1$value)
  maxv=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff),1.5)
  minv=max(min(dat1$value)*0.85,0.05)
  spreadv <- min(1-(min(maxv,1) + minv)/2,(max(maxv,1)+minv)/2)
  frstshr <- dat1[1,10]
  
  sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
 
  fitmodel <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                    lower=c(min=0.05,max=0.5,med=0, spread=0.05),
                    upper=c(min=1,max=2,med=1000, spread=1),
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
  
  dat1 <- subset(page_share_mde,grp==printer)
  if (nrow(dat1) <4 ) {next}
  
  #mindt <- min(dat1$fiscal_year_quarter)
  mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
  dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  dat1<- subset(dat1,timediff>0)
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
  mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
  
  #values for sigmoid curve start
  midtime <- median(dat1$timediff)
  maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
  
  dat1$value <- as.numeric(dat1$value)
  maxv=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff),1.5)
  minv=max(min(dat1$value)*0.85,0.05)
  spreadv <- min(1-(min(maxv,1) + minv)/2,(max(maxv,1)+minv)/2)
  frstshr <- dat1[1,10]
  
  sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
 
  fitmodel <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                    lower=c(min=0.05,max=0.5,med=0, spread=0.05),
                    upper=c(min=1,max=2,med=1000, spread=1),
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
  
  dat1 <- subset(page_share_ctr,grp==printer)
  if (nrow(dat1) <4 ) {next}
  
  #mindt <- min(dat1$fiscal_year_quarter)
  mindt <- as.Date(min(dat1$printer_intro_month),format="%Y-%m-%d")
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt)))
  dat1$timediff <- round(as.numeric(difftime(as.Date(as.yearqtr(dat1$fiscal_year_quarter,format="%YQ%q")),as.Date(dat1$printer_intro_month), units ="days")/(365.25/12)),0)
  dat1<- subset(dat1,timediff>0)
  #dat1$timediff <- ((as.yearqtr(dat1$fiscal_year_quarter)-as.yearqtr(mindt))*4)
  mintimediff <- min(dat1$timediff) #number of quarters between printer introduction and start of data
  
  #values for sigmoid curve start
  midtime <- median(dat1$timediff)
  maxtimediff <- max(2*midtime-max(dat1$timediff),0) #number of quarters between end of data and 30 quarters
  
  dat1$value <- as.numeric(dat1$value)
  maxv=min(max(dat1$value)+max(dat1$value)*(0.02*mintimediff),1.5)
  minv=max(min(dat1$value)*0.85,0.05)
  spreadv <- min(1-(min(maxv,1) + minv)/2,(max(maxv,1)+minv)/2)
  frstshr <- dat1[1,10]
  
  sigmoid <- value~max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))
 
  fitmodel <- nls2(formula=sigmoid, 
                    data=dat1,
                    start=list(min=minv, max=maxv, med=midtime, spread=spreadv),
                    lower=c(min=0.05,max=0.5,med=0, spread=0.05),
                    upper=c(min=0.85,max=2,med=1000, spread=1),
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

cntry <- unique(country_info$country_alpha2)
#devem <- c("D", "E")
printer_list_ctr <- printer_list[rep(seq_len(nrow(printer_list)),each=length(cntry)),]
printer_list_ctr <- cbind(printer_list_ctr,cntry)
printer_list_ctr <- sqldf("select a.*,CASE WHEN c.region_5 is NULL THEN 'null' ELSE c.region_5 END as region_5, c.market10, c.developed_emerging
                          from printer_list_ctr a
                          left join country_info c
                          on a.cntry=c.country_alpha2")
#printer_list_reg <- printer_list_reg[rep(seq_len(nrow(printer_list_reg)),each=2),]
#printer_list_reg <- cbind(printer_list_reg, devem)

printer_list_ctr$grpr <-paste0(printer_list_ctr$printer_platform_name,"_",printer_list_ctr$product_brand,"_",printer_list_ctr$region_5,"_",printer_list_ctr$rtm)
printer_list_ctr$grpm <-paste0(printer_list_ctr$printer_platform_name,"_",printer_list_ctr$product_brand,"_",printer_list_ctr$region_5,"_",printer_list_ctr$market10,"_",printer_list_ctr$rtm)
printer_list_ctr$grpd <-paste0(printer_list_ctr$printer_platform_name,"_",printer_list_ctr$product_brand,"_",printer_list_ctr$region_5,"_",printer_list_ctr$market10,"_",printer_list_ctr$developed_emerging,"_",printer_list_ctr$rtm)
printer_list_ctr$grpc <-paste0(printer_list_ctr$printer_platform_name,"_",printer_list_ctr$product_brand,"_",printer_list_ctr$region_5,"_",printer_list_ctr$market10,"_",printer_list_ctr$developed_emerging,"_",printer_list_ctr$cntry,"_",printer_list_ctr$rtm)
rownames(printer_list_ctr) <- NULL

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
combined1 <- sqldf("select a.printer_platform_name as platform_subset_name
                            , a.rtm
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

                            , a.product_brand
                            , a.platform_division_code
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

predlist <- combined1b[c(1,4,6,2,3,17)]  #printer_platform_name, regions, country_alpha2, rtm, Predecessor, curve_exists#
#predlist <- combined1b[c(1,3,2,10)]  #printer_platform_name, regions, Predecessor, curve_exists#
proxylist1 <- subset(predlist,curve_exists=="Y")
proxylist1$proxy <- proxylist1$printer_platform_name
proxylist1$gen   <- 0


####Same printer group####
predlist_use <- anti_join(predlist,proxylist1,by=c("printer_platform_name","regions","rtm"))
predlist_use$pred_iter <- predlist_use$Predecessor


predlist_iter3 <- sqldf("
                  select distinct
                  a.printer_platform_name
                  , a.regions
                  , a.country_alpha2
                  , a.rtm
                  , a.Predecessor
                  , CASE
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.printer_platform_name as pred_iter
                  from
                      (select a1.*, a2.platform_division_code as tech_split, CASE WHEN charindex(' ',a1.printer_platform_name) =0 THEN a1.printer_platform_name
                            ELSE SUBSTR(a1.printer_platform_name,0,charindex(' ',a1.printer_platform_name)) END as shrtnm
                        from predlist_use a1
                        left join hw_info a2
                        on (a1.printer_platform_name=a2.platform_subset)) a
                  left join
                     (select b1.*, b2.platform_division_code as tech_split, CASE WHEN charindex(' ',b1.printer_platform_name) =0 THEN b1.printer_platform_name
                            ELSE SUBSTR(b1.printer_platform_name,0,charindex(' ',b1.printer_platform_name)) END as shrtnm
                      from predlist b1
                      left join hw_info b2
                      on (b1.printer_platform_name=b2.platform_subset)) b
                    on (a.printer_platform_name != b.printer_platform_name and a.shrtnm=b.shrtnm and a.country_alpha2=b.country_alpha2 and a.rtm=b.rtm)
                    where a.tech_split is not null
                  ")
predlist_iter <- predlist_iter3
proxylist_iter <- subset(predlist_iter,curve_exists=="Y")

if(dim(proxylist_iter)[1] != 0){

proxylist_iter$proxy <- proxylist_iter$pred_iter
proxylist_iter <- proxylist_iter[c(1,2,3,4,5,6,8)]
proxylist_iter$gen   <- 0.5
proxylist1 <-rbind(proxylist1,proxylist_iter)
}
predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","country_alpha2","rtm"))

predlist_use$pred_iter <- predlist_use$Predecessor
predlist_use <- anti_join(predlist,proxylist1,by=c("printer_platform_name","country_alpha2","rtm"))
predlist_use$pred_iter <- predlist_use$Predecessor
####Predecessors####
y <- 0
repeat{
predlist_iter2 <- sqldf("
                  select a.printer_platform_name
                  , a.regions
                  , a.country_alpha2
                  , a.rtm
                  , a.Predecessor
                  , CASE 
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.Predecessor as pred_iter
                  from predlist_use a
                  left join predlist b
                    on (a.pred_iter=b.printer_platform_name and a.country_alpha2=b.country_alpha2 and a.rtm=b.rtm)
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
predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","country_alpha2","rtm"))

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
                  , a.rtm
                  , a.Predecessor
                  , CASE 
                      WHEN b.curve_exists='Y' THEN 'Y'
                    ELSE 'N'
                    END AS curve_exists
                  , b.printer_platform_name as pred_iter
                  from predlist_use a
                  left join predlist b
                    on (a.pred_iter=b.Predecessor and a.country_alpha2=b.country_alpha2 and a.rtm=b.rtm)
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

predlist_use <- anti_join(predlist_iter,proxylist_iter,by=c("printer_platform_name","country_alpha2","rtm"))

if(all(is.na(predlist_use$pred_iter))){break}
if(y>15){break}
}
#successor list empty

proxylist_temp <- proxylist1

leftlist <- anti_join(predlist,proxylist_temp,by=c("printer_platform_name","country_alpha2","rtm"))

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
                  ifelse((leftlist$printer_platform_name=="DENALI MANAGED" & (leftlist$emdm=="E")),"DENALI MFP",
                  ifelse(leftlist$printer_platform_name=="HAWAII MFP","EVEREST MFP",
                  ifelse(leftlist$printer_platform_name=="HUMMINGBIRD PLUS" & (leftlist$regions=="NA" |(leftlist$regions=="AP" & leftlist$emdm=="E")),"JAYBIRD",
                  ifelse(leftlist$printer_platform_name=="HUMMINGBIRD PLUS","BUCK",
                  ifelse(leftlist$printer_platform_name=="KONA","EVEREST MFP",
                  ifelse(leftlist$printer_platform_name=="OUTLAW","STARS 3:1",
                  ifelse(leftlist$printer_platform_name=="RAINIER MFP","DENALI MFP",
                  ifelse((leftlist$printer_platform_name=="SAPPHIRE MANAGED" & leftlist$regions=="EU"),"SAPPHIRE MFP",    
                  ifelse((leftlist$printer_platform_name=="SERRANO LITE" & leftlist$regions=="NA"),"FIJIMFP",
                         NA
                         )))))))))))))))))

proxylist_left <- subset(leftlist, !is.na(proxy))
if(nrow(proxylist_left)>0){proxylist_temp <- bind_rows(proxylist_temp,proxylist_left)}

#manual change of proxy

# COMMAND ----------

proxylist <- proxylist_temp[c(1,2,3,4,7)]

output2<- sqldf("select a.printer_platform_name
                        , a.regions
                        , a.country_alpha2
                        , a.rtm
                        , a.Predecessor
                        , c.proxy
                        , b.min
                        , b.max
                        , b.med
                        , b.spread
                        , b.a
                        , b.b
                        , a.platform_division_code
                        , a.product_brand
                        , a.printer_intro_month
                        from combined1 a
                        left join proxylist c
                        on (a.printer_platform_name=c.printer_platform_name and a.country_alpha2=c.country_alpha2 and a.rtm=c.rtm)
                        left join combined1 b
                        on (c.proxy=b.printer_platform_name and c.country_alpha2=b.country_alpha2 and c.rtm=b.rtm)
              ")

head(output2)

# COMMAND ----------

# need to compare duplicates and get values for missing

missing <- subset(output2,is.na(min))

plotlook <- sqldf("select a.*, b.printer_intro_month as PIM
                  from output2 a
                  left join output2 b
                  on (a.proxy=b.printer_platform_name and a.country_alpha2=b.country_alpha2 and a.rtm=b.rtm)
                  ")

  plotlook$min <- as.numeric(plotlook$min)
  plotlook$max <- as.numeric(plotlook$max)
  plotlook$med <- as.numeric(plotlook$med)
  plotlook$spread <- as.numeric(plotlook$spread)
  plotlook$a <- as.numeric(plotlook$a)
  plotlook$b <- as.numeric(plotlook$b)

  plotlook$dtdiff <- abs(as.Date(plotlook$printer_intro_month)-as.Date(plotlook$PIM))
  
plotlook_srt <- plotlook[order(plotlook$printer_platform_name,plotlook$country_alpha2,plotlook$rtm,plotlook$dtdiff),]
plotlook_dual<- plotlook_srt[which(duplicated(plotlook_srt[,c('printer_platform_name','country_alpha2','rtm')])==T),]
plotlook_out <- plotlook_srt[which(duplicated(plotlook_srt[,c('printer_platform_name','country_alpha2','rtm')])==F),]

proxylist_out <- plotlook_out

plotlook_out$grp <- paste0(plotlook_out$printer_platform_name,"_",plotlook_out$country_alpha2,"_",plotlook_out$rtm)

#sigmoid_pred <- function(min,max,med,spread,timediff){max-(exp((timediff-med)*spread)/(exp((timediff-med)*spread)+1)*(1-(min+(1-max))))}

# for (printer in unique(plotlook_out$grp)){
#   dat1 <- subset(plotlook_out,grp==printer)
#   #dat1 <- subset(plotlook_out,grp=="ALPINE PQ_AP")
#     if (is.na(dat1$proxy)) {next}
#   proxy <- dat1$proxy
#   platform <- dat1$printer_platform_name
#   region <- dat1$printer_region_code
#   rtm <- dat1$rtm
#   timediff <- c(0:59)
#   preddata <- as.data.frame(timediff)
#   dat1 %>% uncount(60)
#   preddata <- cbind(preddata,dat1, row.names=NULL) 
#   preddata$value <- sigmoid_pred(preddata$min,preddata$max,preddata$med,preddata$spread,preddata$timediff)
#   
  #plot(y=preddata$value,x=preddata$timediff, type="l",col="blue",main=paste("Share for ",platform,",",region,",",emdm,"\nUsing proxy: ",proxy),
  #      xlab="Date", ylab="HP Share", ylim=c(0,1))

#}

# COMMAND ----------

# fill in missing

modellist <- subset(proxylist_out,!is.na(min))
modellist$TIJ <- substr(modellist$platform_division_code,1,5)
aggr_models <- do.call(data.frame,aggregate(cbind(min, max, med, spread,a,b) ~ country_alpha2 + TIJ + product_brand + rtm, data=modellist, FUN=function(x) c(mn = mean(x), md = median(x), mx = max(x))))
aggr_models$grp <- paste0(aggr_models$TIJ,"_",aggr_models$product_brand,"_",aggr_models$country_alpha2,"_",aggr_models$rtm)

aggr_models2 <- do.call(data.frame,aggregate(cbind(min, max, med, spread,a,b) ~ country_alpha2 + TIJ + rtm, data=modellist, FUN=function(x) c(mn = mean(x), md = median(x), mx = max(x))))
aggr_models2$grp <- paste0(aggr_models2$TIJ,"_",aggr_models2$country_alpha2,"_",aggr_models2$rtm)

aggr_models3 <- do.call(data.frame,aggregate(cbind(min, max, med, spread,a,b) ~ country_alpha2 + TIJ, data=modellist, FUN=function(x) c(mn = mean(x), md = median(x), mx = max(x))))
aggr_models3$grp <- paste0(aggr_models3$TIJ,"_",aggr_models3$country_alpha2)

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

# pldcd <- as.data.frame(unique(table_quarter$platform_division_code))
# prbrnd <- as.data.frame(unique(table_quarter$product_brand))
# bmlst <- as.data.frame(pldcd[rep(seq_len(nrow(pldcd)),each=nrow(prbrnd)),] )
# colnames(bmlst)<-"platform_division_code"
# bmlst <- as.data.frame(cbind(bmlst,prbrnd))
# bmlst <- as.data.frame(bmlst[rep(seq_len(nrow(bmlst)),each=5),] )
# agg_lst <- as.data.frame(cbind(bmlst,regions))
# colnames(agg_lst)[2] <- "product_brand"
# colnames(agg_lst)[3] <- "printer_region_code"
# aggr_models_f <- merge(agg_lst,aggr_models,by=c("platform_division_code","printer_region_code","product_brand"),all.x=TRUE)

# COMMAND ----------

#fill in with TIJ group?

proxylist_out$TIJ <- substr(proxylist_out$platform_division_code,1,5)

proxylist_b <- sqldf("
                         SELECT a.printer_platform_name, a.regions, a.country_alpha2, a.product_brand, a.rtm, a.Predecessor 
                           ,CASE 
                            WHEN a.min IS NULL THEN b.TIJ||'_'||b.product_brand||'_'||b.rtm
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
                          , a.platform_division_code, a.TIJ
                        FROM proxylist_out a
                        LEFT JOIN  aggr_models b
                          ON (a.TIJ=b.TIJ AND a.country_alpha2=b.country_alpha2 AND a.product_brand=b.product_brand AND a.rtm=b.rtm)
                         ")
proxylist_b$proxy <- ifelse(is.na(proxylist_b$proxy),"TIJ-Brand-rtm",proxylist_b$proxy)

remaining <- subset(proxylist_b,is.na(min))

proxylist_c <- sqldf("
                         SELECT a.printer_platform_name, a.regions, a.country_alpha2, a.product_brand, a.rtm, a.Predecessor 
                           ,CASE 
                            WHEN a.min IS NULL THEN b.TIJ||b.rtm
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
                          , a.platform_division_code
                        FROM proxylist_b a
                        LEFT JOIN  aggr_models2 b
                          ON (a.TIJ=b.TIJ AND a.country_alpha2=b.country_alpha2 AND a.rtm=b.rtm)
                         ")
proxylist_c$proxy <- ifelse(is.na(proxylist_c$proxy),"TIJ-rtm",proxylist_c$proxy)

remaining2 <- subset(proxylist_c,is.na(min))

proxylist_d <- sqldf("
                         SELECT a.printer_platform_name, a.regions, a.country_alpha2, a.product_brand, a.rtm, a.Predecessor 
                           ,CASE 
                            WHEN a.min IS NULL THEN b.TIJ
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
                          , a.platform_division_code
                        FROM proxylist_b a
                        LEFT JOIN  aggr_models2 b
                          ON (a.TIJ=b.TIJ AND a.country_alpha2=b.country_alpha2)
                         ")
proxylist_d$proxy <- ifelse(is.na(proxylist_d$proxy),"TIJ",proxylist_d$proxy)

remaining3 <- subset(proxylist_d,is.na(min))

# COMMAND ----------

###Need to remove files to clear up memory
ibversion <- unique(ibtable$version)
gc()

# COMMAND ----------

###Usage

proxylist_final2 <- sqldf("
                          SELECT a.platform_division_code
                              , d.supplies_family as selectability
                              , a.printer_platform_name
                              , a.regions as printer_region_code
                              , a.country_alpha2
                              , a.Predecessor
                              , a.proxy as ps_proxy
                              , a.min as ps_min
                              , a.max as ps_max
                              , a.med as ps_med
                              , a.spread as ps_spread
                              , a.a as ps_a
                              , a.b as ps_b
                              , a.product_brand
                              , a.rtm
                              , b.intro_yyyymm as printer_intro_month
                            FROM proxylist_c a
                            LEFT JOIN ibintrodt b
                              ON a.printer_platform_name = b.platform_subset and a.rtm=b.customer_engagement
                            LEFT JOIN hw_info d
                              ON a.printer_platform_name = d.platform_subset

                          ")
#proxylist_final2$Supplies_Product_Family <- ifelse(is.na(proxylist_final2$Supplies_Product_Family),proxylist_final2$printer_platform_name,proxylist_final2$Supplies_Product_Family)

UPM <- SparkR::read.parquet(path=paste(aws_bucket_name, "cupsm_outputs", "ink", sparkR.conf("datestamp"), sparkR.conf("timestamp"), "usage_total", sep="/"))
UPMC <- SparkR::read.parquet(path=paste(aws_bucket_name, "cupsm_outputs", "ink", sparkR.conf("datestamp"), sparkR.conf("timestamp"), "usage_color", sep="/"))

createOrReplaceTempView(UPM, "UPM")
createOrReplaceTempView(UPMC, "UPMC")

UPM2 <- SparkR::sql('
              SELECT distinct
                a.Platform_Subset_Nm as printer_platform_name
              , a.Region_3
              , a.Region
              , a.Region_DE
              , a.Country_Cd
              , NULL as Country_Nm
              , a.rtm
              , a.FYearMo
              --, CEIL(avg(a.MoSI)/3) as QSI
              , sum(a.MPV_UPM) as MPV_UPMIB
              , sum(a.MPV_TS) as MPV_TSIB
              , sum(a.MPV_TD) as MPV_TDIB
              , sum(a.MPV_Raw) as MPV_RawIB
              , sum(a.MPV_N) as MPV_n
              --, sum(a.IB) as IB
              , sum(a.MPV_Init) as Init_MPV
              --, avg(a.MUT) as MUT
              , avg(a.Trend) as Trend
              , avg(a.Seasonality) as Seasonality
              --, avg(a.Cyclical) as Cyclical
              , avg(a.Decay) as Decay
              , CASE 
                  WHEN SUBSTR(a.FYearMo,5,6) IN ("11","12") THEN CONCAT((CAST(SUBSTR(a.FYearMo,1,4) AS INT) + 1),"Q1")
                  WHEN SUBSTR(a.FYearMo,5,6) IN ("01") THEN CONCAT(SUBSTR(a.FYearMo,1,4),"Q1")
                  WHEN SUBSTR(a.FYearMo,5,6) IN ("02","03","04") THEN CONCAT(SUBSTR(a.FYearMo,1,4),"Q2")
                  WHEN SUBSTR(a.FYearMo,5,6) IN ("05","06","07") THEN CONCAT(SUBSTR(a.FYearMo,1,4),"Q3")
                  WHEN SUBSTR(a.FYearMo,5,6) IN ("08","09","10") THEN CONCAT(SUBSTR(a.FYearMo,1,4),"Q4")
                END AS FYQtr
              FROM UPM a 
              GROUP BY 
                a.Platform_Subset_Nm
              , a.Region_3
              , a.Region
              , a.Region_DE
              , a.Country_Cd
              , a.Country_Nm
              , a.FYearMo
              , a.rtm
              ')

UPM2c <- SparkR::sql('
              SELECT distinct
                a.Platform_Subset_Nm as printer_platform_name
              , a.Region_3
              , a.Region
              , a.Region_DE
              , a.Country_Cd
              , NULL as Country_Nm
              , a.rtm
              , a.FYearMo
              , sum(a.MPV_UPM) as MPV_UPM
              , sum(a.MPV_TS) as MPV_TSIB
              , sum(a.MPV_TD) as MPV_TDIB
              , sum(a.MPV_Raw) as MPV_Raw
              , sum(a.MPV_N) as MPV_n
              --, sum(a.IB) as IB
              , sum(a.MPV_Init) as Init_MPV
              --, avg(b.MUT) as MUT
              , avg(a.Trend) as Trend
              , avg(a.Seasonality) as Seasonality
              --, avg(a.Cyclical) as Cyclical
              , avg(a.Decay) as Decay
              FROM UPMc a 
              GROUP BY 
                a.Platform_Subset_Nm
              , a.Region_3
              , a.Region
              , a.Region_DE
              , a.Country_Cd
              , a.Country_Nm
              , a.FYearMo
              , a.rtm
              ')

UPM3 <- SparkR::sql('
              SELECT 
                Platform_Subset_Nm as printer_platform_name
              , Region_3
              , Region
              , Region_DE
              , market10
              , Country_Cd 
              , rtm
              , platform_division_code
              , product_brand
              , IMPV_Route
              , MIN(FYearMo) AS FIntroDt
              FROM UPM
              GROUP BY 
              Platform_Subset_Nm
              , Region_3
              , Region
              , Region_DE
              , market10
              , Country_Cd 
              , rtm
              , platform_division_code
              , product_brand
              , IMPV_Route
              ')

#UPM2 <- aggregate(cbind(UPM$MPV_TS*UPM$IB,UPM$MPV_Raw*UPM$IB,UPM$IB,UPM$MPV_N),
#                  by=list(UPM$Platform_Subset_Nm,UPM$Region,UPM$Country_Cd,UPM$FYearQtr),FUN=sum)
#UPM2 <- aggregate(cbind(UPM$IPM_MPV*UPM$IB,UPM$mpva*UPM$IB,UPM$IB,UPM$na),
#                  by=list(UPM$printer_platform_name,UPM$printer_region_code,UPM$FYearQtr),FUN=sum)

#colnames(UPM2)<- c("printer_platform_name","printer_region_code","country","FYQ","UPM_MPVIB","MMPVIB","IB","MPV_sample")
# UPM2$FYQtr   <- gsub("\\.","Q",lubridate::quarter(as.Date(paste0(UPM2$FYearMo,'01'),format='%Y%m%d'),with_year=TRUE,fiscal_start=11))
UPM2$MPV_UPM <- (UPM2$MPV_UPMIB)
UPM2$MPV_Raw <- (UPM2$MPV_RawIB)
UPM2$MPV_TS  <- (UPM2$MPV_TSIB)
UPM2$MPV_TD  <- (UPM2$MPV_TDIB)

UPM2c$MPV_TS  <- (UPM2c$MPV_TSIB)
UPM2c$MPV_TD  <- (UPM2c$MPV_TDIB)

months <- as.data.frame(seq(as.Date("1989101",format="%Y%m%d"),as.Date("20500101",format="%Y%m%d"),"month"))
colnames(months) <- "CalYrDate"
quarters2 <- as.data.frame(months[rep(seq_len(nrow(months)),nrow(proxylist_final2)),])
colnames(quarters2) <- "CalDate"

dashampv <- table_month
dashampv2 <- sqldf("
                       SELECT printer_platform_name
                        , fyearmo
                        , product_brand
                        , platform_division_code
                        , country_alpha2
                        , rtm
                        , sum(usage) as ampv
                        , sum(c_usage) as color_ampv
                        , sum(sumn) as sumn
                      from 
                       dashampv  
                      group by  fyearmo
                        , printer_platform_name
                        , product_brand
                        , platform_division_code
                        , country_alpha2 
                        , rtm
                      order by  fyearmo
                        , printer_platform_name
                        , product_brand
                        , platform_division_code
                        , country_alpha2
                        , rtm
                       ")
#dashampv2$ampv<- dashampv2$num/dashampv2$den

# COMMAND ----------

###Combine Results


createOrReplaceTempView(UPM2, "UPM2")
createOrReplaceTempView(as.DataFrame(proxylist_final2), "proxylist_final2")
createOrReplaceTempView(UPM2c, "UPM2c")
createOrReplaceTempView(as.DataFrame(ibtable), "ibtable")
createOrReplaceTempView(as.DataFrame(page_share_ctr), "page_share_ctr")
createOrReplaceTempView(as.DataFrame(dashampv2), "dashampv2")
createOrReplaceTempView(UPM3, "UPM3")
createOrReplaceTempView(as.DataFrame(hw_info), "hw_info")


final_list2 <- SparkR::sql("
                     SELECT --a.platform_division_code as Platform_Division_Code
                       h.supplies_family AS Selectability
                      , h.platform_division_code
                      --, q.Crg_PL_Name
                      , b.printer_platform_name AS Platform_Subset_Nm
                      , b.Region_3
                      , b.Region_DE
                      , b.Country_Cd
                      , NULL AS Country_Nm
                      -- a.FYQtr AS Fiscal_Quarter
                      , c.ib AS IB
                      , b.FYearMo
                      --, b.rFyearQtr
                      --, b.FYear
                      --, b.FQtr
                      --, b.QSI
                      , b.MPV_UPM
                      , b.MPV_TS
                      , b.MPV_TD
                      , b.MPV_Raw
                      , b.MPV_N
                      , bb.MPV_UPM as MPV_UPMc
                      , bb.MPV_TS as MPV_TSc
                      , bb.MPV_TD as MPV_TDc
                      , bb.MPV_Raw as MPV_Rawc
                      , bb.MPV_N as MPV_Nc
                      , g.FIntroDt
                      , a.rtm
                      --, g.EP
                      --, g.CM
                      --, g.SM
                      --, g.Mkt
                      --, a.FMC
                      --, g.K_PPM
                      --, g.Clr_PPM
                      --, g.Intro_Price
                      , b.Init_MPV
                      , b.Decay
                      , b.Seasonality
                      --, b.Cyclical
                      --, b.MUT
                      , b.Trend
                      , g.IMPV_Route
                      , a.Predecessor
                      , 1 AS BD_Usage_Flag
                      , e.ampv AS MPV_Dash
                      , e.color_ampv as MPVC_Dash
                      , e.sumn AS N_Dash
                      , a.ps_proxy AS Proxy_PS
                      , 1 AS BD_Share_Flag_PS
                      --, a.source
                      , a.ps_min AS Model_Min_PS
                      , a.ps_max AS Model_Max_PS
                      , a.ps_med AS Model_Median_PS
                      , a.ps_spread AS Model_Spread_PS
                      , a.ps_a as Model_a_PS
                      , a.ps_b as Model_b_PS
                      , d.value AS Share_Raw_PS
                      , d.printer_count_month_ps AS Share_Raw_N_PS
                      
                      FROM UPM2 b
                      LEFT JOIN proxylist_final2 a
                        ON (a.printer_platform_name=b.printer_platform_name and a.country_alpha2=b.Country_Cd 
                            and a.rtm=b.rtm)
                      LEFT JOIN UPM2c bb
                        ON (b.printer_platform_name=bb.printer_platform_name and b.Country_Cd=bb.Country_Cd 
                            and b.FYearMo=bb.FYearMo and b.rtm=bb.rtm)
                      LEFT JOIN ibtable c
                        ON (a.printer_platform_name=c.platform_subset and a.Country_alpha2=c.country_alpha2
                              and a.rtm=c.RTM and b.FYearMo=c.month_begin)
                      LEFT JOIN page_share_ctr d
                        ON (b.printer_platform_name=d.platform_name AND b.Country_Cd=d.country_alpha2
                            AND b.FYQtr=d.fiscal_year_quarter and b.rtm=d.rtm)
                      LEFT JOIN dashampv2 e
                        ON (b.printer_platform_name=e.printer_platform_name AND b.Country_Cd=e.country_alpha2  
                          AND b.FYearMo=e.fyearmo and b.rtm=e.rtm)
                      LEFT JOIN UPM3 g
                        ON (b.printer_platform_name=g.printer_platform_name and b.Country_Cd=g.Country_Cd and b.rtm=g.rtm)
                      LEFT JOIN hw_info h
                        ON (a.printer_platform_name=h.platform_subset)
                     ")

sigmoid_pred <- function(input_min, input_max, input_med, input_spread, input_timediff) {
  input_max-(exp((input_timediff-input_med)*input_spread)/(exp((input_timediff-input_med)*input_spread)+lit(1))*(lit(1)-(input_min+(lit(1)-input_max))))
}

final_list2$timediff <- round(datediff(concat_ws(sep="-", substr(final_list2$FYearMo, 1, 4), substr(final_list2$FYearMo, 5, 6), lit("01")), concat_ws(sep="-", substr(final_list2$FIntroDt, 1, 4), substr(final_list2$FIntroDt, 5, 6), lit("01")))/(365.25/12))

final_list2$Share_Model_PS <- sigmoid_pred(final_list2$Model_Min_PS,final_list2$Model_Max_PS,final_list2$Model_Median_PS,final_list2$Model_Spread_PS,final_list2$timediff)
final_list2$Share_Model_PSlin <- (final_list2$Model_a_PS+final_list2$Model_b_PS*final_list2$timediff)
final_list2$Share_Model_PS <- ifelse(final_list2$Share_Model_PS > 1,1,ifelse(final_list2$Share_Model_PS<0.05,0.05,final_list2$Share_Model_PS))

createOrReplaceTempView(final_list2, "final_list2")

final_list2 <- SparkR::sql('
                      select a.*
                      , case
                        when upper(rtm)="I-INK" then "I-INK"
                        when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.BD_Share_Flag_PS = 0 then "MODELLED"
                          when a.Share_Raw_PS IN (NULL, 0) then "MODELLED"
                          when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then "MODELLED"
                          else "HAVE DATA"
                          end
                        else "MODELLED BY PROXY"
                        end as Share_Source_PS
                      , case
                        when upper(rtm)="I-INK" then 1
                        when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.BD_Share_Flag_PS = 0 then a.Share_Model_PS
                          when a.Share_Raw_PS IN (NULL, 0) then a.Share_Model_PS
                          when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then a.Share_Model_PS
                          else a.Share_Raw_PS
                          end
                          else a.Share_Model_PS
                          --else null
                        end as Page_Share_sig
                        , case
                          when upper(rtm)="I-INK" then 1
                          when a.Platform_Subset_Nm=a.Proxy_PS then
                            case when a.BD_Share_Flag_PS = 0 then a.Share_Model_PSlin
                            when a.Share_Raw_PS IN (NULL, 0) then a.Share_Model_PSlin
                            when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then a.Share_Model_PSlin
                            else a.Share_Raw_PS
                            end
                          else a.Share_Model_PS
                          --else null
                        end as Page_Share_lin
                      , case
                        when a.BD_Usage_Flag is NULL then "UPM"
                        when a.BD_Share_Flag_PS = 0 then "UPM"
                        when a.MPV_Dash is NULL then "UPM"
                        when a.N_Dash < 20 then "UPM"
                          else "DASHBOARD"
                          end as Usage_Source
                      , case
                        when a.BD_Usage_Flag is NULL then a.MPV_TD
                        when a.BD_Share_Flag_PS = 0 then a.MPV_TD
                        when a.MPV_Dash is NULL then a.MPV_TD
                        when a.N_Dash < 20 then a.MPV_TD
                          else a.MPV_Dash
                          end as Usage
                      , case
                        when a.BD_Usage_Flag is NULL then "UPM"
                        when a.BD_Share_Flag_PS = 0 then "UPM"
                        when a.MPVC_Dash is NULL then "UPM"
                        when a.N_Dash < 20 then "UPM"
                          else "DASHBOARD"
                          end as Usage_Sourcec
                      , case
                        when a.BD_Usage_Flag is NULL then a.MPV_TDc
                        when a.BD_Share_Flag_PS = 0 then a.MPV_TDc
                        when a.MPVC_Dash is NULL then a.MPV_TDc
                        when a.N_Dash < 20 then a.MPV_TDc
                          else a.MPVC_Dash
                          end as Usage_c
                     from final_list2 a
                     ')

final_list2$Usage_k <- ifelse(isNull(final_list2$Usage_c), final_list2$Usage, final_list2$Usage-final_list2$Usage_c)
final_list2$Usage_c <- ifelse(isNull(final_list2$Usage_c), 0, final_list2$Usage_c)

createOrReplaceTempView(final_list2, "final_list2")

# final_list2$Pages_Device_UPM <- final_list2$MPV_TS*final_list2$IB
# final_list2$Pages_Device_Raw <- final_list2$MPV_Raw*final_list2$IB
# final_list2$Pages_Device_Dash <- final_list2$MPV_Dash*final_list2$IB
# final_list2$Pages_Device_Use <- final_list2$Usage*final_list2$IB
# final_list2$Pages_Share_Model_PS <- final_list2$MPV_TS*final_list2$IB*final_list2$Share_Model_PS
# final_list2$Pages_Share_Raw_PS <- final_list2$MPV_Raw*final_list2$IB*cast(final_list2$Share_Raw_PS, "double"))
# final_list2$Pages_Share_Dash_PS <- final_list2$MPV_Dash*final_list2$IB*cast(final_list2$Share_Raw_PS, "double"))
# final_list2$Pages_PS <- final_list2$Pages_Device_Use*cast(final_list2$Page_Share, "double"))


final_list4 <- filter(final_list2, final_list2$IB>0)
final_list4 <- filter(final_list4, !isNull(final_list4$MPV_UPM))
final_list4 <- filter(final_list4, !isNull(final_list4$Page_Share_sig))

createOrReplaceTempView(final_list4, "final_list4")

# final_list5 <- sqldf("
#                      select distinct a.*
#                       ,a.Page_Share-b.Page_Share as QoQchng_PS
#                       ,a.Page_Share-c.Page_Share as YoYchng_PS
#                      FROM final_list4 a
#                      LEFT JOIN 
#                         (
#                         SELECT 
#                         Platform_Subset_Nm, Region, rtm, Page_Share, FYearMo
#                         ,CASE
#                           WHEN SUBSTR(Fiscal_Quarter,6,6)='4' THEN 
#                             CASE
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2015' THEN REPLACE(Fiscal_Quarter,'2015Q4','2016Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2016' THEN REPLACE(Fiscal_Quarter,'2016Q4','2017Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2017' THEN REPLACE(Fiscal_Quarter,'2017Q4','2018Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2018' THEN REPLACE(Fiscal_Quarter,'2018Q4','2019Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2019' THEN REPLACE(Fiscal_Quarter,'2019Q4','2020Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2020' THEN REPLACE(Fiscal_Quarter,'2020Q4','2021Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2021' THEN REPLACE(Fiscal_Quarter,'2021Q4','2022Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2022' THEN REPLACE(Fiscal_Quarter,'2022Q4','2023Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2023' THEN REPLACE(Fiscal_Quarter,'2023Q4','2024Q1')
#                               WHEN SUBSTR(Fiscal_Quarter,1,4)='2024' THEN REPLACE(Fiscal_Quarter,'2024Q4','2025Q1')
#                             END
#                           WHEN SUBSTR(Fiscal_Quarter,6,6)='1' THEN REPLACE(Fiscal_Quarter,'Q1','Q2')
#                           WHEN SUBSTR(Fiscal_Quarter,6,6)='2' THEN REPLACE(Fiscal_Quarter,'Q2','Q3')
#                           WHEN SUBSTR(Fiscal_Quarter,6,6)='3' THEN REPLACE(Fiscal_Quarter,'Q3','Q4')
#                           END
#                         AS Fiscal_Quarter2
#                         FROM final_list4
#                         ) b
#                       ON (a.Platform_Subset_Nm=b.Platform_Subset_Nm AND a.Region=b.Region AND a.FYearMo=b.FYearMo AND a.rtm=b.rtm)
#                         LEFT JOIN
#                         (
#                         SELECT 
#                         Platform_Subset_Nm, Region, Page_Share, rtm, FYearMo
#                         ,CASE
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2015' THEN REPLACE(Fiscal_Quarter,'2015','2016')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2016' THEN REPLACE(Fiscal_Quarter,'2016','2017')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2017' THEN REPLACE(Fiscal_Quarter,'2017','2018')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2018' THEN REPLACE(Fiscal_Quarter,'2018','2019')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2019' THEN REPLACE(Fiscal_Quarter,'2019','2020')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2020' THEN REPLACE(Fiscal_Quarter,'2020','2021')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2021' THEN REPLACE(Fiscal_Quarter,'2021','2022')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2022' THEN REPLACE(Fiscal_Quarter,'2022','2023')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2023' THEN REPLACE(Fiscal_Quarter,'2023','2024')
#                           WHEN SUBSTR(Fiscal_Quarter,1,4)='2024' THEN REPLACE(Fiscal_Quarter,'2024','2025')
#                           END
#                           AS Fiscal_Quarter2
#                           FROM final_list4
#                           ) c
#                         ON (a.Platform_Subset_Nm=c.Platform_Subset_Nm AND a.Region=c.Region AND a.FYearMo=c.FYearMo AND a.rtm=b.rtm)
#                      ")

final_list6 <- final_list4
final_list6$BD_Usage_Flag <- ifelse(isNull(final_list6$BD_Usage_Flag), 0, final_list6$BD_Usage_Flag)
final_list6$BD_Share_Flag_PS <- ifelse(isNull(final_list6$BD_Share_Flag_PS), 0, final_list6$BD_Share_Flag_PS)
#change FIntroDt from YYYYMM to YYYYQQ
# final_list6$FIntroDt <- gsub(" ", "", cast(as.yearqtr(as.Date(paste0(final_list6$FIntroDt,"01"),"%Y%m%d")+3/4), "string"))

#rm(table_month)

gc(reset = TRUE)
#colnames(final_list5)[6] <- "Emerging/Developed"

#filenm <- paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer (",Sys.Date(),").xlsx")
#write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer (",Sys.Date(),").csv", sep=''), x=final_list6,row.names=FALSE, na="")

createOrReplaceTempView(final_list6, "final_list6")

# COMMAND ----------

# Adjust curve at join

ws <- orderBy(windowPartitionBy("Platform_Subset_Nm", "rtm", "Country_Cd"), "FYearMo")
final_list7 <- mutate(final_list6
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
# preddata$fit4 <- ifelse(!is.na(preddata$hp_share),preddata$hp_share, ifelse(preddata$timediff<maxtimediff2,preddata$fit1,
  #                         adjval^(preddata$timediff-maxtimediff2)*preddata$fit2+(1-(adjval^(preddata$timediff-maxtimediff2)))*preddata$fit1))
					     
adjval <- 0.99
					     
final_list7$Page_Share_Adj <- ifelse(substr(final_list7$Share_Source_PS,1,8)=="MODELLED"
                                     ,ifelse(final_list7$adjust_ps_i <= final_list7$index1, 
                                             lit(adjval)^(final_list7$index1-final_list7$adjust_ps_i+1)*final_list7$Page_Share_lin +(lit(1)-(lit(adjval)^(final_list7$index1-final_list7$adjust_ps_i+1)))*final_list7$Page_Share_sig 
                                             ,ifelse(final_list7$adjust_ps_j>final_list7$index1
                                             ,ifelse(final_list7$Page_Share_sig > final_list7$adjust_psb, final_list7$Page_Share_sig, final_list7$adjust_psb)
                                             #,final_list7$Page_Share_sig)
                                             ,final_list7$Page_Share_sig))
                                     ,final_list7$Share_Raw_PS)

final_list7$Page_Share_Adj <- ifelse(final_list7$Page_Share_Adj>1,1,ifelse(final_list7$Page_Share_Adj<0.001,0.001,final_list7$Page_Share_Adj))

#final_list7$CU_Share_Adj <- ifelse(final_list7$Share_Source_CU=="Modeled",ifelse((final_list7$adjust_cu>0) & final_list7$adjust_cu_i<= final_list7$index1,pmax(final_list7$Crg_Unit_Share -2*(final_list7$adjust_cu),0.05),ifelse((final_list7$adjust_cu< -0.05) & final_list7$adjust_cu_i<= final_list7$index1,pmax(final_list7$Crg_Unit_Share -1/2*(final_list7$adjust_cu),0.05),final_list7$Crg_Unit_Share)),final_list7$Crg_Unit_Share)
#final_list7$CU_Share_Adj <- ifelse(final_list7$CU_Share_Adj>1,1,ifelse(final_list7$CU_Share_Adj)<0,0,final_list7$CU_Share_Adj)

final_list7$adjust_use_i <- ifelse(isNull(final_list7$adjust_use_i),0,final_list7$adjust_use_i)
final_list7$adjust_use_j <- ifelse(isNull(final_list7$adjust_use_j),0,final_list7$adjust_use_j)

#final_list7$adjust_used <- ifelse(final_list7$adjust_used==0, 1, final_list7$adjust_used)
# final_list7$Usage_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse((abs(final_list7$adjust_use/final_list7$adjust_used)>1.5) & final_list7$adjust_use_i<= final_list7$index1,pmax(final_list7$Usage -(final_list7$adjust_use+final_list7$adjust_used),0.05),final_list7$Usage),final_list7$Usage)
final_list7$Usage_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$adjust_use_i <= final_list7$index1, ifelse((final_list7$Usage-final_list7$adjust_useav) > 0.05, (final_list7$Usage-final_list7$adjust_useav), 0.05), ifelse(final_list7$adjust_use_j >= final_list7$index1,final_list7$Usage+final_list7$adjust_used, final_list7$Usage)), final_list7$Usage)

final_list7$Usagec_Adj <- ifelse(final_list7$Usage_Adj!=final_list7$Usage,final_list7$Usage_Adj*final_list7$color_pct,final_list7$Usage_c)
# final_list7$Usagec_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse((abs(final_list7$adjust_usec/final_list7$adjust_used)>1.5) & final_list7$adjust_use_i<= final_list7$index1,pmax(final_list7$Usage_c -(final_list7$adjust_usec+final_list7$adjust_used),0.005),final_list7$Usage_c),final_list7$Usage_c)
final_list7$Usagec_Adj <- ifelse(final_list7$Usage_Source=="UPM",
                                 ifelse((abs(final_list7$adjust_usec/final_list7$adjust_used)>1.5) & final_list7$adjust_use_i<= final_list7$index1,
                                        ifelse((final_list7$Usage_c -(final_list7$adjust_usec+final_list7$adjust_used)) > 0.005,
                                               (final_list7$Usage_c -(final_list7$adjust_usec+final_list7$adjust_used)),
                                             0.005),
                                        final_list7$Usage_c),final_list7$Usage_c)
final_list7$Usagek_Adj <- final_list7$Usage_Adj-final_list7$Usagec_Adj

final_list7$Page_Share_old <- cast(final_list7$Page_Share_sig, "double")
final_list7$Page_Share <- cast(final_list7$Page_Share_Adj, "double")
#final_list7$Crg_Unit_Share <- cast(final_list7$CU_Share_Adj, "double")
final_list7$Usage <- cast(final_list7$Usage_Adj, "double")
final_list7$Usage_c <- cast(final_list7$Usagec_Adj, "double")
final_list7$Usage_k <- cast(final_list7$Usagek_Adj, "double")

final_list7$Usage_c <- ifelse(final_list7$Usage_c>final_list7$Usage,final_list7$Usage*.985,final_list7$Usage_c)
final_list7$Usage_k <- ifelse(final_list7$Usage_k<0,final_list7$Usage-final_list7$Usage_c,final_list7$Usage_k)
final_list7$Color_Pct <- final_list7$Usage_c/final_list7$Usage

# final_list7$Pages_Device_UPM <- final_list7$MPV_TS*final_list7$IB
# final_list7$Pages_Device_Raw <- final_list7$MPV_Raw*final_list7$IB
# final_list7$Pages_Device_Dash <- final_list7$MPV_Dash*final_list7$IB
# final_list7$Pages_Device_Use <- final_list7$Usage*final_list7$IB
# final_list7$Pages_Share_Model_PS <- final_list7$MPV_TS*final_list7$IB*final_list7$Share_Model_PS
# final_list7$Pages_Share_Raw_PS <- final_list7$MPV_Raw*final_list7$IB*as.numeric(final_list7$Share_Raw_PS)
# final_list7$Pages_Share_Dash_PS <- final_list7$MPV_Dash*final_list7$IB*as.numeric(final_list7$Share_Raw_PS)
# final_list7$Pages_PS <- final_list7$Pages_Device_Use*as.numeric(final_list7$Page_Share)

#final_list7$Pages_Share_Model_CU <- final_list7$MPV_TS*final_list7$IB*final_list7$Share_Model_CU
#final_list7$Pages_Share_Raw_CU <- final_list7$MPV_Raw*final_list7$IB*as.numeric(final_list7$Share_Raw_CU)
#final_list7$Pages_Share_Dash_CU <- final_list7$MPV_Dash*final_list7$IB*as.numeric(final_list7$Share_Raw_CU)
#final_list7$Pages_CU <- final_list7$Pages_Device_Use*as.numeric(final_list7$Crg_Unit_Share)

#final_list7 <- final_list7[c(1:79)]
# final_list7 <- as.data.frame(final_list7)


#Change all I-Ink to have share of 1, even when we have telemetry
final_list7$Page_Share <- ifelse(upper(final_list7$rtm) == 'I-INK', 1, final_list7$Page_Share)  
final_list7$Share_Source_PS <- ifelse(upper(final_list7$rtm) == 'I-INK', "Modeled", final_list7$Share_Source_PS)  


#Need to get TIJ_2.XG3 KRONOS?  or just Kronos platform subset?
# final_list7$Page_Share <- ifelse(grepl('KRONOS', final_list7$platform_division_code), 1, final_list7$Page_Share)  #Change all Kronos platforms to have share of 1
# final_list7$Share_Source_PS <- ifelse(grepl('KRONOS', final_list7$platform_division_code), "Modeled", final_list7$Share_Source_PS)  #Change all Kronos to have share of 1
final_list7 <- withColumn(final_list7, "Page_Share", ifelse(like(final_list7$platform_division_code, "%KRONOS%"), 1, final_list7$Page_Share))
final_list7 <- withColumn(final_list7, "Share_Source_PS", ifelse(like(final_list7$platform_division_code, "%KRONOS%"), "Modeled", final_list7$Share_Source_PS))

#write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer Adj (",Sys.Date(),").csv", sep=''), x=final_list7,row.names=FALSE, na="")
#s3write_using(x=final_list9,FUN = write.csv, object = paste0("s3://insights-environment-sandbox/BrentT/ink_100pct_test",Sys.Date(),".csv"), row.names=FALSE, na="")

final_list7$total_pages <- final_list7$Usage*final_list7$ib
final_list7$hp_pages <- final_list7$Usage*final_list7$ib*final_list7$Page_Share
final_list7$total_kpages <- final_list7$Usage_k*final_list7$ib 
final_list7$total_cpages <- final_list7$Usage_c*final_list7$ib
final_list7$hp_kpages <- final_list7$Usage_k*final_list7$ib*final_list7$Page_Share 
final_list7$hp_cpages <- final_list7$Usage_c*final_list7$ib*final_list7$Page_Share
final_list7$nonhp_kpages <- final_list7$Usage_k*final_list7$ib*(lit(1)-final_list7$Page_Share) 
final_list7$nonhp_cpages <- final_list7$Usage_c*final_list7$ib*(lit(1)-final_list7$Page_Share) 
                                             
createOrReplaceTempView(final_list7, "final_list7")

# COMMAND ----------

# Change to match MDM format

final_list8 <- filter(final_list7, !isNull(final_list7$Page_Share))  #missing intro date
final_list8$fiscal_date <- concat_ws(sep = "-", substr(final_list8$FYearMo, 1, 4), substr(final_list8$FYearMo, 5, 6), lit("01"))
#Change from Fiscal Date to Calendar Date
final_list8$year_month_float <- to_date(final_list8$fiscal_date, "yyyy-MM-dd")

today <- Sys.Date()
version <- paste0(gsub("-",".",Sys.Date()),".1")
vsn <- 'NoProxy-2021.11.15.1'
rec1 <- 'usage_share'
geog1 <- 'country'
tempdir(check=TRUE)

gc()

createOrReplaceTempView(final_list8, "final_list8")
cacheTable("final_list8")

# COMMAND ----------

mdm_tbl_share <- SparkR::sql(paste("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Share_Source_PS as data_source
                , '",vsn,"' as version
                , 'hp_share' as measure
                , Page_Share as units
                , Proxy_PS as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                 
                 ",sep=""))

mdm_tbl_usage <- SparkR::sql(paste("select distinct 
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'usage' as measure
                , Usage as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage is not null
                 
                 ",sep=""))

mdm_tbl_usagen <- SparkR::sql(paste("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'Usage_n' as measure
                , MPV_n as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE MPV_n is not null AND MPV_n >0
                 
                 ",sep=""))

mdm_tbl_sharen <- SparkR::sql(paste("select distinct 
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'Share_n' as measure
                , Share_Raw_N_PS as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Share_Raw_N_PS is not null AND Share_Raw_N_PS >0
                 
                 ",sep=""))

mdm_tbl_kusage <- SparkR::sql(paste("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'k_usage' as measure
                , Usage_k as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_k is not null AND Usage_k >0
                 
                 ",sep=""))

mdm_tbl_cusage <- SparkR::sql(paste("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'color_usage' as measure
                , Usage_c as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 ",sep=""))
                                             
mdm_tbl_pages <- SparkR::sql(paste("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'total_pages' as measure
                , total_pages as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 ",sep=""))
                                             
 mdm_tbl_kpages <- SparkR::sql(paste0("select distinct
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
                , 'total_k_pages' as measure
                , total_kpages as units
                , IMPV_Route as proxy_used
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
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'total_c_pages' as measure
                , total_cpages as units
                , IMPV_Route as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))
             
mdm_tbl_hppages <- SparkR::sql(paste("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'hp_pages' as measure
                , hp_pages as units
                , Proxy_PS as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 ",sep=""))
                        
mdm_tbl_khppages <- SparkR::sql(paste0("select distinct
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
                , 'hp_k_pages' as measure
                , hp_kpages as units
                , Proxy_PS as proxy_used
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
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'hp_c_pages' as measure
                , hp_cpages as units
                , Proxy_PS as proxy_used
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
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'non_hp_k_pages' as measure
                , nonhp_kpages as units
                , Proxy_PS as proxy_used
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
                , 'Trad' as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
                , '",today,"' as forecast_created_date
                , Usage_Source as data_source
                , '",vsn,"' as version
                , 'non_hp_c_pages' as measure
                , nonhp_cpages as units
                , Proxy_PS as proxy_used
                , '",ibversion,"' as ib_version
                , '",today,"' as load_date
                from final_list8
                WHERE Usage_c is not null AND Usage_c >0
                 
                 "))
                        
mdm_tbl_ib <- SparkR::sql(paste("select distinct
                '",rec1,"' as record
                , year_month_float as cal_date
                , '",geog1,"' as geography_grain
                , Country_Cd as geography
                , Platform_Subset_Nm as platform_subset
                , rtm as customer_engagement
                , 'Modelled off of: Toner 100%IB process' as forecast_process_note
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
                 
                 ",sep=""))

# mdm_tbl_cusagec <- SparkR::sql(paste("select distinct
#                 '",rec1,"' as record
#                 , year_month_float as cal_date
#                 , '",geog1,"' as geography_grain
#                 , Country_Cd as geography
#                 , Platform_Subset_Nm as platform_subset
#                 , rtm as customer_engagement
#                 , 'Modelled off of: Toner 100%IB process' as forecast_process_note
#                 , '",today,"' as forecast_created_date
#                 , Usage_Source as data_source
#                 , '",vsn,"' as version
#                 , 'color_usage_c' as measure
#                 , Usage_c as units
#                 , IMPV_Route as proxy_used
#                 , '",ibversion,"' as ib_version
#                 , '",today,"' as load_date
#                 from final_list8
#                 WHERE Usage_c is not null AND Usage_c >0
                 
#                  ",sep=""))

# mdm_tbl_cusagem <- SparkR::sql(paste("select distinct
#                 '",rec1,"' as record
#                 , year_month_float as cal_date
#                 , '",geog1,"' as geography_grain
#                 , Country_Cd as geography
#                 , Platform_Subset_Nm as platform_subset
#                 , rtm as customer_engagement
#                 , 'Modelled off of: Toner 100%IB process' as forecast_process_note
#                 , '",today,"' as forecast_created_date
#                 , Usage_Source as data_source
#                 , '",vsn,"' as version
#                 , 'color_usage_m' as measure
#                 , Usage_c as units
#                 , IMPV_Route as proxy_used
#                 , '",ibversion,"' as ib_version
#                 , '",today,"' as load_date
#                 from final_list8
#                 WHERE Usage_c is not null AND Usage_c >0
                 
#                  ",sep=""))

# mdm_tbl_cusagey <- SparkR::sql(paste("select distinct
#                 '",rec1,"' as record
#                 , year_month_float as cal_date
#                 , '",geog1,"' as geography_grain
#                 , Country_Cd as geography
#                 , Platform_Subset_Nm as platform_subset
#                 , rtm as customer_engagement
#                 , 'Modelled off of: Toner 100%IB process' as forecast_process_note
#                 , '",today,"' as forecast_created_date
#                 , Usage_Source as data_source
#                 , '",vsn,"' as version
#                 , 'color_usage_y' as measure
#                 , Usage_c as units
#                 , IMPV_Route as proxy_used
#                 , '",ibversion,"' as ib_version
#                 , '",today,"' as load_date
#                 from final_list8
#                 WHERE Usage_c is not null AND Usage_c >0
                 
#                  ",sep=""))

mdm_tbl <- rbind(mdm_tbl_share, mdm_tbl_usage, mdm_tbl_usagen, mdm_tbl_sharen, mdm_tbl_kusage, mdm_tbl_cusage, mdm_tbl_pages, mdm_tbl_hppages, 
                 mdm_tbl_kpages, mdm_tbl_cpages, mdm_tbl_khppages, mdm_tbl_chppages, mdm_tbl_knhppages, mdm_tbl_cnhppages, mdm_tbl_ib)
                                             
#mdm_tbl <- rbind(mdm_tbl_share, mdm_tbl_usage, mdm_tbl_usagen, mdm_tbl_sharen, mdm_tbl_kusage, mdm_tbl_cusage)
                                             
mdm_tbl$cal_date <- to_date(mdm_tbl$cal_date,format="yyyy-MM-dd")
mdm_tbl$forecast_created_date <- to_date(mdm_tbl$forecast_created_date,format="yyyy-MM-dd")
mdm_tbl$load_date <- to_date(mdm_tbl$load_date,format="yyyy-MM-dd")

createOrReplaceTempView(mdm_tbl, "mdm_tbl")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import re
# MAGIC from pyspark.sql.functions import lit
# MAGIC 
# MAGIC forecast_process_note ="INK {} {}.1".format(outnm_dt.upper(), datestamp[0:4])
# MAGIC 
# MAGIC version = call_redshift_addversion_sproc(configs=configs, record=forecast_process_note, source_name="CUPSM")
# MAGIC 
# MAGIC mdm_tbl = spark.sql("SELECT * FROM mdm_tbl") \
# MAGIC     .withColumn("version", lit(version[0])) \
# MAGIC     .withColumn("forecast_process_note", lit(forecast_process_note))
# MAGIC 
# MAGIC s3_destination = f"{constants['S3_BASE_BUCKET'][stack]}spectrum/cupsm/{version[0]}/{re.sub(' ','_',forecast_process_note.lower())}"
# MAGIC 
# MAGIC write_df_to_s3(df=mdm_tbl, destination=s3_destination, format="parquet", mode="overwrite", upper_strings=True)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC if writeout.upper() == "YES":
# MAGIC     write_df_to_redshift(configs=configs, df=spark.read.parquet(s3_destination).load(), destination="stage.usage_share_ink_landing", mode="append", preactions="TRUNCATE stage.usage_share_ink_landing")

# COMMAND ----------

notebook_end_time <- Sys.time()
notebook_total_time <- notebook_end_time - notebook_start_time;notebook_total_time
