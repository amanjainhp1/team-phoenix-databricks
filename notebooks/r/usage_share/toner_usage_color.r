# Databricks notebook source
# ---
# #Version 2021.03.09.1#
# title: "Toner Color Usage Country Level"
# output:
#   word_document: default
#   html_notebook: default
# ---

# COMMAND ----------

notebook_start_time <- Sys.time()

# COMMAND ----------

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
# MAGIC relevant_tasks = ["all", "usge_color"]
# MAGIC 
# MAGIC # exit if tasks list does not contain a relevant task i.e. "all" or "usge_color"
# MAGIC for task in tasks:
# MAGIC     if task not in relevant_tasks:
# MAGIC         dbutils.notebook.exit("EXIT: Tasks list does not contain a relevant value i.e. {}.".format(", ".join(relevant_tasks)))

# COMMAND ----------

# MAGIC %python
# MAGIC # set vars equal to widget vals for interactive sessions, else retrieve task values 
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
# MAGIC spark.conf.set('aws_bucket_name', constants['S3_BASE_BUCKET'][stack])

# COMMAND ----------

packages <- c("sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR", "spatstat")

# COMMAND ----------

# MAGIC %run ../common/package_check

# COMMAND ----------

options(java.parameters = "-Xmx40g")

options(scipen=999) #remove scientific notation
tempdir(check=TRUE)

# COMMAND ----------

# define s3 bucket
aws_bucket_name <- sparkR.conf('aws_bucket_name')

# COMMAND ----------

# MAGIC %python
# MAGIC # load parquet data and register views
# MAGIC tables = ['bdtbl', 'hardware_xref', 'ib', 'iso_cc_rollup_xref', 'iso_country_code_xref']
# MAGIC for table in tables:
# MAGIC     spark.read.parquet(f'{constants["S3_BASE_BUCKET"][stack]}/cupsm_inputs/toner/{datestamp}/{timestamp}/{table}/').createOrReplaceTempView(f'{table}')

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

# Step 1 - query for Normalized extract specific to PE and RM

zeroi = SparkR::collect(SparkR::sql(" SELECT  tpmib.printer_platform_name as platform_name  
               , tpmib.platform_std_name as platform_standard_name  
               , tpmib.printer_country_iso_code as iso_country_code  
               , tpmib.fiscal_year_quarter 
               , tpmib.date_month_dim_ky as calendar_year_month 
               , tpmib.share_region_incl_flag AS share_region_incl_flag   
               , tpmib.ps_region_incl_flag AS page_share_region_incl_flag 
               , tpmib.usage_region_incl_flag AS usage_region_incl_flag 
               --Get numerator and denominator, already ib weighted at printer level
               , SUM(COALESCE(tpmib.print_pages_mono_ib_ext_sum,0)) as usage_numerator
               , SUM(COALESCE(tpmib.print_pages_color_ib_ext_sum,0)) as color_numerator
               , SUM(COALESCE(tpmib.print_months_ib_ext_sum,0)) as usage_denominator
               , SUM(COALESCE(tpmib.printer_count_month_usage_flag_sum,0)) AS printer_count_month_use
               , SUM(COALESCE(tpmib.printer_count_fyqtr_usage_flag_sum,0)) AS printer_count_fiscal_quarter_use
                FROM  
                  bdtbl tpmib
                WHERE 1=1 
                  AND printer_route_to_market_ib='AFTERMARKET'
                GROUP BY tpmib.printer_platform_name  
               , tpmib.platform_std_name  
               , tpmib.printer_country_iso_code  
               , tpmib.fiscal_year_quarter 
               , tpmib.date_month_dim_ky 
               , tpmib.share_region_incl_flag   
               , tpmib.ps_region_incl_flag 
               , tpmib.usage_region_incl_flag"))

ibtable <- SparkR::collect(SparkR::sql("
                   select  a.platform_subset
                          ,YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          ,d.technology AS hw_type
                          ,b.region_5
                          ,a.country_alpha2
                          ,substring(b.developed_emerging,1,1) as de
                          ,sum(a.units) as ib
                          ,a.version
                          ,d.mono_color
                          ,d.vc_category
                          ,d.business_feature
                    from ib a
                    left join iso_country_code_xref b
                      on (a.country_alpha2=b.country_alpha2)
                    left join hardware_xref d
                       on (a.platform_subset=d.platform_subset)
                    where a.measure = 'IB'
                      and (upper(d.technology)='LASER' or (d.technology='PWA' and (upper(d.hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3', 'TIJ_4.XG2 MORNESTA'))
                         ))
                    group by a.platform_subset
                          ,a.cal_date
                          ,d.technology
                          ,b.region_5
                          ,a.country_alpha2
                          ,b.developed_emerging
                          ,a.version
                          ,d.mono_color
                          ,d.vc_category
                          ,d.business_feature
                   "))


hwval <- SparkR::collect(SparkR::sql("
                    SELECT platform_subset, technology, pl, mono_ppm
                    , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H','SWT-L') then 'PRO'
                      WHEN vc_category in ('SWT-H','SWT-H PRO','DEPT','DEPT-HIGH','WG') then 'ENT'
                      ELSE NULL
                      END as pro_vs_ent
                    , format, mono_color, sf_mf, vc_category, product_structure
                            , CASE WHEN vc_category in ('ULE','PLE-L','PLE-H') THEN 'DSK'
                                  WHEN vc_category in ('SWT-L') THEN 'SWL'
                                  WHEN vc_category in ('SWT-H','SWT-H PRO') THEN 'SWH'
                                  WHEN vc_category in ('DEPT','DEPT-HIGH') THEN 'DPT'
                                  WHEN vc_category in ('WG') THEN 'WGP'
                                  ELSE NULL
                              END AS platform_market_code
                    FROM hardware_xref
                    WHERE (upper(technology)='LASER' or (technology='PWA' and (upper(hw_product_family) in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3', 'TIJ_4.XG2 MORNESTA'))))
                    "))


intro_dt <- SparkR::collect(SparkR::sql("
                     SELECT platform_subset, min(cal_date) as platform_intro_month
                     FROM ib
                     GROUP BY platform_subset
                     "))

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
              ON (a.iso_country_code=ib.country_alpha2 and a.calendar_year_month=ib.month_begin and a.platform_name=ib.platform_subset)
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
                    , (SUM(color_numerator)/SUM(usage_numerator))/3 AS meansummpvwtn
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

zero$pMPV <- ifelse(((zero$FYearMo) >= start1 & (zero$FYearMo <=end1)), zero$meansummpvwtn, NA)
zero$pN <- ifelse(((zero$FYearMo) >= start1 & (zero$FYearMo <=end1)), zero$SumN, NA)
zero$pMPVN <- zero$pMPV*zero$pN

zero_platform <- sqldf('select distinct printer_platform_name, printer_region_code from zero order by printer_platform_name, printer_region_code')
zero_platform$source1 <-"TRI_PRINTER_USAGE_SN"
zero_platformList <- reshape2::dcast(zero_platform, printer_platform_name ~printer_region_code, value.var="source1")

countFACT_LP_MONTH <- sum(!is.na(zero_platformList[, c("AP","EU", "LA","NA")])) 
paste("The total numbers of platform - region combinations that could be retrieved from the FACT PRINTER LASER MONTH table =", countFACT_LP_MONTH)

head(zero)

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
                   , 'AVAIABLE' as dummy
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
threef$sumPgs <- ifelse(threef$sumPgs==0,1,threef$sumPgs)
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

# MAGIC %python
# MAGIC 
# MAGIC spark.read.parquet(f"{constants['S3_BASE_BUCKET'][stack]}cupsm_outputs/toner/{datestamp}/{timestamp}/usage_total").where("CM == 'C'").createOrReplaceTempView("upm")

# COMMAND ----------

createOrReplaceTempView(as.DataFrame(hwval), "hwval")

UPM <- SparkR::sql("select upm.*, hw.sf_mf as SM, hw.pro_vs_ent as EP
              from upm left join hwval hw on upm.Platform_Subset_Nm =hw.platform_subset")

createOrReplaceTempView(UPM, "UPM")

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
                --, upm.MUT
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

createOrReplaceTempView(final9, "final9")

# COMMAND ----------

# MAGIC %python
# MAGIC # Step 85 - exporting final9 to S3
# MAGIC 
# MAGIC output_file_name = f"{constants['S3_BASE_BUCKET'][stack]}cupsm_outputs/toner/{datestamp}/{timestamp}/usage_color"
# MAGIC 
# MAGIC write_df_to_s3(df=spark.sql("SELECT * FROM final9"), destination=output_file_name, format="parquet", mode="overwrite", upper_strings=True)
# MAGIC 
# MAGIC print(output_file_name)

# COMMAND ----------

notebook_end_time <- Sys.time()
notebook_total_time <- notebook_end_time - notebook_start_time;notebook_total_time
