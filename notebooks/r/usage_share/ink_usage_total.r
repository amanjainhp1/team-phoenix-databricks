# Databricks notebook source
# ---
# Version 2022.04.05.1  
# title: "UPM ink at country level"  
# output: html_notebook  
# ---

# COMMAND ----------

notebook_start_time <- Sys.time()

# COMMAND ----------

dbutils.widgets.text("datestamp", "")
dbutils.widgets.text("timestamp", "")

# COMMAND ----------

# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# load needed R packages
packages <- c("tidyverse", "lubridate", "SparkR", "zoo", "sqldf")

# COMMAND ----------

# MAGIC %run ../common/package_check

# COMMAND ----------

# MAGIC %python
# MAGIC # load parquet data and register views
# MAGIC datestamp = dbutils.widgets.get('datestamp')
# MAGIC timestamp = dbutils.widgets.get('timestamp')
# MAGIC 
# MAGIC tables = ['bdtbl', 'hardware_xref', 'ib', 'iso_cc_rollup_xref', 'iso_country_code_xref']
# MAGIC for table in tables:
# MAGIC     spark.read.parquet(f'{constants["S3_BASE_BUCKET"][stack]}/cupsm_inputs/ink/{datestamp}/{timestamp}/{table}/').createOrReplaceTempView(f'{table}')

# COMMAND ----------

# value initialization

todaysDate <- Sys.Date()

nyear <- 2  #number of years

start1 <- as.numeric(ifelse(lubridate::month(todaysDate)<10, (paste((lubridate::year(todaysDate)-nyear), 0,lubridate::month(todaysDate), sep="")), (paste((lubridate::year(todaysDate)-nyear), lubridate::month(todaysDate), sep=""))))

end1 <- as.numeric(ifelse(lubridate::month(todaysDate)<10, (paste(lubridate::year(todaysDate), 0,lubridate::month(todaysDate), sep="")), (paste(lubridate::year(todaysDate), lubridate::month(todaysDate), sep=""))))

MUTminSize <- 200

# ---- Specify the minimum sample size required while calculating MUT (Step 5) -------------------#
minSize <- 200

# ---- Specify the time window that will be considered while calculating MUT (Step 7) ------------#
startMUT <- 201101 #or set to start1
endMUT  <- end1

# ---- Specify the last year avaialble in FIJI IB ------------------------------------------------#
lastAvaiableYear <- 2058

# ---- Specify whether to replace the data of PRO by ENT after the end of MUT creation (Step 7) --#
replace <- 0 # Value 1 will replace the values of PRO by the respective values from ENT set

# ---- Specify time (YYYYMM) that works as a demarcation b/n Old and New Platforms (Step 50) -----#
oldNewDemarcation <- end1

# COMMAND ----------

# Step 1 - query for Normalized extract specific to PE and RM

zero <- SparkR::collect(SparkR::sql("
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
	, total_pages_ib_wtd_avg
	, pct_color	 
	, reporting_printers
    , connected_ib
FROM bdtbl
"))

# COMMAND ----------

ibtable <- SparkR::collect(SparkR::sql("
                   select  a.platform_subset
                          , cal_date
                          ,YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          ,d.technology AS hw_type
                          ,UPPER(a.customer_engagement)  AS RTM
                          ,b.region_5
                          ,a.country
                          ,sum(a.units) as ib
                          ,a.version
                          ,d.hw_product_family as platform_division_code
                    from ib a
                    left join iso_country_code_xref b
                      on (a.country=b.country_alpha2)
                    left join hardware_xref d
                       on (a.platform_subset=d.platform_subset)
                    where a.measure='IB'
                      and (upper(d.technology)='INK' or (d.technology='PWA' and (upper(d.hw_product_family) not in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3'))
                          and a.platform_subset not like 'PANTHER%' and a.platform_subset not like 'JAGUAR%'))
                      and d.product_lifecycle_status not in ('E','M') 
                    group by a.platform_subset, a.customer_engagement, a.cal_date, d.technology, a.version, d.hw_product_family
                            , b.region_5, a.country
"))

# COMMAND ----------

hw_info <- SparkR::collect(SparkR::sql(
                    "SELECT distinct platform_subset, pl as  product_line_code, hw_product_family as platform_division_code, sf_mf as platform_function_code
                    , SUBSTRING(mono_color,1,1) as cm, UPPER(brand) as product_brand
                    , mono_ppm as print_mono_speed_pages, color_ppm as print_color_speed_pages
                    , intro_price, intro_date, vc_category as market_group
                    from hardware_xref
                    "
                    ))

# COMMAND ----------

ibintrodt <- SparkR::collect(SparkR::sql("
            SELECT  a.platform_subset
                    ,min(cal_date) AS intro_date
                    FROM ib a
                    LEFT JOIN hardware_xref d
                      ON (a.platform_subset=d.platform_subset)
                    WHERE a.measure='IB'
                      AND d.technology in ('INK','PWA')
                    GROUP BY a.platform_subset
                   "))
ibintrodt$intro_yyyymm <- paste0(substr(ibintrodt$intro_date,1,4),substr(ibintrodt$intro_date,6,7))

# COMMAND ----------

#Get Market10 Information
country_info <- SparkR::collect(SparkR::sql("
                      WITH mkt10 AS (
                           SELECT country_alpha2, country_level_2 as market10, country_level_4 as emdm
                           FROM iso_cc_rollup_xref
                           WHERE country_scenario='MARKET10'
                      ),
                      rgn5 AS (
                            SELECT country_alpha2, region_5, developed_emerging, country 
                            FROM iso_country_code_xref
                      )
                      SELECT a.country_alpha2, a.region_5, b.market10, a.developed_emerging, country
                            FROM rgn5 a
                            LEFT JOIN mkt10 b
                            ON a.country_alpha2=b.country_alpha2
                           "))

# COMMAND ----------

zero <- sqldf("
            with sub1 as (
              select a.printer_platform_name
              , a.printer_region_code
              , a.country_alpha2
              , ci.market10
              , SUBSTR(ci.developed_emerging,1,1) as developed_emerging
              , hw.platform_function_code
              , hw.cm
              , a.year
              , a.quarter
              , a.date_month_dim_ky as fyearmo
              , a.printer_managed
              , hw.market_group as platform_market_code
              , hw.platform_division_code
              , hw.product_brand
              , hw.intro_price as product_intro_price
              , id.intro_date as product_introduction_date
              , hw.print_mono_speed_pages
              , hw.print_color_speed_pages
              , hw.product_line_code
              , a.tot_cc_ib_wtd_avg as total_cc                                
              , a.tot_cc_ib_wtd_avg as usage
              , a.tot_cc_ib_wtd_avg as MPVa  --Consumed Ink
              , a.reporting_printers as sumn
             from zero a
             LEFT JOIN hw_info hw
              on a.printer_platform_name=hw.platform_subset
             LEFT JOIN country_info ci 
              ON a.country_alpha2=ci.country_alpha2 
             LEFT JOIN ibintrodt id
              ON a.printer_platform_name=id.platform_subset
            )
            select * from sub1
            ")

zero$pMPV <- ifelse(((zero$fyearmo) >= start1 & (zero$fyearmo <=end1)), zero$MPVa,NA)
zero$pN <- ifelse(((zero$fyearmo) >= start1 & (zero$fyearmo <=end1)), zero$sumn, NA)
zero$pMPVN <- zero$pMPV*zero$pN
zero$rtm <- zero$printer_managed

zero_platform <- sqldf('select distinct printer_platform_name, printer_region_code from zero order by printer_platform_name, printer_region_code')
zero_platform$source1 <-"TRI_PRINTER_USAGE_SN"
zero_platformList <- reshape2::dcast(zero_platform, printer_platform_name ~printer_region_code, value.var="source1")

countFACT_LP_MONTH <- sum(!is.na(zero_platformList[, c("AP","EU", "JP","LA","NA")])) 
paste("The total numbers of platform - region combinations that could be retrieved from the FACT PRINTER LASER MONTH table =", countFACT_LP_MONTH)

head(zero)

# COMMAND ----------

# Step 2 - extract Normalized value for RM and PE based on Plat_Nm, Region_Cd and Src_Cd

# Based on the extract from Step 1, for a given group defined by Plat_Nm, Region_Cd and Src_Cd, create the 
# following variables; Sum of pMPVN as SUMpMPVN, Sum of pN as SUMpN	and SUMpMPVN/SUMpN as NormMPV

one <- sqldf(paste("								
                   SELECT *, SUMpMPVN/SUMpN AS NormMPV	
                   , 'AVAIABLE' as dummy
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

two$MPV_Pct <- two$usage/two$NormMPV
two$MPVPN <- two$MPV_Pct*two$sumn
str(two)

# COMMAND ----------

# Step 4 - summary stat for a group  defined by EP, CM, printer_region_code and FYearMo

# Using the extract from Step 3, create the following summary stat for a group defined by EP,CM, printer_region_code 
# and FYearMo (notice, Plat_Nm is no more a part of a group); Sum of MPVPN as SumMPVPN and Sum of SumN as SumN

three <- sqldf('select platform_division_code, product_brand, market10, developed_emerging, FYearMo, rtm
               , sum(MPVPN) as SumMPVPN
               , sum(SumN) as SumN
               FROM two
               GROUP by platform_division_code, product_brand, market10, developed_emerging, FYearMo, rtm
               ORDER by platform_division_code, product_brand, market10, developed_emerging, FYearMo, rtm
               ')

# COMMAND ----------

# Step 5 - drop groups if respective number < 200 and create MUT

four <- sqldf(paste("select platform_division_code, product_brand, market10, developed_emerging, FYearMo, rtm 
                      , SumMPVPN
                      , SumN
                      , SumMPVPN/SumN as MUT
                      from three
                      where SumN >=", minSize, "
                      and platform_division_code is not NULL
                      order by platform_division_code, product_brand, market10, developed_emerging, FYearMo, rtm", sep = " ")
  )

# COMMAND ----------

# Step 6 - Create variable STRATA by concatenating 3 variables

# columns to paste together
cols <- c( 'platform_division_code', 'product_brand', 'market10' ,'developed_emerging', 'rtm')

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
  d <- d[d$fyearmo >= startMUT & d$fyearmo < endMUT,]

  if (nrow(d) <4) next

  yyyy <- as.numeric(substr(d$fyearmo, 1,4))
  mm <- as.numeric(substr(d$fyearmo, 5,6))

  d$J90Mo <- (yyyy - 1990)*12 + (mm-1)

  d$x <- yyyy + (mm-1)/12

  d$y <- log(d$MUT)

  fit <- lm(y ~ x, data=d)
  #abline(fit)
  #summary(fit)
  d$a0 <- fit$coefficients[[1]]
  d$b1 <- fit$coefficients[[2]]
  d$yhat <- d$a0 + d$b1*d$x

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

  ymin <- min(d2$MUT,d2$Eyhat, d2$EIrregular, d2$Eseasonality, d2$Emo_Smooth, na.rm=TRUE )
  ymax <- max(d2$MUT,d2$Eyhat, d2$EIrregular, d2$Eseasonality, d2$Emo_Smooth, na.rm=TRUE )
  xmin <- min(d2$x)
  xmax <- max(d2$x)


  # plot(d2$x, d2$MUT, typ='l', col = "#0096d6", main=paste("Decomposition for",cat),
  #      xlab="Year", ylab="MUT and Other Series", xlim=c(2011, 2019), ylim=c(ymin, ymax)
  #      , lwd = 1, frame.plot=FALSE, las=1, xaxt='n'
  # )#Blue, las=0: parallel to the axis, 1: always horizontal, 2: perpendicular to the axis, 3: always vertical
  # axis(side=1,seq(2011,2019, by=1)) #increase number of years on x axis
  # lines(d2$x, d2$Eyhat,col="#822980",lwd=1) #purple
  # lines(d2$x, d2$EIrregular,col="#838B8B",lwd = 1)
  # lines(d2$x, d2$Eseasonality,col="#fdc643",lwd = 1) #yellow
  # lines(d2$x, d2$Emo_Smooth,col="#de2e43",lwd=1) #red
  # box(bty="l") #puts x and y axis lines back in
  # grid(nx=NULL, ny=NULL, col="cornsilk2", lty="dotted", lwd=par("lwd"))
  # #grid(nx=96, ny=NULL, col="cornsilk2", lty="dotted", lwd=par("lwd"))
  # 
  # legend("bottom", bty="n", # places a legend at the appropriate place 
  #        c("Original", "Trend", "Irregular", "Seasonality", "Cyclical"), # puts text in the legend
  #        xpd = TRUE, horiz = TRUE,
  #        lty=c(1,1,1,1,1), # gives the legend appropriate symbols (lines)
  #        lwd=c(1,1,1,1,1),col=c("#0096d6", "#822980", "#838B8B", "#fdc643", "#de2e43"))

  data[[cat]] <- d2

  #print(cat)

}

# COMMAND ----------

# Step 8 - Combining extended MUT dataset for each Strata

# ---- Also implementing the Source Replacement ---------------------------- #
minslp <- -0.1

outcome0 <- as.data.frame(do.call("rbind", data))
str(outcome0)
rownames(outcome0) <- NULL
outcome0$b1 <- ifelse(outcome0$b1 > minslp ,minslp ,outcome0$b1)

stratpl <- sqldf("SELECT distinct platform_division_code from outcome0")
stratcs <- sqldf("SELECT distinct product_brand from outcome0")
stratrm <- sqldf("SELECT distinct rtm from outcome0")
stratmk <- sqldf("SELECT distinct market10, developed_emerging from outcome0")
stratmn <- sqldf("SELECT distinct month from outcome0")
stratjn <- sqldf("SELECT a.*, b.*, c.*, d.*, e.*
                 from stratpl a , stratcs b , stratrm c, stratmk d, stratmn e")
stratjn <- sqldf("SELECT a.*, b.region_5
                 from stratjn a
                 left join (select distinct region_5, market10 from country_info) b
                 on a.market10=b.market10")
outcome0 <- sqldf("SELECT a.*, b.region_5
                 from outcome0 a
                 left join (select distinct region_5, market10 from country_info) b
                 on a.market10=b.market10")
#Add region, get regional estimates...what else is missing for C4010 E0--India missing
outcome_o <- sqldf("SELECT platform_division_code, product_brand, developed_emerging, month, market10, region_5, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome0
                   group by platform_division_code, product_brand, developed_emerging, month, market10, region_5, rtm")
outcome_r <- sqldf("SELECT platform_division_code, product_brand, developed_emerging, month, region_5, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome_o
                   group by platform_division_code, product_brand, developed_emerging, month, region_5, rtm")

outcome_a <- sqldf("SELECT platform_division_code, product_brand, developed_emerging, month, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome_o
                   group by platform_division_code, product_brand, developed_emerging, month, rtm")

outcome_b <- sqldf("SELECT platform_division_code, market10, developed_emerging, month, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome_o
                   group by platform_division_code, market10, developed_emerging, month, rtm")
outcome_c <- sqldf("SELECT product_brand, market10, developed_emerging, month, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome_o
                   group by product_brand, market10, developed_emerging, month, rtm")
outcome_d <- sqldf("SELECT product_brand, market10, developed_emerging, month, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome_o
                   group by product_brand, market10, developed_emerging, month, rtm")
outcome_e <- sqldf("SELECT  platform_division_code, product_brand,  market10, developed_emerging, month, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome_o
                   group by  platform_division_code, product_brand,  market10, developed_emerging, month, rtm")
outcome_f <- sqldf("SELECT product_brand,  developed_emerging, month, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome_o
                   group by product_brand, developed_emerging, month, rtm")
outcome_w <- sqldf("SELECT product_brand, month, rtm, avg(b1) as b1, avg(seasonality) as seasonality --, avg(mo_Smooth) as mo_Smooth
                   from outcome_o
                   group by product_brand, month, rtm")

outcome <- sqldf(" SELECT distinct s.platform_division_code, s.product_brand, s.market10, s.developed_emerging, s.rtm, s.month
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
               --, CASE --WHEN o.mo_Smooth is not null then o.mo_Smooth
                       --WHEN r.mo_Smooth is not null then r.mo_Smooth
                       --WHEN a.mo_Smooth is not null then a.mo_Smooth
                       --WHEN b.mo_Smooth is not null then b.mo_Smooth
                       --WHEN c.mo_Smooth is not null then c.mo_Smooth
                       --WHEN d.mo_Smooth is not null then d.mo_Smooth
                       --WHEN e.mo_Smooth is not null then e.mo_Smooth
                       --WHEN f.mo_Smooth is not null then f.mo_Smooth
                       --ELSE NULL
                      -- END as mo_Smooth
                , CASE WHEN o.b1 is not null then 'SELF'
                       WHEN r.b1 is not null then 'REG5'
                       WHEN a.b1 is not null then 'NOMKT'
                       WHEN b.b1 is not null then 'NOCS'
                       WHEN c.b1 is not null then 'NOPL'
                       WHEN d.b1 is not null then 'NOMC'
                       WHEN e.b1 is not null then 'NOPFC'
                       WHEN f.b1 is not null then 'NOPLMKT'
                       WHEN w.b1 is not null then 'NODE'
                       ELSE NULL
                  END AS src

                from stratjn s

                left join outcome_o o
                on s.platform_division_code=o.platform_division_code and s.product_brand=o.product_brand  
                  and s.market10=o.market10 and s.developed_emerging=o.developed_emerging and s.month=o.month and s.rtm=o.rtm
                left join outcome_r r
                on s.platform_division_code=r.platform_division_code and s.product_brand=r.product_brand  
                  and s.developed_emerging=r.developed_emerging and s.month=r.month and s.region_5=r.region_5 and s.rtm=r.rtm
                left join outcome_a a
                on s.platform_division_code=a.platform_division_code and s.product_brand=a.product_brand 
                  and s.developed_emerging=a.developed_emerging and s.month=a.month and s.rtm=a.rtm
                left join outcome_b b
                on s.platform_division_code=b.platform_division_code 
                  and s.market10=b.market10 and s.developed_emerging=b.developed_emerging and s.month=b.month and s.rtm=b.rtm
                left join outcome_c c
                on s.product_brand=c.product_brand 
                  and s.market10=c.market10 and s.developed_emerging=c.developed_emerging and s.month=c.month and s.rtm=c.rtm
                left join outcome_d d
                on s.product_brand=d.product_brand  
                  and s.market10=d.market10 and s.developed_emerging=d.developed_emerging and s.month=d.month and s.rtm=d.rtm
                left join outcome_e e
                on s.platform_division_code=e.platform_division_code and s.product_brand=e.product_brand 
                  and s.market10=e.market10 and s.developed_emerging=e.developed_emerging and s.month=e.month and s.rtm=e.rtm
                left join outcome_f f
                on s.product_brand=f.product_brand 
                  and s.developed_emerging=f.developed_emerging and s.month=f.month and s.rtm=f.rtm
                left join outcome_w w
                on s.product_brand=w.product_brand 
                  and s.month=w.month and s.rtm=w.rtm
                 ")
outcome$strata <- apply( outcome[ , cols ] , 1 , paste , collapse = "_" )

  ## What to do when decay is not negative ###  
# plot(outcome$b1)
  ## What to do when decay is not negative ###

# COMMAND ----------

# Step 9 - creating decay matrix

# Create a decay matrix by extracting "b1", specific to each strata (i.e. the combination 
# of EP, CM, and Region_Cd), from the extended MUT dataset.

decay <- sqldf('
               select platform_division_code, product_brand, market10, developed_emerging, rtm, strata, avg(b1) as b1 from outcome 
               group by platform_division_code, product_brand, market10, developed_emerging, rtm, strata
               ')

mkt10lst <- sqldf("SELECT distinct market10,SUBSTR(developed_emerging,1,1) as developed_emerging from decay where market10 is not null")
pltbrlst <- sqldf("SELECT distinct platform_division_code,product_brand,rtm from decay")
lstjn <- sqldf("SELECT a.*, b.* from pltbrlst a left join mkt10lst b")

decay2 <- sqldf('select a.platform_division_code, a.product_brand, a.market10, a.developed_emerging, a.rtm, b.strata, b.b1 
                from lstjn a
                left join decay b
                on a.market10=b.market10 and a.developed_emerging=b.developed_emerging and a.platform_division_code=b.platform_division_code
                  and a.product_brand=b.product_brand and a.rtm=b.rtm
               order by a.platform_division_code, a.product_brand, a.market10, a.developed_emerging, a.rtm
               ')
cols <- c( 'platform_division_code', 'product_brand', 'market10' ,'developed_emerging', 'rtm')

# create a new column `x` with the four columns collapsed together
decay2$strata <- apply( decay2[ , cols ] , 1 , paste , collapse = "_" )
decay2$b1 <- ifelse(is.na(decay2$b1),minslp,decay2$b1)

decay<- decay2

str(decay)

# COMMAND ----------

# Step 10 - Query to create Usage output in de-normalized form

# This is the second big query, will be executed from R on Vertica through ODBC connectivity. 
# The query is to create Usage Matrix in de-normalized form. The query may look similar to the query 
# mentioned in Step1, but it is not. Unlike Step1, this query is considering entire available time 
# period and each of the 5 Sources available: IS, RM, PE, WJ and JA.

#since changing to Cumulus, and sources are no longer separate, zero is not limited to time (just the values zero$pMPV and zero$pN are), so is not different anymore
#zeroa <- zero
#zeroa$product_brand <- ifelse(zeroa$printer_platform_name %in% c('MALBEC','MANHATTAN','WEBER BASE'),"OJP",zeroa$product_brand)  #These are listed as both OJ and OJP

usage <- sqldf("select
  printer_platform_name
, printer_region_code, market10, developed_emerging, country_alpha2,rtm, FYearMo, platform_division_code, product_brand
, MAX(MPVa) AS MPVa
, SUM(sumn) AS Na
from zero
group by 1=1
, printer_platform_name
, printer_region_code, market10, developed_emerging, country_alpha2, rtm, FYearMo, platform_division_code, product_brand
order by printer_platform_name, printer_region_code, market10, developed_emerging, country_alpha2, rtm, FYearMo, platform_division_code, product_brand")
head(usage)
colnames(usage)
dim(usage)
str(usage)

#duphw1 <- sqldf("select distinct printer_platform_name,product_brand from usage")
#duphw <- duphw1[which(duplicated(duphw1[,c('printer_platform_name')])==T),]


usage_platform <- sqldf('select distinct printer_platform_name, printer_region_code, rtm from usage order by printer_platform_name, printer_region_code')
usage_platform$source2 <- "TRI_PRINTER_USAGE_SN"
usage_platformList <- reshape2::dcast(usage_platform, printer_platform_name~printer_region_code+rtm, value.var="source2")


# ----- no. of Plat_Nm found 209 in dataset "usage" -------------------#

# COMMAND ----------

# Step 11 - Calculating sum of N for each of the 5 Sources

# Based on Usage Matrix (outcome of Step 10), Calculate sum of N for each of 
# the 5 Sources for all groups defined by Plat_Nm and Region_Cd.
iblist <- sqldf("select distinct ib.platform_subset as printer_platform_name, ib.rtm, ci.country_alpha2, ci.region_5 as printer_region_code, ci.market10, substr(ci.developed_emerging,1,1) as developed_emerging, platform_division_code
                from ibtable ib left join country_info ci
                on ib.country=ci.country_alpha2")

u2 <- sqldf('with prc as (select printer_platform_name, printer_region_code, platform_division_code, developed_emerging, rtm, FYearMo
            , sum(Na) as SNA
            from usage
            group by printer_platform_name, printer_region_code, platform_division_code, developed_emerging, rtm, FYearMo
            order by printer_platform_name, printer_region_code, rtm,FYearMo
            )
            ,prc2 as (select printer_platform_name, printer_region_code, platform_division_code, rtm
            , max(SNA) as SNA
            from prc
            group by printer_platform_name, printer_region_code, platform_division_code,rtm
            order by printer_platform_name, printer_region_code,rtm
            )
            ,dem as (select printer_platform_name, printer_region_code, market10, developed_emerging, platform_division_code, rtm, FYearMo
            , sum(Na) as SNA
            from usage
            group by printer_platform_name, printer_region_code, market10, developed_emerging, platform_division_code, rtm, FYearMo
            order by printer_platform_name, printer_region_code, market10, developed_emerging, rtm, FYearMo
            )
            ,dem2 as (select printer_platform_name, printer_region_code, market10, developed_emerging, platform_division_code, rtm
            , MAX(SNA) as SNA
            from dem
            group by printer_platform_name, printer_region_code, market10, developed_emerging, platform_division_code,rtm
            order by printer_platform_name, printer_region_code, market10, developed_emerging,rtm
            )
            ,mkt10 as (
            select printer_platform_name, market10, FYearMo, platform_division_code,rtm
            , sum(na) as SNA
            from usage
            group by printer_platform_name, market10, FYearMo,platform_division_code,rtm
            order by printer_platform_name, market10, FYearMo,rtm
            )
            ,mkt102 as (
            select printer_platform_name, market10,platform_division_code,rtm
            , max(SNA) as SNA
            from mkt10
            group by printer_platform_name, market10,platform_division_code,rtm
            order by printer_platform_name, market10,rtm
            )
            ,ctry as (select printer_platform_name, country_alpha2, FYearMo,platform_division_code,rtm
            , sum(na) as SNA
            from usage
            group by printer_platform_name, country_alpha2, FYearMo,platform_division_code,rtm
            order by printer_platform_name, country_alpha2, FYearMo,rtm
            )
            ,ctry2 as (select printer_platform_name, country_alpha2,platform_division_code,rtm
            , max(SNA) as SNA
            from ctry
            group by printer_platform_name, country_alpha2,platform_division_code,rtm
            order by printer_platform_name, country_alpha2,rtm
            )
            select distinct a.printer_platform_name,a.rtm, a.printer_region_code, a.developed_emerging, a.platform_division_code, a.market10, a.country_alpha2
            , b.SNA as prcN, d.SNA as mktN, c.SNA as demN, e.SNA as ctyN
            from iblist a
            left join prc2 b
            on a.printer_platform_name=b.printer_platform_name and a.printer_region_code=b.printer_region_code and a.rtm=b.rtm and a.platform_division_code=b.platform_division_code
            left join dem2 c
            on a.printer_platform_name=c.printer_platform_name and a.printer_region_code=c.printer_region_code and a.developed_emerging=c.developed_emerging
              and a.market10=c.market10 and a.rtm=c.rtm and a.platform_division_code=c.platform_division_code
            left join mkt102 d
            on a.printer_platform_name=d.printer_platform_name and a.market10=d.market10 and a.rtm=d.rtm and a.platform_division_code=d.platform_division_code
            left join ctry2 e
            on a.printer_platform_name=e.printer_platform_name and a.country_alpha2=e.country_alpha2 and a.rtm=e.rtm and a.platform_division_code=e.platform_division_code
            ')

str(u2)

# COMMAND ----------

# Step 12 - Finding the reference Source

# Based on the values of SNIS, SNRM, SNPE, SNWJ and SNJA, find the Reference Source 
# variable created "Source_vlook". The following logic was implemented to create Source_vlook 
# = "PE" when SNPE >= 200, = "IS", when (SNPE is NULL and SNIS >=200) or (SNPE < 200 and SNIS >=200), else = "RM"

sourceR <- sqldf("
                 select ib.printer_platform_name, ib.rtm, ib.printer_region_code, ib.developed_emerging, ib.market10, ib.country_alpha2, ib.platform_division_code,
                 COALESCE(u2.prcN,0) as prcN, COALESCE(u2.mktN,0) as mktN, COALESCE(u2.demN,0) as demN,COALESCE(u2.ctyN,0) as ctyN
                 , case 
                 when ctyN >= 200 then 'COUNTRY'
                 when demN >= 200 then 'DEV/EM'
                 when mktN >= 200 then 'MARKET10'
                 when prcN >= 200 then 'REGION5'
                 else 'NONE'
                 end as Source_vlook
                 from iblist ib
                 left join u2
                 on ib.printer_platform_name=u2.printer_platform_name and ib.printer_region_code=u2.printer_region_code and ib.market10=u2.market10 and ib.country_alpha2=u2.country_alpha2 and UPPER(ib.rtm)=UPPER(u2.rtm)
                 and ib.platform_division_code=u2.platform_division_code
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
                aa1.country_alpha2 = aa2.country_alpha2
                and
                upper(aa1.rtm)=upper(aa2.rtm)
                ORDER BY
                printer_platform_name, country_alpha2, fyearmo, platform_division_code, product_brand, rtm')

# COMMAND ----------

# Step 14 - Create MPV and N that would be finally worked upon

# Using the outcome of Step 13, Create MPV and N that would be finally worked upon. 
#usage3 <- sqldf('select * , MPVa as MPV
#                , Na as N
#                from usage2
#                ORDER BY
#                printer_platform_name, printer_region_code, fyearmo, platform_division_code, product_brand, rtm')

usage3 <- sqldf("with reg as (
                select printer_platform_name, rtm, printer_region_code, fyearmo,platform_division_code, product_brand, avg(MPVa) as rMPV
                 , sum(Na) as SumN
                from usage2
                GROUP BY printer_platform_name, rtm, printer_region_code, fyearmo, platform_division_code, product_brand
                ORDER BY
                printer_platform_name, rtm, printer_region_code, fyearmo, platform_division_code, product_brand
                )
                ,mkt as (
                select printer_platform_name, rtm, market10, fyearmo,platform_division_code, product_brand, avg(MPVa) as mMPV
                 , sum(Na) as SumN
                from usage2
                GROUP BY printer_platform_name, rtm, market10, fyearmo,platform_division_code, product_brand
                ORDER BY
                printer_platform_name, rtm, market10, fyearmo,platform_division_code, product_brand
                )
                ,de as (
                select printer_platform_name, rtm, market10, developed_emerging, fyearmo,platform_division_code, product_brand, avg(MPVa) as dMPV
                 , sum(Na) as SumN
                from usage2
                GROUP BY printer_platform_name, rtm, market10, developed_emerging, fyearmo,platform_division_code, product_brand
                ORDER BY
                printer_platform_name, rtm, market10, developed_emerging, fyearmo,platform_division_code, product_brand
                )

                select a.* , 
                  CASE 
                    WHEN Source_vlook = 'REGION5' THEN b.rMPV
                    WHEN Source_vlook = 'MARKET10' THEN c.mMPV
                    WHEN Source_vlook = 'DEV/EM' THEN d.dMPV
                    WHEN Source_vlook = 'COUNTRY' THEN a.MPVa
                    ELSE 0
                    END as MPV
                  ,CASE 
                    WHEN Source_vlook = 'REGION5' THEN b.SumN
                    WHEN Source_vlook = 'MARKET10' THEN c.SumN
                    WHEN Source_vlook = 'DEV/EM' THEN d.SumN
                    WHEN Source_vlook = 'COUNTRY' THEN a.Na 
                    ELSE 0
                    END as N
                from usage2 a
                LEFT JOIN reg b on a.printer_platform_name=b.printer_platform_name and a.printer_region_code=b.printer_region_code and a.fyearmo=b.fyearmo and a.rtm=b.rtm
                LEFT JOIN mkt c on a.printer_platform_name=c.printer_platform_name and a.market10=c.market10 and a.fyearmo=c.fyearmo and a.rtm=c.rtm
                LEFT JOIN de d on a.printer_platform_name=d.printer_platform_name and a.market10=d.market10 and a.developed_emerging=d.developed_emerging and a.rtm=d.rtm 
                    and a.fyearmo=d.fyearmo
                ORDER BY
                printer_platform_name, rtm, platform_division_code, printer_region_code, country_alpha2, fyearmo
                ")

# COMMAND ----------

# Step 15 - Combining Usage and Decay matrices

# Combine Usage (outcome of Step 14) and Decay (outcome of Step 9) matrices by using Region_Cd, VV, CM and SM.

usage4<-sqldf('select aa1.printer_platform_name, aa1.printer_region_code, aa1.country_alpha2, aa1.market10, aa1.developed_emerging, aa1.rtm, aa1.product_brand, aa1.fyearmo, aa1.MPV, aa1.N
  , aa2.b1, aa1.platform_division_code
              from 
              usage3 aa1 
              inner join
              decay aa2
              on
              aa1.market10 = aa2.market10
              and
              aa1.platform_division_code = aa2.platform_division_code
              and
              aa1.product_brand = aa2.product_brand
              and
              aa1.rtm=aa2.rtm
              and 
              aa1.developed_emerging=aa2.developed_emerging
              and 
              aa1.platform_division_code=aa2.platform_division_code
              order by aa1.printer_platform_name, aa1.printer_region_code, aa1.fyearmo, aa1.rtm
              ' )

# COMMAND ----------

# Step 16 - extracting Intro year for a given Platform n region

introYear <- sqldf(" SELECT a.platform_subset as printer_platform_name, a.rtm
                          , a.Region_5 AS printer_region_code
                          , b.country_alpha2
                          , b.market10
                          , SUBSTR(b.developed_emerging,1,1) as developed_emerging
                          , min(a.month_begin) as Intro_FYearMo
                    FROM ibtable a
                    left join country_info b
                      on a.country=b.country_alpha2
                    GROUP BY a.platform_subset, a.rtm
                          , a.Region_5
                          , b.country_alpha2
                          , b.market10
                          , b.developed_emerging
                   ")

introYear$Source <- "FL_Installed_Base"
head(introYear)
colnames(introYear)
dim(introYear)

introYear2a <- sqldf('select printer_platform_name, rtm, min(intro_fyearmo) as minYear
                    FROM introYear 
                    GROUP BY printer_platform_name, rtm')
introYear2b <- sqldf('select printer_platform_name, rtm, printer_region_code, min(intro_fyearmo) as minYear
                    FROM introYear 
                    GROUP BY printer_platform_name, rtm, printer_region_code')
introYear2c <- sqldf('select printer_platform_name, rtm, market10, min(intro_fyearmo) as minYear
                    FROM introYear 
                    GROUP BY printer_platform_name, rtm, market10')
introYear2d <- sqldf('select printer_platform_name, rtm, market10, developed_emerging, min(intro_fyearmo) as minYear
                    FROM introYear 
                    GROUP BY printer_platform_name, rtm, market10, developed_emerging')

introYear3 <- sqldf('select aa1.printer_platform_name, UPPER(aa1.rtm) as rtm
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
                      on aa1.printer_platform_name = aa2.printer_platform_name and aa1.rtm=aa2.rtm
                    left join introYear2b aa3
                      on aa1.printer_platform_name = aa3.printer_platform_name and aa1.printer_region_code=aa3.printer_region_code and aa1.rtm=aa3.rtm
                    left join introYear2c aa4
                      on aa1.printer_platform_name = aa4.printer_platform_name and aa1.market10=aa4.market10 and aa1.rtm=aa4.rtm
                    left join introYear2d aa5
                      on aa1.printer_platform_name = aa5.printer_platform_name and aa1.market10=aa5.market10 and aa1.developed_emerging=aa5.developed_emerging and aa1.rtm=aa5.rtm
                    ')

# COMMAND ----------

# Step 17 -  Creation of Extended Usage Matrix

usage5 <- sqldf('select aa1.*, 
                  CASE WHEN aa2.minYear is not null THEN aa2.minYear
                    ELSE null
                    END AS Intro_FYearMo 
                  FROM usage4 aa1 
                  LEFT JOIN introYear2a aa2
                  ON aa1.printer_platform_name=aa2.printer_platform_name and upper(aa1.rtm)=upper(aa2.rtm)
                  
                  order by aa1.printer_platform_name, aa1.country_alpha2, aa1.product_brand, aa1.FYearMo, aa1.rtm
                  ')

  nodt <- subset(usage5,is.na(Intro_FYearMo))
  usage5 <- subset(usage5,!is.na(Intro_FYearMo))
  
  usage5 <- subset(usage5,MPV>0)
  
  str(usage5)
  usage5$printer_platform_name <- factor(usage5$printer_platform_name)
  str(usage5)
  
  usage5$RFYearMo <- (as.numeric(substr(usage5$fyearmo, 1,4)))*1 + (as.numeric(substr(usage5$fyearmo, 5,6))-1)/12
  usage5$RFIntroYear <- (as.numeric(substr(usage5$Intro_FYearMo, 1,4)))*1 + (as.numeric(substr(usage5$Intro_FYearMo, 5,6))-1)/12
  usage5$MoSI <- (usage5$RFYearMo-usage5$RFIntroYear)*12
  usage5$NMoSI	<- usage5$N*usage5$MoSI
  usage5$LnMPV	<- log(usage5$MPV)
  usage5$NLnMPV <- usage5$N*usage5$LnMPV

# COMMAND ----------

# Step 18 -  Creation of Usage Summary Matrix

# Using extended Usage Matrix (outcome of Step 17), for every combinations of Plat_Nm and Region_Cd, 
# the following variables will be created;

usageSummary <- sqldf('select printer_platform_name, rtm, product_brand, country_alpha2
                      , sum(NMoSI) as SumNMOSI
                      , sum(N) as SumN
                      , sum(NLnMPV) as SumNLNMPV
                      from usage5
                      group by printer_platform_name, rtm, product_brand, country_alpha2
                      order by printer_platform_name, rtm, product_brand, country_alpha2
                      ')

usageSummary$x <- usageSummary$SumNMOSI/usageSummary$SumN
usageSummary$y <- usageSummary$SumNLNMPV/usageSummary$SumN


usageSummaryMkt <- sqldf('select printer_platform_name, rtm, product_brand, market10
                      , sum(NMoSI) as SumNMOSI
                      , sum(N) as SumN
                      , sum(NLnMPV) as SumNLNMPV
                      from usage5
                      group by printer_platform_name, rtm, product_brand, market10
                      order by printer_platform_name, rtm, product_brand, market10
                      ')

usageSummaryMkt$x <- usageSummaryMkt$SumNMOSI/usageSummaryMkt$SumN
usageSummaryMkt$y <- usageSummaryMkt$SumNLNMPV/usageSummaryMkt$SumN

usageSummaryDE <- sqldf('select printer_platform_name, rtm, product_brand, market10, developed_emerging
                      , sum(NMoSI) as SumNMOSI
                      , sum(N) as SumN
                      , sum(NLnMPV) as SumNLNMPV
                      from usage5
                      group by printer_platform_name, rtm, product_brand, market10, developed_emerging
                      order by printer_platform_name, rtm, product_brand, market10, developed_emerging
                      ')

usageSummaryDE$x <- usageSummaryDE$SumNMOSI/usageSummaryDE$SumN
usageSummaryDE$y <- usageSummaryDE$SumNLNMPV/usageSummaryDE$SumN

usageSummaryR5 <- sqldf('select printer_platform_name, rtm, product_brand, printer_region_code
                      , sum(NMoSI) as SumNMOSI
                      , sum(N) as SumN
                      , sum(NLnMPV) as SumNLNMPV
                      from usage5
                      group by printer_platform_name, rtm, product_brand, printer_region_code
                      order by printer_platform_name, rtm, product_brand, printer_region_code
                      ')

usageSummaryR5$x <- usageSummaryR5$SumNMOSI/usageSummaryR5$SumN
usageSummaryR5$y <- usageSummaryR5$SumNLNMPV/usageSummaryR5$SumN

# COMMAND ----------

# Step 19 -  decay matrix for every combinations of Plat_Nm and Region_Cd

# Using extended Usage Matrix (outcome of Step 17), for every combinations of Plat_Nm and Region_Cd, 
# extract respective "b1" or the decay rate.

decay2 <- sqldf('select distinct printer_platform_name, country_alpha2, product_brand, rtm, b1 from usage5 order by printer_platform_name, country_alpha2')
decay2Mkt <- sqldf('select printer_platform_name, market10, product_brand, rtm, sum(b1*N)/sum(N) as b1 from usage5 group by printer_platform_name, market10, product_brand, rtm')
decay2DE <- sqldf('select printer_platform_name, market10,developed_emerging,product_brand, rtm, sum(b1*N)/sum(N) as b1 from usage5 group by printer_platform_name, market10,developed_emerging,product_brand, rtm')
decay2R5 <- sqldf('select printer_platform_name, printer_region_code, product_brand, rtm, sum(b1*N)/sum(N) as b1 from usage5 group by printer_platform_name, printer_region_code, product_brand, rtm')

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
                       and 
                       aa1.product_brand=aa2.product_brand
                       and
                       aa1.rtm=aa2.rtm
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
                       and 
                       aa1.product_brand=aa2.product_brand
                       and
                       aa1.rtm=aa2.rtm
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
                       and 
                       aa1.product_brand=aa2.product_brand
                       and
                       aa1.rtm=aa2.rtm
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
                       and 
                       aa1.product_brand=aa2.product_brand
                       and
                       aa1.rtm=aa2.rtm
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


usageSummary2$iMPV <- exp(usageSummary2$y -(usageSummary2$x*usageSummary2$b1/12))

#write.csv(paste(mainDir,subDir1,"/","usageSummary2_iMPV_all available Platforms",".csv", sep=''), x=usageSummary2,row.names=FALSE, na="")


usageSummary2Drop <- usageSummary2[which(usageSummary2$SumN <50),]
usageSummary2Drop$printer_platform_name <- factor(usageSummary2Drop$printer_platform_name)

usageSummary2Keep <- usageSummary2[which(usageSummary2$SumN >=50),]
usageSummary2Keep$printer_platform_name <- factor(usageSummary2Keep$printer_platform_name)

# COMMAND ----------

# Step 21 -  De-normalizing Initial MPV matrix

#usageSummary2T <- dcast(usageSummary2Keep, printer_platform_name+product_brand+rtm ~printer_region_code, value.var="iMPV")
usageSummary2Tctry <- reshape2::dcast(usageSummary2Keep, printer_platform_name+product_brand+rtm ~country_alpha2, value.var="iMPV",fun.aggregate = mean)
usageSummary2Tmkt10 <- reshape2::dcast(usageSummary2MktKeep, printer_platform_name+product_brand+rtm ~market10, value.var="iMPV",fun.aggregate = mean)
usageSummary2Tde <- reshape2::dcast(usageSummary2DEKeep, printer_platform_name+product_brand+rtm ~market10+developed_emerging, value.var="iMPV",fun.aggregate = mean)
usageSummary2Tr5 <- reshape2::dcast(usageSummary2R5Keep, printer_platform_name+product_brand+rtm ~printer_region_code, value.var="iMPV",fun.aggregate = mean)

# COMMAND ----------

# Step 22 - Extracting Platforms which have iMPVs calculated for NA and for at least for one more Region

# Extract Platforms which have iMPVs calculated for North America (NA) and for at least one more Region 
# and having a sample size of at least 50. Initially, iMPVs could be calculated for 201 platforms, however, 
# the sample size restriction reduces the platform counts to 187. Further, the restriction on platform with 
# iMPV populated for NA and for at least one other region, reduces the platform counts to 148. 
# These selected 148 platforms will be used to calculate regional coefficients.

# For each of the 148 platforms, create weights by dividing the iMPVs of the 4 regions 
# (i.e. EU, AP, LA and JP) by the iMPV of NA.
strata <- unique(usage[c("platform_division_code", "product_brand", "printer_platform_name", "rtm")])

  usageSummary2TEr5 <- sqldf('select aa1.printer_platform_name, aa1.product_brand, aa2.platform_division_code, aa1.rtm, aa1.NA, aa1.EU, aa1.AP, aa1.LA
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

usageSummary2TEmkt10 <- sqldf('select aa1.printer_platform_name, aa1.product_brand, aa2.platform_division_code, aa1.rtm, aa1.[Central Europe], aa1.[Greater Asia], aa1.[Greater China], aa1.[India SL & BL], aa1.[ISE], aa1.[Latin America], aa1.[North America], aa1.[Northern Europe], aa1.[Southern Europe], aa1.[UK&I]
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

usageSummary2TEde <- sqldf("select aa1.printer_platform_name, aa1.product_brand, aa2.platform_division_code, aa1.rtm,aa1.[Central Europe_D],aa1.[Central Europe_E]
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

usageSummary2TE <- sqldf("select a.[printer_platform_name], a.product_brand, a.platform_division_code, a.rtm
,a.[NA],a.[EU],a.[AP],a.[LA] ,b.[Central Europe],b.[Greater Asia],b.[Greater China],b.[India SL & BL],b.[ISE],b.[Latin America],b.[North America]
,b.[Northern Europe],b.[Southern Europe],b.[UK&I]
,a.[NA_wt],a.[EU_wt],a.[AP_wt],a.[LA_wt],a.[NA_wt2] ,a.[EU_wt2],a.[AP_wt2],a.[LA_wt2],a.[NA_wt3] ,a.[EU_wt3],a.[AP_wt3],a.[LA_wt3] ,b.[North America_wt],b.[Central Europe_wt],b.[Greater Asia_wt],b.[Greater China_wt],b.[India_wt],b.[ISE_wt]    ,b.[Latin America_wt],b.[Northern Europe_wt],b.[Southern Europe_wt],b.[UK&I_wt],b.[North America_wt2],b.[Central Europe_wt2],b.[Greater Asia_wt2],b.[Greater China_wt2] ,b.[India_wt2],b.[ISE_wt2],b.[Latin America_wt2],b.[Northern Europe_wt2],b.[Southern Europe_wt2],b.[UK&I_wt2],b.[North America_wt3],b.[Central Europe_wt3]
,b.[Greater Asia_wt3],b.[Greater China_wt3],b.[India_wt3],b.[ISE_wt3],b.[Latin America_wt3],b.[Northern Europe_wt3],b.[Southern Europe_wt3],b.[UK&I_wt3]            ,b.[North America_wt4],b.[Central Europe_wt4],b.[Greater Asia_wt4],b.[Greater China_wt4],b.[India_wt4],b.[ISE_wt4],b.[Latin America_wt4],b.[Northern Europe_wt4] ,b.[Southern Europe_wt4],b.[UK&I_wt4],b.[North America_wt5],b.[Central Europe_wt5],b.[Greater Asia_wt5],b.[Greater China_wt5],b.[India_wt5],b.[ISE_wt5]             ,b.[Latin America_wt5],b.[Northern Europe_wt5],b.[Southern Europe_wt5],b.[UK&I_wt5],b.[North America_wt6],b.[Central Europe_wt6],b.[Greater Asia_wt6]
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
                        on a.printer_platform_name=b.printer_platform_name and a.product_brand=b.product_brand and a.rtm=b.rtm
                       left join usageSummary2TEde c
                        on a.printer_platform_name=c.printer_platform_name and a.product_brand=c.product_brand and a.rtm=c.rtm

")

# COMMAND ----------

wtaverage <- sqldf('select platform_division_code, rtm, product_brand
                   , avg(EU_wt) as EUa
                   , avg([Central Europe_wt]) as Central_Europena
                   , avg([Central Europe_D_wt]) as Central_Europe_Dna
                   , avg([Central Europe_E_wt]) as Central_Europe_Ena
                   , avg([Northern Europe_wt]) as Northern_Europena   --dont need to split as only D
                   , avg([Southern Europe_wt]) as Southern_Europena
                   , avg([ISE_wt]) as ISEna
                   , avg([UK&I_wt]) as UKIna
                   , avg([Greater Asia_wt]) as Greater_Asiana
                   , avg([Greater Asia_D_wt]) as Greater_Asia_Dna
                   , avg([Greater Asia_E_wt]) as Greater_Asia_Ena
                   , avg([Greater China_wt]) as Greater_Chinana
                   , avg([Greater China_D_wt]) as Greater_China_Dna
                   , avg([Greater China_E_wt]) as Greater_China_Ena
                   , avg([India_wt]) as Indiana
                   , avg([Latin America_wt]) as Latin_Americana
                   --
                   , avg([North America_wt2]) as North_Americace
                   , avg([Central Europe_E_wt2]) as Central_Europe_Eced
                   , avg([Central Europe_D_wt3]) as Central_Europe_Dcee
                   , avg([Northern Europe_wt2]) as Northern_Europece
                   , avg([Southern Europe_wt2]) as Southern_Europece
                   , avg([ISE_wt2]) as ISEce
                   , avg([UK&I_wt2]) as UKIce
                   , avg([Greater Asia_wt2]) as Greater_Asiace
                   , avg([Greater Asia_D_wt2]) as Greater_Asia_Dced
                   , avg([Greater Asia_E_wt2]) as Greater_Asia_Eced
                   , avg([Greater Asia_D_wt3]) as Greater_Asia_Dcee
                   , avg([Greater Asia_E_wt3]) as Greater_Asia_Ecee
                   , avg([Greater China_wt2]) as Greater_Chinace
                   , avg([Greater China_D_wt2]) as Greater_China_Dced
                   , avg([Greater China_E_wt2]) as Greater_China_Eced
                   , avg([Greater China_D_wt3]) as Greater_China_Dcee
                   , avg([Greater China_E_wt3]) as Greater_China_Ecee
                   , avg([India_wt2]) as Indiace
                   , avg([Latin America_wt2]) as Latin_Americace
                   --
                   , avg([Central Europe_wt3]) as Central_Europega
                   , avg([Central Europe_E_wt4]) as Central_Europe_Egad
                   , avg([Central Europe_D_wt4]) as Central_Europe_Dgad
                   , avg([Central Europe_E_wt5]) as Central_Europe_Egae
                   , avg([Central Europe_D_wt5]) as Central_Europe_Dgae
                   , avg([Northern Europe_wt3]) as Northern_Europega
                   , avg([Southern Europe_wt3]) as Southern_Europega
                   , avg([ISE_wt3]) as ISEga
                   , avg([UK&I_wt3]) as UKIga
                   , avg([North America_wt3]) as North_Americaga
                   , avg([Greater Asia_D_wt5]) as Greater_Asia_Dgae
                   , avg([Greater Asia_E_wt4]) as Greater_Asia_Egad
                   , avg([Greater China_wt3]) as Greater_Chinaga
                   , avg([Greater China_D_wt4]) as Greater_China_Dgad
                   , avg([Greater China_E_wt4]) as Greater_China_Egad
                   , avg([Greater China_D_wt5]) as Greater_China_Dgae
                   , avg([Greater China_E_wt5]) as Greater_China_Egae
                   , avg([India_wt3]) as Indiaga
                   , avg([Latin America_wt3]) as Latin_Americaga
                   --
                   , avg([Central Europe_wt4]) as Central_Europegc
                   , avg([Central Europe_E_wt6]) as Central_Europe_Egcd
                   , avg([Central Europe_D_wt6]) as Central_Europe_Dgcd
                   , avg([Central Europe_E_wt7]) as Central_Europe_Egce
                   , avg([Central Europe_D_wt7]) as Central_Europe_Dgce
                   , avg([Northern Europe_wt4]) as Northern_Europegc
                   , avg([Southern Europe_wt4]) as Southern_Europegc
                   , avg([ISE_wt4]) as ISEgc
                   , avg([UK&I_wt4]) as UKIgc
                   , avg([Greater Asia_wt4]) as Greater_Asiagc
                   , avg([Greater Asia_D_wt6]) as Greater_Asia_Dgcd
                   , avg([Greater Asia_E_wt6]) as Greater_Asia_Egcd
                   , avg([Greater Asia_D_wt7]) as Greater_Asia_Dgce
                   , avg([Greater Asia_E_wt7]) as Greater_Asia_Egce
                   , avg([Greater China_D_wt7]) as Greater_China_Dgce
                   , avg([Greater China_E_wt6]) as Greater_China_Egcd
                   , avg([North America_wt4]) as North_Americagc
                   , avg([India_wt4]) as Indiagc
                   , avg([Latin America_wt4]) as Latin_Americagc
                   --
                   , avg([Central Europe_wt5]) as Central_Europeia
                   , avg([Central Europe_E_wt8]) as Central_Europe_Eia
                   , avg([Central Europe_D_wt8]) as Central_Europe_Dia
                   , avg([Northern Europe_wt5]) as Northern_Europeia
                   , avg([Southern Europe_wt5]) as Southern_Europeia
                   , avg([ISE_wt5]) as ISEia
                   , avg([UK&I_wt5]) as UKIia
                   , avg([Greater Asia_wt5]) as Greater_Asiaia
                   , avg([Greater Asia_D_wt8]) as Greater_Asia_Dia
                   , avg([Greater Asia_E_wt8]) as Greater_Asia_Eia
                   , avg([Greater China_wt5]) as Greater_Chinaia
                   , avg([Greater China_D_wt8]) as Greater_China_Dia
                   , avg([Greater China_E_wt8]) as Greater_China_Eia
                   , avg([North America_wt5]) as North_Americaia
                   , avg([Latin America_wt5]) as Latin_Americaia
                   --
                   , avg([Central Europe_wt6]) as Central_Europeis
                   , avg([Central Europe_E_wt9]) as Central_Europe_Eis
                   , avg([Central Europe_D_wt9]) as Central_Europe_Dis
                   , avg([Northern Europe_wt6]) as Northern_Europeis
                   , avg([Southern Europe_wt6]) as Southern_Europeis
                   , avg([North America_wt6]) as North_Americais
                   , avg([UK&I_wt6]) as UKIis
                   , avg([Greater Asia_wt6]) as Greater_Asiais
                   , avg([Greater Asia_D_wt9]) as Greater_Asia_Dis
                   , avg([Greater Asia_E_wt9]) as Greater_Asia_Eis
                   , avg([Greater China_wt6]) as Greater_Chinais
                   , avg([Greater China_D_wt9]) as Greater_China_Dis
                   , avg([Greater China_E_wt9]) as Greater_China_Eis
                   , avg([India_wt6]) as Indiais
                   , avg([Latin America_wt6]) as Latin_Americais
                   --
                   , avg([Central Europe_wt7]) as Central_Europela
                   , avg([Central Europe_E_wt10]) as Central_Europe_Ela
                   , avg([Central Europe_D_wt10]) as Central_Europe_Dla
                   , avg([Northern Europe_wt7]) as Northern_Europela
                   , avg([Southern Europe_wt7]) as Southern_Europela
                   , avg([ISE_wt7]) as ISEla
                   , avg([UK&I_wt7]) as UKIla
                   , avg([Greater Asia_wt7]) as Greater_Asiala
                   , avg([Greater Asia_D_wt10]) as Greater_Asia_Dla
                   , avg([Greater Asia_E_wt10]) as Greater_Asia_Ela
                   , avg([Greater China_wt7]) as Greater_Chinala
                   , avg([Greater China_D_wt10]) as Greater_China_Dla
                   , avg([Greater China_E_wt10]) as Greater_China_Ela
                   , avg([India_wt7]) as Indiala
                   , avg([North America_wt7]) as North_Americala
                   --
                   , avg([Central Europe_wt8]) as Central_Europene
                   , avg([Central Europe_E_wt11]) as Central_Europe_Ene
                   , avg([Central Europe_D_wt11]) as Central_Europe_Dne
                   , avg([North America_wt8]) as Nort_Americane
                   , avg([Southern Europe_wt8]) as Southern_Europene
                   , avg([ISE_wt8]) as ISEne
                   , avg([UK&I_wt8]) as UKIne
                   , avg([Greater Asia_wt8]) as Greater_Asiane
                   , avg([Greater Asia_D_wt11]) as Greater_Asia_Dne
                   , avg([Greater Asia_E_wt11]) as Greater_Asia_Ene
                   , avg([Greater China_wt8]) as Greater_Chinane
                   , avg([Greater China_D_wt11]) as Greater_China_Dne
                   , avg([Greater China_E_wt11]) as Greater_China_Ene
                   , avg([India_wt8]) as Indiane
                   , avg([Latin America_wt8]) as Latin_Americane
                   --
                   , avg([Central Europe_wt9]) as Central_Europese
                   , avg([Central Europe_E_wt12]) as Central_Europe_Ese
                   , avg([Central Europe_D_wt12]) as Central_Europe_Dse
                   , avg([Northern Europe_wt9]) as Northern_Europese
                   , avg([North America_wt9]) as North_Americase
                   , avg([ISE_wt9]) as ISEse
                   , avg([UK&I_wt9]) as UKIse
                   , avg([Greater Asia_wt9]) as Greater_Asiase
                   , avg([Greater Asia_D_wt12]) as Greater_Asia_Dse
                   , avg([Greater Asia_E_wt12]) as Greater_Asia_Ese
                   , avg([Greater China_wt9]) as Greater_Chinase
                   , avg([Greater China_D_wt12]) as Greater_China_Dse
                   , avg([Greater China_E_wt12]) as Greater_China_Ese
                   , avg([India_wt9]) as Indiase
                   , avg([Latin America_wt9]) as Latin_Americase
                   --
                   , avg([Central Europe_wt10]) as Central_Europeuk
                   , avg([Central Europe_E_wt13]) as Central_Europe_Euk
                   , avg([Central Europe_D_wt13]) as Central_Europe_Duk
                   , avg([Northern Europe_wt10]) as Northern_Europeuk
                   , avg([Southern Europe_wt10]) as Southern_Europeuk
                   , avg([ISE_wt10]) as ISEuk
                   , avg([North America_wt10]) as North_Americauk
                   , avg([Greater Asia_wt10]) as Greater_Asiauk
                   , avg([Greater Asia_D_wt13]) as Greater_Asia_Duk
                   , avg([Greater Asia_E_wt13]) as Greater_Asia_Euk
                   , avg([Greater China_wt10]) as Greater_Chinauk
                   , avg([Greater China_D_wt13]) as Greater_China_Duk
                   , avg([Greater China_E_wt13]) as Greater_China_Euk
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
                   product_brand is not null 
                   group by platform_division_code,rtm, product_brand')
wtaverage[is.na(wtaverage)] <- 1

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
                              (aa1.rtm = aa2.rtm
                              and 
                              aa1.platform_division_code = aa2.platform_division_code
                              and 
                              aa1.product_brand = aa2.product_brand)
                              order by rtm, product_brand, platform_division_code')

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
                                WHEN NA is not null THEN 'SELF'
                                WHEN NA is null and EU is not null THEN 'EU'
                                WHEN NA is null and EU is null and AP is not null then 'AP'
                                WHEN NA is null and EU is null and AP is null and LA is not null then 'LA'
                                ELSE 'Six' 
                                END as NAroute
                                , CASE
                                WHEN [North America] is not null then 'SELF'
                                WHEN [North America] is null and [UK&I] is not null then 'UK&I'
                                WHEN [North America] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [North America] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [North America] is null and [ISE] is not null then 'ISE'
                                WHEN [North America] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [North America] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [North America] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [North America] is null and [Greater China] is not null then 'GREATER CHINA'
                                WHEN [North America] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as North_AmericaRoute
                                , CASE
                                WHEN [North America_D] is not null then 'SELF'
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
                                WHEN [UK&I] is not null then 'SELF'
                                WHEN [UK&I] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [UK&I] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [UK&I] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [UK&I] is null and [ISE] is not null then 'ISE'
                                WHEN [UK&I] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [UK&I] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [UK&I] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [UK&I] is null and [Greater China] is not null then 'GREATER CHINA'
                                WHEN [UK&I] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as UKIRoute
                                , CASE
                                WHEN [UK&I] is not null then 'SELF'
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
                                WHEN [Northern Europe] is not null then 'SELF'
                                WHEN [Northern Europe] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [Northern Europe] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Northern Europe] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [Northern Europe] is null and [ISE] is not null then 'ISE'
                                WHEN [Northern Europe] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [Northern Europe] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [Northern Europe] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [Northern Europe] is null and [Greater China] is not null then 'GREATER CHINA'
                                WHEN [Northern Europe] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as Northern_EuropeRoute
                                , CASE
                                WHEN [Northern Europe] is not null then 'SELF'
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
                                WHEN [Southern Europe] is not null then 'SELF'
                                WHEN [Southern Europe] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [Southern Europe] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Southern Europe] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [Southern Europe] is null and [ISE] is not null then 'ISE'
                                WHEN [Southern Europe] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [Southern Europe] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [Southern Europe] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [Southern Europe] is null and [Greater China] is not null then 'GREATER CHINA'
                                WHEN [Southern Europe] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as Southern_EuropeRoute
                                , CASE
                                WHEN [Southern Europe] is not null then 'SELF'
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
                                WHEN [ISE] is not null then 'SELF'
                                WHEN [ISE] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [ISE] is null and [UK&I] is not null then 'UK&I'
                                WHEN [ISE] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [ISE] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [ISE] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [ISE] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [ISE] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [ISE] is null and [Greater China] is not null then 'GREATER CHINA'
                                WHEN [ISE] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as ISERoute
                                , CASE
                                WHEN [ISE] is not null then 'SELF'
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
                                WHEN [Central Europe] is not null then 'SELF'
                                WHEN [Central Europe] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [Central Europe] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Central Europe] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [Central Europe] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [Central Europe] is null and [ISE] is not null then 'ISE'
                                WHEN [Central Europe] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [Central Europe] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [Central Europe] is null and [Greater China] is not null then 'GREATER CHINA'
                                WHEN [Central Europe] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as Central_EuropeRoute
                                , CASE
                                WHEN [Central Europe_D] is not null then 'SELF'
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
                                WHEN [Central Europe_E] is not null then 'SELF'
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
                                WHEN AP is not null THEN 'SELF'
                                WHEN AP is null and NA is not null THEN 'NA'
                                WHEN AP is null and NA is null and EU is not null then 'EU'
                                WHEN AP is null and NA is null and EU is null and LA is not null then 'LA'
                                ELSE 'Six'
                                END as AProute
                                , CASE
                                WHEN [India SL & BL] is not null then 'SELF'
                                WHEN [India SL & BL] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [India SL & BL] is null and [UK&I] is not null then 'UK&I'
                                WHEN [India SL & BL] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [India SL & BL] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [India SL & BL] is null and [ISE] is not null then 'ISE'
                                WHEN [India SL & BL] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [India SL & BL] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [India SL & BL] is null and [Greater China] is not null then 'GREATER CHINA'
                                WHEN [India SL & BL] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as IndiaRoute
                                , CASE
                                WHEN [India SL & BL] is not null then 'SELF'
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
                                WHEN [Greater Asia] is not null then 'SELF'
                                WHEN [Greater Asia] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [Greater Asia] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Greater Asia] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [Greater Asia] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [Greater Asia] is null and [ISE] is not null then 'ISE'
                                WHEN [Greater Asia] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [Greater Asia] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [Greater Asia] is null and [Greater China] is not null then 'GREATER CHINA'
                                WHEN [Greater Asia] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as Greater_AsiaRoute
                                , CASE
                                WHEN [Greater Asia_D] is not null then 'SELF'
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
                                WHEN [Greater Asia_E] is not null then 'SELF'
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
                                WHEN [Greater China] is not null then 'SELF'
                                WHEN [Greater China] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [Greater China] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Greater China] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [Greater China] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [Greater China] is null and [ISE] is not null then 'ISE'
                                WHEN [Greater China] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [Greater China] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [Greater China] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [Greater China] is null and [Latin America] is not null then 'LATIN AMERICA'
                                ELSE null
                                END as Greater_ChinaRoute
                                , CASE
                                WHEN [Greater China_D] is not null then 'SELF'
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
                                WHEN [Greater China_E] is not null then 'SELF'
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
                                WHEN LA is not null THEN 'SELF'
                                WHEN LA is null and NA is not null THEN 'NA'
                                WHEN LA is null and NA is null and EU is not null then 'EU'
                                WHEN LA is null and NA is null and EU is null and AP is not null then 'AP'
                                ELSE 'Six' 
                                END as LAroute
                                , CASE
                                WHEN [Latin America] is not null then 'SELF'
                                WHEN [Latin America] is null and [North America] is not null then 'NORTH AMERICA'
                                WHEN [Latin America] is null and [UK&I] is not null then 'UK&I'
                                WHEN [Latin America] is null and [Northern Europe] is not null then 'NORTHERN EUROPE'
                                WHEN [Latin America] is null and [Southern Europe] is not null then 'SOUTHERN EUROPE'
                                WHEN [Latin America] is null and [ISE] is not null then 'ISE'
                                WHEN [Latin America] is null and [India SL & BL] is not null then 'INDIA SL & BL'
                                WHEN [Latin America] is null and [Central Europe] is not null then 'CENTRAL EUROPE'
                                WHEN [Latin America] is null and [Greater Asia] is not null then 'GREATER ASIA'
                                WHEN [Latin America] is null and [Greater China] is not null then 'GREATER CHINA'
                                ELSE null
                                END as Latin_AmericaRoute
                                , CASE
                                WHEN [Latin America] is not null then 'SELF'
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
                              order by rtm, product_brand
                              ")
   
   #Using Market10, can switch to Market10D/E?
      sourceRiMPV <- sqldf("
                       SELECT a.printer_platform_name, a.rtm, a.printer_region_code, a.developed_emerging, a.market10, a.country_alpha2
                        ,CASE WHEN a.Source_vlook = 'COUNTRY'  then b.iMPV
                             WHEN a.Source_vlook = 'DEV/EM'   then c.iMPV
                             WHEN a.Source_vlook = 'MARKET10' then d.iMPV
                             WHEN a.Source_vlook = 'REGION5'  then e.iMPV
                             WHEN a.Source_vlook = 'NONE' then 
                              CASE
                              WHEN a.market10 = 'NORTH AMERICA' then f.North_America2
                              WHEN a.market10 = 'LATIN AMERICA' then f.Latin_America2
                              WHEN a.market10 = 'UK&I' then f.UKI2
                              WHEN a.market10 = 'NORTHERN EUROPE' then f.Northern_Europe2
                              WHEN a.market10 = 'SOUTHERN EUROPE' then f.Southern_Europe2
                              WHEN a.market10 = 'CENTRAL EUROPE' AND a.developed_emerging='D' THEN f.Central_Europe_D2
                              WHEN a.market10 = 'CENTRAL EUROPE' AND a.developed_emerging='E' THEN f.Central_Europe_E2
                              WHEN a.market10 = 'ISE' then f.ISE2
                              WHEN a.market10 = 'GREATER ASIA' AND a.developed_emerging='D' then f.Greater_Asia_D2
                              WHEN a.market10 = 'GREATER ASIA' AND a.developed_emerging='E' then f.Greater_Asia_E2
                              WHEN a.market10 = 'GREATER CHINA' AND a.developed_emerging='D' then f.Greater_China_D2
                              WHEN a.market10 = 'GREATER CHINA' AND a.developed_emerging='E' then f.Greater_China_E2
                              WHEN a.market10 = 'INDIA SL & BL' then f.India2
                              END
                             ELSE null
                             END as iMPV
                          ,CASE 
                             WHEN a.Source_vlook = 'NONE' then 
                              CASE
                              WHEN a.market10 = 'NORTH AMERICA' then f.North_AmericaRoute
                              WHEN a.market10 = 'LATIN AMERICA' then f.Latin_AmericaRoute
                              WHEN a.market10 = 'UK&I' then f.UKIRoute
                              WHEN a.market10 = 'NORTHERN EUROPE' then f.Northern_EuropeRoute
                              WHEN a.market10 = 'SOUTHERN EUROPE' then f.Southern_EuropeRoute
                              WHEN a.market10 = 'CENTRAL EUROPE' then f.Central_EuropeRoute
                              WHEN a.market10 = 'ISE' then f.ISERoute
                              WHEN a.market10 = 'GREATER ASIA' then f.Greater_AsiaRoute
                              WHEN a.market10 = 'GREATER CHINA' then f.Greater_ChinaRoute
                              WHEN a.market10 = 'INDIA SL & BL' then f.IndiaRoute
                              END
                             ELSE Source_vlook
                        END as Route
                        ,CASE 
                             WHEN a.Source_vlook = 'NONE' then 
                              CASE
                              WHEN a.market10 = 'NORTH AMERICA' then f.North_America_DRoute
                              WHEN a.market10 = 'LATIN AMERICA' then f.Latin_America_ERoute
                              WHEN a.market10 = 'UK&I' then f.UKI_DRoute
                              WHEN a.market10 = 'NORTHERN EUROPE' then f.Northern_Europe_DRoute
                              WHEN a.market10 = 'SOUTHERN EUROPE' then f.Southern_Europe_DRoute
                              WHEN a.market10 = 'CENTRAL EUROPE' AND a.developed_emerging='D' then f.Central_Europe_DRoute
                              WHEN a.market10 = 'CENTRAL EUROPE' AND a.developed_emerging='E' then f.Central_Europe_ERoute
                              WHEN a.market10 = 'ISE' then f.ISE_ERoute
                              WHEN a.market10 = 'GREATER ASIA' AND a.developed_emerging='D' then f.Greater_Asia_DRoute
                              WHEN a.market10 = 'GREATER ASIA' AND a.developed_emerging='E' then f.Greater_Asia_ERoute
                              WHEN a.market10 = 'GREATER CHINA' AND a.developed_emerging='D' then f.Greater_China_DRoute
                              WHEN a.market10 = 'GREATER CHINA' AND a.developed_emerging='E' then f.Greater_China_ERoute
                              WHEN a.market10 = 'INDIA SL & BL' then f.India_ERoute
                              END
                             ELSE Source_vlook
                        END as RouteDE
                       FROM sourceR a
                       LEFT JOIN usageSummary2 b
                        ON a.printer_platform_name=b.printer_platform_name and a.country_alpha2=b.country_alpha2 and a.rtm=b.rtm
                       LEFT JOIN usageSummary2DE c
                        ON a.printer_platform_name=c.printer_platform_name and a.market10=c.market10 and a.developed_emerging=c.developed_emerging and a.rtm=c.rtm
                       LEFT JOIN usageSummary2Mkt d
                        ON a.printer_platform_name=d.printer_platform_name and a.market10=d.market10 and a.rtm=d.rtm
                       LEFT JOIN usageSummary2R5 e
                        ON a.printer_platform_name=e.printer_platform_name and a.printer_region_code=e.printer_region_code and a.rtm=e.rtm 
                       LEFT JOIN usageSummary2TE_D3 f
                        ON a.printer_platform_name=f.printer_platform_name and a.rtm=f.rtm and a.platform_division_code=f.platform_division_code
                       ")

# COMMAND ----------

# Step 48 - combining AP results with all rows where iMPVs for NA and EU are populated

usagesummaryNAEUAP <- usageSummary2TE_D3

# COMMAND ----------

# Step 49 - Extracting PoR informastion for all platforms from DIM_PLATFORM table

#strata <- unique(usage[c("platform_division_code", "product_brand", "printer_platform_name")])
PoRtst <- SparkR::collect(SparkR::sql("
    SELECT DISTINCT platform_subset AS printer_platform_name
          , hw_product_family as platform_division_code
          , upper(brand) as product_brand
          , intro_price as product_intro_price
          , mono_ppm AS print_mono_speed_pages
          , color_ppm AS print_color_speed_pages
          , vc_category AS platform_finance_market_category_code
          , pl as product_line_code
          , CASE WHEN por_ampv > 0 THEN por_ampv 
			          ELSE NULL
                END AS product_usage_por_pages
        FROM
          hardware_xref
        WHERE (upper(technology)='INK' or (technology='PWA' and (upper(hw_product_family) not in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3'))
                            and platform_subset not like 'PANTHER%' and platform_subset not like 'JAGUAR%'))
        AND product_lifecycle_status in ('C','N')
    "
  ))

  PoRtst$product_usage_por_pages <- as.numeric(PoRtst$product_usage_por_pages)  
  PoRtst$print_mono_speed_pages <- as.numeric(PoRtst$print_mono_speed_pages)  
  PoRtst$print_color_speed_pages <- as.numeric(PoRtst$print_color_speed_pages)  
  POR_porest <- aggregate(zero$usage,by=list(zero$product_line_code),FUN=mean)
  PoR <- sqldf("
               select a.printer_platform_name
               ,a.platform_division_code
               ,a.product_brand
               ,a.product_intro_price
               ,a.print_mono_speed_pages
               ,a.print_color_speed_pages
               ,a.platform_finance_market_category_code
               ,a.product_line_code
               ,CASE WHEN a.product_usage_por_pages is not null then a.product_usage_por_pages
                ELSE b.x
                END AS product_usage_por_pages
               --,c.rtm
               from PoRtst a 
               left join POR_porest b on a.product_line_code=b.[Group.1]
               --left join PoRprice c on a.printer_platform_name=c.printer_platform_name
               ")
  PoR$platform_division_code <- ifelse(PoR$printer_platform_name=="THINKJET","TIJ_1.0",ifelse(PoR$printer_platform_name=="SQUIRT 5M","TIJ_1.0",
                                      ifelse(PoR$printer_platform_name=="DAKOTA CISJAP","TIJ_2.XG2 MATURE",
                                      ifelse(PoR$printer_platform_name=="VOYAGER AND DINO","TIJ_2.XG2 MATURE",
                                      ifelse(PoR$printer_platform_name=="CARNIVAL CR","TIJ_2.XG2 MATURE",PoR$platform_division_code)))))
  
  
  PoR_platformLIST <- sqldf('select distinct printer_platform_name,product_brand,"product_ref" as Source from PoR')

# COMMAND ----------

# Step - 50 -- extracting platform specific general IntroDate and create Old-Future type

old <- SparkR::collect(SparkR::sql(paste("
   WITH ibset as (
        SELECT ib.platform_subset, ib.customer_engagement, cc.country_level_2 as region_code, cr.developed_emerging
        ,cal_date 
        ,CASE
          WHEN MONTH(cal_date) > 10 THEN  concat(YEAR(cal_date)+1,reverse(substring(reverse(concat('0000',MONTH(cal_date)-10)),1,2)))
          ELSE concat(YEAR(cal_date),reverse(substring(reverse(concat('0000',MONTH(cal_date)+2)),1,2)))
          END as fyearmo
        FROM ib ib
        LEFT JOIN iso_country_code_xref cr
        ON ib.country=cr.country_alpha2
        LEFT JOIN (select * from iso_cc_rollup_xref where country_scenario='MARKET10') cc
        ON ib.country=cc.country_alpha2
   )
   , aa0 AS (
      SELECT UPPER(ref.brand) as product_brand
    , ref.hw_product_family as platform_division_code
    , ib.region_code AS printer_region_code
    , ib.customer_engagement as rtm
    , ib.platform_subset AS printer_platform_name
    , ib.developed_emerging
    , MIN(ib.fyearmo * 1) AS INTRODATE
    FROM hardware_xref ref
    INNER JOIN
      ibset ib
    ON (ref.platform_subset=ib.platform_subset)
    WHERE ref.platform_subset != '?' 
        and (upper(ref.technology)='INK' or (ref.technology='PWA' and (upper(ref.hw_product_family) not in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3'))
        and ref.platform_subset not like 'PANTHER%' and ref.platform_subset not like 'JAGUAR%'))
    GROUP BY ref.brand, ref.hw_product_family
    , ib.platform_subset
    , ib.customer_engagement
    , ib.region_code
    , ib.developed_emerging

  )

  , aa1b as (
    SELECT
    product_brand ,platform_division_code,rtm, printer_platform_name, printer_region_code, INTRODATE
    , CASE WHEN printer_region_code='NORTH AMERICA' THEN  INTRODATE ELSE NULL END AS INTRODATE_NA
    , CASE WHEN printer_region_code='NORTHERN EUROPE' THEN INTRODATE ELSE NULL END AS INTRODATE_NE
    , CASE WHEN printer_region_code='SOUTHERN EUROPE' THEN INTRODATE ELSE NULL END AS INTRODATE_SE
    , CASE WHEN printer_region_code='CENTRAL EUROPE' AND developed_emerging='D' THEN INTRODATE ELSE NULL END AS INTRODATE_CED
    , CASE WHEN printer_region_code='CENTRAL EUROPE' AND developed_emerging='E' THEN INTRODATE ELSE NULL END AS INTRODATE_CEE
    , CASE WHEN printer_region_code='UK&I' THEN INTRODATE ELSE NULL END AS INTRODATE_UK
    , CASE WHEN printer_region_code='ISE' THEN INTRODATE ELSE NULL END AS INTRODATE_IS
    , CASE WHEN printer_region_code='INDIA SL & BL' THEN  INTRODATE ELSE NULL END AS INTRODATE_IN
    , CASE WHEN printer_region_code='GREATER ASIA' AND developed_emerging='D' THEN INTRODATE ELSE NULL END AS INTRODATE_GAD
    , CASE WHEN printer_region_code='GREATER ASIA' AND developed_emerging='E' THEN INTRODATE ELSE NULL END AS INTRODATE_GAE
    , CASE WHEN printer_region_code='GREATER CHINA' AND developed_emerging='D' THEN INTRODATE ELSE NULL END AS INTRODATE_GCD
    , CASE WHEN printer_region_code='GREATER CHINA' AND developed_emerging='E' THEN INTRODATE ELSE NULL END AS INTRODATE_GCE
    , CASE WHEN printer_region_code='LATIN AMERICA' THEN  INTRODATE ELSE NULL END AS INTRODATE_LA
    FROM
    aa0
    )
  , aa1 as (
  SELECT
    product_brand ,rtm, platform_division_code, printer_platform_name
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
  GROUP BY product_brand ,platform_division_code, rtm, printer_platform_name
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
    ORDER BY product_brand, platform_division_code,rtm, printer_platform_name
    ",sep = " ", collapse = NULL
)))

  
  head(old)
  colnames(old)
  dim(old)
  str(old)
  
  # "dropList" is the list of platforms which are recorded both as Old and Future
  
  dropList <- old[old$platform_type=="FUTURE",]$printer_platform_name[old[old$platform_type=="FUTURE",]$printer_platform_name %in% (usagesummaryNAEUAP$printer_platform_name)]; dropList

# COMMAND ----------

# Step - 51 attaching introdate, platform type with PoR table

PoR2 <- sqldf('select aa1.*, aa2.rtm , aa2.INTRODATE_NA, aa2.INTRODATE_NE, aa2.INTRODATE_SE, aa2.INTRODATE_CED, aa2.INTRODATE_CEE, aa2.INTRODATE_UK
  , aa2.INTRODATE_IS, aa2.INTRODATE_IN, aa2.INTRODATE_GAD, aa2.INTRODATE_GAE, aa2.INTRODATE_GCD, aa2.INTRODATE_GCE, aa2.INTRODATE_LA
  , aa2.INTRODATE as Intro_FYearMo, aa2.platform_type
              from PoR aa1
              inner join
              old aa2
              on
              aa1.printer_platform_name=aa2.printer_platform_name
              and aa1.platform_division_code=aa2.platform_division_code
              --and aa1.rtm=aa2.rtm
              ')

PoR2$J90Mo <- (as.numeric(substr(PoR2$Intro_FYearMo, 1,4)) - 1990)*12 + (as.numeric(substr(PoR2$Intro_FYearMo, 5,6))-1)
str(PoR2)

# COMMAND ----------

# Step 51A extracting platform-dim

PoR2B <- PoR2
PoR2B$product_intro_price <- as.numeric(PoR2B$product_intro_price)
str(PoR2B)

# COMMAND ----------

# Step - 52 Creating raw_iMPV for all old Platforms using model information

PoR2model <- PoR2B

PoR2model$plval <- ifelse(PoR2model$product_line_code=="2N",-22.9550319,
                          ifelse(PoR2model$product_line_code=="3Y",-11.1106518,
                          ifelse(PoR2model$product_line_code=="4H",-23.1348069,
                          ifelse(PoR2model$product_line_code=="5M",-23.9593689,
                          ifelse(PoR2model$product_line_code=="7T",-21.0799829,
                          ifelse(PoR2model$product_line_code=="DL",-22.8566638,
                          ifelse(PoR2model$product_line_code=="DU",-22.0843013,
                          ifelse(PoR2model$product_line_code=="GC",-18.5020260,0
                                 ))))))))
PoR2model$ys80 <- (PoR2model$Intro_FYearMo-198001)/12

PoR2model$model <- ifelse(PoR2model$platform_type=="OLD",ifelse(is.na(PoR2model$print_color_speed_pages),(32.1376433-0.0283614*PoR2model$ys80 + PoR2model$plval+0.0149292*PoR2model$product_intro_price -0.1487806*PoR2model$print_mono_speed_pages),(32.1376433-0.0283614*PoR2model$ys80 + PoR2model$plval+0.0149292*PoR2model$product_intro_price -0.1487806*PoR2model$print_mono_speed_pages +0.2031720*PoR2model$print_color_speed_pages)),NA)



PoR2model$rawMPV <- (as.numeric(PoR2model$model))
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
                        and aa1.product_brand = aa2.product_brand
                        and aa1.rtm=aa2.rtm
                        where PLATFORM_TYPE = "OLD"
                        order by product_brand
                        ')

# COMMAND ----------

# Step - 54 Calculating the ratio of calculated_iMPV and raw_iMPV for NA 

PoR2model_iMPV$ratio1 <- PoR2model_iMPV$North_America2/PoR2model_iMPV$rawMPV
PoR2model_iMPV$ratio2 <- PoR2model_iMPV$NA2/PoR2model_iMPV$rawMPV
PoR2model_iMPV$ratio3 <- PoR2model_iMPV$North_America_D2/PoR2model_iMPV$rawMPV

# COMMAND ----------

# Step - 55 Calculating the average of iMPV ratio specific to groups defined by SM, CM

#avgRatio <- sqldf('select platform_division_code, product_brand, rtm, avg(ratio) as avgratio from PoR2model_iMPV group by platform_division_code, product_brand, rtm')
avgRatio1 <- sqldf('select platform_division_code, product_brand, rtm, avg(ratio1) as avgratio1 from PoR2model_iMPV group by platform_division_code, product_brand, rtm')
avgRatio2 <- sqldf('select platform_division_code, product_brand, rtm, avg(ratio2) as avgratio2 from PoR2model_iMPV group by platform_division_code, product_brand, rtm')
avgRatio3 <- sqldf('select platform_division_code, product_brand, rtm, avg(ratio3) as avgratio3 from PoR2model_iMPV group by platform_division_code, product_brand, rtm')

# COMMAND ----------

# Step - 56 attaching the average iMPV ratio

PoR2model_iMPV2 <- sqldf(' select aa1.*, aa2.avgratio1, aa3.avgratio2, aa4.avgratio3
                         from 
                         PoR2model_iMPV aa1
                         inner join 
                         avgRatio1 aa2
                         on 
                         aa1.platform_division_code = aa2.platform_division_code
                         and
                         aa1.product_brand = aa2.product_brand
                         and aa1.rtm = aa2.rtm
                         inner join 
                         avgRatio2 aa3
                         on 
                         aa1.platform_division_code = aa3.platform_division_code
                         and
                         aa1.product_brand = aa3.product_brand
                         and aa1.rtm = aa3.rtm
                         inner join 
                         avgRatio3 aa4
                         on 
                         aa1.platform_division_code = aa4.platform_division_code
                         and
                         aa1.product_brand = aa4.product_brand
                         and aa1.rtm = aa4.rtm
                         order by platform_division_code, product_brand
                         ')

PoR2model_iMPV3 <- sqldf('select *
                         , case
                         when NA2 is not null then NA2
                         when avgratio2 is not null then rawMPV*avgratio2
                         else rawMPV 
                         end as NA3
                          , case
                         when North_America2 is not null then North_America2
                         when avgratio2 is not null then rawMPV*avgratio2
                         else rawMPV
                         end as North_America3
                         from
                         PoR2model_iMPV2
                         order by  product_brand
                         ')

# COMMAND ----------

# ---- Step - 58 attaching regional coefficients -------------------------------------------#

PoR2model_iMPV4 <- sqldf('select aa1.*, 1 as NAa, aa2.EUa, aa2.APa, aa2.LAa, 1 as North_Americana, aa2.UKIna, aa2.Northern_Europena, aa2.Southern_Europena, aa2.ISEna, aa2.Central_Europena, aa2.Indiana, aa2.Greater_Asiana, aa2.Greater_Chinana,aa2.Latin_Americana, aa2.Greater_Asia_Dna, aa2.Greater_Asia_Ena, aa2.Greater_China_Dna, aa2.Greater_China_Ena, aa2.Central_Europe_Dna, aa2.Central_Europe_Ena
                         from PoR2model_iMPV3 aa1
                         left join 
                         wtaverage aa2
                         on 
                         aa1.platform_division_code =aa2.platform_division_code
                         and
                         aa1.product_brand = aa2.product_brand
                         and aa1.rtm=aa2.rtm
                         order by platform_division_code, product_brand')

PoR2model_iMPV4a <- sqldf("SELECT product_brand, UPPER(rtm) as rtm, avg(NAa) as NAa, avg(EUa) as EUa, avg(APa) as APa, avg(LAa) as LAa, avg(North_Americana) as North_Americana
    , avg(UKIna) as UKIna, avg(Northern_Europena) as Northern_Europena, avg(Southern_Europena) as Southern_Europena, avg(ISEna) as ISEna, avg(Central_Europena) as Central_Europena
    , avg(Indiana) as Indiana, avg(Greater_Asiana) as Greater_Asiana, avg(Greater_Chinana) as Greater_Chinana, avg(Latin_Americana) as Latin_Americana
    , avg(Greater_Asia_Dna) as Greater_Asia_Dna, avg(Greater_Asia_Ena) as Greater_Asia_Ena, avg(Greater_China_Dna) as Greater_China_Dna
    , avg(Greater_China_Ena) as Greater_China_Ena, avg(Central_Europe_Dna) as Central_Europe_Dna, avg(Central_Europe_Ena) as Central_Europe_Ena
    FROM PoR2model_iMPV4
    GROUP BY product_brand, UPPER(rtm)
                          ")

PoR2model_iMPV4 <- sqldf("select aa1.*
                , 1 as NAa, CASE WHEN aa2.EUa is not null THEN aa2.EUa ELSE aa3.EUa END as EUa, CASE WHEN aa2.APa is not null THEN aa2.APa ELSE aa3.APa END AS APa
                , CASE WHEN aa2.LAa is not null THEN aa2.LAa ELSE aa3.LAa END as LAa
                , 1 as North_Americana, CASE WHEN aa2.UKIna is not null THEN aa2.UKIna ELSE aa3.UKIna END AS UKIna, CASE WHEN aa2.Northern_Europena is not null THEN aa2.Northern_Europena ELSE aa3.Northern_Europena END as Northern_Europena, CASE WHEN aa2.Southern_Europena is not null THEN aa2.Southern_Europena ELSE aa3.Southern_Europena END AS Southern_Europena, CASE WHEN aa2.ISEna is not null THEN aa2.ISEna ELSE aa3.ISEna END AS ISEna, CASE WHEN aa2.Central_Europena is not null THEN aa2.Central_Europena ELSE aa3.Central_Europena END AS Central_Europena, CASE WHEN aa2.Indiana is not null THEN aa2.Indiana ELSE aa3.Indiana END AS Indiana, CASE WHEN aa2.Greater_Asiana is not null THEN aa2.Greater_Asiana ELSE aa3.Greater_Asiana END AS Greater_Asiana, CASE WHEN aa2.Greater_Chinana is not null THEN aa2.Greater_Chinana ELSE aa3.Greater_Chinana END AS Greater_Chinana, CASE WHEN aa2.Latin_Americana is not null THEN aa2.Latin_Americana ELSE aa3.Latin_Americana END AS Latin_Americana, CASE WHEN aa2.Greater_Asia_Dna is not null THEN aa2.Greater_Asia_Dna ELSE aa3.Greater_Asia_Dna END AS Greater_Asia_Dna, CASE WHEN aa2.Greater_Asia_Ena is not null THEN aa2.Greater_Asia_Ena ELSE aa3.Greater_Asia_Ena END AS Greater_Asia_Ena, CASE WHEN aa2.Greater_China_Dna THEN aa2.Greater_China_Dna ELSE aa3.Greater_China_Dna END AS Greater_China_Dna, CASE WHEN aa2.Greater_China_Ena is not null THEN aa2.Greater_China_Ena ELSE aa3.Greater_China_Ena END AS Greater_China_Ena, CASE WHEN aa2.Central_Europe_Dna is not null THEN aa2.Central_Europe_Dna ELSE aa3.Central_Europe_Dna END AS Central_Europe_Dna, CASE WHEN aa2.Central_Europe_Ena is not null THEN aa2.Central_Europe_Ena ELSE aa3.Central_Europe_Ena END AS Central_Europe_Ena
                 FROM PoR2model_iMPV3 aa1
                         LEFT JOIN wtaverage aa2
                         on aa1.platform_division_code =aa2.platform_division_code and aa1.product_brand = aa2.product_brand and aa1.rtm=aa2.rtm
                         LEFT JOIN PoR2model_iMPV4a aa3 
                         on aa1.product_brand = aa3.product_brand and aa1.rtm=aa3.rtm
                         ")

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
                         order by platform_division_code, product_brand
                         ')

#test2 <- subset(PoR2model_iMPV5,!is.na(EU3))
#write.csv('C:/UPM/iMPV_OLD_complete.csv', x=PoR2model_iMPV5,row.names=FALSE, na="")

# COMMAND ----------

# Step 60 - Populating the route matrix for iMPV creation based on modeled values
 
route5 <- sqldf("select platform_division_code, product_brand,printer_platform_name,rtm
                , case 
                when NA2 is not null then 'populated'
                when NA3 is null then null
                else 'MODELED'
                end as NAs
               -- , case 
               -- when North_America2 is not null then 'populated'
               -- when North_America3 is null then null
               -- else 'MODELED'
               -- end as North_Americas
                --, case 
                --when EU2 is not null then 'populated'
               -- when EU3 is null then null
                --else 'MODELED'
                --end as EUs  
                , case 
                when UKI2 is not null then 'populated'
                when UKI3 is null then null
                else 'MODELED'
                end as UKs
                , case
                when Northern_Europe2 is not null then 'populated'
                when Northern_Europe3 is null then null
                else 'MODELED'
                end as NEs
                , case
                when Southern_Europe2 is not null then 'populated'
                when Southern_Europe3 is null then null
                else 'MODELED'
                end as SEs
                --, case
                --when Central_Europe2 is not null then 'populated'
                --when Central_Europe3 is null then null
                --else 'MODELED'
                --end as Central_Europes
                , case
                when Central_Europe2 is not null then 'populated'
                when Central_Europe_D3 is null then null
                else 'MODELED'
                end as CEDs
                , case
                when Central_Europe2 is not null then 'populated'
                when Central_Europe_E3 is null then null
                else 'MODELED'
                end as CEEs
                , case
                when ISE2 is not null then 'populated'
                when ISE3 is null then null
                else 'MODELED'
                end as ISs
                --, case 
                --when AP2 is not null then 'populated'
                --when AP3 is null then null
                --else 'MODELED'
                --end as APs     
                , case when India2 is not null then 'populated'
                when India3 is null then null
                else 'MODELED'
                end as INs
                --, case 
                --when Greater_Asia2 is not null then 'populated'
                --when Greater_Asia3 is null then null
                --else 'MODELED'
                --end as Greater_Asias
                , case 
                when Greater_Asia2 is not null then 'populated'
                when Greater_Asia_D3 is null then null
                else 'MODELED'
                end as GADs
                , case 
                when Greater_Asia2 is not null then 'populated'
                when Greater_Asia_E3 is null then null
                else 'MODELED'
                end as GAEs
                --, case
                --when Greater_China2 is not null then 'populated'
                --when Greater_China3 is null then null
                --else 'MODELED'
                --end as Greater_Chinas
                , case
                when Greater_China2 is not null then 'populated'
                when Greater_China_D3 is null then null
                else 'MODELED'
                end as GCDs
                , case
                when Greater_China2 is not null then 'populated'
                when Greater_China_E3 is null then null
                else 'MODELED'
                end as GCEs
                , case 
                when LA2 is not null then 'populated'
                when LA3 is null then null
                else 'MODELED'
                end as LAs 
                --, case
                --when Latin_America2 is not null then 'populated'
                --when Latin_America3 is null then null
                --else 'MODELED'
                --end as Latin_Americas

                from PoR2model_iMPV5
                ")

route5B <- reshape2::melt(route5 , id.vars = c("platform_division_code", "product_brand","printer_platform_name","rtm"),variable.name = "printer_region_code", value.name = "Route")

route5B <- route5B[which((route5B$Route=="Modeled")|is.na(route5B$Route)),]
route5B$printer_platform_name <- factor(route5B$printer_platform_name)

# COMMAND ----------

# Step 61 - Calculating percentage of platform installed base across regions

q6 <- ("SELECT distinct platform_subset AS printer_platform_name 
                 , platform_division_code
                 , product_brand
                 FROM hw_info
                  ")

new1 <- sqldf(q6)
  new1b <- sqldf("SELECT a.*
            , c.market10
            , c.developed_emerging
            , b.rtm
            , b.month_begin as fiscal_year_month 
            , SUM(b.ib) AS printer_installed_base_month 
            FROM new1 a 
            INNER JOIN (select * from ibtable where ib IS NOT NULL) b 
              ON a.printer_platform_name=b.platform_subset
            LEFT JOIN country_info c
              ON b.country=c.country_alpha2
              GROUP BY
               a.printer_platform_name, a.platform_division_code, a.product_brand
              , b.rtm
              , c.market10, c.developed_emerging
              , b.month_begin")
new1b <- subset(new1b,!is.na(market10))
new1c <- sqldf("SELECT platform_division_code, product_brand, printer_platform_name, rtm as rtm, market10, SUBSTR(developed_emerging,1,1) as developed_emerging
                , sum(printer_installed_base_month) as sumINSTALLED_BASE_COUNT
             FROM
             new1b
              where printer_installed_base_month IS NOT NULL
             group by platform_division_code, product_brand, printer_platform_name, rtm, market10
             order by platform_division_code, product_brand, printer_platform_name, rtm, market10")
new1c$sumINSTALLED_BASE_COUNT <- as.numeric(new1c$sumINSTALLED_BASE_COUNT)
new1cc <- sqldf("SELECT platform_division_code, product_brand, printer_platform_name, rtm as rtm
                , sum(printer_installed_base_month) as sumINSTALLED_BASE_COUNT
             FROM
             new1b
              where printer_installed_base_month IS NOT NULL
             group by platform_division_code, product_brand, printer_platform_name, rtm
             order by platform_division_code, product_brand, printer_platform_name, rtm")

new1cc$sumINSTALLED_BASE_COUNT <- as.numeric(new1cc$sumINSTALLED_BASE_COUNT)
new1d <- reshape2::dcast(new1c, platform_division_code+product_brand+printer_platform_name+rtm ~ market10+developed_emerging, value.var = "sumINSTALLED_BASE_COUNT", fun.aggregate=sum)
new <- sqldf("SELECT a.* ,b.sumINSTALLED_BASE_COUNT 
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
            on a.printer_platform_name=b.printer_platform_name and a.rtm=b.rtm
            ORDER BY platform_division_code, product_brand, printer_platform_name, rtm
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
              where aa2.platform_type = "FUTURE"
              ')

# COMMAND ----------

# Step 63 - Attaching the Regional Coeffieicnt (RC) to the respective platform

wtaverage2 <- sqldf("select product_brand, platform_division_code, rtm, avg(EUa) as EUa, avg([Central_Europena]) as [Central_Europena], avg([Central_Europe_Dna]) as [Central_Europe_Dna] 
                    ,avg([Central_Europe_Ena]) as [Central_Europe_Ena], avg([Northern_Europena]) as [Northern_Europena], avg([Southern_Europena]) as [Southern_Europena]
                    ,avg([ISEna]) as [ISEna], avg([UKIna]) as [UKIna], avg([Greater_Asiana]) as [Greater_Asiana], avg([Greater_Asia_Dna]) as [Greater_Asia_Dna]
                    ,avg([Greater_Asia_Ena]) as [Greater_Asia_Ena], avg([Greater_Chinana]) as [Greater_Chinana], avg([Greater_China_Dna]) as [Greater_China_Dna]
                    ,avg([Greater_China_Ena]) as [Greater_China_Ena], avg([Indiana]) as [Indiana], avg([Latin_Americana]) as [Latin_Americana]
                    from wtaverage
                    group by product_brand, platform_division_code, rtm")

new3 <- sqldf('select distinct aa1.*, aa2.[Central_Europena],aa2.[Central_Europe_Dna],aa2.[Central_Europe_Ena],aa2.[Northern_Europena],aa2.[Southern_Europena],aa2.[ISEna]
  ,aa2.[UKIna],aa2.[Greater_Asiana],aa2.[Greater_Asia_Dna],aa2.[Greater_Asia_Ena],aa2.[Greater_Chinana],aa2.[Greater_China_Dna],aa2.[Greater_China_Ena]
  ,aa2.[Indiana],aa2.[Latin_Americana]
              from new2 aa1
              inner join 
              wtaverage2 aa2
              on 
              aa1.platform_division_code = aa2.platform_division_code
              and
              aa1.product_brand = aa2.product_brand
              and
              aa1.rtm = aa2.rtm
              order by platform_division_code, product_brand')

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

decayT <- reshape2::dcast(decay, platform_division_code + product_brand +rtm  ~market10+developed_emerging, value.var="b1")

new4 <- sqldf('select distinct aa1.*
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
              aa1.platform_division_code = aa2.platform_division_code
              and
              aa1.product_brand = aa2.product_brand
              and aa1.rtm=aa2.rtm
              order by platform_division_code, product_brand')

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

# COMMAND ----------

# Step 68 - Populating the route matrix of iMPV creations for Futre products

route6 <- sqldf('select platform_division_code, product_brand, printer_platform_name, rtm as rtm
                 , case when NA3 is not null then "FUTURE" else null end as NAs
                , case when CED3 is not null then "FUTURE" else null end as CEDs                
                , case when CEE3 is not null then "FUTURE" else null end as CEEs               
                , case when GAD3 is not null then "FUTURE" else null end as GADs               
                , case when GAE3 is not null then "FUTURE" else null end as GAEs  
                , case when GCD3 is not null then "FUTURE" else null end as GCDs  
                , case when GCE3 is not null then "FUTURE" else null end as GCEs
                , case when IN3 is not null then "FUTURE" else null end as INs
                , case when IS3 is not null then "FUTURE" else null end as ISs
                , case when LA3 is not null then "FUTURE" else null end as LAs
                , case when NE3 is not null then "FUTURE" else null end as NEs
                , case when SE3 is not null then "FUTURE" else null end as SEs
                , case when UK3 is not null then "FUTURE" else null end as UKs
                from new4
                ')

route6B <- reshape2::melt(route6 , id.vars = c("platform_division_code", "product_brand","printer_platform_name", "rtm"),variable.name = "printer_region_code", value.name = "Route")

route1 <- usagesummaryNAEUAP[c(2,4,3,1, 510, 513, 515, 517, 519, 521, 522, 525, 527, 528, 530, 531, 534)]
route1B <- reshape2::melt(route1 , id.vars = c("platform_division_code", "product_brand","printer_platform_name","rtm"),variable.name = "printer_region_code", value.name = "Route")
route1B <- sqldf("SELECT distinct platform_division_code, product_brand, printer_platform_name, rtm
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

route <- subset(route, !is.na(Route))
#route$printer_region_code <- substr(route$printer_region_code, 1, 2)

routeT <- reshape2::dcast(route, platform_division_code + product_brand +printer_platform_name +rtm ~printer_region_code, value.var="Route")

# COMMAND ----------

# Step - 69 - combining old and future iMPV matrix to create complete iMPV matrix

normdataOLD <- sqldf('select distinct platform_division_code, product_brand, rtm 
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

normdataNew <- sqldf('select distinct platform_division_code, product_brand, rtm
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
 
normdata2 <- reshape2::melt(normdata, id.vars = c("platform_division_code", "product_brand", "printer_platform_name","rtm"),
                  variable.name = "printer_region_code", 
                  value.name = "iMPV")

# COMMAND ----------

# Step - 71 Extracting region specific introDate for each of the platforms

normdatadate <- sqldf('select platform_division_code, product_brand, rtm
                      , printer_platform_name
                      , INTRODATE_NA AS NA, INTRODATE_NE AS NE, INTRODATE_SE AS SE, INTRODATE_CED AS CED, INTRODATE_CEE AS CEE, INTRODATE_UK AS UK, INTRODATE_IS AS [IS], INTRODATE_IN AS [IN]
                      , INTRODATE_GAD AS GAD, INTRODATE_GAE AS GAE, INTRODATE_GCD AS GCD, INTRODATE_GCE AS GCE, INTRODATE_LA AS LA
                      FROM PoR2model
                      ')

# COMMAND ----------

# Step - 72 creating detailed iMPV matrix for all platforms

combined <- sqldf('select aa1.PLATFORM_TYPE
                  , aa2.platform_division_code
                  , aa2.product_brand
                  , aa2.printer_platform_name
                  , aa2.rtm
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
                  --aa1.platform_division_code = aa2.platform_division_code
                  --and
                  --aa1.product_brand = aa2.product_brand
                  --and
                  aa1.printer_platform_name = aa2.printer_platform_name
                  and aa1.rtm = aa2.rtm
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
                   aa1.platform_division_code = aa2.platform_division_code
                   and
                   aa1.product_brand = aa2.product_brand
                   and
                   aa1.printer_platform_name = aa2.printer_platform_name
                   and aa1.rtm=aa2.rtm
                   ')

# COMMAND ----------

# Step - 73 Normalize introDate data with respect to VV, CM, SM and Plat_Nm

normdatadate2 <- reshape2::melt(normdatadate, id.vars = c("platform_division_code", "product_brand", "printer_platform_name", "rtm"),
                      variable.name = "printer_region_code", 
                      value.name = "introdate")

# COMMAND ----------

# Step - 74 Combine Normalized introDate and iMPV data for each Platform

sourceRiMPV$mde <- ifelse(sourceRiMPV$market10=='CENTRAL EUROPE',paste0('CE',substr(sourceRiMPV$developed_emerging,1,1)),
                   ifelse(sourceRiMPV$market10=='GREATER ASIA',paste0('GA',substr(sourceRiMPV$developed_emerging,1,1)), 
                   ifelse(sourceRiMPV$market10=='GREATER CHINA',paste0('GC',substr(sourceRiMPV$developed_emerging,1,1)),
                   ifelse(sourceRiMPV$market10=='NORTH AMERICA','NA',
                   ifelse(sourceRiMPV$market10=='NORTHERN EUROPE','NE',
                   ifelse(sourceRiMPV$market10=='SOUTHERN EUROPE','SE',
                   ifelse(sourceRiMPV$market10=='UK&I','UK',
                   ifelse(sourceRiMPV$market10=='ISE','IS',
                   ifelse(sourceRiMPV$market10=='INDIA SL & BL','IN',
                   ifelse(sourceRiMPV$market10=='LATIN AMERICA','LA',
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
                   , por.product_brand
                   , por.platform_division_code
                   , src.RTM as rtm
                   , CASE WHEN src.Route='COUNTRY' THEN ctry.iMPV 
                          WHEN src.Route='DEV/EM' THEN de.iMPV
                          WHEN src.Route='MARKET10' THEN mkt.iMPV
                          WHEN src.Route='REGION5' THEN reg.iMPV
                          WHEN src.Route='SELF' THEN mkt.iMPV
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
                    and src.rtm=ctry.rtm
                   LEFT JOIN usageSummary2DE de
                    ON src.printer_platform_name=de.printer_platform_name 
                     AND src.market10=de.market10 AND src.developed_emerging=de.developed_emerging
                     and src.rtm=de.rtm
                   LEFT JOIN usageSummary2Mkt mkt
                    ON src.printer_platform_name=mkt.printer_platform_name 
                     AND src.market10=mkt.market10
                     and src.rtm=mkt.rtm
                   LEFT JOIN usageSummary2R5 reg
                    ON src.printer_platform_name=reg.printer_platform_name 
                     AND src.printer_region_code=reg.printer_region_code
                     and src.rtm=reg.rtm
                   LEFT JOIN PoR2model_iMPV5 por
                    ON src.printer_platform_name=por.printer_platform_name
                    and src.rtm=por.rtm
                   LEFT JOIN normdatadate2 dt
                    ON src.printer_platform_name=dt.printer_platform_name AND src.mde=dt.printer_region_code
                    and src.rtm=dt.rtm
                   ")

normdataFinal <- sqldf('select aa1.*, aa2.minYear as introdate from normdataFinal0 aa1
                       inner join
                       introYear2a aa2
                       on
                       aa1.printer_platform_name = aa2.printer_platform_name
                       and 
                       upper(aa1.rtm)=upper(aa2.rtm)')
#normdataFinal$ep <- ifelse(normdataFinal$ep=="E","ENT","PRO")

cols <- c('platform_division_code', 'product_brand',   'country_alpha2' , 'rtm')
cols2 <- c('platform_division_code', 'product_brand',   'market10','developed_emerging' , 'rtm')
normdataFinal$strata1 <- apply( normdataFinal[ , cols2 ] , 1 , paste , collapse = "_" )
normdataFinal$strata2 <- apply( normdataFinal[ , cols ] , 1 , paste , collapse = "_" )

# COMMAND ----------

# Step 75 - Creating two artifical columns to capture year and month since Jan 1990

#clean up memory  
# rm(list= ls()[!(ls() %in% c('normdataFinal','outcome','usage','cprod'))])  ###Clear up memory- gets rid of all but these 3 tables
gc(reset=TRUE, full=TRUE, verbose=TRUE)
s1 <- as.data.frame(seq(1990, 2057, 1))
s2 <- as.data.frame(seq(1, 12, 1))
s3 <- merge(s1,s2,all=TRUE)
# names(s3)[names(s3)=="seq(1990, 2020, 1)"] <- "year"
# names(s3)[names(s3)=="seq(1, 12, 1)"] <- "month"te
names(s3)[1] <- "year"
names(s3)[2] <- "month"

# COMMAND ----------

# Step 76 - start loop to create timeseries dataset for each of the platform-region

outcome_SparkR <- as.DataFrame(outcome)
normdataFinal_SparkR <- as.DataFrame(normdataFinal)
s3_SparkR <- as.DataFrame(s3)

outcome_filtered <- SparkR::filter(outcome_SparkR, !isNull(outcome_SparkR$strata))

normdataFinal_filtered <- SparkR::filter(normdataFinal_SparkR, !isNull(normdataFinal_SparkR$introdate) & !isNull(normdataFinal_SparkR$iMPV) & !isNull(normdataFinal_SparkR$strata1) & !isNull(normdataFinal_SparkR$printer_platform_name) & !isNull(normdataFinal_SparkR$rtm) & !isNull(normdataFinal_SparkR$country_alpha2))

# for loop 0 replacement
d0 <- normdataFinal_filtered
d0$printerrtm <- concat_ws(sep="_", d0$printer_platform_name, d0$rtm)

d1 <- d0

d2 <- SparkR::merge(s3_SparkR, d1, by = NULL)

d2$yyyy <- cast(substr(d2$introdate, 1,4), "double")
d2$mm <- cast(substr(d2$introdate, 5,6), "double")
d2$year <- cast(d2$year, "integer")
d2$month <- cast(d2$month, "integer")
d2$yyyymm <- ifelse(d2$month<10, concat_ws(sep="0", d2$year, d2$month), concat(d2$year, d2$month))
d2 <- filter(d2, d2$yyyymm >= d2$introdate)
d2 <- filter(d2, d2$month <= 12)

# dseason
avg_slope_by_strata <- agg(groupBy(outcome_filtered, "strata"), avg_b1 = mean(outcome_filtered$b1))

outcome_filtered_no_slope <- filter(outcome_filtered, isNull(outcome_filtered$b1))
outcome_filtered_no_slope <- drop(join(outcome_filtered_no_slope, avg_slope_by_strata, outcome_filtered_no_slope$strata == avg_slope_by_strata$strata, "left"), avg_slope_by_strata$strata)
outcome_filtered_no_slope$b1 <- outcome_filtered_no_slope$avg_b1
outcome_filtered_no_slope <- drop(outcome_filtered_no_slope, "avg_b1")

outcome_filtered <- SparkR::union(filter(outcome_filtered,!isNull(outcome_filtered$b1)), select(outcome_filtered_no_slope, colnames(outcome_filtered)))
outcome_filtered$seasonality <- ifelse(isNull(outcome_filtered$seasonality), 0, outcome_filtered$seasonality)
dseason <- unique(select(outcome_filtered, c("b1", "month", "seasonality", "strata")))

d3 <- drop(join(d2, dseason, d2$month == dseason$month & d2$strata1 == dseason$strata, "leftouter"), dseason$month)

d4 <- d3

d4$rFYearMo <- (d4$year)*1 + (d4$month-2)/12
d4$MoSI <- (d4$year - d4$yyyy)*12 + (d4$month - d4$mm)

d4$Trend <-exp(log(d4$iMPV) + d4$MoSI*d4$b1/12)
d4$UPM_MPV <- exp(log(d4$Trend) + d4$seasonality)
d4$TS_MPV <-  exp(log(d4$Trend) + d4$seasonality) # without considering the cyclical part
d4$TD_MPV <-  exp(log(d4$Trend)) # Trend Only

d4 <- withColumnRenamed(d4, "b1", "Decay")
d4 <- withColumnRenamed(d4, "seasonality", "Seasonality")

createOrReplaceTempView(d4, "final1")

# COMMAND ----------

# Step 80 - Attaching Segment Specific MPV and N

createOrReplaceTempView(as.DataFrame(usage), "usage")

final1 <- SparkR::sql("SELECT DISTINCT aa1.*
                , aa2.mpva
                , aa2.na
                FROM final1 aa1
                LEFT JOIN
                usage aa2
                ON
                aa1.printer_platform_name = aa2.printer_platform_name
                AND
                aa1.country_alpha2 = aa2.country_alpha2
                AND
                aa1.yyyymm = aa2.FYearMo
                AND
                aa1.platform_division_code = aa2.platform_division_code
                AND
                aa1.product_brand = aa2.product_brand
                AND
                aa1.rtm=aa2.rtm
                ")

createOrReplaceTempView(final1, "final1")

# COMMAND ----------

# Step 84 - Renaming and aligning Variables

final9 <- SparkR::sql('
                SELECT DISTINCT 
                printer_platform_name AS Platform_Subset_Nm
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
                , CAST(null AS STRING) AS Country_Nm
                , yyyymm AS FYearMo
                --, rFYearMo_Ad AS rFYearMo
                --, FYearQtr
                --, rFYearQtr
                --, year AS FYear
                --, month AS FMo
                --, MoSI
                , UPM_MPV AS MPV_UPM
                , TS_MPV AS MPV_TS
                , TD_MPV AS MPV_TD
                , MPVA AS MPV_Raw
                , NA AS MPV_N
                --, IB
                --, introdate AS FIntroDt
                , platform_division_code
                , product_brand
                , rtm
                , iMPV AS MPV_Init
                , Decay
                , Seasonality
                --, Cyclical
                --, MUT
                , Trend
                , Route AS IMPV_Route
                FROM final1  
                --where IB is not null --and intro_price != 0
                ')

final9$FYearMo <- cast(final9$FYearMo, "string")

# COMMAND ----------

# MAGIC %python
# MAGIC # Step 85 - exporting final9 to S3
# MAGIC 
# MAGIC output_file_name = f"{constants['S3_BASE_BUCKET'][stack]}cupsm_outputs/ink/{datestamp}/{timestamp}/usage_total"
# MAGIC 
# MAGIC write_df_to_s3(df=spark.sql("SELECT * FROM final9"), destination=output_file_name, format="parquet", mode="overwrite", upper_strings=True)
# MAGIC 
# MAGIC print(output_file_name)

# COMMAND ----------

notebook_end_time <- Sys.time()
notebook_total_time <- notebook_end_time - notebook_start_time;notebook_total_time
