# Databricks notebook source
# ---
# Version 2021.11.29.1
# title: "100% IB Ink Share DE with IE 2.0 IB"
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
dbutils.widgets.text("upm_date", "")
dbutils.widgets.text("upm_date_color", "")
dbutils.widgets.text("writeout", "")

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

packages <- c("rJava", "RJDBC", "DBI", "sqldf", "zoo", "plyr", "reshape2", "lubridate", "data.table", "tidyverse", "SparkR", "spatstat", "nls2")

# COMMAND ----------

# MAGIC %run ../common/package_check.r

# COMMAND ----------

options(scipen=999)
sessionInfo()

writeout <- dbutils.widgets.get("writeout") #change to YES to write to MDM
UPMDate <- dbutils.widgets.get("upm_date") #Sys.Date() #Change to date if not running on same date as UPM '2021-07-22' #
UPMDateC <- dbutils.widgets.get("upm_date_color") #Sys.Date() #Change to date if not running on same date as UPM '2021-07-22' #

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

sfai <- dbConnect(sqlserver_driver, paste0("jdbc:sqlserver://sfai.corp.hpicloud.net:1433;database=IE2_Prod;user=", sparkR.conf("sfai_username"), ";password=", sparkR.conf("sfai_password")))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val cutoffDate = dbutils.widgets.get("cutoff_dt")
# MAGIC 
# MAGIC val tableMonthQuery = s"""
# MAGIC            --Share and Usage Splits (Trad)
# MAGIC WITH sub_a AS (
# MAGIC 	SELECT printer_id
# MAGIC 	-- Ref
# MAGIC 		, CASE
# MAGIC 			WHEN CHARINDEX(' ',use.platform_subset_reporting_name)=0 THEN use.platform_subset_reporting_name
# MAGIC 			ELSE LEFT(use.platform_subset_reporting_name,CHARINDEX(' ',use.platform_subset_reporting_name)-1)
# MAGIC 			END AS printer_short_name
# MAGIC 		, use.platform_subset_reporting_name AS printer_platform_name
# MAGIC 		, ref.rdma_platform_subset_name
# MAGIC 		, use.hp_strategic_region_code AS printer_region_code
# MAGIC 		, use.derived_country_name
# MAGIC 		, use.hp_country_common_name
# MAGIC 		, SUBSTRING(use.fiscal_year_quarter,1,4) AS year
# MAGIC 		, SUBSTRING(use.fiscal_year_quarter,5,6) AS quarter
# MAGIC 		, use.date_month_dim_ky
# MAGIC 		, CASE WHEN use.printer_instant_ink_state = 'ENROLLED' THEN 'I-INK'
# MAGIC 		       WHEN use.printer_instant_ink_state = 'SUBSCRIBED' THEN 'I-INK'
# MAGIC 		       ELSE 'TRAD'
# MAGIC 		       END
# MAGIC 		       AS printer_managed
# MAGIC 		, use.product_price_range
# MAGIC 		, use.product_technology_type
# MAGIC 		, use.product_technology_split
# MAGIC 		, ref.platform_name
# MAGIC 		, TRIM(regexp_replace(ref.product_model_name,'[0-9]+')) AS product_model_name
# MAGIC 		, ref.product_line_code
# MAGIC 		, ref.product_function_code
# MAGIC 		, ref.product_business_model
# MAGIC 	-- Black CCs
# MAGIC 		, COALESCE(use.k_total_cc,0) AS k_total
# MAGIC 		, COALESCE(use.k_no_host_cc,0) AS k_no_host
# MAGIC 		, COALESCE(use.k_host_cc,0) AS k_host		
# MAGIC 		, COALESCE(use.k_hp_cc,0) AS k_hp
# MAGIC 		, COALESCE(use.k_refill_cc,0) AS k_refill
# MAGIC 		, COALESCE(use.k_altered_cc,0) AS k_reman
# MAGIC 		, COALESCE(use.k_emulated_cc,0) AS k_clone
# MAGIC 		, k_refill+k_reman+k_clone AS k_nohp
# MAGIC 	-- Cyan CCs
# MAGIC 		, COALESCE(use.c_total_cc,0) AS c_total
# MAGIC 		, COALESCE(use.c_no_host_cc,0) AS c_no_host
# MAGIC 		, COALESCE(use.c_host_cc,0) AS c_host
# MAGIC 		, COALESCE(use.c_hp_cc,0) AS c_hp
# MAGIC 		, COALESCE(use.c_refill_cc,0) AS c_refill
# MAGIC 		, COALESCE(use.c_altered_cc,0) AS c_reman
# MAGIC 		, COALESCE(use.c_emulated_cc,0) AS c_clone		
# MAGIC 		, c_refill+c_reman+c_clone AS c_nohp
# MAGIC 	-- Magenta CCs		
# MAGIC 		, COALESCE(use.m_total_cc,0) AS m_total
# MAGIC 		, COALESCE(use.m_no_host_cc,0) AS m_no_host
# MAGIC 		, COALESCE(use.m_host_cc,0) AS m_host
# MAGIC 		, COALESCE(use.m_hp_cc,0) AS m_hp
# MAGIC 		, COALESCE(use.m_refill_cc,0) AS m_refill
# MAGIC 		, COALESCE(use.m_altered_cc,0) AS m_reman
# MAGIC 		, COALESCE(use.m_emulated_cc,0) AS m_clone
# MAGIC 		, m_refill+m_reman+m_clone AS m_nohp
# MAGIC 	-- Yellow CCs
# MAGIC 		, COALESCE(use.y_total_cc,0) AS y_total
# MAGIC 		, COALESCE(use.y_no_host_cc,0) AS y_no_host
# MAGIC 		, COALESCE(use.y_host_cc,0) AS y_host
# MAGIC 		, COALESCE(use.y_hp_cc,0) AS y_hp
# MAGIC 		, COALESCE(use.y_refill_cc,0) AS y_refill
# MAGIC 		, COALESCE(use.y_altered_cc,0) AS y_reman
# MAGIC 		, COALESCE(use.y_emulated_cc,0) AS y_clone
# MAGIC 		, y_refill+y_reman+y_clone AS y_nohp
# MAGIC 	-- Photo Black CCs
# MAGIC 		, COALESCE(use.lk_total_cc,0) AS lk_total
# MAGIC 		, COALESCE(use.lk_no_host_cc,0) AS lk_no_host
# MAGIC 		, COALESCE(use.lk_host_cc,0) AS lk_host
# MAGIC 		, COALESCE(use.lk_hp_cc,0) AS lk_hp
# MAGIC 		, COALESCE(use.lk_refill_cc,0) AS lk_refill
# MAGIC 		, COALESCE(use.lk_altered_cc,0) AS lk_reman
# MAGIC 		, COALESCE(use.lk_emulated_cc,0) AS lk_clone
# MAGIC 		, lk_refill+lk_reman+lk_clone AS lk_nohp		
# MAGIC 	-- Color CCs
# MAGIC 		, c_total+m_total+y_total+lk_total AS col_total
# MAGIC 		, c_no_host+m_no_host+y_no_host+lk_no_host AS col_no_host
# MAGIC 		, c_host+m_host+y_host+lk_host AS col_host
# MAGIC 		, c_hp+m_hp+y_hp+lk_hp AS col_hp
# MAGIC 		, c_refill+m_refill+y_refill+lk_refill AS col_refill
# MAGIC 		, c_reman+m_reman+y_reman+lk_reman AS col_reman
# MAGIC 		, c_clone+m_clone+y_clone+lk_clone AS col_clone
# MAGIC 		, c_nohp+m_nohp+y_nohp+lk_nohp AS col_nohp		
# MAGIC 	-- Total CCs
# MAGIC 		, k_total+col_total AS tot_cc
# MAGIC 		, COALESCE(use.total_cc,0) AS total_cc_check
# MAGIC 	-- Total No Host CCs
# MAGIC 		, k_no_host+col_no_host AS tot_no_host_cc
# MAGIC 		, COALESCE(use.total_no_host_cc,0) AS total_no_host_cc_check		
# MAGIC 	-- HP CCs
# MAGIC 		, k_hp+c_hp+m_hp+y_hp+lk_hp AS tot_hp
# MAGIC 		, COALESCE(use.hp_cc,0) AS hp_cc_check		
# MAGIC 	-- HP Inserted CCs
# MAGIC 		, COALESCE(use.fw_hp_ink_inserted_cc,0) AS inserted_hp_cc		
# MAGIC 	-- Total Pages
# MAGIC 		, COALESCE(use.pages_total,0) AS total_pages		
# MAGIC 	-- Other
# MAGIC 		, use.days
# MAGIC 		, use.ib_weight*use.ratio_month1_filter AS ib_weight_m1_adj	
# MAGIC 		, use.ib_weight
# MAGIC 	FROM cumulus_prod02_biz_trans.biz_trans.idst_print_share_usage use
# MAGIC 	LEFT OUTER JOIN cumulus_prod02_ref_enrich.ref_enrich.product_ref ref
# MAGIC 		ON (use.product_number=ref.product_number)
# MAGIC 	WHERE 1=1
# MAGIC 		AND month_order > 1
# MAGIC     AND printer_months >= 100 AND ib_weight*ratio_month1_filter <= 1000
# MAGIC     AND loyalty_subclass IN ('Host','HP Loyal','Disloyal','Defector','Non-HP Only','Suspect')
# MAGIC     AND use.product_technology_split NOT IN ('TIJ_2.XG1','TIJ_2.XG2 BCP','TIJ_2.XG2 MATURE','TIJ_2.XG3 GEO OJ','TIJ_2.XG3 GEO OJ MMOTR','TIJ_2.XG3 GEO STD','TIJ_2.XG3 GEO STD MMOTR','TIJ_4.0 GEKKO','TIJ_4.0 MPENNY','TIJ_4.0 OASIS GHID')
# MAGIC     AND use.product_number NOT IN ('Y0G49B','Z3M46B') -- Cancelled SKU's intended for EU (CO does not want them in our data)
# MAGIC 		AND use.share_printer_filter = 'New'
# MAGIC 		  --Remove if on Restricted Country List (Cuba, Iran, Sudan, Syria, North Korea) or no Market Ten mapping (Antarctica, Netherlands Antilles) 
# MAGIC     AND use.hp_country_common_name NOT IN ('CUBA','IRAN, ISLAMIC REPUBLIC OF','SUDAN','SYRIAN ARAB REPUBLIC','ANTARCTICA','NETHERLANDS ANTILLES')
# MAGIC     --AND use.ciss_printer=0  ---CISS included 
# MAGIC 
# MAGIC      )
# MAGIC , sub_b AS (
# MAGIC 	SELECT
# MAGIC 	-- Ref
# MAGIC 		--printer_short_name
# MAGIC 		printer_platform_name
# MAGIC 		, rdma_platform_subset_name
# MAGIC 		, printer_region_code
# MAGIC 		, derived_country_name
# MAGIC 		, hp_country_common_name
# MAGIC 		, year
# MAGIC 		, quarter
# MAGIC 		, date_month_dim_ky
# MAGIC 		, printer_managed
# MAGIC 		--, product_price_range
# MAGIC 		--, product_technology_type
# MAGIC 		--, product_technology_split
# MAGIC 		--, platform_name
# MAGIC 		--, product_model_name
# MAGIC 		--, product_line_code
# MAGIC 		--, product_function_code
# MAGIC 		--, product_business_model
# MAGIC 	-- Black CCs
# MAGIC 		, ROUND(SUM(k_total*ib_weight_m1_adj),2) AS k_total_cc_sum	
# MAGIC 		, ROUND(SUM(k_no_host*ib_weight_m1_adj),2) AS k_no_host_cc_sum
# MAGIC 		, ROUND(SUM(k_host*ib_weight_m1_adj),2) AS k_host_cc_sum
# MAGIC 		, ROUND(SUM(k_hp*ib_weight_m1_adj),2) AS k_hp_cc_sum
# MAGIC 		, ROUND(SUM(k_refill*ib_weight_m1_adj),2) AS k_refill_cc_sum
# MAGIC 		, ROUND(SUM(k_reman*ib_weight_m1_adj),2) AS k_reman_cc_sum
# MAGIC 		, ROUND(SUM(k_clone*ib_weight_m1_adj),2) AS k_clone_cc_sum	
# MAGIC 		, ROUND(SUM(k_nohp*ib_weight_m1_adj),2) AS k_nohp_cc_sum		
# MAGIC 	-- Cyan CCs
# MAGIC 		, ROUND(SUM(c_total*ib_weight_m1_adj),2) AS c_total_cc_sum	
# MAGIC 		, ROUND(SUM(c_no_host*ib_weight_m1_adj),2) AS c_no_host_cc_sum
# MAGIC 		, ROUND(SUM(c_host*ib_weight_m1_adj),2) AS c_host_cc_sum
# MAGIC 		, ROUND(SUM(c_hp*ib_weight_m1_adj),2) AS c_hp_cc_sum
# MAGIC 		, ROUND(SUM(c_refill*ib_weight_m1_adj),2) AS c_refill_cc_sum
# MAGIC 		, ROUND(SUM(c_reman*ib_weight_m1_adj),2) AS c_reman_cc_sum
# MAGIC 		, ROUND(SUM(c_clone*ib_weight_m1_adj),2) AS c_clone_cc_sum
# MAGIC 		, ROUND(SUM(c_nohp*ib_weight_m1_adj),2) AS c_nohp_cc_sum	
# MAGIC 	-- Magenta CCs		
# MAGIC 		, ROUND(SUM(m_total*ib_weight_m1_adj),2) AS m_total_cc_sum		
# MAGIC 		, ROUND(SUM(m_no_host*ib_weight_m1_adj),2) AS m_no_host_cc_sum
# MAGIC 		, ROUND(SUM(m_host*ib_weight_m1_adj),2) AS m_host_cc_sum
# MAGIC 		, ROUND(SUM(m_hp*ib_weight_m1_adj),2) AS m_hp_cc_sum
# MAGIC 		, ROUND(SUM(m_refill*ib_weight_m1_adj),2) AS m_refill_cc_sum
# MAGIC 		, ROUND(SUM(m_reman*ib_weight_m1_adj),2) AS m_reman_cc_sum
# MAGIC 		, ROUND(SUM(m_clone*ib_weight_m1_adj),2) AS m_clone_cc_sum
# MAGIC 		, ROUND(SUM(m_nohp*ib_weight_m1_adj),2) AS m_nohp_cc_sum
# MAGIC 	-- Yellow CCs
# MAGIC 		, ROUND(SUM(y_total*ib_weight_m1_adj),2) AS y_total_cc_sum
# MAGIC 		, ROUND(SUM(y_no_host*ib_weight_m1_adj),2) AS y_no_host_cc_sum
# MAGIC 		, ROUND(SUM(y_host*ib_weight_m1_adj),2) AS y_host_cc_sum
# MAGIC 		, ROUND(SUM(y_hp*ib_weight_m1_adj),2) AS y_hp_cc_sum
# MAGIC 		, ROUND(SUM(y_refill*ib_weight_m1_adj),2) AS y_refill_cc_sum
# MAGIC 		, ROUND(SUM(y_reman*ib_weight_m1_adj),2) AS y_reman_cc_sum
# MAGIC 		, ROUND(SUM(y_clone*ib_weight_m1_adj),2) AS y_clone_cc_sum
# MAGIC 		, ROUND(SUM(y_nohp*ib_weight_m1_adj),2) AS y_nohp_cc_sum		
# MAGIC 	-- Photo Black CCs
# MAGIC 		, ROUND(SUM(lk_total*ib_weight_m1_adj),2) AS lk_total_cc_sum
# MAGIC 		, ROUND(SUM(lk_no_host*ib_weight_m1_adj),2) AS lk_no_host_cc_sum
# MAGIC 		, ROUND(SUM(lk_host*ib_weight_m1_adj),2) AS lk_host_cc_sum
# MAGIC 		, ROUND(SUM(lk_hp*ib_weight_m1_adj),2) AS lk_hp_cc_sum
# MAGIC 		, ROUND(SUM(lk_refill*ib_weight_m1_adj),2) AS lk_refill_cc_sum
# MAGIC 		, ROUND(SUM(lk_reman*ib_weight_m1_adj),2) AS lk_reman_cc_sum
# MAGIC 		, ROUND(SUM(lk_clone*ib_weight_m1_adj),2) AS lk_clone_cc_sum
# MAGIC 		, ROUND(SUM(lk_nohp*ib_weight_m1_adj),2) AS lk_nohp_cc_sum		
# MAGIC 	-- Color CCs
# MAGIC 		, ROUND(SUM(col_total*ib_weight_m1_adj),2) AS col_total_cc_sum	
# MAGIC 		, ROUND(SUM(col_no_host*ib_weight_m1_adj),2) AS col_no_host_cc_sum	
# MAGIC 		, ROUND(SUM(col_host*ib_weight_m1_adj),2) AS col_host_cc_sum	
# MAGIC 		, ROUND(SUM(col_hp*ib_weight_m1_adj),2) AS col_hp_cc_sum	
# MAGIC 		, ROUND(SUM(col_refill*ib_weight_m1_adj),2) AS col_refill_cc_sum	
# MAGIC 		, ROUND(SUM(col_reman*ib_weight_m1_adj),2) AS col_reman_cc_sum	
# MAGIC 		, ROUND(SUM(col_clone*ib_weight_m1_adj),2) AS col_clone_cc_sum	
# MAGIC 		, ROUND(SUM(col_nohp*ib_weight_m1_adj),2) AS col_nohp_cc_sum		
# MAGIC 	-- Total CCs
# MAGIC 		, ROUND(SUM(tot_cc*ib_weight_m1_adj),2) AS tot_cc_sum
# MAGIC 		, ROUND(SUM(total_cc_check*ib_weight_m1_adj),2) AS total_cc_check_sum
# MAGIC 	-- Total No Host CCs
# MAGIC 		, ROUND(SUM(tot_no_host_cc*ib_weight_m1_adj),2) AS tot_no_host_cc_sum
# MAGIC 		, ROUND(SUM(total_no_host_cc_check*ib_weight_m1_adj),2) AS total_no_host_cc_check_sum		
# MAGIC 	-- HP CCs
# MAGIC 		, ROUND(SUM(tot_hp*ib_weight_m1_adj),2) AS tot_hp_cc_sum
# MAGIC 		, ROUND(SUM(hp_cc_check*ib_weight_m1_adj),2) AS hp_cc_check_sum	
# MAGIC 	-- HP Inserted CCs
# MAGIC 		, ROUND(SUM(inserted_hp_cc*ib_weight_m1_adj),2) AS inserted_hp_cc_sum		
# MAGIC 	-- Total Pages
# MAGIC 		, ROUND(SUM(total_pages*ib_weight_m1_adj),2) AS total_pages_sum		
# MAGIC 	-- Other
# MAGIC 		, SUM(days*ib_weight_m1_adj) AS days_ib_ext_sum
# MAGIC 		, COUNT(DISTINCT printer_id) AS printer_count
# MAGIC 		, sum(ib_weight) as ib_weight
# MAGIC 	FROM sub_a  
# MAGIC 	WHERE 1=1
# MAGIC 		--AND printer_managed = 'TRADITIONAL'
# MAGIC 	GROUP BY
# MAGIC 		--printer_short_name
# MAGIC 		printer_platform_name
# MAGIC 		, rdma_platform_subset_name
# MAGIC 		, printer_region_code
# MAGIC 		, derived_country_name
# MAGIC 		, hp_country_common_name
# MAGIC 		, year
# MAGIC 		, quarter
# MAGIC 		, date_month_dim_ky
# MAGIC 		, printer_managed
# MAGIC 		--, product_price_range
# MAGIC 		--, product_technology_type
# MAGIC 		--, product_technology_split
# MAGIC 		--, platform_name
# MAGIC 		--, product_model_name
# MAGIC 		--, product_line_code
# MAGIC 		--, product_function_code
# MAGIC 		--, product_business_model
# MAGIC )
# MAGIC SELECT 
# MAGIC -- Ref
# MAGIC 	--printer_short_name
# MAGIC 	printer_platform_name
# MAGIC 	, rdma_platform_subset_name
# MAGIC 	, printer_region_code
# MAGIC 	, derived_country_name as country_alpha2
# MAGIC 	, hp_country_common_name
# MAGIC 	, year
# MAGIC 	, quarter
# MAGIC 	, date_month_dim_ky
# MAGIC 	, printer_managed
# MAGIC 	--, product_price_range
# MAGIC 	--, product_technology_type
# MAGIC 	--, product_technology_split
# MAGIC 	--, platform_name
# MAGIC 	--, product_model_name
# MAGIC 	--, product_line_code
# MAGIC 	--, product_function_code
# MAGIC 	--, product_business_model
# MAGIC 	, tot_hp_cc_sum/nullif(tot_no_host_cc_sum,0) AS hp_share
# MAGIC 	, tot_hp_cc_sum as ps_num
# MAGIC   , tot_no_host_cc_sum as ps_den
# MAGIC 	, k_total_cc_sum/nullif((days_ib_ext_sum/30.4),0) AS black_cc_ib_wtd_avg
# MAGIC 	, col_total_cc_sum/nullif((days_ib_ext_sum/30.4),0) AS color_cc_ib_wtd_avg
# MAGIC 	, c_total_cc_sum/nullif((days_ib_ext_sum/30.4),0) AS cyan_cc_ib_wtd_avg
# MAGIC 	, m_total_cc_sum/nullif((days_ib_ext_sum/30.4),0) AS magenta_cc_ib_wtd_avg
# MAGIC 	, y_total_cc_sum/nullif((days_ib_ext_sum/30.4),0) AS yellow_cc_ib_wtd_avg
# MAGIC 	, tot_cc_sum/nullif((days_ib_ext_sum/30.4),0) as tot_cc_ib_wtd_avg
# MAGIC 	, total_pages_sum/nullif((days_ib_ext_sum/30.4),0) AS total_pages_ib_wtd_avg
# MAGIC 	, col_total_cc_sum/nullif(col_total_cc_sum+k_total_cc_sum,0) AS pct_color
# MAGIC 	, printer_count AS reporting_printers
# MAGIC 	, ib_weight
# MAGIC FROM sub_b
# MAGIC WHERE 1=1
# MAGIC 	--AND printer_platform_name = 'CESAR'
# MAGIC 	--AND printer_region_code = 'EU'
# MAGIC 	--AND date_month_dim_ky = '201905'
# MAGIC 	AND date_month_dim_ky < ${cutoffDate}
# MAGIC 	--AND printer_managed != 'ENROLLED' #decision to include enrolled as i-ink
# MAGIC ORDER BY date_month_dim_ky
# MAGIC 	, printer_region_code
# MAGIC 	, printer_platform_name
# MAGIC """
# MAGIC 
# MAGIC val tableMonth = readRedshiftToDF(configs)
# MAGIC   .option("query", tableMonthQuery)
# MAGIC   .load()
# MAGIC 
# MAGIC tableMonth.createOrReplaceTempView("table_month")

# COMMAND ----------

table_month <- SparkR::collect(SparkR::sql("SELECT * FROM table_month"))

# COMMAND ----------

#table_month$rtm <- ifelse(table_month$printer_managed=="SUBSCRIBED","I-INK", ifelse(table_month$printer_managed=="ENROLLED","I-INK","TRAD"))
table_month$rtm <- table_month$printer_managed

table_month$rdma_platform_subset_name <- ifelse(str_sub(table_month$rdma_platform_subset_name,start=-3)==' EM',substr(table_month$rdma_platform_subset_name,1,nchar(table_month$rdma_platform_subset_name)-3),table_month$rdma_platform_subset_name)
table_monthrdma_platform_subset_name <- ifelse(str_sub(table_month$rdma_platform_subset_name,start=-3)==' DM',substr(table_month$rdma_platform_subset_name,1,nchar(table_month$rdma_platform_subset_name)-3),table_month$rdma_platform_subset_name)


ib_version <- dbutils.widgets.get("ib_version") #SELECT SPECIFIC VERSION
#ib_version <- as.character(dbGetQuery(cprod,"select max(version) as ib_version from IE2_Prod.dbo.ib with (NOLOCK)"))  #Phoenix

#IB Table from MDM  
  ibtable <- dbGetQuery(sfai,paste("
                   select  a.platform_subset
                          ,YEAR(a.cal_date)*100+MONTH(a.cal_date) AS month_begin
                          ,d.technology AS hw_type
                          ,a.customer_engagement  AS RTM
                          ,b.region_5
                          ,a.country
                          --,substring(b.developed_emerging,1,1) as de
                          ,sum(a.units) as ib
                          ,a.version
                          ,d.hw_product_family as platform_division_code
                    from IE2_Prod.dbo.ib a
                    left join IE2_Prod.dbo.iso_country_code_xref b
                      on (a.country=b.country_alpha2)
                    left join IE2_Prod.dbo.hardware_xref d
                       on (a.platform_subset=d.platform_subset)
                    where a.measure='ib'
                      and a.version='",ib_version,"'
                      and (upper(d.technology)='INK' or (d.technology='PWA' and (upper(d.hw_product_family) not in ('TIJ_4.XG2 ERNESTA ENTERPRISE A4','TIJ_4.XG2 ERNESTA ENTERPRISE A3'))
                          and a.platform_subset not like 'PANTHER%' and a.platform_subset not like 'JAGUAR%'))
                      and product_lifecycle_status not in ('E','M')
                    group by a.platform_subset, a.cal_date, d.technology, a.customer_engagement,a.version
                          --, b.developed_emerging
                            , b.region_5, a.country,d.hw_product_family
                   ",sep="", collapse = NULL))

#Get Market10 Information
country_info <- dbGetQuery(sfai,"
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
country_info <- sqldf("SELECT * from country_info where country_alpha2 in (select distinct country from ibtable)")
hw_info <- dbGetQuery(sfai,
                    "SELECT distinct platform_subset, pl as  product_line_code, epa_family as platform_division_code, sf_mf as platform_function_code
                    , SUBSTRING(mono_color,1,1) as cm, UPPER(brand) as product_brand
                    , mono_ppm as print_mono_speed_pages, color_ppm as print_color_speed_pages
                    , intro_price, intro_date, vc_category as market_group
                    , predecessor
                    , hw_product_family as supplies_family
                    from IE2_Prod.dbo.hardware_xref
                    "
                    )
#Get Intro Dates
ibintrodt <- dbGetQuery(sfai,"
            SELECT  a.platform_subset, a.customer_engagement
                    ,min(cal_date) AS intro_date
                    FROM IE2_Prod.dbo.ib a
                    LEFT JOIN IE2_Prod.dbo.hardware_xref d
                      ON (a.platform_subset=d.platform_subset)
                    WHERE a.measure='ib'
                      AND a.version = (select max(version) from IE2_Prod.dbo.ib)
                      AND d.technology in ('INK','PWA')
                    GROUP BY a.platform_subset, customer_engagement
                   ")
ibintrodt$intro_yyyymm <- paste0(substr(ibintrodt$intro_date,1,4),substr(ibintrodt$intro_date,6,7))

table_month <- sqldf("
                     with sub1 as (
             select a.printer_platform_name, a.rdma_platform_subset_name, a.printer_region_code
              , a.country_alpha2, ci.market10, ci.region_5, ci.developed_emerging, a.rtm
              , hw.platform_function_code, hw.cm
              --, a.platform_business_code
              , a.year, a.quarter, a.date_month_dim_ky as fyearmo, a.printer_managed --, hw.market_group as platform_market_code
              , hw.platform_division_code, hw.product_brand
              , hw.intro_price as product_intro_price, id.intro_yyyymm as product_introduction_date --, a.product_printhead_technology_designator
              , hw.print_mono_speed_pages, hw.print_color_speed_pages
              --, a.product_model_name ,a.product_financial_market_code
              , hw.product_line_code --, a.product_function_code, a.product_business_model

              , a.tot_cc_ib_wtd_avg as total_cc                                
              , a.tot_cc_ib_wtd_avg as usage      --Consumed Ink
              , a.color_cc_ib_wtd_avg as c_usage  --Consumed color Ink

              , a.hp_share  --Ink Share
              , a.ps_num
              , a.ps_den
              , a.reporting_printers as sumn
              , a.pct_color


             from table_month a
             --LEFT JOIN sub0 
             --on a.rdma_platform_subset_name=sub0.rdma_platform_subset_name 
             --and a.printer_region_code=sub0.printer_region_code
             --and a.printer_managed=sub0.printer_managed
             LEFT JOIN hw_info hw
              on a.rdma_platform_subset_name=hw.platform_subset
             LEFT JOIN country_info ci 
              ON a.country_alpha2=ci.country_alpha2 
             LEFT JOIN ibintrodt id
              ON a.rdma_platform_subset_name=id.platform_subset and a.rtm=id.customer_engagement
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
                       SELECT rdma_platform_subset_name, country_alpha2, platform_function_code, cm --, platform_business_code
                       --,printer_platform_name, full_platform_name
                        , market10, region_5, developed_emerging
                        , year, quarter 
                        , (year||quarter) as  fiscal_year_quarter
                        , rtm --, platform_market_code
                        , platform_division_code, product_brand
                        , min(product_intro_price) as product_intro_price
                        , product_introduction_date --, product_printhead_technology_designator
                        , print_mono_speed_pages, print_color_speed_pages --, product_model_name
                        --, product_financial_market_code
                        , product_line_code --, product_function_code, product_business_model

                        , avg(pct_color) as pct_color
                        --, avg(k_hp_avg) as k_hp_avg
                        , sum(sumn) as sumn
                        --, sum(days) as days
                        , avg(usage) as usage
                        , avg(hp_share) as hp_share_avg        --straight average hp_share
                        , sum(ps_num)/sum(ps_den) AS hp_share  --weighted average hp_share

                        FROM table_month
                        group by
                        --printer_platform_name, full_platform_name, 
                        rdma_platform_subset_name, country_alpha2, platform_function_code, cm --, platform_business_code
                        , market10, region_5, developed_emerging
                        , year, quarter 
                        , rtm --, platform_market_code
                        , platform_division_code, product_brand --, product_intro_price
                        , product_introduction_date --, product_printhead_technology_designator
                        , print_mono_speed_pages, print_color_speed_pages --, product_model_name
                        --, product_financial_market_code
                        , product_line_code --, product_function_code, product_business_model
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

# Pred_Raw <- Pred_Raw_in %>%
#   mutate(Count = 1,
#          Predecessor = ifelse(grepl(Predecessor, pattern = "N/A", ignore.case = T), yes = NA, no = trimws(toupper(as.character(Predecessor)))),
#          platform_subset_name = trimws(toupper(as.character(platform_subset_name)))) %>%
#   group_by(platform_subset_name, Predecessor) %>%
#   dplyr::summarise(Count = sum(Count)) %>% 
#   filter(!Predecessor %in% c("?", ""),
#          !is.na(Predecessor)) %>%
#   select(-Count) %>%
#   group_by(platform_subset_name) %>%
#   mutate(Count = seq_along(platform_subset_name),
#          Count_max = max(Count),
#         Count_2 = ifelse(Predecessor == "NONE" & Count == 1 & Count < Count_max, yes = 0, no = Count)) %>%
#   filter(Count_2 != 0) %>%
#    group_by(platform_subset_name) %>%
#   filter(Count_2 == min(Count_2)) %>%
#   select(-Count, -Count_max, -Count_2)

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

#printer_list<-subset(printer_list,platform_page_category=="A4")
#printer_list<-printer_list[c(1:8)]

#product_ib2a <- read_csv(file="C:/Users/timothy/Documents/IB/IB_30Oct19.csv", na="")
#product_ib2a$region <- ifelse(product_ib2a$region_5=="N.Amer","NA",as.character(product_ib2a$Region_5))
#product_ib2a$de <- ifelse(product_ib2a$Country_Alpha2=="check",substring(as.character(product_ib2a$SubRegion.25),nchar(as.character(product_ib2a$SubRegion.25))), ifelse(product_ib2a$Country_Alpha2 %in% c("AU","CA","DE","ES","FR","GB","IT","US"),"D","E"))

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
                       SELECT rdma_platform_subset_name as platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5 as region_code 
                       --, de                 
                        , avg(hp_share) as hp_share
                        , SUM(sumn) as printer_count_month_ps
                      from  page_share_2
                      group by rdma_platform_subset_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5
                       --, de                 
                      order by platform_name,fiscal_year_quarter
                       , product_brand, rtm
                       ,region_code 
                       
                       ")
page_share_m10 <- sqldf("
                       SELECT rdma_platform_subset_name as platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5 as region_code 
                       , market10
                       --, de                 
                        , avg(hp_share) as hp_share
                        , SUM(sumn) as printer_count_month_ps
                      from  page_share_2
                      group by rdma_platform_subset_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5
                       , market10
                      order by platform_name,fiscal_year_quarter
                       , product_brand, rtm
                       ,region_code 

                       ")
page_share_mde <- sqldf("
                       SELECT rdma_platform_subset_name as platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5 as region_code 
                       , market10
                       , developed_emerging                 
                        , avg(hp_share) as hp_share
                        , SUM(sumn) as printer_count_month_ps
                      from  page_share_2
                      group by rdma_platform_subset_name, fiscal_year_quarter
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
                       SELECT rdma_platform_subset_name as platform_name, fiscal_year_quarter
                       , product_brand
                       , rtm
                       , region_5 as region_code 
                       , market10
                       , developed_emerging  
                       , country_alpha2
                        , avg(hp_share) as hp_share
                        , SUM(sumn) as printer_count_month_ps
                      from  page_share_2
                      group by rdma_platform_subset_name, fiscal_year_quarter
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
  
  #fitmodel3 <- lm((value) ~ timediff, data=dat1)
    
  # timediff <- c(0:59)
  # preddata <- as.data.frame(timediff)
  # 
  # #dat2<- dat1[,c("timediff")]
  # preddata <- merge(preddata,dat1, by="timediff", all.x=TRUE)
  # 
  # fit1 <- as.data.frame(predict(object=fitmodel,newdata=preddata, type="response", se.fit=F))
  # fit2 <- as.data.frame(exp(predict(object=fitmodel2,newdata=preddata, type="response", se.fit=F)))
  # #fit3 <- as.data.frame(predict(object=fitmodel3,newdata=preddata, type="response", se.fit=F))
  # 
  # colnames(fit1) <- "fit1"
  # colnames(fit2) <- "fit2"
  # colnames(fit3) <- "fit3"
  # preddata <- cbind(preddata,fit1,fit2,fit3)
  # preddata <- sqldf(paste("select a.*, b.value as obs
  #                           --, b.supply_count_share
  #                         from preddata a left join dat1 b on a.timediff=b.timediff"))
  # adjval <- 0.9
  # 
  # preddata$fit4 <- ifelse(!is.na(preddata$hp_share),preddata$hp_share, ifelse(preddata$timediff<maxtimediff2,preddata$fit1,
  #                         adjval^(preddata$timediff-maxtimediff2)*preddata$fit2+(1-(adjval^(preddata$timediff-maxtimediff2)))*preddata$fit1))
  #preddata$fit5 <- ifelse(!is.na(preddata$hp_share),preddata$hp_share, ifelse(preddata$timediff<maxtimediff2,preddata$fit1,
   #                       0.5^(1+(preddata$timediff-maxtimediff2) %/% 3)*preddata$fit2+(1-0.5^(1+(preddata$timediff-maxtimediff2) %/% 3))*preddata$fit1))
  #preddata$fit5 <- ifelse(!is.na(preddata$hp_share),preddata$hp_share, ifelse(preddata$timediff<maxtimediff2,preddata$fit1,
  #                        0.5^(preddata$timediff-maxtimediff2)*preddata$fit3+(1-0.5^(preddata$timediff-maxtimediff2))*preddata$fit1))
  #preddata$rpgp <- ifelse(preddata$supply_count_share<200,1,ifelse(preddata$supply_count_share<1000,2,
   #                              ifelse(preddata$supply_count_share<10000,3,4)))

  # plot(y=preddata$value,x=preddata$timediff, type="p", col=preddata$rpgp
  #       ,main=paste("Share for ",printer)
  #        ,xlab="Date", ylab="HP Share", ylim=c(0,1))
  # lines(y=preddata$value,x=preddata$timediff, col='black')
  # lines(y=preddata$fit1,x=preddata$timediff,col="blue")
  # lines(y=preddata$fit2,x=preddata$timediff,col="red")
  # lines(y=preddata$fit3,x=preddata$timediff,col="green")
  # lines(y=preddata$fit4,x=preddata$timediff,col="purple", lty=3)
  # lines(y=preddata$fit5,x=preddata$timediff,col="orange", lty=4)
  #legend(x=50,y=.5, c("<500","<1,000","<10,000",">10,000"),cex=0.4,col=c(1,2,3,4),pch=1)

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

#write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","Page Share for 100 percent IB DE ",".csv", sep=''), x=proxylist_final,row.names=FALSE, na="")

# COMMAND ----------

###Need to remove files to clear up memory
ibversion <- unique(ibtable$version)
#rm(product_ib2a)
#rm(ibtable)
# rm(data_coefc)
# rm(data_coefd)
# rm(data_coefr)
# rm(data_coefm)
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

UPM <- SparkR::read.parquet(path=paste0("s3://", aws_bucket_name, "UPM_Ink_Ctry(",UPMDate,").parquet"))
UPMC <- SparkR::read.parquet(path=paste0("s3://", aws_bucket_name, "UPM_Ink_Ctry_Color(",UPMDate,").parquet"))

#UPM$MPV_N <- as.numeric(as.character(UPM$MPV_N))
#UPM$MPV_Raw <- as.numeric(as.character(UPM$MPV_Raw))

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
UPM2$FYQtr   <- gsub("\\.","Q",lubridate::quarter(as.Date(paste0(UPM2$FYearMo,'01'),format='%Y%m%d'),with_year=TRUE,fiscal_start=11))
UPM2$MPV_UPM <- (UPM2$MPV_UPMIB)
UPM2$MPV_Raw <- (UPM2$MPV_RawIB)
UPM2$MPV_TS  <- (UPM2$MPV_TSIB)
UPM2$MPV_TD  <- (UPM2$MPV_TDIB)

#UPM2c$MPV_UPM <- (UPM2c$MPV_UPMIB)
#UPM2c$MPV_Raw <- (UPM2c$MPV_RawIB)
UPM2c$MPV_TS  <- (UPM2c$MPV_TSIB)
UPM2c$MPV_TD  <- (UPM2c$MPV_TDIB)

months <- as.data.frame(seq(as.Date("1989101",format="%Y%m%d"),as.Date("20500101",format="%Y%m%d"),"month"))
colnames(months) <- "CalYrDate"
quarters2 <- as.data.frame(months[rep(seq_len(nrow(months)),nrow(proxylist_final2)),])
colnames(quarters2) <- "CalDate"
# final_list <- proxylist_final2[rep(seq_len(nrow(proxylist_final2)),each=nrow(months)),] 
# final_list <- cbind(final_list,quarters2)
# 
# final_list$printer_intro_month <- as.Date(paste0(final_list$printer_intro_month,'01'),format="%Y%m%d")
# final_list$timediff <- as.numeric(((as.Date(final_list$CalDate, format="%Y-%m-%d")-as.Date(final_list$printer_intro_month,format="%Y-%m-%d"))/365)*4)
# final_list$FYearMo <- format(final_list$CalDate,"%Y%m")
# final_list$FYQtr <- lubridate::quarter(as.Date(final_list$'CalDate', format="%Y-%m-%d"),with_year=TRUE, fiscal_start=11)
# final_list$FYQtr <- gsub("[.]","Q",final_list$FYQtr)
# 
# final_list$model_share_ps <- ifelse(final_list$timediff<0,NA,sigmoid_pred(final_list$ps_min,final_list$ps_max,final_list$ps_med,final_list$ps_spread,final_list$timediff))

# rm(UPM) #remove for space
# rm(UPMc)#remove for space
# gc()

dashampv <- table_month
dashampv2 <- sqldf("
                       SELECT rdma_platform_subset_name
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
                        , rdma_platform_subset_name
                        , product_brand
                        , platform_division_code
                        , country_alpha2 
                        , rtm
                      order by  fyearmo
                        , rdma_platform_subset_name
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
createOrReplaceTempView(UPM2C, "UPM2C")
createOrReplaceTempView(as.DataFrame(ib_table), "ib_table")
createOrReplaceTempView(as.DataFrame(page_share_ctr), "page_share_ctr")
createOrReplaceTempView(as.DataFrame(dashampv2), "dashampv2")
createOrReplaceTempView(UPM3, "UPM3")
createOrReplaceTempView(hw_info, "hw_info")


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
                        ON (a.printer_platform_name=c.platform_subset and a.Country_alpha2=c.country
                              and a.rtm=c.RTM and b.FYearMo=c.month_begin)
                      LEFT JOIN page_share_ctr d
                        ON (b.printer_platform_name=d.platform_name AND b.Country_Cd=d.country_alpha2
                            AND b.FYQtr=d.fiscal_year_quarter and b.rtm=d.rtm)
                      LEFT JOIN dashampv2 e
                        ON (b.printer_platform_name=e.rdma_platform_subset_name AND b.Country_Cd=e.country_alpha2  
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

final_list2 <- SparkR::sql("
                      select a.*
                      , case
                        when upper(rtm)='I-INK' then 'I-INK'
                        when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.BD_Share_Flag_PS is 0 then 'Modeled'
                          when a.Share_Raw_PS is NULL or 0 then 'Modeled'
                          when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then 'Modeled'
                          else 'Have Data'
                          end
                        else 'Modeled by Proxy'
                        end as Share_Source_PS
                      , case
                        when upper(rtm)='I-INK' then 1
                        when a.Platform_Subset_Nm=a.Proxy_PS then
                          case when a.BD_Share_Flag_PS is 0 then a.Share_Model_PS
                          when a.Share_Raw_PS is NULL or 0 then a.Share_Model_PS
                          when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then a.Share_Model_PS
                          else a.Share_Raw_PS
                          end
                          else a.Share_Model_PS
                          --else null
                        end as Page_Share_sig
                        , case
                          when upper(rtm)='I-INK' then 1
                          when a.Platform_Subset_Nm=a.Proxy_PS then
                            case when a.BD_Share_Flag_PS is 0 then a.Share_Model_PSlin
                            when a.Share_Raw_PS is NULL or 0 then a.Share_Model_PSlin
                            when a.Share_Raw_N_PS is NULL or a.Share_Raw_N_PS < 20 then a.Share_Model_PSlin
                            else a.Share_Raw_PS
                            end
                          else a.Share_Model_PS
                          --else null
                        end as Page_Share_lin
                      , case
                        when a.BD_Usage_Flag is NULL then 'UPM'
                        when a.BD_Share_Flag_PS is 0 then 'UPM'
                        when a.MPV_Dash is NULL then 'UPM'
                        when a.N_Dash < 20 then 'UPM'
                          else 'Dashboard'
                          end as Usage_Source
                      , case
                        when a.BD_Usage_Flag is NULL then a.MPV_TD
                        when a.BD_Share_Flag_PS is 0 then a.MPV_TD
                        when a.MPV_Dash is NULL then a.MPV_TD
                        when a.N_Dash < 20 then a.MPV_TD
                          else a.MPV_Dash
                          end as Usage
                      , case
                        when a.BD_Usage_Flag is NULL then 'UPM'
                        when a.BD_Share_Flag_PS is 0 then 'UPM'
                        when a.MPVC_Dash is NULL then 'UPM'
                        when a.N_Dash < 20 then 'UPM'
                          else 'Dashboard'
                          end as Usage_Sourcec
                      , case
                        when a.BD_Usage_Flag is NULL then a.MPV_TDc
                        when a.BD_Share_Flag_PS is 0 then a.MPV_TDc
                        when a.MPVC_Dash is NULL then a.MPV_TDc
                        when a.N_Dash < 20 then a.MPV_TDc
                          else a.MPVC_Dash
                          end as Usage_c
                     from final_list2 a
                     
                     ")

final_list2$Usage_k <- ifelse(is.na(final_list2$Usage_c),final_list2$Usage,final_list2$Usage-final_list2$Usage_c)
final_list2$Usage_c <- ifelse(is.na(final_list2$Usage_c),0,final_list2$Usage_c)

createOrReplaceTempView(final_list2, "final_list2")

# final_list2$Pages_Device_UPM <- final_list2$MPV_TS*final_list2$IB
# final_list2$Pages_Device_Raw <- final_list2$MPV_Raw*final_list2$IB
# final_list2$Pages_Device_Dash <- final_list2$MPV_Dash*final_list2$IB
# final_list2$Pages_Device_Use <- final_list2$Usage*final_list2$IB
# final_list2$Pages_Share_Model_PS <- final_list2$MPV_TS*final_list2$IB*final_list2$Share_Model_PS
# final_list2$Pages_Share_Raw_PS <- final_list2$MPV_Raw*final_list2$IB*as.numeric(final_list2$Share_Raw_PS)
# final_list2$Pages_Share_Dash_PS <- final_list2$MPV_Dash*final_list2$IB*as.numeric(final_list2$Share_Raw_PS)
# final_list2$Pages_PS <- final_list2$Pages_Device_Use*as.numeric(final_list2$Page_Share)


final_list4 <- subset(final_list2,IB>0)
final_list4 <- subset(final_list4,!is.na(MPV_UPM))
final_list4 <- subset(final_list4,!is.na(Page_Share_sig))

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
final_list6$BD_Usage_Flag <- ifelse(is.na(final_list6$BD_Usage_Flag),0,final_list6$BD_Usage_Flag)
final_list6$BD_Share_Flag_PS <- ifelse(is.na(final_list6$BD_Share_Flag_PS),0,final_list6$BD_Share_Flag_PS)
#change FIntroDt from YYYYMM to YYYYQQ
final_list6$FIntroDt <- gsub(" ","",as.character(as.yearqtr(as.Date(paste0(final_list6$FIntroDt,"01"),"%Y%m%d")+3/4)))

#rm(table_month)

gc(reset = TRUE)
#colnames(final_list5)[6] <- "Emerging/Developed"

#filenm <- paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer (",Sys.Date(),").xlsx")
#write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer (",Sys.Date(),").csv", sep=''), x=final_list6,row.names=FALSE, na="")

createOrReplaceTempView(final_list6, "final_list6")

# COMMAND ----------

# Adjust curve at join

# tempdir(check=TRUE)
#rm(list= ls()[!(ls() %in% c('ibversion','final_list6','writeout','sfai'))])  ###Clear up memory- gets rid of all but these tables
# gc(reset = TRUE)

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

#adjval^(preddata$timediff-maxtimediff2)*preddata$fit2+(1-(adjval^(preddata$timediff-maxtimediff2)))*preddata$fit1)
final_list7$hd_mchange_ps <- ifelse(final_list7$Share_Source_PS=="Modeled"|final_list7$Share_Source_PS=="Modeled by Proxy",ifelse(final_list7$lagShare_Source_PS=="Have Data",final_list7$Page_Share_sig-final_list7$lagShare_PS, NA ),NA)
final_list7$hd_mchange_ps_i <- ifelse(!is.na(final_list7$hd_mchange_ps),final_list7$index1,NA)
#final_list7$hd_mchange_cu <- ifelse(final_list7$Share_Source_CU=="Modeled",ifelse(final_list7$lagShare_Source_CU=="Have Data",final_list7$Crg_Unit_Share-final_list7$lagShare_CU, NA ),NA)
#final_list7$hd_mchange_cu_i <- ifelse(!is.na(final_list7$hd_mchange_cu),final_list7$index1,NA)
final_list7$hd_mchange_use <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage-final_list7$lagShare_Usage, NA ),NA)
final_list7$hd_mchange_usec <- ifelse(final_list7$Usage_Source=="UPM",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage_c-final_list7$lagShare_Usagec, NA ),NA)
final_list7$hd_mchange_used <- ifelse(final_list7$Usage_Source=="Dashboard",ifelse(final_list7$lagUsage_Source=="Dashboard",final_list7$Usage-final_list7$lagShare_Usage, NA ),NA)
final_list7$hd_mchange_use_i <- ifelse(!is.na(final_list7$hd_mchange_use),final_list7$index1,NA)

createOrReplaceTempView(final_list7, "final_list7")

final_list7 <- SparkR::sql("
                with sub0 as (
                    SELECT Platform_Subset_Nm,Country_Cd,rtm
                        ,max(hd_mchange_ps_i) as hd_mchange_ps_i
                        --,max(hd_mchange_cu_i) as hd_mchange_cu_i
                        ,max(hd_mchange_use_i) as hd_mchange_use_i
                        FROM final_list7
                        GROUP BY Platform_Subset_Nm,Country_Cd,rtm
                )
                , sub1ps as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,final_list7.rtm,sub0.hd_mchange_ps_i
                        ,final_list7.Page_Share_sig-final_list7.lagShare_PS AS hd_mchange_ps
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd and final_list7.rtm=sub0.rtm
                          and final_list7.hd_mchange_ps_i=sub0.hd_mchange_ps_i
                  )
                 /*, sub1cu as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,final_list7.rtm,sub0.hd_mchange_cu_i
                        ,final_list7.Page_Share-final_list7.lagShare_CU AS hd_mchange_cu
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd and final_list7.rtm=sub0.rtm 
                          and final_list7.hd_mchange_cu_i=sub0.hd_mchange_cu_i
                  )*/
                  , sub1use as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,final_list7.rtm,sub0.hd_mchange_use_i
                        ,final_list7.Usage-final_list7.lagShare_Usage AS hd_mchange_use
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd and final_list7.rtm=sub0.rtm
                          and final_list7.index1 = sub0.hd_mchange_use_i 
                  )
                  , sub1usec as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd,final_list7.FYearMo,final_list7.rtm,sub0.hd_mchange_use_i
                        ,final_list7.Usage_c-final_list7.lagShare_Usagec AS hd_mchange_usec
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd and final_list7.rtm=sub0.rtm  
                          and final_list7.index1 = sub0.hd_mchange_use_i 
                  )
                  , sub1used as( 
                    SELECT final_list7.Platform_Subset_Nm,final_list7.Country_Cd, final_list7.FYearMo,final_list7.rtm,sub0.hd_mchange_use_i
                        ,final_list7.Usage-final_list7.lagShare_Usage AS hd_mchange_used
                        FROM final_list7
                        INNER JOIN 
                        sub0 ON final_list7.Platform_Subset_Nm=sub0.Platform_Subset_Nm and final_list7.Country_Cd=sub0.Country_Cd and final_list7.rtm=sub0.rtm  
                          and final_list7.index1=sub0.hd_mchange_use_i-1
                  )
                  
                  , sub2 as (
                     SELECT distinct a.*
                      ,sub1ps.hd_mchange_ps as adjust_ps
                      ,sub1ps.hd_mchange_ps_i as adjust_ps_i
                      ,sub1use.hd_mchange_use as adjust_use
                      ,sub1usec.hd_mchange_usec as adjust_usec
                      ,sub1used.hd_mchange_used as adjust_used
                      ,sub1use.hd_mchange_use_i as adjust_use_i
                      --,sub1cu.hd_mchange_ps as adjust_cu
                      --,sub1cu.hd_mchange_ps_i as adjust_cu_i
                        
                      FROM final_list7 a
                      LEFT JOIN 
                        sub1ps
                        ON a.Platform_Subset_Nm=sub1ps.Platform_Subset_Nm and a.Country_Cd=sub1ps.Country_Cd and a.rtm=sub1ps.rtm
                      LEFT JOIN 
                        sub1use
                        ON a.Platform_Subset_Nm=sub1use.Platform_Subset_Nm and a.Country_Cd=sub1use.Country_Cd and a.rtm=sub1use.rtm
                      LEFT JOIN 
                        sub1usec
                        ON a.Platform_Subset_Nm=sub1usec.Platform_Subset_Nm and a.Country_Cd=sub1usec.Country_Cd and a.rtm=sub1usec.rtm
                      LEFT JOIN 
                        sub1used
                        ON a.Platform_Subset_Nm=sub1used.Platform_Subset_Nm and a.Country_Cd=sub1used.Country_Cd and a.rtm=sub1used.rtm
                      --LEFT JOIN 
                        --sub1cu
                        --ON a.Platform_Subset_Nm=sub1cu.Platform_Subset_Nm and a.Country_Cd=sub1cu.Country_Cd and a.rtm=sub1cu.rtm
                  )
                  SELECT *
                  FROM sub2
                     ")

#test1s <- subset(final_list7,Platform_Subset_Nm=="WEBER BASE I-INK" & Region=="NA")

final_list7$adjust_ps <- ifelse(final_list7$adjust_ps == -Inf | is.na(final_list7$adjust_ps), 0, final_list7$adjust_ps)
#final_list7$adjust_cu <- ifelse(final_list7$adjust_cu == -Inf, 0, final_list7$adjust_cu)
final_list7$adjust_used <- ifelse(is.na(final_list7$adjust_used),0,final_list7$adjust_used)
final_list7$adjust_use <- ifelse(is.na(final_list7$adjust_use),0,final_list7$adjust_use)
final_list7$adjust_usec <- ifelse(is.na(final_list7$adjust_usec),0,final_list7$adjust_usec)

final_list7$adjust_ps_i <- ifelse(is.na(final_list7$adjust_ps_i),1000000,final_list7$adjust_ps_i)
# preddata$fit4 <- ifelse(!is.na(preddata$hp_share),preddata$hp_share, ifelse(preddata$timediff<maxtimediff2,preddata$fit1,
  #                         adjval^(preddata$timediff-maxtimediff2)*preddata$fit2+(1-(adjval^(preddata$timediff-maxtimediff2)))*preddata$fit1))
adjval <- 0.99
final_list7$Page_Share_Adj <- ifelse(final_list7$Share_Source_PS=="Modeled"|final_list7$Share_Source_PS=="Modeled by Proxy",ifelse(final_list7$adjust_ps_i <= final_list7$index1, adjval^(final_list7$index1-final_list7$adjust_ps_i+1)*final_list7$Page_Share_lin +(1-(adjval^(final_list7$index1-final_list7$adjust_ps_i+1)))*final_list7$Page_Share_sig ,final_list7$Page_Share_sig),final_list7$Share_Raw_PS)

final_list7$Page_Share_Adj <- ifelse(final_list7$Page_Share_Adj>1,1,ifelse(final_list7$Page_Share_Adj<0.001,0.001,final_list7$Page_Share_Adj))

#final_list7$CU_Share_Adj <- ifelse(final_list7$Share_Source_CU=="Modeled",ifelse((final_list7$adjust_cu>0) & final_list7$adjust_cu_i<= final_list7$index1,pmax(final_list7$Crg_Unit_Share -2*(final_list7$adjust_cu),0.05),ifelse((final_list7$adjust_cu< -0.05) & final_list7$adjust_cu_i<= final_list7$index1,pmax(final_list7$Crg_Unit_Share -1/2*(final_list7$adjust_cu),0.05),final_list7$Crg_Unit_Share)),final_list7$Crg_Unit_Share)
#final_list7$CU_Share_Adj <- ifelse(final_list7$CU_Share_Adj>1,1,ifelse(final_list7$CU_Share_Adj)<0,0,final_list7$CU_Share_Adj)

final_list7$adjust_used <- ifelse(final_list7$adjust_used==0,1,final_list7$adjust_used)
final_list7$Usage_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse((abs(final_list7$adjust_use/final_list7$adjust_used)>1.5) & final_list7$adjust_use_i<= final_list7$index1,pmax(final_list7$Usage -(final_list7$adjust_use+final_list7$adjust_used),0.05),final_list7$Usage),final_list7$Usage)

final_list7$Usagec_Adj <- ifelse(final_list7$Usage_Source=="UPM",ifelse((abs(final_list7$adjust_usec/final_list7$adjust_used)>1.5) & final_list7$adjust_use_i<= final_list7$index1,pmax(final_list7$Usage_c -(final_list7$adjust_usec+final_list7$adjust_used),0.005),final_list7$Usage_c),final_list7$Usage_c)
final_list7$Usagek_Adj <- final_list7$Usage_Adj-final_list7$Usagec_Adj

final_list7$Page_Share <- as.numeric(final_list7$Page_Share_Adj)
#final_list7$Crg_Unit_Share <- as.numeric(final_list7$CU_Share_Adj)
final_list7$Usage <- as.numeric(final_list7$Usage_Adj)
final_list7$Usage_c <- as.numeric(final_list7$Usagec_Adj)
final_list7$Usage_k <- as.numeric(final_list7$Usagek_Adj)

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
final_list7$Page_Share <- ifelse(toupper(final_list7$rtm) == 'I-INK', 1, final_list7$Page_Share)  
final_list7$Share_Source_PS <- ifelse(toupper(final_list7$rtm) == 'I-INK', "Modeled", final_list7$Share_Source_PS)  


#Need to get TIJ_2.XG3 KRONOS?  or just Kronos platform subset?
final_list7$Page_Share <- ifelse(grepl('KRONOS', final_list7$platform_division_code), 1, final_list7$Page_Share)  #Change all Kronos platforms to have share of 1
final_list7$Share_Source_PS <- ifelse(grepl('KRONOS', final_list7$platform_division_code), "Modeled", final_list7$Share_Source_PS)  #Change all Kronos to have share of 1


#write.csv(paste("C:/Users/timothy/Documents/Insights2.0/Share_Models_files/","DUPSM Explorer Adj (",Sys.Date(),").csv", sep=''), x=final_list7,row.names=FALSE, na="")
#s3write_using(x=final_list9,FUN = write.csv, object = paste0("s3://insights-environment-sandbox/BrentT/ink_100pct_test",Sys.Date(),".csv"), row.names=FALSE, na="")

createOrReplaceTempView(final_list7, "final_list7")

# COMMAND ----------

# Change to match MDM format

final_list8 <- filter(final_list7, !isNull(Page_Share))  #missing intro date
final_list8$fiscal_date <- concat_ws(sep = "-", substr(final_list8$FYearMo, 1, 4), substr(final_list8$FYearMo, 5, 6), lit("01"))
#Change from Fiscal Date to Calendar Date
final_list8$year_month_float <- to_date(final_list8$fiscal_date, "yyyy-MM-dd")
#month(final_list8$year_month_float) <- month(final_list8$year_month_float)-2
#test3 <- subset(final_list8,Platform_Subset_Nm=="MALBEC I-INK"  & FYearMo < 202701)

#test data
# final_list9 <- sqldf("
#                      select a.Selectability, a.Platform_Subset_Nm as platform_subset, a.rtm, h.platform_division_code, a.Country_Cd, c.region_5, c.market10, a.Region_DE, 
#                       i.ib, a.FYearMo, a.MPV_UPM, a.MPV_TS, a.MPV_Raw, a.MPV_n, a.Usage, a.Page_Share, h.product_line_code, h.supplies_family
#                      from final_list8 a
#                      left join country_info c
#                       on a.Country_Cd=c.country_alpha2
#                      left join ibtable i
#                       on a.Platform_Subset_Nm=i.platform_subset and a.FYearMo=i.month_begin and a.rtm=i.RTM and a.Country_cd=i.country
#                      left join hw_info h
#                       on a.Platform_Subset_Nm=h.platform_subset
#                      where a.FYearMo between '201511' and '202611'
#                       --and Platform_Subset_Nm='WEBER MID I-INK'
#                      ")
# s3write_using(x=final_list8,FUN = write.csv, object = paste0("s3://insights-environment-sandbox/BrentT/ink_100pct_test",Sys.Date(),".csv"), row.names=FALSE, na="")

#ibversion <- unique(ibtable$version)
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

mdm_tbl <- rbind(mdm_tbl_share, mdm_tbl_usage, mdm_tbl_usagen, mdm_tbl_sharen, mdm_tbl_kusage, mdm_tbl_cusage)

mdm_tbl$cal_date <- to_date(mdm_tbl$cal_date,format="yyyy-MM-dd")
mdm_tbl$forecast_created_date <- to_date(mdm_tbl$forecast_created_date,format="yyyy-MM-dd")
mdm_tbl$load_date <- to_date(mdm_tbl$load_date,format="yyyy-MM-dd")

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
# MAGIC   .save(s"""s3://${spark.conf.get("aws_bucket_name")}ink_usage_share_75_${dbutils.widgets.get("outnm_dt")}.parquet""")
# MAGIC 
# MAGIC if (dbutils.widgets.get("writeout") == "YES") {
# MAGIC   
# MAGIC   submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), "TRUNCATE stage.usage_share_ink_landing")
# MAGIC   
# MAGIC   mdmTbl.write
# MAGIC     .format("com.databricks.spark.redshift")
# MAGIC     .option("url", configs("redshiftUrl"))
# MAGIC     .option("tempdir", configs("redshiftTempBucket"))
# MAGIC     .option("aws_iam_role", configs("redshiftAwsRole"))
# MAGIC     .option("user", configs("redshiftUsername"))
# MAGIC     .option("password", configs("redshiftPassword"))
# MAGIC     .option("dbtable", "stage.usage_share_ink_landing")
# MAGIC     .partitionBy("measure")
# MAGIC     .mode("append")
# MAGIC     .save()
# MAGIC   
# MAGIC   submitRemoteQuery(configs("redshiftUrl"), configs("redshiftUsername"), configs("redshiftPassword"), "GRANT ALL ON ALL TABLES IN schema stage TO group dev_arch_eng")
# MAGIC }

# COMMAND ----------

notebook_end_time <- Sys.time()
notebook_total_time <- notebook_end_time - notebook_start_time;notebook_total_time
