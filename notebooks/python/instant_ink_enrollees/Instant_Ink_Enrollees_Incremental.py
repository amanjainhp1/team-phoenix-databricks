# Databricks notebook source
dbutils.widgets.text("REDSHIFT_SECRET_NAME", "")
dbutils.widgets.dropdown("REDSHIFT_REGION_NAME", "us-west-2", ["us-west-2", "us-east-2"])

# COMMAND ----------

import boto3
import io
import yaml
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../common/s3_utils

# COMMAND ----------

# set common parameters
sfai_hostname = "sfai.corp.hpicloud.net"
sfai_port = 1433
sfai_url = f"jdbc:sqlserver://{sfai_hostname}:{sfai_port};database="
sfai_username = "databricks_user" # placeholder
sfai_password = "databricksdemo123" # placeholder

# COMMAND ----------

redshift_secret_name = dbutils.widgets.get("REDSHIFT_SECRET_NAME")
redshift_region_name = dbutils.widgets.get("REDSHIFT_REGION_NAME")

redshift_username = secrets_get(redshift_secret_name, redshift_region_name)["username"]
redshift_password = secrets_get(redshift_secret_name, redshift_region_name)["password"]

# COMMAND ----------

# set table specific parameters
database = "IE2_Landing"
schema = "dbo"
table = "instant_ink_enrollees_landing_jupyter"

# extract data from SFAI
iink_df = spark.read \
  .format("jdbc") \
  .option("url", sfai_url + database) \
  .option("dbTable", f"{schema}.{table}") \
  .option("user", sfai_username) \
  .option("password", sfai_password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()

# COMMAND ----------

ns_df.read \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
  .option("dbtable", "prod.norm_shipments") \
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
  .option("user", redshift_username) \
  .option("password", redshift_password) \
  .mode("append") \
  .load()


# COMMAND ----------

# extract data from Redshift
ns_df = spark.read \
  .format("jdbc") \
  .option("url", sfai_url + database) \
  .option("dbTable", f"{schema}.{table}") \
  .option("user", sfai_username) \
  .option("password", sfai_password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()

# COMMAND ----------

cc_df.read \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
  .option("dbtable", "mdm.iso_cc_rollup_xref") \
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
  .option("user", redshift_username) \
  .option("password", redshift_password) \
  .mode("append") \
  .load()


# COMMAND ----------

# extract data from Redshift
cc_df = spark.read \
  .format("jdbc") \
  .option("url", sfai_url + database) \
  .option("dbTable", f"{schema}.{table}") \
  .option("user", sfai_username) \
  .option("password", sfai_password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()

# COMMAND ----------

iink_df.createOrReplaceTempView("iink_table")
transformed_iink_nosplits = spark.sql("SELECT DISTINCT iel.record ,iel.country_expanded ,iel.platform_subset ,COALESCE(sub_brand,'UNKNOW') sub_brand,CONCAT(LEFT(year_month,4),'-',RIGHT(year_month,2),'-01') cal_date ,year_fiscal ,quarter_fiscal ,yrqtr_fiscal ,data_source ,yr_mo_max_for_actuals ,cartridge_tech_n_name ,ctg_type ,ink_platform ,region ,printer_sell_out_units ,printer_sell_out_units_participating ,enroll_card_units ,all_enrollments_customer ,all_enrollments_replacement ,p1_kit_enrollments ,p1_kitless_enrollments ,p2_kitless_enrollments ,cancellations_customer ,cancellations_replacement ,cum_enrollees_month ,cum_enrollees_quarter ,iel.load_date ,iel.version ,iel.official FROM iink_table iel WHERE year_month IS NOT NULL AND iel.platform_subset IS NOT NULL and iel.country_expanded NOT IN ('Greater China')")

transformed_iink_nosplits.createOrReplaceTempView("iink_nosplits")


# COMMAND ----------

ns_df.createOrReplaceTempView("ns_table")
cc_df.createOrReplaceTempView("cc_table")

ns_splits = spark.sql("SELECT DISTINCT platform_subset, n.country_alpha2, COALESCE(cc.country_level_1,n.country_alpha2) country_group, COALESCE(SUM(n.units) OVER (PARTITION BY platform_subset, n.country_alpha2)/NULLIF(SUM(n.units) OVER (PARTITION BY platform_subset,COALESCE(cc.country_level_1,n.country_alpha2)),0),1) csplit FROM ns_table n LEFT JOIN cc_table cc on cc.country_alpha2 = n.country_alpha2 and country_scenario = 'I-Ink' WHERE n.version = (SELECT max(version) FROM ns_table) AND cal_date BETWEEN '2019-11-01' AND '2020-10-31' AND cc.country_level_1 IS NOT NULL ORDER BY country_group")
missing_cntry = spark.sql("SELECT n.country_alpha2,c.country_level_1,SUM(units) units,ROW_NUMBER () OVER (PARTITION BY c.country_level_1 ORDER BY SUM(units) DESC) rn FROM ns_table n INNER JOIN cc_table c ON c.country_alpha2 = n.country_alpha2 WHERE country_scenario = 'I-INK' and country_level_1 is not null and c.country_alpha2 !=country_level_1 AND n.version = (SELECT max(version) FROM ns_table ) AND cal_date BETWEEN '2019-11-01' AND '2020-10-31' GROUP BY n.country_alpha2,c.country_level_1")


# COMMAND ----------

ns_splits.createOrReplaceTempView("splits_table")
missing_cntry.createOrReplaceTempView("msng_cntry")

transformed_iink_cntrysplits = spark.sql("SELECT iel.record ,cc.country_alpha2 ,iel.platform_subset ,COALESCE(sub_brand,'UNKNOWN') sub_brand ,CONCAT(LEFT(year_month,4),'-',RIGHT(year_month,2),'-01') cal_date ,year_fiscal ,quarter_fiscal ,yrqtr_fiscal ,data_source ,yr_mo_max_for_actuals ,cartridge_tech_n_name ,ctg_type ,ink_platform ,region ,printer_sell_out_units ,printer_sell_out_units_participating ,enroll_card_units ,all_enrollments_customer ,all_enrollments_replacement ,p1_kit_enrollments ,p1_kitless_enrollments ,p2_kitless_enrollments ,cancellations_customer ,cancellations_replacement ,cum_enrollees_month * COALESCE(cc.csplit,1) cum_enrollees_month ,cum_enrollees_quarter ,iel.load_date ,iel.version ,iel.official FROM iink_table iel INNER JOIN splits_table cc on cc.platform_subset = iel.platform_subset and cc.country_group = iel.country WHERE year_month IS NOT NULL AND iel.platform_subset IS NOT NULL UNION SELECT iel.record ,cl.country_alpha2 ,iel.platform_subset ,COALESCE(sub_brand,'UNKNOWN') sub_brand ,CONCAT(LEFT(year_month,4),'-',RIGHT(year_month,2),'-01') cal_date ,year_fiscal ,quarter_fiscal ,yrqtr_fiscal ,data_source ,yr_mo_max_for_actuals ,cartridge_tech_n_name ,ctg_type ,ink_platform ,region ,printer_sell_out_units ,printer_sell_out_units_participating ,enroll_card_units ,all_enrollments_customer ,all_enrollments_replacement ,p1_kit_enrollments ,p1_kitless_enrollments ,p2_kitless_enrollments ,cancellations_customer ,cancellations_replacement ,cum_enrollees_month * COALESCE(cc.csplit,1) cum_enrollees_month ,cum_enrollees_quarter,iel.load_date ,iel.version ,iel.official FROM iink_table iel LEFT JOIN splits_table cc on cc.platform_subset = iel.platform_subset and cc.country_group = iel.country INNER JOIN msng_cntry cl on cl.country_level_1 = iel.country and cl.rn = 1 WHERE year_month IS NOT NULL AND iel.platform_subset IS NOT NULL AND iel.country IN ('Greater China') AND cc.country_alpha2 IS NULL AND NOT EXISTS (SELECT 1 FROM splits_table c WHERE c.platform_subset = iel.platform_subset and c.country_alpha2 = cl.country_alpha2)")

transformed_iink_cntrysplits.createOrReplaceTempView("iink_with_splits")



# COMMAND ----------

final_data = spark.sql("SELECT * FROM iink_nosplits UNION SELECT * FROM iink_with_splits")

display(final_data)

# COMMAND ----------

final_data.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://dataos-redshift-core-dev-01.hp8.us:5439/dev?ssl_verify=None") \
  .option("dbtable", "stage.instant_ink_enrollees") \
  .option("tempdir", "s3a://dataos-core-dev-team-phoenix/redshift_temp/") \
  .option("aws_iam_role", "arn:aws:iam::740156627385:role/team-phoenix-role") \
  .option("user", redshift_username) \
  .option("password", redshift_password) \
  .mode("append") \
  .save()

