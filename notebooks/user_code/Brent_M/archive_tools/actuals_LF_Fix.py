# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

stage_odw_actuals_query = """
SELECT
	cal_date,
	country_alpha2,
	base_product_number,
	base_quantity
FROM fin_prod.odw_revenue_units_base_actuals
WHERE 1=1
	and cal_date = (SELECT MAX(cal_date) FROM fin_prod.odw_revenue_units_base_actuals)
	AND base_quantity <> 0
"""

redshift_stage_odw_actuals_records = read_redshift_to_df(configs) \
    .option("query", stage_odw_actuals_query) \
    .load()




# COMMAND ----------

write_df_to_redshift(configs, redshift_stage_odw_actuals_records, "stage.odw_revenue_units_base_landing", "overwrite")

# COMMAND ----------

from datetime import date
from pyspark.sql.functions import col, lit, when

datestamp = date.today().strftime("%Y%m%d")

staging_actuals_lf_query = f"""
SELECT
	'ACTUALS_LF' AS record,
	e.cal_date,
	e.country_alpha2,
	e.base_product_number,
	r.Platform_Subset,
	SUM(e.base_quantity) AS base_quantity,
	NULL AS load_date,
	1 AS official,
	NULL AS version,
	'ODW-LF' AS source
FROM stage.odw_revenue_units_base_landing e
LEFT JOIN mdm.rdma r on r.Base_Prod_Number = e.base_product_number
LEFT JOIN mdm.hardware_xref hw on hw.platform_subset = r.Platform_Subset
WHERE 1=1
    AND hw.technology = 'LF'
GROUP BY
	e.cal_date,
	e.country_alpha2,
	e.base_product_number,
	r.Platform_Subset
"""

staging_actuals_lf = read_redshift_to_df(configs) \
    .option("query", staging_actuals_lf_query) \
    .load()

staging_actuals_lf = staging_actuals_lf.withColumn("load_date", col("load_date").cast("timestamp"))
    
if staging_actuals_lf.count() > 0:
        #   save "landing" data to S3
        s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/actuals_hw/lf/20221027/"
        write_df_to_s3(staging_actuals_lf, s3_output_bucket, "parquet", "overwrite")
        # write_df_to_s3(staging_actuals_lf, constants["S3_BASE_BUCKET"][dbutils.widgets.get("stack")] + f"/product/actuals_hw/lf/20221027/", "parquet", "overwrite")

        max_version_info = call_redshift_addversion_sproc(configs, 'ACTUALS_LF', 'ODW-LF')

        max_version = max_version_info[0]
        max_load_date = max_version_info[1]

        staging_actuals_lf = staging_actuals_lf \
            .withColumn("version", lit(max_version)) \
            .withColumn("load_date", lit(max_load_date))

        write_df_to_redshift(configs, staging_actuals_lf, "stage.actuals_hw", "overwrite")

# COMMAND ----------

query = f"""
    SELECT
        record,
        cal_date,
        country_alpha2,
        base_product_number,
        a.platform_subset,
        source,
        base_quantity,
        a.official,
        a.load_date,
        a.version
    FROM stage.actuals_hw a
    LEFT JOIN mdm.hardware_xref b ON a.platform_subset = b.platform_subset
    LEFT JOIN mdm.product_line_xref c ON b.pl = c.pl
    WHERE c.PL_category = 'HW'
        AND c.Technology IN ('INK','LASER','PWA','LF')
        AND a.base_quantity <> 0
    """

stage_actuals_hw = read_redshift_to_df(configs) \
    .option("query", query) \
    .load()

write_df_to_redshift(configs, stage_actuals_hw, "prod.actuals_hw", "append")
