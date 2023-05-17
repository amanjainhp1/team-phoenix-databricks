# Databricks notebook source
# 3/17/2023 - Brent Merrick
# LLC actuals and WD3 data load process


# COMMAND ----------

# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actuals

# COMMAND ----------

# pull the Asia Pacific LLC actuals data and do a little bit of ETL
LLC_AP_Actuals_query = """
SELECT
	'SHIP-ACTUALS-LLC' AS record,
	'AP' AS geography,
	'REG4' AS geography_grain,
	ships.base_product_number AS base_product_number,
	ships.cal_date,
	sum(base_quantity) AS units,
	ships.load_date,
	NULL as [version]
FROM prod.odw_actuals_deliveries ships
left join mdm.iso_country_code_xref xref on ships.country_alpha2=xref.country_alpha2
left join mdm.product_line_xref plxref on ships.pl=plxref.PL
WHERE 1=1
	AND ships.load_date = (select max(load_date) from prod.odw_actuals_deliveries)
	and xref.region_5 IN ('AP','JP')
	and plxref.PL_category='LLC'
	and ships.unit_reporting_description = 'OTHER'
GROUP BY
	ships.base_product_number,
	ships.cal_date,
	ships.load_date
"""

# execute query from stage table
LLC_AP_Actuals_records = read_redshift_to_df(configs) \
    .option("query", LLC_AP_Actuals_query) \
    .load()

# COMMAND ----------

LLC_AP_Actuals_records.display()

# COMMAND ----------

# pull the EU, NA, and LA LLC actuals data and do a little bit of ETL
LLC_EU_LA_NA_Actuals_query = """
SELECT
	'SHIP-ACTUALS-LLC' AS record,
    xref.region_5 as geography,
	'REG4' AS geography_grain,
	ships.base_product_number AS base_product_number,
	ships.cal_date,
	sum(base_quantity) AS units,
	ships.load_date,
	NULL as [version]
FROM prod.odw_actuals_deliveries ships
left join mdm.iso_country_code_xref xref on ships.country_alpha2=xref.country_alpha2
left join mdm.product_line_xref plxref on ships.pl=plxref.PL
WHERE 1=1
	AND ships.load_date = (select max(load_date) from prod.odw_actuals_deliveries)
	and xref.region_5 IN ('NA','LA', 'EU')
	and plxref.PL_category='LLC'
	and ships.unit_reporting_description = 'OTHER'
GROUP BY
	xref.region_5,
	ships.base_product_number,
	ships.cal_date,
	ships.load_date
"""

# execute query from stage table
LLC_EU_LA_NA_Actuals_records = read_redshift_to_df(configs) \
    .option("query", LLC_EU_LA_NA_Actuals_query) \
    .load()

# COMMAND ----------

LLC_EU_LA_NA_Actuals_records.display()

# COMMAND ----------

# combine the AP and Non-AP dataframes
unionActuals_records = LLC_AP_Actuals_records.union(LLC_EU_LA_NA_Actuals_records)

# COMMAND ----------

unionActuals_records.display()

# COMMAND ----------

# write the combined dataframe to the stage.actuals_llc_stage table
write_df_to_redshift(configs, unionActuals_records, "stage.actuals_llc_stage", "overwrite")

# COMMAND ----------

# add version and format date column
from pyspark.sql.functions import lit
max_version_info = call_redshift_addversion_sproc(configs, 'SHIP-ACTUALS-LLC', 'ODW')

max_version = max_version_info[0]
max_load_date = max_version_info[1]

# replace the existing version and load_date values with the retrieved results from the version table
unionActuals_records1 = unionActuals_records \
    .withColumn("load_date", lit(max_load_date)) \
    .withColumn("version", lit(max_version)) \
    .withColumn("cal_date",col("cal_date").cast("DATE"))

# COMMAND ----------

unionActuals_records1.display()

# COMMAND ----------

# insert into prod
write_df_to_redshift(configs, unionActuals_records1, "prod.actuals_llc", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## WD3 LLC

# COMMAND ----------

WD3_LLC_query = """
SELECT
    'WD3-LLC' as record,
    a.geography as geo,
    'REG4' as geo_type,
    'REG4' as geo_input,
    a.base_product_number as bpn,
    a.base_product_number as sku,
    a.cal_date as calendar_month,
    sum(a.units) as units,
    a.load_date,
    a.version,
    cast(Left(a.load_date,7) as varchar) as record_name,
    '' as comment
FROM stage.supplies_stf_landing a
    left join mdm.rdma b on a.base_product_number=b.base_prod_number
WHERE 1=1
    and b.pl in ('65','HF','UD')
    and a.geography <> 'JP'
GROUP BY
    a.geography,
    a.base_product_number,
    a.cal_date,
    a.load_date,
    a.version
"""

# execute query from stage table
WD3_LLC_records = read_redshift_to_df(configs) \
    .option("query", WD3_LLC_query) \
    .load()

# COMMAND ----------

WD3_LLC_records.display()

# COMMAND ----------

max_wd3_llc_version_info = call_redshift_addversion_sproc(configs, 'WD3-LLC', 'CO FORECAST')



# COMMAND ----------

max_wd3_llc_version = max_wd3_llc_version_info[0]
max_wd3_llc_load_date = max_wd3_llc_version_info[1]

# replace the existing version and load_date values with the retrieved results from the version table
WD3_LLC_records1 = WD3_LLC_records \
    .withColumn("load_date", lit(max_wd3_llc_load_date)) \
    .withColumn("version", lit(max_wd3_llc_version))

# COMMAND ----------

WD3_LLC_records1.display()

# COMMAND ----------

# insert into prod
write_df_to_redshift(configs, WD3_LLC_records1, "prod.wd3_llc", "append")

# COMMAND ----------

# Check Version over version units to make sure there isn't a big spike or drop

WD3_LLC_VoV_check_query = """
with previous_version as
(
    select top 1
        'LLC' as record,
        version as prev_version,
        sum(units) as units
    from prod.wd3_llc
    where version <> (select max(version) from prod.wd3_llc)
    GROUP BY version
    order by version desc
),

current_version as
(
    select
        'LLC' as record,
        version as curr_version,
        sum(units) as units
    from prod.wd3_llc
    where version = (select max(version) from prod.wd3_llc)
    GROUP BY version
)

SELECT
    a.units as prev_units,
    b.units as curr_units,
    a.units - b.units as unit_diff,
    ROUND(((a.units - b.units)/a.units) * 100,2) as pct_diff
from previous_version a INNER JOIN current_version b on 1=1
"""

# execute query
WD3_LLC_VoV_check_records = read_redshift_to_df(configs) \
    .option("query", WD3_LLC_VoV_check_query) \
    .load()

WD3_LLC_VoV_check_records.display()
