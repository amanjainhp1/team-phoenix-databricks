# Databricks notebook source
# 5/09/2023 - Brent Merrick
# This get ran AFTER the CO forecast process where I pull from IBP via spreadsheet and load to stage.supplies_stf_landing


# COMMAND ----------

# MAGIC %run ../../python/common/configs

# COMMAND ----------

# MAGIC %run ../../python/common/database_utils

# COMMAND ----------

# pull supplies actuals, create split pct, and allocate the CO forecast to country from that miz
stf_allocate_query = """

with supplies_actuals AS
(
    --supplies actuals numerator
    select
        a.country_alpha2,
        b.market8,
        sum(a.base_quantity) as numerator
    from prod.actuals_supplies a left join mdm.iso_country_code_xref b on a.country_alpha2=b.country_alpha2
    where cal_date > dateadd(year,-2,getdate())
    group by a.country_alpha2, b.market8, a.country_alpha2
),

supplies_actuals_totals as
(
    --supplies actuals denominator
    select
        b.market8,
        sum(a.base_quantity) as denominator
    from prod.actuals_supplies a left join mdm.iso_country_code_xref b on a.country_alpha2=b.country_alpha2
    where cal_date > dateadd(year,-2,getdate())
    group by b.market8
),

mix_percent as
(
    --create mix percent
    SELECT
        supplies_actuals_totals.market8,
        supplies_actuals.country_alpha2,
        (supplies_actuals.numerator / supplies_actuals_totals.denominator) as split_pct
    from supplies_actuals_totals INNER JOIN supplies_actuals on supplies_actuals_totals.market8=supplies_actuals.market8
),

country_mix_pct as
(
    --cleaned up version of mix percent
    select
        market8,
        country_alpha2,
        split_pct
    from mix_percent
),

ibp_fcst_stage_cleaned as
(
    --normalize the geographies to match out market8
    select
        CASE
            WHEN market = 'CENTRAL AND EASTERN EUROPE' THEN 'CENTRAL & EASTERN EUROPE'
            WHEN market = 'INDIA B SL' THEN 'INDIA SL & BL'
            WHEN market = 'SOUTHERN EUROPE MIDDLE EAST AND AFRICA' THEN 'SOUTHERN EUROPE, ME & AFRICA'
            ELSE market
        END AS market,
        primary_base_product,
        cal_date,
        units,
        load_date,
        'UNKNOWN' as version
    from stage.ibp_fcst_stage
    where 1=1
        and units <> 0
),
    
final as
(
    --bring it all together
    select
        'IBP_SUPPLIES_FCST' as record,
        b.country_alpha2,
        a.primary_base_product as sales_product_number,
        a.cal_date,
        a.units * b.split_pct as units,
        a.load_date,
        a.version
    from ibp_fcst_stage_cleaned a inner join country_mix_pct b on a.market=b.market8
    where 1=1
    order by 1,2,3,4
)

select *
from final
order by 4,2,1

"""

stf_allocate_records = read_redshift_to_df(configs) \
    .option("query", stf_allocate_query) \
    .load()

# COMMAND ----------

stf_allocate_records.show()

# COMMAND ----------

# Add record to version table for 'IBP_SUPPLIES_FCST'
max_info = call_redshift_addversion_sproc(configs, 'IBP_SUPPLIES_FCST', 'CO FORECAST PROCESS')
max_version = max_info[0]
max_load_date = str(max_info[1])

# COMMAND ----------

# merge the dataset with the new version from the prod.version table
from pyspark.sql.functions import *

stf_allocate_records_final = stf_allocate_records \
    .withColumn("load_date", lit(max_load_date).cast("timestamp")) \
    .withColumn("version", lit(max_version))


# COMMAND ----------

stf_allocate_records_final.show()

# COMMAND ----------

# output dataset to S3 for archival purposes
# s3a://dataos-core-dev-team-phoenix/product/currency_hedge/[version]/

# set the bucket name
s3_output_bucket = constants["S3_BASE_BUCKET"][stack] + "product/ibp_supplies_forecast/" + max_version

# write the data out in parquet format
write_df_to_s3(stf_allocate_records_final, s3_output_bucket, "parquet", "overwrite")

# COMMAND ----------

# write the new data to the prod table
write_df_to_redshift(configs, stf_allocate_records_final, "prod.ibp_supplies_forecast", "overwrite")
