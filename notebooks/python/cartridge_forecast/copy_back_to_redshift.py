# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

##yield load for npi

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import BooleanType

bucket = f"dataos-core-{stack}-team-phoenix" 
bucket_prefix = "landing/ODW/"



mix_override = "Yield Missing Products.xlsx"

print(mix_override)

yield_lf = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{mix_override}")

yield_lf.createOrReplaceTempView("yield_lf")

# COMMAND ----------

query = """

with region_5_temp as (

    select distinct region_5
	from mdm.iso_country_code_xref iccx 
	where region_5 not in ('XW' , 'XU')
)
select y.record 
    , y.geography_grain
    , r.region_5 as geography
    , y.base_product_number
    , y.value
    , cast (y.effective_date as date) as effective_date
    , cast (NULL as timestamp) as active_at
    , cast (NULL as timestamp) as inactive_at
    , cast (1 as boolean) as official
    , cast (getdate() as timestamp) as last_modified_date
    , cast (getdate() as timestamp) as load_date
    , "2023.06.13.1" as version
from yield_lf y
cross join region_5_temp r
"""

yield_df = spark.sql(query)
write_df_to_redshift(configs, yield_df, "mdm.yield", "append")
#usage_df.filter( (col("platform_subset") == 'BEAM 36 CISS') & (col("measure")== 'USAGE') & (col("geography")== 'SOUTHERN EUROPE')).display()

# COMMAND ----------

yield_df.count()

# COMMAND ----------

##usage_share load for npi

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import BooleanType

bucket = f"dataos-core-{stack}-team-phoenix" 
bucket_prefix = "landing/ODW/"



mix_override = "Usage_Share 060623.xlsx"

print(mix_override)

usage_share = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{mix_override}")

usage_share.createOrReplaceTempView("usage_share")

# COMMAND ----------

query = """

with market10_temp as (

    select distinct market10
    from mdm.iso_country_code_xref
)
select us.record 
    , c.date as cal_date
    , market10 as geography
    , platform_subset
    , customer_engagement
    , measure
    , units
    , cast (NULL as string) as ib_version
    , cast (NULL as string) as version
    , cast (NULL as timestamp) as load_date
    , "MARKET10" as geography_grain
from usage_share us
inner join mdm.calendar c
    ON us.year = c.calendar_yr
cross join market10_temp
where c.day_of_month = 1
"""

usage_df = spark.sql(query)
usage_df.filter( (col("platform_subset") == 'BEAM 36 CISS') & (col("measure")== 'USAGE') & (col("geography")== 'SOUTHERN EUROPE')).display()

# COMMAND ----------

write_df_to_redshift(configs, usage_df, "stage.usage_share_lf", "append")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import BooleanType

bucket = f"dataos-core-{stack}-team-phoenix" 
bucket_prefix = "landing/ODW/"



mix_override = "lf_cc_mix_overrides.xlsx"

print(mix_override)

mix_override = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{mix_override}")

# COMMAND ----------

from pyspark.sql.functions import col,lit
mix_override = mix_override.withColumn("record",lit("CRG_MIX_OVERRIDE"))
mix_override = mix_override.withColumn("geography_grain",lit("REGION_5"))
mix_override = mix_override.withColumn("record",lit("CRG_MIX_OVERRIDE"))
mix_override = mix_override.withColumn("product_lifecycle_status",lit("N"))
mix_override = mix_override.withColumn("customer_engagement",lit("TRAD"))
mix_override = mix_override.withColumnRenamed("region_5","geography")
mix_override = mix_override.withColumnRenamed("crg_base_product_number","crg_base_prod_number")
mix_override = mix_override.withColumn("load_date", current_timestamp())

# COMMAND ----------

mix_override = mix_override.select('platform_subset', 'crg_base_prod_number','geography' , 'geography_grain' , 'mix_pct' , 'product_lifecycle_status' , 'customer_engagement','load_date')

mix_override.createOrReplaceTempView("mix_override")

# COMMAND ----------

calendar = read_redshift_to_df(configs) \
    .option("dbtable", "mdm.calendar") \
    .load()

# COMMAND ----------

actuals_hw = read_redshift_to_df(configs) \
    .option("dbtable", "prod.actuals_hw") \
    .load()

# COMMAND ----------

tables = [
          ['mdm.calendar' ,calendar]
         ]


for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = table[1]
    print(f'loading {table[0]}...')
    # Write the data to its target.
    df.write \
      .format(write_format) \
      .mode("overwrite") \
      .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

mix_override = mix_override
mix_override.CreateOrReplaceTempView("mix_override")

# COMMAND ----------

query = """

select platform_subset
    , crg_base_prod_number
    , geography
    , geography_grain
    , c.date as cal_date 
    , mix_pct
    , product_lifecycle_status
    , customer_engagement
    , load_date
    , cast(1 as boolean) AS official
from mix_override 
cross join calendar c
where c.date between '2023-11-01' and '2027-10-01'
and day_of_month = 1
"""
mix_override_new = spark.sql(query)
write_df_to_redshift(configs, mix_override_new, "stage.cartridge_mix_override_lf", "overwrite")

# COMMAND ----------

## before running norm_shipment

f_report_units_query = """
SELECT *
FROM ie2_prod.dbo.hardware_ltf
"""

f_report_units = read_sql_server_to_df(configs) \
    .option("query", f_report_units_query) \
    .load()

write_df_to_redshift(configs, f_report_units, "prod.hardware_ltf", "overwrite")

# COMMAND ----------

actuals_supplies_lf = """
SELECT cal_date
    , country_alpha2
    , market10
    , base_product_number
    , platform_subset
    , customer_engagement
    , mix as base_quantity
FROM IE2_Prod.dbo.actuals_supplies_lf_0330
"""

actuals_supplies_lf = read_sql_server_to_df(configs) \
    .option("query", actuals_supplies_lf) \
    .load()

write_df_to_redshift(configs, actuals_supplies_lf, "stage.actuals_supplies_lf", "overwrite")

# COMMAND ----------


usage_share_lf = """
SELECT 
FROM IE2_Staging.test.usage_share_lf 
"""

usage_share_lf = read_sql_server_to_df(configs) \
    .option("query", usage_share_lf) \
    .load()

write_df_to_redshift(configs, usage_share_lf, "stage.usage_share_lf", "overwrite")

# COMMAND ----------

lf_ib_covid_adj = """
SELECT *
FROM [IE2_Landing].[lfd].[lf__ib_covid_adj]
"""

lf_ib_covid_adj = read_sql_server_to_df(configs) \
    .option("query", lf_ib_covid_adj) \
    .load()

write_df_to_redshift(configs, lf_ib_covid_adj, "stage.lf_ib_covid_adj", "overwrite")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import BooleanType

bucket = f"dataos-core-{stack}-team-phoenix" 
bucket_prefix = "landing/ODW/"

supplies_xref_2 = "supplies_xref_2.xlsx"

print(supplies_xref_2)

supplies_xref = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{supplies_xref_2}")

# COMMAND ----------

from pyspark.sql.functions import col,lit
supplies_xref = supplies_xref.withColumn("record",lit("CRG_MIX_OVERRIDE"))
supplies_xref = supplies_xref.withColumn("geography_grain",lit("REGION_5"))
supplies_xref = supplies_xref.withColumn("record",lit("CRG_MIX_OVERRIDE"))
supplies_xref = supplies_xref.withColumn("product_lifecycle_status",lit("N"))
supplies_xref = supplies_xref.withColumn("customer_engagement",lit("TRAD"))
supplies_xref = supplies_xref.withColumnRenamed("region_5","geography")
mix_override = mix_override.withColumnRenamed("crg_base_product_number","crg_base_prod_number")
mix_override = mix_override.withColumn("load_date", current_timestamp())

# COMMAND ----------


supplies_xref_lf = """
SELECT  id
      , record
      , base_product_number
      , pl
      , cartridge_alias
      , regionalization
      , toner_category
      , type
      , single_multi
      , crg_chrome
      , k_color
      , crg_intro_dt
      , size
      , technology
      , supplies_family
      , supplies_group
      , supplies_technology
      , equivalents_multiplier
      , active
      , last_modified_date
      , load_date
FROM IE2_Landing.lfd.supplies_xref_lf_landing
"""

supplies_xref_lf = read_sql_server_to_df(configs) \
    .option("query", supplies_xref_lf) \
    .load()

write_df_to_redshift(configs, supplies_xref_lf, "stage.supplies_xref_lf", "overwrite")

# COMMAND ----------



# COMMAND ----------


supplies_xref_lf = """
SELECT  id
      , record
      , base_product_number
      , pl
      , cartridge_alias
      , regionalization
      , toner_category
      , type
      , single_multi
      , crg_chrome
      , k_color
      , crg_intro_dt
      , size
      , technology
      , supplies_family
      , supplies_group
      , supplies_technology
      , equivalents_multiplier
      , active
      , last_modified_date
      , load_date
FROM IE2_Landing.lfd.supplies_xref_lf_landing
"""

supplies_xref_lf = read_sql_server_to_df(configs) \
    .option("query", supplies_xref_lf) \
    .load()

write_df_to_redshift(configs, supplies_xref_lf, "stage.supplies_xref_lf", "overwrite")

# COMMAND ----------

##shm load for npi

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import BooleanType

bucket = f"dataos-core-{stack}-team-phoenix" 
bucket_prefix = "landing/ODW/"



mix_override = "shm_remaning_2.xlsx"

print(mix_override)

shm_df_v = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "True") \
        .option("header","True") \
        .option("treatEmptyValuesAsNulls", "False")\
        .load(f"s3a://{bucket}/{bucket_prefix}/{mix_override}")

shm_df_v.createOrReplaceTempView("shm_df_v")

# COMMAND ----------

query = """
with region_5 as (

select distinct region_5
from mdm.iso_country_code_xref
where region_5 not in ('XU','XW')
)
, tempas as (
select * 
from shm_df_v
cross join region_5
) select "SUPPLIES_HW_MAP_LF" as record
    , "REGION_5" as geography_grain
    , region_5 as geography
    , base_product_number
    , "TRAD" as customer_engagement
    , platform_subset
    , 1 as active
    , cast(NULL AS TIMESTAMP) as load_date
    , cast(NULL as string) as versiona
    , 1 as official
    , cast(NULL as int) as eol
    , cast(NULL as int) as eol_date
    , cast(NULL as int) as host_multiplier
from tempas
"""
shm_df = spark.sql(query)
shm_df.display()
write_df_to_redshift(configs, shm_df, "stage.supplies_hw_mapping_lf", "append")
#write_df_to_redshift(configs, shm_df, "stage.supplies_hw_mapping_lf", "append")

# COMMAND ----------

## not required

supplies_hw_mapping_lf = """

SELECT record
      ,geograpy_grain as geography_grain
      ,geography
      ,base_product_number
      ,customer_engagement
      ,platform_subset
      ,active
      ,load_date
      ,version
      ,official
      ,eol
      ,eol_date
      ,host_multiplier
  FROM IE2_Landing.lfd.supplies_hw_mapping_lf
"""
supplies_hw_mapping_lf = read_sql_server_to_df(configs) \
    .option("query", supplies_hw_mapping_lf) \
    .load()

write_df_to_redshift(configs, supplies_hw_mapping_lf, "stage.supplies_hw_mapping_lf", "overwrite")
