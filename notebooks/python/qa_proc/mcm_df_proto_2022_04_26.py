# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Dataframes development work
# MAGIC 
# MAGIC Testing dataframes - joins, filters, and analysis/trouble-shooting

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Notebook setup

# COMMAND ----------

# MAGIC %run "../../notebooks/python/common/configs"

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/secrets_manager_utils

# COMMAND ----------

# MAGIC %run ../../notebooks/python/common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Supplies HW Mapping - modified df

# COMMAND ----------

shm_sql = """
with shm_01_iso as
(
    select distinct market10
        , region_5
    from mdm.iso_country_code_xref
    where 1=1
        and not market10 is null
        and region_5 not in ('XU','XW')
)

, shm_02_geo as
(
    select distinct shm.platform_subset
        , shm.base_product_number
        , shm.geography
        , case when hw.technology = 'LASER' and shm.platform_subset like '%STND%' then 'STD'
               when hw.technology = 'LASER' and shm.platform_subset like '%YET2%' then 'HP+'
               when hw.technology = 'LASER' then 'TRAD'
               else shm.customer_engagement end as customer_engagement
    from mdm.supplies_hw_mapping as shm
    join mdm.hardware_xref as hw
        on hw.platform_subset = shm.platform_subset
    where 1=1
        and shm.official = 1
        and shm.geography_grain = 'MARKET10'

    union all

    select distinct shm.platform_subset
        , shm.base_product_number
        , iso.market10 as geography
        , case when hw.technology = 'LASER' and shm.platform_subset like '%STND%' then 'STD'
               when hw.technology = 'LASER' and shm.platform_subset like '%YET2%' then 'HP+'
               when hw.technology = 'LASER' then 'TRAD'
               else shm.customer_engagement end as customer_engagement
    from mdm.supplies_hw_mapping as shm
    join mdm.hardware_xref as hw
        on hw.platform_subset = shm.platform_subset
    join shm_01_iso as iso
        on iso.region_5 = shm.geography     -- changed geography_grain to geography
    where 1=1
        and shm.official = 1
        and shm.geography_grain = 'REGION_5'

    union all

    select distinct shm.platform_subset
        , shm.base_product_number
        , iso.market10 as geography
        , case when hw.technology = 'LASER' and shm.platform_subset like '%STND%' then 'STD'
               when hw.technology = 'LASER' and shm.platform_subset like '%YET2%' then 'HP+'
               when hw.technology = 'LASER' then 'TRAD'
               else shm.customer_engagement end as customer_engagement
    from mdm.supplies_hw_mapping as shm
    join mdm.hardware_xref as hw
        on hw.platform_subset = shm.platform_subset
    join mdm.iso_cc_rollup_xref as cc
        on cc.country_level_1 = shm.geography  -- gives us cc.country_alpha2
    join mdm.iso_country_code_xref as iso
        on iso.country_alpha2 = cc.country_alpha2     -- changed geography_grain to geography
    where 1=1
        and shm.official = 1
        and shm.geography_grain = 'REGION_8'
        and cc.country_scenario = 'HOST_REGION_8'
)

select distinct platform_subset
    , base_product_number
    , geography
    , customer_engagement
    , platform_subset + ' ' + base_product_number + ' ' +
        geography + ' ' + customer_engagement as composite_key
from shm_02_geo
"""

# COMMAND ----------

shm_records = read_redshift_to_df(configs) \
  .option("query", shm_sql) \
  .load()

# COMMAND ----------

shm_records.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Review shm_mod df

# COMMAND ----------

# multiple CKs check
shm_records.groupby(shm_records.composite_key).count().filter("count > 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Join to geo DF to filter

# COMMAND ----------

geo_filter = """
select distinct market10
from mdm.iso_country_code_xref
where 1=1
    and market10 = 'ISE'
"""

# COMMAND ----------

geo_filter = read_redshift_to_df(configs) \
  .option("query", geo_filter) \
  .load()

geo_filter.show()

# COMMAND ----------

# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join
shm_ise_filter = shm_records.join(geo_filter, shm_records.geography == geo_filter.market10, 'inner')

# COMMAND ----------

shm_ise_filter.show()
