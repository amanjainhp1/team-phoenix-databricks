# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Pages-CCs Mix
# MAGIC 
# MAGIC Stepwise process:
# MAGIC + supplies hw mapping helper :: scrubbed data from supplies_hw_mapping
# MAGIC + cartridge units :: actual_supplies (historica/actuals time period)
# MAGIC + demand
# MAGIC   + production IB
# MAGIC   + promoted U/S (in development)
# MAGIC + page_cc_mix
# MAGIC   

# COMMAND ----------

# Global Variables
query_list = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## supplies_hw_mapping modified helper
# MAGIC 
# MAGIC Table inputs:
# MAGIC + mdm.hardware_xref
# MAGIC + mdm.iso_country_code_xref
# MAGIC + mdm.iso_cc_rollup_xref
# MAGIC + mdm.supplies_hw_mapping

# COMMAND ----------

shm_base_helper = """
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

query_list.append(["stage.shm_base_helper", shm_base_helper, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cartridge Units (formerly cartridge_volumes)
# MAGIC 
# MAGIC Table inputs:
# MAGIC + mdm.iso_cc_rollup_xref
# MAGIC + mdm.supplies_xref
# MAGIC + prod.actuals_supplies
# MAGIC + stage.shm_base_helper - above

# COMMAND ----------

cartridge_units = """
select 'ACTUALS' as source
    , acts.cal_date
    , acts.base_product_number
    , cref.country_level_2 as geography
    , xref.k_color
    , xref.crg_chrome
    , case when xref.crg_chrome in ('DRUM') then 'DRUM'
           else 'CARTRIDGE' end as consumable_type
    , sum(acts.base_quantity) as cartridge_volume
from prod.actuals_supplies as acts
join mdm.supplies_xref as xref
    on xref.base_product_number = acts.base_product_number
join mdm.iso_cc_rollup_xref as cref
    on cref.country_alpha2 = acts.country_alpha2
    and cref.country_scenario = 'MARKET10'
join stage.shm_base_helper as shm 
    on shm.base_product_number = acts.base_product_number
    and shm.geography = cref.country_level_2
where 1=1
    and acts.customer_engagement in ('EST_INDIRECT_FULFILLMENT', 'I-INK', 'TRAD')
    and not xref.crg_chrome in ('HEAD', 'UNK')
group by acts.cal_date
    , acts.base_product_number
    , cref.country_level_2
    , xref.k_color
    , xref.crg_chrome
    , case when xref.crg_chrome in ('DRUM') then 'DRUM'
           else 'CARTRIDGE' end
"""

query_list.append(["stage.cartridge_units", cartridge_units, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demand

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pages-Ccs Mix (engine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Redshift

# COMMAND ----------

# MAGIC %run "../common/output_to_redshift" $query_list=query_list
