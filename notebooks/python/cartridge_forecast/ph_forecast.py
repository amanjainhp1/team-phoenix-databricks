# Databricks notebook source
# MAGIC %run ../common/s3_utils

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## ph_forecast

# COMMAND ----------

ph_forecast = """

with market_ccs as (


SELECT cal_date
      ,hw.hw_product_family 
      ,sum(mvtc_market_ccs) mvtc_adjusted_market_ccs
FROM stage.vtc_lf c
LEFT JOIN mdm.hardware_xref hw 
    ON hw.platform_subset = c.platform_subset
GROUP BY cal_date
      ,hw.hw_product_family
), ph_sku_ps as (

select distinct base_product_number
    , swm.platform_subset
FROM stage.supplies_xref_design sx
left join stage.swm_lf swm on sx.sku = swm.base_product_number
WHERE  Type IN ('PH','KITS') 

UNION

SELECT distinct base_product_number
    , swm.platform_subset
FROM stage.supplies_xref_pro sx
left join stage.swm_lf swm on sx.sku = swm.base_product_number
WHERE  Type IN ('PH','KITS')
), ph_units as (


select ac.cal_date
    , hw.hw_product_family
    , sum(ac.base_quantity) base_quantity
from stage.actuals_supplies_lf12 ac
inner join ph_sku_ps p on p.base_product_number = ac.base_product_number and p.platform_subset = ac.platform_subset
inner join mdm.hardware_xref hw on hw.platform_subset = ac.platform_subset
WHERE  ac.cal_date between '2021-11-01' and '2022-10-31' 
group by 
ac.cal_date,
hw.hw_product_family
), ph_life as (


select m.cal_date,m.hw_product_family,m.mvtc_adjusted_market_ccs/p.base_quantity ph_life
from market_ccs m 
inner join ph_units p on p.cal_date = m.cal_date and p.hw_product_family =m.hw_product_family    
), max_date as (


select max(cal_date) max_cal_date
from ph_life
), avg_ph_life as (


select hw_product_family, avg(ph_life) avg_ph_life
from ph_life
cross join max_date m
where cal_date between DATEADD(month , -9 , m.max_cal_date) and m.max_cal_date
group by hw_product_family    
)
, ph_forecast  as (


select m.cal_date,m.hw_product_family,m.mvtc_adjusted_market_ccs,p.base_quantity ph_units, pl.ph_life/1000 ph_life, ap.avg_ph_life
from market_ccs m
inner join ph_units p on p.cal_date = m.cal_date and p.hw_product_family = m.hw_product_family
inner join avg_ph_life ap on ap.hw_product_family = m.hw_product_family
inner join ph_life pl on pl.cal_date = m.cal_date and pl.hw_product_family = m.hw_product_family
cross join max_date md
where m.cal_date <= md.max_cal_date

union 

select m.cal_date,m.hw_product_family,m.mvtc_adjusted_market_ccs,m.mvtc_adjusted_market_ccs/ap.avg_ph_life ph_units, ap.avg_ph_life/1000 ph_life, ap.avg_ph_life
from market_ccs m
inner join avg_ph_life ap on ap.hw_product_family = m.hw_product_family
cross join max_date md
where m.cal_date > md.max_cal_date
) 
, mix_cc as (


select v.cal_date,v.geography,hw.hw_product_family,v.platform_subset,v.base_product_number,v.mvtc_market_ccs,
sum(mvtc_market_ccs)  over  (partition by  v.cal_date,hw.hw_product_family,v.geography,v.platform_subset,v.base_product_number)/
nullif(sum(mvtc_market_ccs)  over  (partition by  v.cal_date,hw.hw_product_family),0) mix
from  stage.vtc_lf v
left join mdm.hardware_xref hw on hw.platform_subset = v.platform_subset
), final_forecast as (


select p.cal_date,m.geography,p.hw_product_family,m.base_product_number,m.platform_subset
, p.ph_units * m.mix ph_units
, p.ph_life
, (p.mvtc_adjusted_market_ccs *  m.mix)  as mvtc_adjusted_market_ccs
from ph_forecast p
left join mix_cc m on m.hw_product_family = p.hw_product_family and m.cal_date = p.cal_date
where p.cal_date between '2022-11-01' and '2023-10-31' and m.geography is not null
)select *
from final_forecast
"""

final_forecast = read_redshift_to_df(configs) \
  .option("query", ph_forecast) \
  .load()

# COMMAND ----------

write_df_to_redshift(configs, final_forecast, "stage.ph_forecast", "overwrite")
