# Databricks notebook source
# MAGIC %md # Import libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as func
from pyspark.sql.types import *
import numpy as np
import pandas

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md # Widgets

# COMMAND ----------

dbutils.widgets.text("discounting_factor",'')
dbutils.widgets.text("op_ex_rate",'')
dbutils.widgets.text("tax_rate",'')

# COMMAND ----------

discounting_factor = dbutils.widgets.get("discounting_factor")
op_ex_rate = dbutils.widgets.get("op_ex_rate")
tax_rate = dbutils.widgets.get("tax_rate")

# COMMAND ----------

q1 = """
select ifs2.record
	, ifs2.region_5
	, ifs2.platform_subset
	, ifs2.base_product_number
	, ifs2.crg_chrome
	, ifs2.customer_engagement
	--, year
	, ifs2.gross_usage
	, ifs2.trade_split
	, ifs2.yield
	, hyu.host as life_of_host
	, (ifs2.gross_usage * ifs2.trade_split) as trade_usage
from
(
	select record
	, region_5
	, platform_subset
	, base_product_number
	, crg_chrome
	, customer_engagement
	--, year
	, max(usage) as gross_usage
	, avg(trade_split) as trade_split
	, avg(yield) as yield
	from ifs2.ifs2_country_fsb_04_24 icf    --change table name
	group by record
	, region_5
	, platform_subset
	, base_product_number
	, crg_chrome
	, customer_engagement
	--, year
) ifs2
inner join ifs2.host_yield_usage_vw hyu
on ifs2.record = hyu.record
and ifs2.region_5 = hyu.geography
and ifs2.platform_subset = hyu.platform_subset
and ifs2.crg_chrome = hyu.crg_chrome
and ifs2.customer_engagement = hyu.customer_engagement
"""

pen_details = read_redshift_to_df(configs) \
    .option("query", q1) \
    .load()

pen_details.createOrReplaceTempView("pen_details")

# COMMAND ----------

q2 = """
select record
,platform_subset
,base_product_number
,region_5
,customer_engagement
,"year"
,year_num
,max(usage) as usage
,max(baseprod_gru) as pen_list_price
,(max(baseprod_gru) - max(baseprod_contra_per_unit)) as net_revenue_per_pen
,avg(baseprod_variable_cost_per_unit) as pen_vcos
,avg(baseprod_variable_cost_per_unit + baseprod_fixed_cost_per_unit) as pen_tcos
,max(vtc) as chan_pan
,max(decay) as decay
,min(remaining_amount) as remaining_amount
,max(hp_share) as market_share 
from ifs2.ifs2_country_fsb_04_24      -- change table name
group by record
,platform_subset
,base_product_number
,region_5
,customer_engagement
,"year"
,year_num
"""

pen_year_details = read_redshift_to_df(configs) \
    .option("query", q2) \
    .load()

pen_year_details.createOrReplaceTempView("pen_year_details")

# COMMAND ----------

q3 = """
select pd.*
,pyd.usage as usage
,pyd.year
,pyd.year_num
,(pyd.year_num*12) as month_count
,pyd.pen_list_price
,pyd.net_revenue_per_pen
,pyd.pen_vcos
,pyd.pen_tcos
,pyd.chan_pan
,pyd.decay
,pyd.remaining_amount
,pyd.market_share
,case when ((pyd.year_num == 1) and life_of_host > (pyd.year_num*12)) then 0
    when (pyd.year_num == 1) then (usage * trade_split * chan_pan * remaining_amount * ((pyd.year_num*12) - life_of_host) * market_share)
    when ((pyd.year_num <> 1) and (life_of_host >= (pyd.year_num*12))) then 0
    when ((pyd.year_num <> 1) and (life_of_host < (pyd.year_num*12)) and (life_of_host > ((pyd.year_num-1)*12))) then (usage * trade_split * chan_pan * remaining_amount * ((pyd.year_num*12) - life_of_host) * market_share)
    else (usage * trade_split * chan_pan * remaining_amount * 12 * market_share)
    end as net_usage_cc_per_year
from pen_details pd
inner join pen_year_details pyd
on pd.record = pyd.record
and pd.platform_subset = pyd.platform_subset
and pd.region_5 = pyd.region_5
and pd.base_product_number = pyd.base_product_number
and pd.customer_engagement = pyd.customer_engagement
"""

pen_agg = spark.sql(q3)
pen_agg.createOrReplaceTempView("pen_agg")

# COMMAND ----------

pen_agg.filter((col('platform_subset') == 'EDWIN') & (col('region_5') == 'NA') & (col('base_product_number') == 'L0S52A')).orderBy('year').display()

# COMMAND ----------

q4 = '''
select *
, power(1+'{}',year_num) as denom
, power(1+'{}',year_num-1) as denom_minus_one
, (net_usage_cc_per_year/yield) as pens_per_year
, ((net_usage_cc_per_year/yield) * net_revenue_per_pen) as net_revenue
, ((net_revenue_per_pen - pen_vcos) * (net_usage_cc_per_year/yield)) as vpm
, ((net_revenue_per_pen - pen_tcos) * (net_usage_cc_per_year/yield)) as gm
, ((((net_revenue_per_pen - pen_tcos) * (net_usage_cc_per_year/yield)) - ((net_usage_cc_per_year/yield) * net_revenue_per_pen) * '{}')) as operating_profit
, (((((net_revenue_per_pen - pen_tcos) * (net_usage_cc_per_year/yield)) - ((net_usage_cc_per_year/yield) * net_revenue_per_pen) * '{}')) * (1 - '{}')) as pen_net_profit
from pen_agg

'''.format(discounting_factor,discounting_factor,op_ex_rate,op_ex_rate,tax_rate)
pen_inputs = spark.sql(q4)
pen_inputs.createOrReplaceTempView("pen_inputs")

# COMMAND ----------

pen_inputs.filter((col('platform_subset') == 'EDWIN') & (col('region_5') == 'NA') & (col('base_product_number') == 'F6U15A')).orderBy('year').display()

# COMMAND ----------

q = '''
select record
, region_5
, platform_subset
, customer_engagement
, base_product_number
, crg_chrome
, customer_engagement
, year
, net_revenue/denom as pv_net_revenue
, net_revenue/denom_minus_one as pv_net_revenue_x
, vpm/denom as pv_vpm
, vpm/denom_minus_one as pv_vpm_x
, gm/denom as pv_gm
, gm/denom_minus_one as pv_gm_x
, operating_profit/denom as pv_operating_profit
, operating_profit/denom_minus_one as pv_operating_profit_x
, pen_net_profit/denom as pv_pen_net_profit
, pen_net_profit/denom_minus_one as pv_pen_net_profit_x
from pen_inputs
'''
pen_pv_cf = spark.sql(q)
pen_pv_cf.createOrReplaceTempView("pen_pv_cf")

# COMMAND ----------

pen_pv_cf.filter((col('platform_subset') == 'EDWIN') & (col('region_5') == 'NA') & (col('base_product_number') == 'F6U15A')).orderBy('year').display()

# COMMAND ----------

q6 = '''
select record
, region_5
, platform_subset
, customer_engagement
, base_product_number
, (sum(pv_net_revenue) + sum(pv_net_revenue_x))/2 as supplies_net_revenue
, (sum(pv_vpm) + sum(pv_vpm_x))/2 as supplies_vpm
, (sum(pv_gm) + sum(pv_gm_x))/2 as supplies_gm
, (sum(pv_operating_profit) + sum(pv_operating_profit_x))/2 as supplies_operating_profit
, (sum(pv_pen_net_profit) + sum(pv_pen_net_profit_x))/2 as pen_net_profit
from pen_pv_cf
group by
record
, region_5
, platform_subset
, customer_engagement
, base_product_number
'''
pen_pv_agg = spark.sql(q6)
pen_pv_agg.createOrReplaceTempView("pen_pv_agg")

# COMMAND ----------

pen_pv_agg.filter((col('platform_subset') == 'MANHATTAN YET1') & (col('region_5') == 'NA') ).display()

# COMMAND ----------

q7 = '''
select record
, region_5
, platform_subset
, customer_engagement
, sum(supplies_net_revenue)
, sum(supplies_vpm)
, sum(supplies_gm)
, sum(supplies_operating_profit)
, sum(pen_net_profit)
from pen_pv_agg
group by
record
, region_5
, platform_subset
, customer_engagement
'''
platform_subset_region = spark.sql(q7)
platform_subset_region.createOrReplaceTempView("platform_subset_region")

# COMMAND ----------

platform_subset_region.filter((col('platform_subset') == 'MANHATTAN YET1') & (col('region_5') == 'NA') ).display()

# COMMAND ----------

q5 = '''
select record
, region_5
, platform_subset
, customer_engagement
, year
, sum(net_revenue) as supplies_net_revenue
, sum(vpm) as supplies_vpm
, sum(gm) as supplies_gm
, sum(operating_profit) as supplies_operating_profit
, sum(pen_net_profit) as pen_net_profit
from pen_inputs
group by
record
, region_5
, platform_subset
, customer_engagement
, year
'''
pen_agg_year = spark.sql(q5)
pen_agg_year.createOrReplaceTempView("pen_agg_year")

# COMMAND ----------

pen_agg_year.filter((col('platform_subset') == 'MALBEC YET1') & (col('region_5') == 'NA')).display()

# COMMAND ----------


