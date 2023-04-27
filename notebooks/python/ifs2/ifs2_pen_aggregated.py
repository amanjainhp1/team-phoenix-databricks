# Databricks notebook source
# MAGIC %md
# MAGIC Import libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as func
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

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

pen_agg.filter((col('platform_subset') == 'MALBEC YET1') & (col('region_5') == 'NA') & (col('base_product_number') == '3YL58A')).orderBy('year').display()

# COMMAND ----------

q4 = '''
select *
, (net_usage_cc_per_year/yield) as pens_per_year
, ((net_usage_cc_per_year/yield) * net_revenue_per_pen) as net_revenue
, ((net_revenue_per_pen - pen_vcos) * (net_usage_cc_per_year/yield)) as vpm
, ((net_revenue_per_pen - pen_tcos) * (net_usage_cc_per_year/yield)) as gm
, ((((net_revenue_per_pen - pen_tcos) * (net_usage_cc_per_year/yield)) - ((net_usage_cc_per_year/yield) * net_revenue_per_pen) * '{}')) as operating_profit
, (((((net_revenue_per_pen - pen_tcos) * (net_usage_cc_per_year/yield)) - ((net_usage_cc_per_year/yield) * net_revenue_per_pen) * '{}')) * (1 - '{}')) as pen_net_profit
from pen_agg

'''.format(op_ex_rate,op_ex_rate,tax_rate)
pen_inputs = spark.sql(q4)
pen_inputs.createOrReplaceTempView("pen_inputs")

# COMMAND ----------

pen_inputs.filter((col('platform_subset') == 'MALBEC YET1') & (col('region_5') == 'NA') & (col('base_product_number') == '3YL58A')).orderBy('year').display()

# COMMAND ----------


