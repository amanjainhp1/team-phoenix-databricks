# Databricks notebook source
# MAGIC %md # Import libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as func
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md # Widgets

# COMMAND ----------

dbutils.widgets.text("discounting_factor",'')
dbutils.widgets.text("forecast_supplies_baseprod_mkt10_version",'')

# COMMAND ----------

discounting_factor = dbutils.widgets.get("discounting_factor")
if discounting_factor == "":
    discounting_factor = 0.135

forecast_supplies_baseprod_mkt10_version = dbutils.widgets.get("forecast_supplies_baseprod_mkt10_version")
if forecast_supplies_baseprod_mkt10_version == "":
    forecast_supplies_baseprod_mkt10_version = read_redshift_to_df(configs) \
        .option("query", "SELECT MAX(version) FROM fin_prod.forecast_supplies_baseprod_mkt10") \
        .load() \
        .rdd.flatMap(lambda x: x).collect()[0]

# COMMAND ----------

q1 = f"""
select a.record
	, a.market10
	, a.platform_subset
	, a.base_product_number
	, a.crg_chrome
	, a.customer_engagement
	, a.year
	, a.year_num
	, case when a.crg_chrome not in ('CYN','MAG','YEL') then b.k_usage
	else b.color_usage
	end as yearly_usage
	, trade_split
	, share
	, vtc
	, decay
	, remaining_amount
	, yield
	, host_yield
	, list_price
	, variable_cost
	, fixed_cost
	, contra
from
(
select icf.record
	, icf.market10
	, icf.platform_subset
	, icf.base_product_number
	, icf.crg_chrome
	, icf.customer_engagement
	, icf.year
	, icf.year_num
	, avg(usage) as gross_usage
	, avg(after_market_usage_cumulative) as after_market_cumulative
	, avg(trade_split) as trade_split
	, avg(hp_share) as share
	, avg(yield) as yield
	, avg(fsbm.baseprod_gru) as list_price
	, avg(vtc) as vtc
	, avg(decay) as decay
	, avg(remaining_amount) as remaining_amount
	, avg(host_yield) as host_yield
	, avg(fsbm.baseprod_variable_cost_per_unit) as variable_cost
	, avg(fsbm.baseprod_fixed_cost_per_unit) as fixed_cost
	, avg(fsbm.baseprod_contra_per_unit) as contra
	from ifs2.ifs2_country_fsb icf
	inner join fin_prod.forecast_supplies_baseprod_mkt10 fsbm
	--on icf.record = fsbm.record
	on icf.market10 = fsbm.market10
	and icf.base_product_number = fsbm.base_product_number
	and icf.cal_date = fsbm.cal_date
	where icf.record = 'TONER' and year_num <= 10
--	and icf.platform_subset = 'AGATE 23 MGD EPA'
--	and icf.base_product_number = 'W9215MC'
--	and icf.market10 = 'GREATER CHINA'
	and fsbm.version = '{forecast_supplies_baseprod_mkt10_version}'
	group by icf.record
	, icf.market10
	, icf.platform_subset
	, icf.base_product_number
	, icf.crg_chrome
	, icf.customer_engagement
	, icf.year
	, icf.year_num
) a
inner join 
(
	select platform_subset
	, market10
	, "year"
	, customer_engagement
	, sum(color_usage) as color_usage
	, sum(k_usage) as k_usage
	from ifs2.usage_share_toner ust
--	where 
 --	platform_subset = 'AGATE 23 MGD EPA'
 --	and market10 = 'GREATER CHINA'
	group by platform_subset
	, market10
	, "year"
	, customer_engagement
) b
on a.platform_subset = b.platform_subset
and a.market10 = b.market10
and a.year = b.year
and a.customer_engagement = b.customer_engagement
"""
toner_pen_details = read_redshift_to_df(configs) \
    .option("query", q1) \
    .load()

toner_pen_details.createOrReplaceTempView("toner_pen_details")

# COMMAND ----------

toner_pen_details.count()

# COMMAND ----------

q2 = """
select record
	, market10
	, platform_subset
	, base_product_number
	, crg_chrome
	, customer_engagement
	, year
	, year_num
	, yearly_usage
	, sum(yearly_usage) over (partition by market10, platform_subset, base_product_number, crg_chrome, customer_engagement order by year rows between unbounded preceding and current row) as cumulative_usage
	, case when (sum(yearly_usage) over (partition by market10, platform_subset, base_product_number, crg_chrome, customer_engagement order by year rows between unbounded preceding and current row) - host_yield) < 0 then 0
		else (sum(yearly_usage) over (partition by market10, platform_subset, base_product_number, crg_chrome, customer_engagement order by year rows between unbounded preceding and current row) - host_yield)
		end as after_market_usage
	, trade_split
	, share
	, vtc
	, decay
	, remaining_amount
	, yield
	, host_yield
	, list_price
	, variable_cost
	, fixed_cost
	, contra
from toner_pen_details
"""
toner_pen_year_details = spark.sql(q2)
toner_pen_year_details.createOrReplaceTempView("toner_pen_year_details")

# COMMAND ----------

q3 = """
select record
	, market10
	, platform_subset
	, base_product_number
	, crg_chrome
	, customer_engagement
	, year
	, year_num
	, (after_market_usage)
	, (lag(after_market_usage) over (partition by market10, platform_subset, base_product_number, crg_chrome, customer_engagement order by year))
	, case when year_num = 1 and (after_market_usage)<=0 then 0
		when year_num = 1 and (after_market_usage) > 0 then ((after_market_usage)/yield)*share*remaining_amount*vtc*trade_split
	else ((after_market_usage-(coalesce((lag(after_market_usage) over (partition by market10, platform_subset, base_product_number, crg_chrome, customer_engagement order by year)),0)))/yield)*share*remaining_amount*trade_split
	end as cartridge
	, list_price
	, variable_cost
	, fixed_cost
	, contra
from toner_pen_year_details
"""
toner_pen_agg = spark.sql(q3)
toner_pen_agg.createOrReplaceTempView("toner_pen_agg")

# COMMAND ----------

q4 = """
select record
	, market10
	, platform_subset
	, base_product_number
	, crg_chrome
	, customer_engagement
	, year
	, year_num
	, cartridge
	, list_price
	, contra
	, variable_cost
	, fixed_cost
	, cartridge*(list_price - contra) as net_revenue
	, cartridge*(list_price - contra - (variable_cost)) as gross_margin
	, power(1+{},(year_num - 0.5)) as discounting_factor
	, (cartridge*(list_price - contra))/power(1+{},(year_num - 0.5)) as ifs2_aru
	, (cartridge*(list_price - contra - (variable_cost)))/power(1+{},(year_num - 0.5)) as ifs2_gm
from toner_pen_agg
""".format(discounting_factor,discounting_factor,discounting_factor)
toner_pen_year_agg = spark.sql(q4)
toner_pen_year_agg.createOrReplaceTempView("toner_pen_year_agg")

# COMMAND ----------

toner_pen_year_agg.display()

# COMMAND ----------

q5 = """
select record
	, market10
	, platform_subset
	, base_product_number
	, crg_chrome
	, customer_engagement
    , sum(net_revenue) as net_revenue
	, sum(ifs2_aru) as ifs2_aru
    , sum(ifs2_gm) as ifs2_gm
from toner_pen_year_agg
group by 
    record
	, market10
	, platform_subset
	, base_product_number
	, crg_chrome
	, customer_engagement
"""
toner_pen_ifs2 = spark.sql(q5)
toner_pen_ifs2.createOrReplaceTempView("toner_pen_ifs2")

# COMMAND ----------

toner_pen_ifs2.display()

# COMMAND ----------

q6 = """
select record
	, market10
	, platform_subset
	, customer_engagement
    , sum(net_revenue) as net_revenue
	, sum(ifs2_aru) as ifs2_aru
    , sum(ifs2_gm) as ifs2_gm
from toner_pen_year_agg
group by 
    record
	, market10
	, platform_subset
	, customer_engagement
"""
toner_ifs2_m10 = spark.sql(q6)
toner_ifs2_m10.createOrReplaceTempView("toner_ifs2_m10")

# COMMAND ----------

toner_ifs2_m10.display()

# COMMAND ----------


