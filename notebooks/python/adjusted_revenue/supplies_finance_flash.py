# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Supplies Finance Flash for Adjusted Revenue

# COMMAND ----------

## Global Variables
query_list = []

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add Version

# COMMAND ----------

add_record = 'FORECAST - SUPPLIES FINANCE FLASH'
add_source = 'FROM FINANCE SPREADSHEET'

# COMMAND ----------

supp_flsh_add_version = call_redshift_addversion_sproc(configs, add_record, add_source)

version = supp_flsh_add_version[0]
print(version)

# COMMAND ----------

# Cells 8-9: Refreshes version table in delta lakes, to bring in new version from previous step, above.
version = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

adjusted_revenue_staging = read_redshift_to_df(configs) \
    .option("dbtable", "fin_stage.adjusted_revenue_staging") \
    .load()

tables = [['prod.version', version, "overwrite"], ['fin_stage.adjusted_revenue_staging', adjusted_revenue_staging, "overwrite"]]

# COMMAND ----------

# MAGIC %run "../finance_etl/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Flash for Insights Supplies

# COMMAND ----------

ci_flash = spark.sql("""
	select  fiscal_year_qtr_ as fiscal_year_qtr
      , pl 
      , business_description 
      , market 
      , channel_inv_k
      , ink_toner 
      , current_date() as  load_date 
      , '{}' as  version 
	from fin_prod.ci_flash_for_insights_supplies_temp
""".format(version))

ci_flash.createOrReplaceTempView("ci_flash_for_insights_supplies")
write_df_to_redshift(configs, ci_flash, "fin_stage.ci_flash_for_insights_supplies", "append")

# COMMAND ----------

spark.sql("""select * from ci_flash_for_insights_supplies""").show()

# COMMAND ----------

rev_flash = spark.sql("""
	select fiscal_year_qtr
      , pl
      , ink_toner
      , market
      , business_description
      , net_revenues_k
      , hedge_k
      , concatenate
      , current_date() as load_date
      ,'{}' as version
	from fin_prod.rev_flash_for_insights_supplies_temp
	where pl is not null
""".format(version))

rev_flash.createOrReplaceTempView("rev_flash_for_insights_supplies")
write_df_to_redshift(configs, rev_flash, "fin_stage.rev_flash_for_insights_supplies", "append")

# COMMAND ----------

spark.sql("""select * from rev_flash_for_insights_supplies""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Finance Flash Transform

# COMMAND ----------

flash_transform = spark.sql("""
with
supplies_revenue_flash as
(
	select 
		flash.fiscal_year_qtr,
		substring(fiscal_year_qtr, 1.0, 4.0) as fiscal_yr,
		plx.pl,
		technology,
		ltrim(market) as market,
		l6_description,
		 net_revenues_k  * 1000 as net_revenue,
		coalesce(hedge_k , 0.0) * 1000 as hedge
	from rev_flash_for_insights_supplies flash
	join mdm.product_line_xref plx on
		flash.pl =  plx.plxx
	where 1=1
	and flash.load_date = (select max(load_date) from rev_flash_for_insights_supplies)
	-- fiscal year where clause is a 'cheat' to prevent timing overlap between flash and history
	and flash.fiscal_year_qtr <> (select min(fiscal_year_qtr) from ci_flash_for_insights_supplies)
	and flash.pl <> 'AU00' and flash.pl <> 'AU'
	and flash.pl <> 'N600' and flash.pl <> 'N6'
	and  ink_toner  <> 'MEDIA'
),
supplies_revenue_flash2 as
(
	select cal.date as cal_date,
		flash.fiscal_year_qtr,
		flash.fiscal_yr,
		pl,
		technology,
		case
			when market = 'WW HQ, GF AND OTHERS' then 'WORLD WIDE'
			when market = 'INDIA, B&SL' then 'INDIA SL & BL'
				else market
			end as market,
		case
			when market in ('APJ HQ', 'AMS HQ') then 'HQ' 
				else 'NON-HQ' 
			end as hq_flag,
		l6_description,
		sum(net_revenue) as net_revenue,
		sum(hedge) as hedge,
		max(flash.fiscal_year_qtr) over (partition by market, pl order by pl) as max_cal_date
	from supplies_revenue_flash flash
	left join mdm.calendar cal on cal.fiscal_year_qtr = flash.fiscal_year_qtr and cal.fiscal_yr = flash.fiscal_yr
	where day_of_month = 1
	and fiscal_month in ('3.0', '6.0', '9.0', '12.0')
		group by flash.fiscal_year_qtr, flash.fiscal_yr, pl, technology, market, l6_description, cal.date
),
supplies_ci_flash as
(
	select cal.date as cal_date,
		flash.fiscal_year_qtr,
		substring(flash.fiscal_year_qtr, 1, 4) as fiscal_yr,
		plx.pl,
		technology,
		ltrim(market) as market,
		l6_description,
		 channel_inv_k  * 1000 as channel_inventory
	from ci_flash_for_insights_supplies flash
	join mdm.calendar cal on cal.fiscal_year_qtr = flash.fiscal_year_qtr
	join mdm.product_line_xref plx on
		flash.pl =  plx.plxx
	where 1=1 
		and flash.load_date = (select max(load_date) from ci_flash_for_insights_supplies)
		and plx.pl <> 'AU00' and plx.pl <> 'AU'
and plx.pl <> 'N600' and plx.pl <> 'N6'
and ink_toner  <> 'MEDIA'
and day_of_month = 1
and fiscal_month in ('3.0', '6.0', '9.0', '12.0')
		and  channel_inv_k  is not null
),
supplies_ci_flash2 as
(
	select cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		case
			when market = 'INDIA' then 'INDIA SL & BL'
            when market = 'OTHER GEO ROLLUP' then 'WORLD WIDE'
                else market
        end as market,
        'NON-HQ' as hq_flag,
		l6_description,
		sum(channel_inventory) as channel_inventory
	from supplies_ci_flash flash
	group by cal_date, fiscal_year_qtr, fiscal_yr, pl, technology, market, l6_description
), 
date_helper as
(
	select
		date_key
		,  date  as cal_date
	from mdm.calendar
	where day_of_month = 1 
	and fiscal_month in ('3.0', '6.0', '9.0', '12.0')
), 
yoy_analytic_setup as
(
	select cal_date,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		channel_inventory,
			(select min(cal_date) from supplies_ci_flash2) as min_cal_date,
			(select max(cal_date) from supplies_ci_flash2) as max_cal_date
	from supplies_ci_flash2
), 
yoy_full_cross_join as
(
	select distinct d.cal_date,
		pl,
		technology,
		market,
		hq_flag,
		l6_description
	from date_helper d
	cross join yoy_analytic_setup ar
	where d.cal_date between ar.min_cal_date and ar.max_cal_date
), 
fill_gap1 as
(
	select
		c.cal_date,
		coalesce(c.pl, s.pl) as pl,
		coalesce(c.technology, s.technology) as technology,
		coalesce(c.market, s.market) as market,
		coalesce(c.hq_flag, s.hq_flag) as hq_flag,
		coalesce(c.l6_description, s.l6_description) as l6_description,
		channel_inventory
	from yoy_full_cross_join c
	left join yoy_analytic_setup s	on
		c.cal_date = s.cal_date and
		c.pl = s.pl and
		c.technology = s.technology and
		c.market = s.market and
		c.hq_flag = s.hq_flag and
		c.l6_description = s.l6_description 
), 
fill_gap2 as
(
	select cal_date,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		channel_inventory,
			min(cal_date) over (partition by market, pl order by cal_date) as min_cal_date
	from fill_gap1
), 
ci_flash_time_series as
(
	select
		cal_date,
		min_cal_date,
			pl,
			technology,
			market,
			hq_flag,
			l6_description,
			channel_inventory,
			case when min_cal_date < cast(current_date() - day(current_date()) + 1 as date) then 1 else 0 end as actuals_flag,
			count(cal_date) over (partition by market, pl
                    order by cal_date rows between unbounded preceding and current row) as running_count
		from fill_gap2
), 
ci_flash_full_calendar as
(
	select 
		cal_date,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		coalesce(sum(channel_inventory), 0) as channel_inventory
	from ci_flash_time_series ar
	join mdm.calendar cal on ar.cal_date = cal.date
	where day_of_month = 1
	group by cal_date, pl, technology, market, l6_description, hq_flag
), 
ci_flash_lagged as 
(
	select cal_date,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		sum(channel_inventory) as channel_inventory,
		lag(coalesce(sum(channel_inventory), 0), 1) over
			(partition by market,  pl
				order by cal_date) as prior_quarter_channel_inventory -- quarterly lag
	from ci_flash_full_calendar
	group by cal_date,
		pl,
		technology,
		market,
		hq_flag,
		l6_description
),
ci_flash_change as 
(
	select cal_date,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		sum(channel_inventory) as channel_inventory,
		sum(channel_inventory) - coalesce(sum(prior_quarter_channel_inventory),0) as channel_inventory_change
	from ci_flash_lagged
	group by cal_date,
		pl,
		technology,
		market,
		hq_flag,
		l6_description
), 
ci_flash_change2 as 
(
	select cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		coalesce(sum(channel_inventory), 0) as channel_inventory,
		coalesce(sum(channel_inventory_change), 0) as channel_inventory_change
	from ci_flash_change ci
	join mdm.calendar cal on cal_date = cal.date
	where day_of_month = 1
	group by cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description
),
quarters as
(
select distinct fiscal_year_qtr,
	fiscal_yr,
	0 as net_revenue,
	0 as hedge,
	0 as channel_inventory,
	0 as ci_change
from mdm.calendar cal
where day_of_month = 1
and fiscal_yr > '2015'
and fiscal_year_qtr <= (select max(fiscal_year_qtr) from rev_flash_for_insights_supplies
								where version = (select max(version) from rev_flash_for_insights_supplies))
),
selected_ie2_market10 as
(
	select
		distinct fiscal_year_qtr,
		fiscal_yr,
		s.pl,
		technology,
		s.market10 as market,
		'non-hq' as hq_flag,
	l6_description
from fin_stage.adjusted_revenue_staging s
join mdm.calendar cal on cal.date = s.cal_date
join mdm.product_line_xref plx on s.pl = plx.pl
join mdm.iso_country_code_xref iso on iso.country_alpha2 = s.country_alpha2
where 1=1
	and s.version = (select max(version) from fin_stage.adjusted_revenue_staging)
	and day_of_month = 1
	and region_3 in ('EMEA', 'APJ', 'WORLD WIDE')
),
dummy_flash_mkt_forecast as
(
select distinct q.fiscal_year_qtr,
		q.fiscal_yr,
		market,
		hq_flag,
		pl,
		technology,
		l6_description,
		net_revenue,
		hedge,
		channel_inventory,
		ci_change
from quarters q
cross join selected_ie2_market10 f 
where q.fiscal_year_qtr <= (select distinct max_cal_date from supplies_revenue_flash2)
),
final_dummy_fcst as
(
select cal.date as cal_date,
	dmy.fiscal_year_qtr,
	dmy.fiscal_yr,
	market,
	hq_flag,
	dmy.pl,
	l6_description,
	technology,
		sum(net_revenue) as net_revenue,	
		sum(hedge) as hedge,	
		sum(channel_inventory) as channel_inventory,
		sum(ci_change) as ci_change
from dummy_flash_mkt_forecast dmy
left join mdm.calendar cal on cal.fiscal_year_qtr = dmy.fiscal_year_qtr
where dmy.fiscal_year_qtr in (select distinct fiscal_year_qtr from supplies_revenue_flash)--ie2_financials.dbo.supplies_finance_flash)
and day_of_month = 1 
and fiscal_month in ('3.0', '6.0', '9.0', '12.0')
group by dmy.fiscal_year_qtr, market, hq_flag, dmy.pl, l6_description, technology, dmy.fiscal_yr, cal.date
),
supplies_finance_flash as
(
	select cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		sum(net_revenue) as net_revenue,
		sum(hedge) as hedge,
		0 as channel_inventory,
		0 as ci_change
	from supplies_revenue_flash2
	group by
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		cal_date

	union all

	select cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		0 as net_revenue,
		0 as hedge,
		sum(channel_inventory) as channel_inventory,
		sum(channel_inventory_change) as ci_change
	from ci_flash_change2
	where fiscal_year_qtr in (select distinct fiscal_year_qtr from supplies_revenue_flash2)
	group by cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description

	union all

	select cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		sum(net_revenue) as net_revenue,
        sum(hedge) as hedge,
		sum(channel_inventory) as channel_inventory,
		sum(ci_change) as ci_change
	from final_dummy_fcst
	group by cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description
),
supplies_finance_flash2 as
(
	select cal_date,
		flash.fiscal_year_qtr,
		flash.fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		coalesce(sum(net_revenue), 0) as net_revenue,
		coalesce(sum(hedge), 0) as hedge,
		coalesce(sum(channel_inventory), 0) as channel_inventory,
		coalesce(sum(ci_change), 0) as ci_change
	from supplies_finance_flash flash
	where 1=1 
	group by cal_date,
		flash.fiscal_year_qtr,
		flash.fiscal_yr,
		pl,
		technology,
		hq_flag,
		market,
		l6_description
)
	select
	cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description,
		coalesce(sum(net_revenue), 0) as net_revenue,
		coalesce(sum(hedge), 0) as hedge,
		coalesce(sum(channel_inventory), 0) as channel_inventory,
		coalesce(sum(ci_change), 0) as ci_change
	from supplies_finance_flash2
	where 1=1 
	group by cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		hq_flag,
		market,
		l6_description
""")

flash_transform.createOrReplaceTempView("flash")

# COMMAND ----------

spark.sql("""select * from flash""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write Supplies Finance Flash Table

# COMMAND ----------

supp_fin_flash = spark.sql(""" 
	select 
        'FORECAST - SUPPLIES FINANCE FLASH' as record,
        cal_date,
        fiscal_year_qtr,
        fiscal_yr,
        pl,
        technology,
        market,
        hq_flag,
        l6_description,
        sum(net_revenue) as net_revenue,
        sum(hedge) as hedge,
        sum(channel_inventory) as channel_inventory,
        sum(ci_change) as ci_change,
        1 as official,
        (
            select distinct load_date from prod.version where record = 'FORECAST - SUPPLIES FINANCE FLASH' 
            and load_date = (select max(load_date) from prod.version where record = 'FORECAST - SUPPLIES FINANCE FLASH')
        ) as load_date,
        (
            select distinct version from prod.version where record = 'FORECAST - SUPPLIES FINANCE FLASH'
            and version = (select max(version) from prod.version where record = 'FORECAST - SUPPLIES FINANCE FLASH')
        ) as version
    from flash
	group by
		cal_date,
		fiscal_year_qtr,
		fiscal_yr,
		pl,
		technology,
		market,
		hq_flag,
		l6_description;

""")

write_df_to_redshift(configs, supp_fin_flash, "fin_prod.supplies_finance_flash", "append")

# COMMAND ----------


