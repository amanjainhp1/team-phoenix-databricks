# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Adjusted Revenue

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Notebook/Process Prep

# COMMAND ----------

## Global Variables
query_list = []

## Supplies History 3
cur_period = '2022-10-01'  # accounting rate

## Channel Inventory Prep 1
cbm_st_month = '2015-10-01'
product_xref_month = '2021-03-01'
ci_adjust_cal_date = '2017-11-01'

## CI with Accounting Rates 3
inv_curr2 = '2018-08-01'

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add Version

# COMMAND ----------

add_record = 'ACTUALS - ADJUSTED_REVENUE - SALES PRODUCT'
add_source = 'ADDS CURRENCY AND CI ADJUSTS'

# COMMAND ----------

supp_flsh_add_version = call_redshift_addversion_sproc(configs, add_record, add_source)

version = supp_flsh_add_version[0]
print(version)

# COMMAND ----------

# Cells 9-10: Refreshes version table in delta lakes, to bring in new version from previous step, above.
version = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

tables = [['prod.version', version, "overwrite"]]

# COMMAND ----------

# MAGIC %run "../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Supplies History Constant Currency

# COMMAND ----------

supp_hist_1 = spark.sql("""
 -- start adjusted revenue computations
 -- bring in sales product actual information; select only the supplies PLs included in adjusted revenue reporting, i.e., just supplies
 -- bring in entire data set (i.e., all history) because all history restates to the current accounting rate
	with
		salesprod_raw as
		(
			select
				cal_date,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(revenue_units) as revenue_units
			from fin_prod.actuals_supplies_salesprod s
			group by cal_date, country_alpha2, sales_product_number, pl, customer_engagement, currency
		),
		salesprod_actuals as
		(
			select cal_date,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(revenue_units) as revenue_units
			from salesprod_raw
			where pl in
				(
					select distinct pl
					from mdm.product_line_xref
                    where pl_category in ('SUP')
                        and technology in ('INK', 'LASER', 'PWA', 'LF')
                        and pl not in ('GY', 'LZ')
			)
			group by cal_date, country_alpha2, sales_product_number, pl, customer_engagement, currency
		)

			select
				cal_date,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				gross_revenue,
				net_currency,
				contractual_discounts,
				discretionary_discounts,
				net_revenue,
				total_cos,
				gross_profit,
				revenue_units
			from salesprod_actuals
""")

supp_hist_1.createOrReplaceTempView("salesproduct_actuals")

# COMMAND ----------

supp_hist_2 = spark.sql("""
-- redundant code block from sales product sproc but needed for channel inventory that doesn't map to financial actuals
		with

		emea_currency_table as
		(
			select
				case
					when country = 'AFGHANISTAN' then 'AP'
					else region_5
				end as region_5,
				country_alpha2,
				country,
				case
					when currency = 'EURO' then 'EUR'
					else 'USD'
				end as currency
			from mdm.list_price_eu_country_list
			where country_alpha2 not in ('ZM', 'ZW')
		),
		row_currency_table as
		(
			select
				region_5,
				cmap.country_alpha2,
				cmap.country,
				currency_iso_code as currency
			from mdm.country_currency_map cmap
			left join mdm.iso_country_code_xref iso on cmap.country_alpha2 = iso.country_alpha2 and cmap.country = iso.country
			where cmap.country_alpha2 not in (
				select distinct country_alpha2
				from emea_currency_table
				)
		),
		all_currencies as
		(
			select *
			from emea_currency_table
			union all
			select *
			from row_currency_table
		)

			select distinct
				region_5,
				country_alpha2,
				country,
				case
					when currency in ('ZMW', 'ZWD') then 'USD'
					else currency
				end as currency
			from all_currencies
""")

supp_hist_2.createOrReplaceTempView("currency")

# COMMAND ----------

supp_hist_3 = spark.sql("""
-- accounting rates

	with

		accounting_rates_table as
		(
			select
				effectivedate,
				accountingrate,
				isocurrcd
			from prod.acct_rates
			where accountingrate != 0
		)
			select
				distinct(isocurrcd),
				effectivedate as accounting_rate,
				accountingrate
			from accounting_rates_table
			where effectivedate = (SELECT MAX(EffectiveDate) AS current_period FROM prod.acct_rates)
            --(select distinct '{cur_period}' as current_period from prod.acct_rates)
""")

supp_hist_3.createOrReplaceTempView("current_accounting_rate")

# COMMAND ----------

supp_hist_4 = spark.sql("""
-- select * from #current_accounting_rate
	with

		accounting_rates_table as
		(
			select
				effectivedate,
				accountingrate,
				isocurrcd
			from prod.acct_rates
			where accountingrate != 0
		)

			select
				distinct(isocurrcd),
				effectivedate,
				accountingrate
			from accounting_rates_table
""")

supp_hist_4.createOrReplaceTempView("original_accounting_rate")

# COMMAND ----------

supp_hist_5 = spark.sql("""
-- select * from #original_accounting_rate
-- apply accounting rates to the financial actuals to determine currency adjustments to those actuals

with

		salesprod_actuals_currency as
		(
			select cal_date,
				act.country_alpha2,
				iso.region_5,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(revenue_units) as revenue_units
			from salesproduct_actuals act
			left join mdm.iso_country_code_xref iso 
				on act.country_alpha2 = iso.country_alpha2
			group by cal_date, act.country_alpha2, iso.region_5, currency, sales_product_number, pl, customer_engagement
		),
		actuals_join_acct_rates as
		(
			select
				act.cal_date,
				region_5,
				country_alpha2,
				act.currency,
				sales_product_number,
				pl,
				customer_engagement,
				coalesce(sum(rate.accountingrate), 1) as original_rate,
				coalesce(sum(curr_rate.accountingrate), 1) as current_rate,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(gross_revenue) - sum(contractual_discounts) - sum(discretionary_discounts) as net_revenue_before_hedge,
				sum(revenue_units) as revenue_units
			from salesprod_actuals_currency act
			left join current_accounting_rate curr_rate
				on curr_rate.isocurrcd = act.currency
			left join original_accounting_rate rate
				on act.currency = rate.isocurrcd and act.cal_date = rate.effectivedate
			group by act.cal_date, country_alpha2, region_5, act.currency, sales_product_number, pl, customer_engagement, curr_rate.accountingrate, rate.accountingrate
		),
		actuals_join_acct_rates2 as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				sum(original_rate) as original_rate,
				case
					when currency = 'VEF' 
					then 
					(
						select accountingrate 
						from prod.acct_rates 
						where isocurrcd = 'VEF'
							and effectivedate = 
							(
							select max(effectivedate) from prod.acct_rates where isocurrcd = 'VEF' and accountingrate != 0
							)
					)
					else sum(current_rate) 
				end as current_rate,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(net_revenue_before_hedge) as net_revenue_before_hedge,
				sum(revenue_units) as revenue_units
			from actuals_join_acct_rates
			group by cal_date, country_alpha2, region_5, currency, sales_product_number, pl, customer_engagement
		),
		actuals_adjust_accounting_rates as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(net_revenue_before_hedge) as net_revenue_before_hedge,
				sum(revenue_units) as revenue_units,				
				-- usd is denominator of all accounting rates
				sum(original_rate) as original_rate,
				sum(current_rate) as current_rate,
				coalesce((nullif(original_rate, 0) / coalesce(current_rate, 0)), 1) as currency_rate_adjustment,
				coalesce((nullif(original_rate, 0) / coalesce(current_rate, 0)), 1) * sum(net_revenue_before_hedge) as cc_net_revenue
			from actuals_join_acct_rates2
			group by cal_date, country_alpha2, region_5, currency, sales_product_number, pl, customer_engagement, original_rate, current_rate
		)

			
			select
				cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				gross_revenue,
				net_currency,
				contractual_discounts,
				discretionary_discounts,
				net_revenue,
				total_cos,
				gross_profit,
				net_revenue_before_hedge,
				revenue_units,	
				original_rate,
				current_rate,
				currency_rate_adjustment,
				cc_net_revenue
			from actuals_adjust_accounting_rates
""")

supp_hist_5.createOrReplaceTempView("supplies_history_constant_currency")
query_list.append(["fin_stage.adj_rev_supplies_history_constant_currency", supp_hist_5, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Channel Inventory Prepped AMS Unadjusted

# COMMAND ----------

chann_inv_ams = spark.sql("""

	with cbm_st_database as 
		(
			select 
				case
					when st.data_type = 'Actuals' then 'ACTUALS - CBM_ST_SALES_QTY'
					when st.data_type = 'Proxy Adjustment' then 'PROXY ADJUSTMENT'
                 else st.data_type
				 end as record,
				cast(st.month as date) as cal_date,  
				case
					when st.country_code = '0A' then 'XB'
					when st.country_code = '0M' then 'XH'
					when st.country_code = 'CS' then 'XA'
					when st.country_code = 'KV' then 'XA'
				else st.country_code
				end as country_alpha2,
                case
                    when st.product_number = 'Proxy Adjustment' then 'PROXY ADJUSTMENT'
                else st.product_number
                end as sales_product_number,
				st.product_line_id as pl, 
				st.partner,
				st.rtm_2 as rtm2,
				sum(cast(coalesce(st.sell_thru_usd,0) as float)) as sell_thru_usd,
				sum(cast(coalesce(st.sell_thru_qty,0) as float)) as sell_thru_qty,
				sum(cast(coalesce(st.channel_inventory_usd,0) as float)) as channel_inventory_usd,
				(sum(cast(coalesce(st.channel_inventory_qty,0) as float)) + 0.0) as channel_inventory_qty
			from fin_stage.cbm_st_data st
			where 1=1
				and	st.month > '{}'
				and (coalesce(st.sell_thru_usd,0) + coalesce(st.sell_thru_qty,0) + coalesce(st.channel_inventory_usd,0) + coalesce(st.channel_inventory_qty,0)) <> 0
			group by st.data_type, st.month, st.country_code, st.product_number, st.product_line_id, st.partner, st.rtm_2
		),
		channel_inventory as
		(
			select 
				cal_date,
				country_alpha2,
				case
					when sales_product_number like '%#%'
					then left(sales_product_number, 7)
					else sales_product_number
				end as sales_product_number,
				pl,
				sum(channel_inventory_usd) as channel_inventory_usd,
				sum(channel_inventory_qty) as channel_inventory_qty,
				sum(channel_inventory_usd) + sum(channel_inventory_qty) as total
			from cbm_st_database	
			where (cal_date between (select min(cal_date) from fin_prod.actuals_supplies_salesprod)
				and (select max(cal_date) from fin_prod.actuals_supplies_salesprod))
				and pl in 
				(
					select distinct pl 
					from mdm.product_line_xref 
					where pl_category = 'SUP' 
						and technology in ('INK', 'LASER', 'PWA', 'LF')
						and pl not in ('GY', 'LZ')
				)
			group by cal_date, country_alpha2, sales_product_number, pl
		),
		channel_inventory2 as
		(
			select 
				cal_date,
				country_alpha2,
				case
					when sales_product_number like '%#%'
					then left(sales_product_number, 6)
					else sales_product_number
				end as sales_product_number,
				case
					when sales_product_number = '4HY97A' and pl = 'G0'
					then 'E5'
					else pl
				end as pl,
				sum(channel_inventory_usd) as channel_inventory_usd,
				sum(channel_inventory_qty) as channel_inventory_qty
			from channel_inventory 
			where total <> 0
			group by cal_date, country_alpha2, sales_product_number, pl
		),
		rdma_updated_pl as
		(
			select 
				distinct(sales_product_number),
				 sales_product_line_code
			from mdm.rdma_base_to_sales_product_map
		),
		channel_inventory_cleaned as
		(
			select 
				cal_date,
				country_alpha2,
				st.sales_product_number,
				pl as st_pl,
				sales_product_line_code as rdma_pl,
				sum(channel_inventory_usd) as channel_inventory_usd,
				sum(channel_inventory_qty) as channel_inventory_qty
			from channel_inventory2  st	
			left join rdma_updated_pl rdma on st.sales_product_number = rdma.sales_product_number
			group by cal_date, country_alpha2, st.sales_product_number, sales_product_line_code, pl
		),
		channel_inventory_cleaned2 as
		(
			select
				cal_date,
				country_alpha2,
				CASE
					WHEN sales_product_number = 'PROXY ADJUSTMENT'
					THEN 'PROXY_ADJUSTMENT'
					ELSE sales_product_number
				END AS sales_product_number,
				case 
					when rdma_pl is null then st_pl
					else rdma_pl
				end as pl, 
				sum(channel_inventory_usd) as channel_inventory_usd,
				sum(channel_inventory_qty) as channel_inventory_qty
			from channel_inventory_cleaned
			group by cal_date, country_alpha2, sales_product_number, rdma_pl, st_pl
		),
		channel_inventory_supplies_pl as
		(	
			select
				cal_date,
				country_alpha2,
				sales_product_number,
				pl,
				sum(channel_inventory_usd) as channel_inventory_usd,
				sum(channel_inventory_qty) as channel_inventory_qty
			from channel_inventory_cleaned2
			where pl in 
				(	
					select distinct pl 
					from mdm.product_line_xref 
					where pl_category in ('SUP')
						and technology in ('INK', 'LASER', 'PWA', 'LF')
						and pl not in ('GY', 'LZ')
				)
			group by cal_date, country_alpha2, sales_product_number, pl
		),
		channel_inventory_region5 as
		(
			select
				cal_date,
				region_5,
				st.country_alpha2,
				case
					when sales_product_number = 'PROXY_ADJUSTMENT'
					then concat('PROXY_ADJUSTMENT', pl)
					else sales_product_number
				end as sales_product_number,
				pl,
				sum(channel_inventory_usd) as inventory_usd,
				sum(channel_inventory_qty) as inventory_qty
			from channel_inventory_supplies_pl st
			join mdm.iso_country_code_xref geo on st.country_alpha2 = geo.country_alpha2
			group by cal_date, st.country_alpha2, sales_product_number, pl, region_5
		)
		
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				inventory_usd,
				inventory_qty
			from channel_inventory_region5
            
""".format(cbm_st_month))

chann_inv_ams.createOrReplaceTempView("channel_inventory_prepped_ams_unadjusted")
query_list.append(["fin_stage.channel_inventory_prepped_ams_unadjusted", chann_inv_ams, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Channel Inventory Prep 1

# COMMAND ----------

ch_inv_prep_1 = spark.sql("""
		with
		cbm_st_database2 as 
		(
			select 
				case
					when st.data_type = 'Actuals' then 'ACTUALS - CBM_ST_BASE_QTY'
					when st.data_type = 'Proxy Adjustment' then 'PROXY ADJUSTMENT'
				 end as record,
				cast(st.month as date) as cal_date,  
				case
					when st.country_code = '0A' then 'XB'
					when st.country_code = '0M' then 'XH'
					when st.country_code = 'CS' then 'XA'
					when st.country_code = 'KV' then 'XA'
				else st.country_code
				end as country_alpha2,
				case
					when st.product_number = 'Actuals' then 'ACTUALS - CBM_ST_BASE_QTY'
					when st.product_number = 'Proxy Adjustment' then 'PROXY ADJUSTMENT'
				 end as sales_product_number,
				st.product_line_id as pl, 
				st.partner,
				st.rtm_2 as rtm2,
				sum(cast(coalesce(st.sell_thru_usd,0) as float)) as sell_thru_usd,
				sum(cast(coalesce(st.sell_thru_qty,0) as float)) as sell_thru_qty,
				sum(cast(coalesce(st.channel_inventory_usd,0) as float)) as channel_inventory_usd,
				(sum(cast(coalesce(st.channel_inventory_qty,0) as float)) + 0.0) as channel_inventory_qty
			from fin_stage.cbm_st_data st
			where month > '{}'
				and (coalesce(st.sell_thru_usd,0) + coalesce(st.sell_thru_qty,0) + coalesce(st.channel_inventory_usd,0) + coalesce(st.channel_inventory_qty,0)) <> 0
				and product_line_id in 
				(	select distinct pl 
					from mdm.product_line_xref 
					where pl_category in ('SUP')
						and technology in ('INK', 'PWA')
						or pl = 'AU'
				)
				/** special finance calc discontinued in march 2021 **/
				and month < '{}'
			group by data_type, month, country_code, product_number, product_line_id, partner, rtm_2
		),
		--cbm_st_database2 as 
		--(
		--	select record,
		--		cal_date,
		--		region_5, 
		--		country_alpha2,
		--		sales_product_number,
		--		pl, 
		--		isnull(sum(inventory_usd), 0) as channel_inventory_usd,
		--		isnull(sum(inventory_qty), 0) as channel_inventory_qty,
		--		load_date
		--	from ie2_landing.dbo.cbm_st_archived_for_adjusted_revenue_landing
		--	where 1=1
		--	and (isnull(inventory_usd,0) + isnull(inventory_qty,0)) <> 0
		--	group by record, cal_date, country_alpha2, sales_product_number, pl, region_5, load_date
		--),
		cbm_quarterly as
		(
			select cal_date,
				region_3,
				pl,
				fiscal_year_qtr,
				sum(channel_inventory_usd) as channel_inventory_usd
			from cbm_st_database2 ci
			join mdm.calendar cal on ci.cal_date = cal.date 
			join mdm.iso_country_code_xref iso on iso.country_alpha2 = ci.country_alpha2
			where day_of_month = 1
			and fiscal_month in ('3.0', '6.0', '9.0', '12.0')
			and region_3 = 'AMS'
			group by fiscal_year_qtr, cal_date, region_3, pl
		),
		americas_ink_media as
		(
			select
				cal_date,
				region_3,
				sum(channel_inventory_usd) as ci_usd,
				sum(channel_inventory_usd * 0.983) as finance_ink_ci
			from cbm_quarterly
			group by cal_date, region_3 
		),
		cbm_ink_only as
		(
			select
				cal_date,
				region_3,
				sum(channel_inventory_usd) as cbm_ink_ci
			from cbm_quarterly 
			where pl != 'au'
			group by cal_date, region_3 
		),
		official_finance_ci_adjust as
		(
			select
				fin.cal_date,
				fin.region_3,
				sum(cbm_ink_ci) as cbm_ink_ci,
				sum(finance_ink_ci) as official_ink_ci,
				sum(finance_ink_ci) - sum(cbm_ink_ci) as official_finance_ci_plug
			from americas_ink_media fin 
			join cbm_ink_only cbm on cbm.cal_date = fin.cal_date and fin.region_3 = cbm.region_3
			group by fin.cal_date, fin.region_3
		),
		official_finance_ci_adjust_final as
		(
			select
				cal_date,
				'NA' as region_5,
				'US' as country_alpha2,
				'AMS_FINANCE_ADJUSTMENT' as sales_product_number, --'proxy_adjustment1n' as sales_product_number,
				'1N' as pl,
				sum(official_finance_ci_plug) as inventory_usd,
				0 as inventory_qty
			from official_finance_ci_adjust
			-- finance feedback:  due to possible restatements, adjustment does not tie out well prior to fy2018.  only apply adjustment to 2018 forward.
			where cal_date >= '{}'
			group by cal_date, region_3
		)

		
		select
			cal_date,
			country_alpha2,
			region_5,
			sales_product_number,
			pl,
			inventory_usd,
			inventory_qty
		from official_finance_ci_adjust_final
""".format(cbm_st_month, product_xref_month, ci_adjust_cal_date))

ch_inv_prep_1.createOrReplaceTempView("official_finance_adjustment")

# COMMAND ----------

ch_inv_prep_2 = spark.sql("""
--select * from #official_finance_adjustment
-- add media ams adjustment to proxy adjustments so that its period over period change will be captured

	with		
		ams_adjusted_ci as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty
			from channel_inventory_prepped_ams_unadjusted
			group by cal_date, region_5, country_alpha2, sales_product_number, pl
		
			union all

			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from official_finance_adjustment
			group by cal_date, region_5, country_alpha2, sales_product_number, pl
		)

		select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty	
			from ams_adjusted_ci
			group by cal_date, region_5, country_alpha2, sales_product_number, pl
""")

ch_inv_prep_2.createOrReplaceTempView("channel_inventory_prepped1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## CBM Sellthru XCode Adjusted 5

# COMMAND ----------

cbm_sell_adj5_1 = spark.sql("""
		
	with		
		
		channel_inventory_with_proxy as
		(
			select 
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_prepped1
			where sales_product_number like 'PROXY_ADJUSTMENT%'
			group by cal_date, region_5, country_alpha2, sales_product_number, pl
		),
		total_proxy as
		(
			select 
				cal_date,
				region_5,
				sales_product_number,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_with_proxy
			group by cal_date, region_5, sales_product_number, pl
		),
		proxy_with_sellthru_country_mix as
		(
			select 
				cal_date,
				region_5,
				country_alpha2,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_prepped1
			where country_alpha2  
            not in (
                        select country_alpha2
                        from mdm.iso_country_code_xref
                        where country_alpha2 like 'X%'
                        and country_alpha2 != 'XK'
					)
			group by cal_date, region_5, pl, country_alpha2
		),
		proxy_with_sellthru_country_mix2 as
		(
			select 
				cal_date,
				region_5,
				country_alpha2,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty,
				case
					when sum(inventory_usd) over (partition by cal_date, region_5, pl) = 0 then null
					else inventory_usd / sum(inventory_usd) over (partition by cal_date, region_5, pl)
				end as country_proxy_mix
			from proxy_with_sellthru_country_mix
			group by cal_date, region_5, pl, country_alpha2, inventory_usd
		),
		channel_inventory_with_proxy2 as
		(
			select 
				mix.cal_date,
				mix.region_5,
				country_alpha2,
				sales_product_number,
				mix.pl,
				sum(total.inventory_usd * country_proxy_mix) as inventory_usd,
				sum(total.inventory_qty * country_proxy_mix) as inventory_qty
			from proxy_with_sellthru_country_mix2 mix
			join total_proxy total on total.cal_date = mix.cal_date and total.region_5 = mix.region_5 
				and total.pl = mix.pl
			group by mix.cal_date, mix.region_5, country_alpha2, sales_product_number, mix.pl
		),
		channel_inventory_proxy_adjust as
		(
			select
				cal_date,
				country_alpha2,
				region_5,
				sales_product_number,
				pl, 
				case
					when pl = 'GD' then 'I-INK'
					else 'TRAD'
				end as customer_engagement,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty
			from channel_inventory_with_proxy2
			group by cal_date, country_alpha2, pl, region_5, sales_product_number
		)


			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_proxy_adjust
			group by cal_date, country_alpha2, pl, region_5, sales_product_number, customer_engagement

""")

cbm_sell_adj5_1.createOrReplaceTempView("channel_inventory_wproxy")

# COMMAND ----------

cbm_sell_adj5_2 = spark.sql("""
 
	--select
	--	sum(inventory_usd) as inventory_usd,
	--	sum(inventory_qty) as inventory_qty 
	--from #channel_inventory_wproxy

	-- process sell thru without proxy adjustment

	with

		channel_inventory_without_proxy as
		(
			select 
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_prepped1
			where sales_product_number not like 'PROXY_ADJUSTMENT%'
				and sales_product_number not like 'AMS%'
			group by cal_date, region_5, country_alpha2, sales_product_number, pl
		),
		channel_inventory_totals as
		(
			select
				cal_date,
				region_5,
				sales_product_number,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_without_proxy
			group by cal_date, region_5, sales_product_number, pl
		),
		channel_inventory_no_xcode_mix as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_without_proxy
			where country_alpha2  
            not in (
                        select country_alpha2
                        from mdm.iso_country_code_xref
                        where country_alpha2 like 'X%'
                        and country_alpha2 != 'XK'
                    )
			group by cal_date, region_5, sales_product_number, pl, country_alpha2, inventory_qty
		),
		channel_inventory_no_xcode_mix2 as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty,
				case
					when sum(inventory_qty) over (partition by cal_date, region_5, sales_product_number, pl) = 0 then null
					else inventory_qty / sum(inventory_qty) over (partition by cal_date, region_5, sales_product_number, pl)
				end as country_mix
			from channel_inventory_no_xcode_mix
			group by cal_date, region_5, sales_product_number, pl, country_alpha2, inventory_qty
		),
		channel_inventory_without_xcodes as
		(
			select
				inv.cal_date,
				inv.region_5,
				country_alpha2,
				inv.sales_product_number,
				inv.pl,
				sum(totals.inventory_usd * country_mix) as inventory_usd,
				sum(totals.inventory_qty * country_mix) as inventory_qty
			from channel_inventory_no_xcode_mix2 inv
			join channel_inventory_totals totals on inv.cal_date = totals.cal_date and inv.region_5 = totals.region_5 and inv.pl = totals.pl 
				and inv.sales_product_number = totals.sales_product_number
			group by inv.cal_date, inv.region_5, country_alpha2, inv.sales_product_number, inv.pl				
		),
		channel_inventory_without_xcodes2 as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty
			from channel_inventory_without_xcodes
			group by cal_date, region_5, country_alpha2, sales_product_number, pl				
		),		
		direct_indirect_mix as
		(
			select
				cal_date,
				country_alpha2,
				sales_product_number,
				customer_engagement,
				case
					when sum(revenue_units) over (partition by cal_date, country_alpha2, sales_product_number) = 0 then null
					else revenue_units / sum(revenue_units) over (partition by cal_date, country_alpha2, sales_product_number)
				end as unit_ce_mix
			from fin_prod.actuals_supplies_salesprod
			-- no dollar impact associated with direct fulfillment or i-ink at this time (i-ink subject to change)
			where customer_engagement not in ('EST_DIRECT_FULFILLMENT', 'I-INK')
			and revenue_units > 0
			group by cal_date, country_alpha2, sales_product_number, customer_engagement, revenue_units
		),
		inventory_adjust_for_customer_engagement as
		(
			select
				inv.cal_date,
				inv.country_alpha2,
				region_5,
				inv.sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd * coalesce(unit_ce_mix, 1)) as inventory_usd,
				sum(inventory_qty * coalesce(unit_ce_mix, 1))  as inventory_qty
			from channel_inventory_without_xcodes2 inv
			left join direct_indirect_mix mix on inv.cal_date = mix.cal_date and inv.country_alpha2 = mix.country_alpha2 
				and inv.sales_product_number = mix.sales_product_number
			group by inv.cal_date, inv.country_alpha2, region_5, inv.sales_product_number, inv.pl, customer_engagement
		)

			select
				cal_date,
				country_alpha2,
				region_5,
				sales_product_number,
				pl,
				case
					when customer_engagement is null and pl != 'GD' then 'TRAD'
					when customer_engagement is null and pl = 'GD' then  'I-INK'
					else customer_engagement
				end as customer_engagement,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(inventory_qty),0)  as inventory_qty
			from inventory_adjust_for_customer_engagement
			group by cal_date, country_alpha2, region_5, sales_product_number, pl, customer_engagement

 """)

cbm_sell_adj5_2.createOrReplaceTempView("inventory_adjust_for_customer_engagement2")

# COMMAND ----------

cbm_sell_adj5_3 = spark.sql("""
 

/** test code block

select * from #inventory_adjust_for_customer_engagement2 where country_alpha2 = 'xw'

select 
	isnull(sum(inventory_usd), 0) as inventory_usd,
	isnull(sum(inventory_qty),0)  as inventory_qty
from #inventory_adjust_for_customer_engagement2
where pl = 'g0'

**/

	with
		combined_channel_inventory as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from inventory_adjust_for_customer_engagement2
			group by cal_date, region_5, country_alpha2, sales_product_number, pl, customer_engagement

			union all

			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_wproxy
			group by cal_date, region_5, country_alpha2, sales_product_number, pl, customer_engagement	
			
			union all

			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				'TRAD' as customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from channel_inventory_prepped1
			where sales_product_number like 'AMS%'
			group by cal_date, region_5, country_alpha2, sales_product_number, pl

		),
		combined_channel_inventory2 as
		(	
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty
			from combined_channel_inventory act
			group by cal_date, region_5, country_alpha2, sales_product_number, pl, customer_engagement
		)
		
		--select * from combined_channel_inventory2
		

			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from combined_channel_inventory2
			group by cal_date, region_5, country_alpha2, sales_product_number, pl, customer_engagement
 """)

cbm_sell_adj5_3.createOrReplaceTempView("cbm_sellthru_xcode_adjusted5")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## CI with Accounting Rates 3

# COMMAND ----------

ci_rates_1 = spark.sql("""
			select
				cal_date,
				act.country_alpha2,
				currency,
				act.region_5,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from cbm_sellthru_xcode_adjusted5  as act
			left join currency cmap 
				on act.country_alpha2 = cmap.country_alpha2
			where 1=1
				and act.region_5 = 'EU'
			group by cal_date, act.country_alpha2, currency, act.region_5, pl, sales_product_number, customer_engagement
""")

ci_rates_1.createOrReplaceTempView("ci_currency_emea")

# COMMAND ----------

ci_rates_2 = spark.sql("""

			select
				cal_date,
				country_alpha2,
				case
					when currency is null then 'USD'
					else currency
				end as currency,
				region_5,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from ci_currency_emea
			group by cal_date, country_alpha2, currency, region_5, pl, sales_product_number, customer_engagement
""")

ci_rates_2.createOrReplaceTempView("ci_currency_emea2")

# COMMAND ----------

edw_fin_s3_bucket= f"s3://dataos-core-{stack}-team-phoenix-fin/"
edw_ships_s3_bucket= f"s3://dataos-core-{stack}-team-phoenix/product/"

# COMMAND ----------

# load parquet files to df
edw_revenue_document_currency_landing = spark.read.parquet(edw_fin_s3_bucket + "EDW/edw_revenue_document_currency_landing")

# COMMAND ----------

# delta tables

import re

tables = [
    ['fin_stage.edw_revenue_document_currency_staging', edw_revenue_document_currency_landing]
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
    
    for column in df.dtypes:
         renamed_column = re.sub('\$', '_dollars', re.sub(' ', '_', column[0])).lower()
         df = df.withColumnRenamed(column[0], renamed_column)
         print(renamed_column) 
        
     # Write the data to its target.
    df.write \
       .format(write_format) \
       .option("overwriteSchema", "true") \
       .mode("overwrite") \
       .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
     # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

ci_rates_3 = spark.sql("""
/** allocate document currency to country for row (apj, ams) **/
-- raw data out of edw report

select 'edw' as record,
	cal.date as cal_date,
	cal.fiscal_year_qtr,
	cal.fiscal_yr,
	plx.pl,
	iso.country_alpha2,
	iso.region_5,
	doc.document_currency_code,
	cast(sum(net_k_dollars) * 1000 as double) as revenue -- at net revenue level but sources does not have hedge, so equivalent to revenue before hedge
from fin_stage.edw_revenue_document_currency_staging doc
left join mdm.calendar cal on fiscal_year_month_code = edw_fiscal_yr_mo
left join mdm.profit_center_code_xref pcx on pcx.profit_center_code = doc.profit_center_code
left join mdm.iso_country_code_xref iso on pcx.country_alpha2 = iso.country_alpha2
left join mdm.product_line_xref plx on plx.plxx = doc.business_area_code
where 1=1
	and cal.day_of_month = 1
	and net_k_dollars <> 0
	and iso.country_alpha2 <> 'XW'
	and doc.document_currency_code <> '?'
group by cal.date, iso.country_alpha2, doc.document_currency_code, plx.pl, iso.region_5, cal.fiscal_year_qtr, cal.fiscal_yr
""")

ci_rates_3.createOrReplaceTempView("edw_document_currency_raw_original")

# COMMAND ----------

ci_rates_4 = spark.sql("""
-- raw data out of odw report

select 'odw' as record,
	cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	pl,
	doc.country_alpha2,
	doc.region_5,
	document_currency_code,
	sum(revenue) as revenue
from fin_stage.odw_document_currency doc
left join mdm.calendar cal on cal.date = doc.cal_date
left join mdm.profit_center_code_xref pcx on doc.segment = pcx.profit_center_code
where 1=1
	and day_of_month = 1
	and revenue <> 0
	and doc.country_alpha2 <> 'XW'
	and document_currency_code <> '?'
group by cal_date, doc.country_alpha2, document_currency_code, pl, doc.region_5, fiscal_year_qtr, fiscal_yr
 """)

ci_rates_4.createOrReplaceTempView("odw_document_currency_raw")

# COMMAND ----------

ci_rates_5 = spark.sql("""
 
with document_currency as
(
select 
    record, 
    cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	pl,
	country_alpha2,
	region_5,
	document_currency_code,
	revenue
from edw_document_currency_raw_original

union all

select
    record,
    cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	pl,
	country_alpha2,
	region_5,
	document_currency_code,
	revenue
from odw_document_currency_raw
)

select 	record,
    cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	pl,
	country_alpha2,
	region_5,
	document_currency_code,
	coalesce(sum(revenue), 0) as revenue
from document_currency
group by record,
    cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	pl,
	country_alpha2,
	region_5,
	document_currency_code
 """)

ci_rates_5.createOrReplaceTempView("edw_document_currency_raw")

# COMMAND ----------

ci_rates_6 = spark.sql("""
-- where is revenue at a country level essentially zero?
select
	cal_date,
	case 
		when pl = 'IX' then 'TX'
		else pl
	end as pl,
	country_alpha2,
	concat(cal_date, country_alpha2, pl) as filter_me,
	sum(revenue) as revenue
from edw_document_currency_raw
where 1=1	
	and country_alpha2 not like 'X%' -- x-codes will be set to usd per finance 
group by cal_date, country_alpha2, pl
""")

ci_rates_6.createOrReplaceTempView("country_revenue")

# COMMAND ----------

 ci_rates_7 = spark.sql("""
 select
	cal_date,
	pl,
	country_alpha2,
	filter_me,
	sum(revenue) as revenue
from country_revenue
where 1=1
	and revenue > 100
	--and (pl <> 'g0' or fiscal_year_qtr not in ('2018q4') or region_5 not in ('na', 'la'))		
	--and (pl <> 'gp' or fiscal_year_qtr not in ('2020q2') or region_5 not in ('la'))
group by cal_date, country_alpha2, pl, filter_me
 """)
    
ci_rates_7.createOrReplaceTempView("above_zero_country_revenue1")

# COMMAND ----------

 ci_rates_8 = spark.sql("""
 -- eliminate corner cases 
select
	cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	case 
		when pl = 'IX' then 'TX'
		else pl
	end as pl,
	country_alpha2,
	region_5,
	document_currency_code,
	sum(revenue) as revenue
from edw_document_currency_raw
where 1=1
	and concat(cal_date, country_alpha2, pl) in (select distinct filter_me from above_zero_country_revenue1)
group by cal_date, country_alpha2, document_currency_code, pl, region_5, fiscal_year_qtr, fiscal_yr
 """)
    
ci_rates_8.createOrReplaceTempView("edw_document_currency1")

# COMMAND ----------

ci_rates_9 = spark.sql("""
-- calculate the currency as a mix of a region


select
	cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	pl,
	region_5,
	document_currency_code,
	concat(cal_date, pl, region_5, document_currency_code) as doc_date,
	sum(revenue) as revenue,
	case
		when sum(revenue) over (partition by cal_date, region_5, pl) = 0 then null
		else revenue / sum(revenue) over (partition by cal_date, region_5, pl)
	end as document_currency_mix1
from edw_document_currency1
group by cal_date, document_currency_code, pl, region_5, fiscal_year_qtr, fiscal_yr, revenue

-- absolute value
""")

ci_rates_9.createOrReplaceTempView("doc_currency_mix1")

# COMMAND ----------

ci_rates_10 = spark.sql("""
 select
	cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	pl,
	region_5,
	document_currency_code,
	doc_date,
	sum(revenue) as revenue,
	abs(sum(document_currency_mix1)) as document_currency_mix1
from doc_currency_mix1
group by cal_date, document_currency_code, pl, region_5, fiscal_year_qtr, fiscal_yr, revenue, doc_date
 """)

ci_rates_10.createOrReplaceTempView("doc_currency_mix2_absval")

# COMMAND ----------

ci_rates_11 = spark.sql("""
 -- exclude outlier currencies

--declare @currency_value_threshold document_currency_mix1 = 0.01

select
	cal_date,
	fiscal_year_qtr,
	fiscal_yr,
	pl,
	region_5,
	document_currency_code,
	doc_date,
	sum(revenue) as revenue
from doc_currency_mix2_absval
where document_currency_mix1 > 0.01
group by cal_date, document_currency_code, pl, region_5, fiscal_year_qtr, fiscal_yr, revenue, doc_date
 """)

ci_rates_11.createOrReplaceTempView("doc_currency_mix3")

# COMMAND ----------

ci_rates_12 = spark.sql("""
 -- edw currency mix excluding emea
select
	cal_date,
	country_alpha2,
	region_5,
	document_currency_code,
	concat(cal_date, pl, region_5, document_currency_code) as doc_date,
	pl,
	sum(revenue) as revenue
from edw_document_currency1
where 1=1
	and region_5 <> 'eu'
group by cal_date, country_alpha2, document_currency_code, pl, revenue, region_5
 """)

ci_rates_12.createOrReplaceTempView("edw_doc_currency_non_eu")

# COMMAND ----------

ci_rates_13 = spark.sql("""
 -- calc currency mix for countries

select
	cal_date,
	country_alpha2,
	region_5,
	document_currency_code,
	pl,
	sum(revenue) as revenue,
	case
		when sum(revenue) over (partition by cal_date, country_alpha2, pl) = 0 then null
		else revenue / sum(revenue) over (partition by cal_date, country_alpha2, pl)
	end as country_proxy_mix
from edw_doc_currency_non_eu
where 1=1
	and doc_date in (select distinct doc_date from doc_currency_mix3)
group by cal_date, country_alpha2, document_currency_code, pl, revenue, region_5
 """)

ci_rates_13.createOrReplaceTempView("country_currency_mix")

# COMMAND ----------

ci_rates_14 = spark.sql("""
 --select * from #country_currency_mix

-- mix only
select
	cal_date,
	country_alpha2,
	document_currency_code as currency,
	region_5,
	pl,
	sum(country_proxy_mix) as country_currency_mix
from country_currency_mix c
where 1=1
group by cal_date, country_alpha2, document_currency_code, pl, region_5
--select * from #country_currency_mix2
 """)

ci_rates_14.createOrReplaceTempView("country_currency_mix2")

# COMMAND ----------

ci_rates_15 = spark.sql("""
 -- build out history from edw for ie2 history
select distinct cal_date
from country_currency_mix2
 """)

ci_rates_15.createOrReplaceTempView("document_currency_time_availability")

# COMMAND ----------

ci_rates_16 = spark.sql("""
 select distinct sales.cal_date
from fin_prod.actuals_supplies_salesprod sales
left join document_currency_time_availability doc on doc.cal_date = sales.cal_date
where doc.cal_date is null
 """)

ci_rates_16.createOrReplaceTempView("currency_history_needed")

# COMMAND ----------

ci_rates_17 = spark.sql("""
 select
	cal_date,
	country_alpha2,
	currency,
	region_5,
	pl,
	sum(country_currency_mix) as country_currency_mix
from country_currency_mix2
where cal_date = (select min(cal_date) from country_currency_mix2)
group by cal_date, country_alpha2, currency, region_5, pl
 """)

ci_rates_17.createOrReplaceTempView("placeholder_mix")

# COMMAND ----------

ci_rates_18 = spark.sql("""
 select history.cal_date,
	country_alpha2,
	currency,
	region_5,
	pl,
	country_currency_mix
from placeholder_mix pm
cross join currency_history_needed history
 """)

ci_rates_18.createOrReplaceTempView("currency_history_needed2")

# COMMAND ----------

ci_rates_19 = spark.sql("""
 with

country_currency_mix3 as
(
select
	cal_date,
	country_alpha2,
	currency,
	region_5,
	pl,
	country_currency_mix
from country_currency_mix2 c

union all

select
	cal_date,
	country_alpha2,
	currency,
	region_5,
	pl,
	country_currency_mix
from currency_history_needed2
)


select
	cal_date,
	country_alpha2,
	currency,
	region_5,
	pl,
	coalesce(sum(country_currency_mix), 0) as country_currency_mix
from country_currency_mix3 c
group by cal_date, country_alpha2, currency, region_5, pl
 """)

ci_rates_19.createOrReplaceTempView("country_currency_mix4")

# COMMAND ----------

ci_rates_20 = spark.sql("""
 select
	cal_date,
	country_alpha2,
	currency,
	region_5,
	pl,
	sum(country_currency_mix) as country_currency_mix
from country_currency_mix4
where country_alpha2 not like 'X%'
group by cal_date, country_alpha2, currency, region_5, pl

--select * from #country_currency_mix5
 """)

ci_rates_20.createOrReplaceTempView("country_currency_mix5")

# COMMAND ----------

ci_rates_21 = spark.sql("""
 -- transition back to adjusted revenue code
			select
				c.cal_date,
				c.country_alpha2,
				currency,
				c.region_5,
				sales_product_number,
				c.pl,
				customer_engagement,
				country_currency_mix,
				sum(inventory_usd * coalesce(country_currency_mix, 1)) as inventory_usd,
				sum(inventory_qty * coalesce(country_currency_mix, 1)) as inventory_qty
			from cbm_sellthru_xcode_adjusted5 c
			left join country_currency_mix5 mix on
				c.cal_date = mix.cal_date and
				c.country_alpha2 = mix.country_alpha2 and
				c.pl = mix.pl
			where c.region_5 <> 'EU'
			group by c.cal_date, c.country_alpha2, currency, c.region_5, c.pl, sales_product_number, customer_engagement
			,country_currency_mix
	
                """)

ci_rates_21.createOrReplaceTempView("ci_before_constant_currency")

# COMMAND ----------

ci_rates_22 = spark.sql("""
 -- transition back to adjusted revenue code

with

		ci_currency_ams_apj as
		(
			select
				c.cal_date,
				c.country_alpha2,
				currency,
				c.region_5,
				sales_product_number,
				c.pl,
				customer_engagement,
				country_currency_mix,
				sum(inventory_usd * coalesce(country_currency_mix, 1)) as inventory_usd,
				sum(inventory_qty * coalesce(country_currency_mix, 1)) as inventory_qty
			from cbm_sellthru_xcode_adjusted5 c
			left join country_currency_mix5 mix on
				c.cal_date = mix.cal_date and
				c.country_alpha2 = mix.country_alpha2 and
				c.pl = mix.pl
			where c.region_5 <> 'EU'
			group by c.cal_date, c.country_alpha2, currency, c.region_5, c.pl, sales_product_number, customer_engagement
			,country_currency_mix
		),
		ci_currency_ams_apj2 as
		(
			select
				cal_date,
				country_alpha2,
				case
					when currency is null then 'USD'
					else currency
				end as currency,
				region_5,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from ci_currency_ams_apj
			group by cal_date, country_alpha2, currency, region_5, pl, sales_product_number, customer_engagement
		),
		inventory_with_currency as
		(
			select
				cal_date,
				country_alpha2,
				currency,
				region_5,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from ci_currency_emea2
			group by cal_date, country_alpha2, currency, region_5, pl, sales_product_number, customer_engagement

			union all

			select
				cal_date,
				country_alpha2,
				currency,
				region_5,
				sales_product_number,
				pl,
				customer_engagement,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from ci_currency_ams_apj2
			group by cal_date, country_alpha2, currency, region_5, pl, sales_product_number, customer_engagement
		),
		inventory_with_currency2 as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,	
				case
					when currency is null then 'USD'
					when currency = 'VES' and cal_date < '{inv_curr2}' then 'VEF' 
					when country_alpha2 = 'BR' 
                       then 'USD'
					when sales_product_number like '%AMS%' then 'USD'
					when sales_product_number like '%PROXY%' then 'USD'
					else currency
				end as currency,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from inventory_with_currency
            where pl in (
              select pl 
                from mdm.product_line_xref
                where 1=1
                    and business_division = 'OPS'
                    and pl_category = 'SUP'
                    and l6_description not like '%OEM%'
                )
			group by cal_date, country_alpha2, region_5, sales_product_number, pl, customer_engagement, currency
          
            UNION ALL 
            
            select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,	
				case
					when currency is null then 'USD'
					when currency = 'VES' and cal_date < '{inv_curr2}' then 'VEF' 
					when sales_product_number like '%AMS%' then 'USD'
					when sales_product_number like '%PROXY%' then 'USD'
					else currency
				end as currency,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from inventory_with_currency
            where pl not in (
              select pl 
                from mdm.product_line_xref
                where 1=1
                    and business_division = 'OPS'
                    and pl_category = 'SUP'
                    and l6_description not like '%OEM%'
                )
			group by cal_date, country_alpha2, region_5, sales_product_number, pl, customer_engagement, currency
		)
		
			select
				cal_date,
				region_5,
				country_alpha2,				
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				sum(inventory_usd) as inventory_usd,
				sum(inventory_qty) as inventory_qty
			from inventory_with_currency2			
			group by cal_date, country_alpha2, sales_product_number, pl, customer_engagement, currency, region_5
 """)

ci_rates_22.createOrReplaceTempView("ci_before_constant_currency")

# COMMAND ----------

ci_rates_23 = spark.sql("""
 with

		date_helper as
		(
			select
				date_key, 
                date as cal_date
			from mdm.calendar
			where day_of_month = 1
		),
		channel_inventory_analytic_setup as
		(
			select 
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				inventory_usd,
				inventory_qty,
					(select min(cal_date) from cbm_sellthru_xcode_adjusted5) as min_cal_date,
					(select max(cal_date) from cbm_sellthru_xcode_adjusted5) as max_cal_date
					--min(cal_date) over (partition by country_alpha2, sales_product_number, pl, customer_engagement, currency 
						--order by sales_product_number) as min_cal_date,
					--max(cal_date) over (partition by country_alpha2, sales_product_number, pl, customer_engagement, currency 
						--order by sales_product_number) as max_cal_date
			from ci_before_constant_currency
		),
		channel_inventory_full_calendar_cross_join as
		(
			select distinct d.cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency
			from date_helper d
			cross join channel_inventory_analytic_setup ci
			where d.cal_date between ci.min_cal_date and ci.max_cal_date				
		),
		fill_gap1 as
		(
			select
				c.cal_date,
				coalesce(c.region_5, s.region_5) as region_5,
				coalesce(c.country_alpha2, s.country_alpha2) as country_alpha2,
				coalesce(c.sales_product_number, s.sales_product_number) as sales_product_number,
				coalesce(c.pl, s.pl) as pl,
				coalesce(c.customer_engagement, s.customer_engagement) as customer_engagement,
				coalesce(c.currency, s.currency) as currency,
				inventory_usd,
				inventory_qty
			from channel_inventory_full_calendar_cross_join as c
			left join channel_inventory_analytic_setup as s
				on c.cal_date = s.cal_date
				and c.country_alpha2 = s.country_alpha2
				and c.region_5 = s.region_5
				and c.sales_product_number = s.sales_product_number
				and c.pl = s.pl
				and c.customer_engagement = s.customer_engagement
				and c.currency = s.currency
		),
		fill_gap2 as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				inventory_usd,
				inventory_qty,
					min(cal_date) over (partition by country_alpha2, sales_product_number, 
						pl, customer_engagement, currency order by cal_date) as min_cal_date
			from fill_gap1
		),
		channel_inventory_time_series as
		(
			select
				cal_date,
				min_cal_date,
					region_5,
					country_alpha2,
					sales_product_number,
					pl,
					customer_engagement,
					currency,
					inventory_usd,
					inventory_qty,
					case when min_cal_date < cast(current_date() - day(current_date()) + 1 as date) then 1 else 0 end as actuals_flag,
					count(cal_date) over (partition by country_alpha2, sales_product_number, pl, customer_engagement, currency
	                        order by cal_date rows between unbounded preceding and current row) as running_count
				from fill_gap2
		)

			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				inventory_usd,
				inventory_qty
			from channel_inventory_time_series
 """)

ci_rates_23.createOrReplaceTempView("channel_inventory_full_calendar")

# COMMAND ----------

ci_rates_24 = spark.sql("""
 with
		
		data_prep as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty
			from channel_inventory_full_calendar
			group by cal_date, region_5, country_alpha2, sales_product_number, pl, customer_engagement, currency	
		),
		lagged_data as
		(
			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty,
				(lag(coalesce(sum(inventory_usd), 0)) over
					(partition by sales_product_number, country_alpha2, customer_engagement, currency order by cal_date)) as prev_inv_usd,
				(lag(coalesce(sum(inventory_qty), 0)) over
					(partition by sales_product_number, country_alpha2, customer_engagement, currency order by cal_date)) as prev_inv_qty
			from data_prep
			group by cal_date, country_alpha2, region_5, sales_product_number, pl, customer_engagement, currency
		)

			select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(prev_inv_usd), 0) as prev_inv_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty,
				coalesce(sum(prev_inv_qty), 0) as prev_inv_qty
			from lagged_data
			group by cal_date, country_alpha2, region_5, sales_product_number, pl, customer_engagement, currency
 """)

ci_rates_24.createOrReplaceTempView("lagged_data_cte3")

# COMMAND ----------

ci_rates_25 = spark.sql("""
 select
				cal_date,
				region_5,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty, 
				sum(inventory_qty) - sum(prev_inv_qty) as monthly_unit_change,
				sum(inventory_usd) - sum(prev_inv_usd) as monthly_usd_change,
				coalesce(sum(inventory_usd) / nullif(sum(inventory_qty), 0), 0)  as reported_inventory_valuation_rate,
				coalesce(sum(prev_inv_usd) / nullif(sum(prev_inv_qty), 0), 0)  as previous_inventory_valuation_rate
			from lagged_data_cte3
			group by cal_date, country_alpha2, region_5, sales_product_number, pl, customer_engagement, currency
 """)

ci_rates_25.createOrReplaceTempView("ci_calc_unit_change")

# COMMAND ----------

ci_rates_26 = spark.sql("""
 select
				cal_date,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				coalesce(sum(prev_inv_qty), 0) as prev_inv_qty,
				coalesce(sum(monthly_unit_change), 0) as monthly_unit_change,
				coalesce(sum(monthly_usd_change), 0) as monthly_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,
				sum(previous_inventory_valuation_rate) as previous_inventory_valuation_rate
			from ci_calc_unit_change
			group by cal_date, country_alpha2, sales_product_number, pl, customer_engagement, currency
 """)

ci_rates_26.createOrReplaceTempView("ci_calc_unit_change2")

# COMMAND ----------

ci_rates_27 = spark.sql("""
 select
				cal_date,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(prev_inv_usd), 0) as prev_inv_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty,
				coalesce(sum(prev_inv_qty), 0) as prev_inv_qty,
				coalesce(sum(monthly_unit_change), 0) as monthly_unit_change,
				coalesce(sum(monthly_usd_change), 0) as monthly_usd_change,
				coalesce(sum(reported_inventory_valuation_rate), 0) as reported_inventory_valuation_rate,
				case
					when sales_product_number like 'PROXY_ADJUSTMENT%' then sum(monthly_usd_change)
					when sales_product_number like 'AMS%' then sum(monthly_usd_change)
					when reported_inventory_valuation_rate = 0 then sum(previous_inventory_valuation_rate * monthly_unit_change)
					else sum(reported_inventory_valuation_rate * monthly_unit_change) 
				end as inventory_change_impact
			from ci_calc_unit_change2
			group by cal_date, country_alpha2, sales_product_number, pl, customer_engagement, currency, reported_inventory_valuation_rate
 """)

ci_rates_27.createOrReplaceTempView("ci_calc_unit_change3")

# COMMAND ----------

ci_rates_28 = spark.sql("""
 select
				cal_date,
				ci.country_alpha2,
				region_5, 
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(prev_inv_usd), 0) as prev_inv_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty,
				coalesce(sum(prev_inv_qty), 0) as prev_inv_qty,
				coalesce(sum(monthly_unit_change), 0) as monthly_unit_change,
				coalesce(sum(monthly_usd_change), 0) as monthly_usd_change,
				coalesce(sum(reported_inventory_valuation_rate), 0) as reported_inventory_valuation_rate,
				coalesce(sum(inventory_change_impact), 0) as inventory_change_impact
			from ci_calc_unit_change3 ci
			left join mdm.iso_country_code_xref iso on ci.country_alpha2 = iso.country_alpha2
			group by cal_date, ci.country_alpha2, sales_product_number, pl, customer_engagement, region_5, currency
 """)

ci_rates_28.createOrReplaceTempView("ci_calc_unit_change4")

# COMMAND ----------

ci_rates_29 = spark.sql("""
 select
				act.cal_date,
				region_5,
				country_alpha2,				
				sales_product_number,
				pl,
				customer_engagement,
				act.currency,
				coalesce(sum(rate.accountingrate), 1) as original_rate,
				coalesce(sum(curr_rate.accountingrate), 1) as current_rate,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty,
				sum(monthly_unit_change) as monthly_unit_change,
				sum(monthly_usd_change) as monthly_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,
				sum(inventory_change_impact) as inventory_change_impact
			from ci_calc_unit_change4 act				
			left join current_accounting_rate curr_rate
				on curr_rate.isocurrcd = act.currency
			left join original_accounting_rate rate
				on act.currency = rate.isocurrcd and act.cal_date = rate.effectivedate
			group by act.cal_date, country_alpha2, sales_product_number, pl, 
				customer_engagement, act.currency, curr_rate.accountingrate, rate.accountingrate, region_5		
 """)

ci_rates_29.createOrReplaceTempView("ci_with_accounting_rates")

# COMMAND ----------

ci_rates_30 = spark.sql("""
 select
				cal_date,
				region_5,
				country_alpha2,				
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				sum(original_rate) as original_rate,
				case
					when currency = 'VEF' 
					then 
					(
						select accountingrate 
						from prod.acct_rates 
						where isocurrcd = 'VEF'
							and effectivedate = 
							(
							select max(effectivedate) from prod.acct_rates where isocurrcd = 'VEF' and accountingrate != 0
							)
					)
					else sum(current_rate) 
				end as current_rate,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty,
				sum(monthly_unit_change) as monthly_unit_change,
				sum(monthly_usd_change) as monthly_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,
				sum(inventory_change_impact) as inventory_change_impact
			from ci_with_accounting_rates	
			group by cal_date, country_alpha2, sales_product_number, pl, 
				customer_engagement, currency, original_rate, current_rate, region_5
 """)

ci_rates_30.createOrReplaceTempView("ci_with_accounting_rates2")

# COMMAND ----------

ci_rates_31 = spark.sql("""
 select
				cal_date,
				region_5,
				country_alpha2,				
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty,
				sum(monthly_unit_change) as monthly_unit_change,
				sum(monthly_usd_change) as monthly_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,
				sum(inventory_change_impact) as inventory_change_impact,				
				-- usd is denominator of all accounting rates
				sum(original_rate) as original_rate,
				sum(current_rate) as current_rate,
				coalesce((nullif(original_rate, 0) / nullif(current_rate, 0)), 1) as currency_rate_adjustment,
				coalesce((nullif(original_rate, 0) / nullif(current_rate, 0)), 1) * sum(inventory_change_impact) as cc_inventory_impact
			from ci_with_accounting_rates2	
			group by cal_date, country_alpha2, sales_product_number, pl, 
				customer_engagement, currency, original_rate, current_rate, region_5
 """)

ci_rates_31.createOrReplaceTempView("ci_with_accounting_rates3")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adjusted Revenue

# COMMAND ----------

adj_rev_1 = spark.sql("""
with ccrev_plus_ccci as
	(
			select
				cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(net_revenue_before_hedge) as net_revenue_before_hedge,
				sum(revenue_units) as revenue_units,	
				original_rate,
				current_rate,
				currency_rate_adjustment,
				sum(cc_net_revenue) as cc_net_revenue,
				0 as inventory_usd,
				0 as prev_inv_usd,
				0 as inventory_qty,
				0 as prev_inv_qty,
				0 as monthly_unit_change,
				0 as monthly_usd_change,
				0 as reported_inventory_valuation_rate,
				0 as inventory_change_impact,
				0 as cc_inventory_impact
			from supplies_history_constant_currency
			group by 
				cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,	
				original_rate,
				current_rate,
				currency_rate_adjustment

		union all

		select
				cal_date,
				region_5,
				country_alpha2,	
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				0 as gross_revenue,
				0 as net_currency,
				0 as contractual_discounts,
				0 as discretionary_discounts,
				0 as net_revenue,
				0 as total_cos,
				0 as gross_profit,
				0 as net_revenue_before_hedge,
				0 as revenue_units,
				original_rate,
				current_rate,
				currency_rate_adjustment,
				0 as cc_net_revenue,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty,
				sum(monthly_unit_change) as monthly_unit_change,
				sum(monthly_usd_change) as monthly_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,
				sum(inventory_change_impact) as inventory_change_impact,
				sum(cc_inventory_impact) as cc_inventory_impact
			from ci_with_accounting_rates3
			group by cal_date,
				region_5,
				country_alpha2,	
				currency,
				sales_product_number,
				pl,
				customer_engagement,	
				original_rate,
				current_rate,
				currency_rate_adjustment
	)

			select 
				cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				original_rate,
				current_rate,
				currency_rate_adjustment,
				coalesce(sum(gross_revenue), 0) as gross_revenue,
				coalesce(sum(net_currency),0) as net_currency,
				coalesce(sum(contractual_discounts), 0) as contractual_discounts,
				coalesce(sum(discretionary_discounts), 0) as discretionary_discounts,
				coalesce(sum(net_revenue), 0) as net_revenue,
				coalesce(sum(total_cos), 0) as total_cos,
				coalesce(sum(gross_profit), 0) as gross_profit,			
				coalesce(sum(net_revenue_before_hedge), 0) as net_revenue_before_hedge,
				coalesce(sum(revenue_units), 0) as revenue_units,
				coalesce(sum(cc_net_revenue), 0) as cc_net_revenue,
				coalesce(sum(inventory_usd), 0) as inventory_usd,
				coalesce(sum(prev_inv_usd), 0) as prev_inv_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty,
				coalesce(sum(prev_inv_qty), 0) as prev_inv_qty,
				coalesce(sum(monthly_unit_change), 0) as monthly_unit_change,
				coalesce(sum(monthly_usd_change), 0) as monthly_inv_usd_change,
				coalesce(sum(reported_inventory_valuation_rate), 0) as reported_inventory_valuation_rate,
				coalesce(sum(inventory_change_impact), 0) as inventory_change_impact,
				coalesce(sum(cc_inventory_impact), 0) as cc_inventory_impact
			from ccrev_plus_ccci
			group by cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				original_rate,
				current_rate,
				currency_rate_adjustment

""")

adj_rev_1.createOrReplaceTempView("pre_adjusted_revenue")

# COMMAND ----------

adj_rev_2 = spark.sql("""
 select
				cal_date,
				country_alpha2,
				sales_product_number,
				pl,
				customer_engagement,
				currency,
				sum(original_rate) as original_rate_usd,
				sum(current_rate) as current_rate_usd,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(revenue_units) as revenue_units,				
				sum(net_revenue_before_hedge) as net_revenue_before_hedge,
				sum(currency_rate_adjustment) as currency_rate_adjustment,
				sum(cc_net_revenue) - sum(net_revenue) as currency_impact,
				sum(cc_net_revenue) as cc_net_revenue,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty,
				sum(monthly_unit_change) as monthly_unit_change,
				sum(monthly_inv_usd_change) as monthly_inv_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,
				coalesce(sum(inventory_change_impact), 0) as inventory_change_impact,
				coalesce(sum(cc_inventory_impact), 0) as cc_inventory_impact,
				sum(cc_net_revenue) - coalesce(sum(cc_inventory_impact), 0) as adjusted_revenue 
			from pre_adjusted_revenue
			group by cal_date, country_alpha2, sales_product_number, pl, customer_engagement, currency
 """)

adj_rev_2.createOrReplaceTempView("adjusted_revenue")

# COMMAND ----------

adj_rev_3 = spark.sql("""
 select
				cal_date,
				ar.country_alpha2,
				country,
				market8 as geography,
                'MARKET8' as geography_grain,
				region_5,
				sales_product_number,
				ar.pl,
				l5_description,
				customer_engagement,
				currency,
				sum(original_rate_usd) as original_rate_usd,
				sum(current_rate_usd) as current_rate_usd,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(revenue_units) as revenue_units,				
				sum(net_revenue_before_hedge) as net_revenue_before_hedge,
				sum(currency_rate_adjustment) as currency_rate_adjustment,
				sum(currency_impact) as currency_impact,
				sum(cc_net_revenue) as cc_net_revenue,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty,
				sum(monthly_unit_change) as monthly_unit_change,
				sum(monthly_inv_usd_change) as monthly_inv_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,
				sum(inventory_change_impact) as inventory_change_impact,
				sum(cc_inventory_impact) - sum(inventory_change_impact) as currency_impact_ch_inventory,
				sum(cc_inventory_impact) as cc_inventory_impact,
				sum(adjusted_revenue) as adjusted_revenue
			from adjusted_revenue ar
			join mdm.iso_country_code_xref iso on ar.country_alpha2 = iso.country_alpha2
			join mdm.product_line_xref plx on plx.pl = ar.pl
            where market8 is not null
			group by cal_date, ar.country_alpha2, sales_product_number, ar.pl, customer_engagement, 
			currency, market8, region_5, country, l5_description
 """)

adj_rev_3.createOrReplaceTempView("adjusted_revenue2")

# COMMAND ----------

adj_rev_4 = spark.sql("""
 select
				cal_date,
				country_alpha2,
				country,
				geography,
                geography_grain,
				region_5,
				sales_product_number,
				pl,
				l5_description,
				case
					when pl = 'GD' then 'I-INK'
					else customer_engagement
				end as customer_engagement,
				currency,
				sum(original_rate_usd) as original_rate_usd,
				sum(current_rate_usd) as current_rate_usd,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(revenue_units) as revenue_units,				
				sum(net_revenue_before_hedge) as net_revenue_before_hedge,
				sum(currency_rate_adjustment) as currency_rate_adjustment,
				sum(currency_impact) as currency_impact,
				sum(cc_net_revenue) as cc_net_revenue,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty,
				sum(monthly_unit_change) as monthly_unit_change,
				sum(monthly_inv_usd_change) as monthly_inv_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,				
				coalesce(sum(prev_inv_usd) / nullif(sum(prev_inv_qty), 0), 0)  as previous_inventory_valuation_rate,
				--case
				--	when cal_date in ('2021-11-01', '2021-12-01', '2022-01-01') then sum(monthly_inv_usd_change)
				--	else sum(inventory_change_impact)
				--end as inventory_change_impact,
				sum(inventory_change_impact) as inventory_change_impact,
				sum(currency_impact_ch_inventory) as currency_impact_ch_inventory,
				sum(cc_inventory_impact) as cc_inventory_impact,
				sum(adjusted_revenue) as adjusted_revenue 
			from adjusted_revenue2
			group by cal_date, country_alpha2, sales_product_number, pl, customer_engagement, 
			currency, geography, geography_grain, region_5, country, l5_description
 """)

adj_rev_4.createOrReplaceTempView("adjusted_revenue3")

# COMMAND ----------

adj_rev_5 = spark.sql("""
 select
				cal_date,
				country_alpha2,
				country,
				geography,
                geography_grain,
				region_5,
				sales_product_number,
				pl,
				l5_description,
				customer_engagement,
				currency,
				sum(original_rate_usd) as original_rate_usd,
				sum(current_rate_usd) as current_rate_usd,
				sum(gross_revenue) as gross_revenue,
				sum(net_currency) as net_currency,
				sum(contractual_discounts) as contractual_discounts,
				sum(discretionary_discounts) as discretionary_discounts,
				sum(net_revenue) as net_revenue,
				sum(total_cos) as total_cos,
				sum(gross_profit) as gross_profit,
				sum(revenue_units) as revenue_units,				
				sum(net_revenue_before_hedge) as net_revenue_before_hedge,
				sum(currency_rate_adjustment) as currency_rate_adjustment,
				sum(currency_impact) as currency_impact,
				sum(cc_net_revenue) as cc_net_revenue,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				sum(inventory_qty) as inventory_qty,
				sum(prev_inv_qty) as prev_inv_qty,
				sum(monthly_unit_change) as monthly_unit_change,
				sum(monthly_inv_usd_change) as monthly_inv_usd_change,
				sum(reported_inventory_valuation_rate) as reported_inventory_valuation_rate,
				coalesce(sum(previous_inventory_valuation_rate), 0) as previous_inventory_valuation_rate,

				case
					when sales_product_number like '%PROXY%' then 0
					else sum(reported_inventory_valuation_rate) - coalesce(sum(previous_inventory_valuation_rate),0) 
				end as period_price_change,

				case 
					when sales_product_number like '%PROXY%' then 0
					when sales_product_number like 'AMS%' then 0
					--when reported_inventory_valuation_rate = 0 then 0
					--else (sum(reported_inventory_valuation_rate) - isnull(sum(previous_inventory_valuation_rate), 0)) * sum(prev_inv_qty)
					else sum(monthly_inv_usd_change) - sum(inventory_change_impact)
				end as price_change_x_prev_inv_qty,

				case 
					when sales_product_number like '%PROXY%' then 0
					when sales_product_number like 'AMS%' then 0
					when sum(reported_inventory_valuation_rate) = 0 then 0
					else (sum(reported_inventory_valuation_rate) - coalesce(sum(previous_inventory_valuation_rate), 0)) * sum(prev_inv_qty)
				end as price_change_check,

				sum(reported_inventory_valuation_rate) - coalesce(sum(previous_inventory_valuation_rate),0) as period_price_change2,

				(sum(reported_inventory_valuation_rate) - coalesce(sum(previous_inventory_valuation_rate), 0)) * sum(prev_inv_qty) as price_change_x_prev_inv_qty2,

				sum(inventory_change_impact) as inventory_change_impact,

				case
					when sales_product_number like '%PROXY%' then 0
					else coalesce(sum(previous_inventory_valuation_rate), 0) * sum(monthly_unit_change) 
				end as monthly_unit_change_dollar_impact,

				case
					when sales_product_number not like '%PROXY%' then 0
					else sum(monthly_inv_usd_change)
				end as proxy_change,

				sum(currency_impact_ch_inventory) as currency_impact_ch_inventory,

				sum(inventory_change_impact) + sum(currency_impact_ch_inventory) as cc_inventory_impact,

				sum(cc_net_revenue) - (sum(inventory_change_impact) + sum(currency_impact_ch_inventory)) as adjusted_revenue 
			from adjusted_revenue3
			group by cal_date, country_alpha2, sales_product_number, pl, customer_engagement, 
			currency, geography, geography_grain, region_5, country, l5_description

 """)

adj_rev_5.createOrReplaceTempView("adjusted_revenue4")

# COMMAND ----------

adj_rev_6 = spark.sql("""
			select
				'ACTUALS - ADJUSTED_REVENUE - SALES PRODUCT' as record,
				cal_date,
				country_alpha2,
				country,
				geography,
                geography_grain,
				region_5,
				sales_product_number,
				pl,
				l5_description,
				customer_engagement,
				currency,
				(select distinct accounting_rate from current_accounting_rate) as accounting_rate,
				sum(net_revenue_before_hedge) as net_revenue_before_hedge,
				case
					when net_currency = 1 then 0
					else sum(net_currency) 
				end as net_hedge_benefit,
				sum(net_revenue) as net_revenue,
				sum(original_rate_usd) as original_rate_usd,
				sum(current_rate_usd) as current_rate_usd,
				coalesce(sum(currency_rate_adjustment),0) as currency_rate_adjustment,
				sum(currency_impact) as currency_impact,
				sum(cc_net_revenue) as cc_net_revenue,
				sum(inventory_usd) as inventory_usd,
				sum(prev_inv_usd) as prev_inv_usd,
				coalesce(sum(inventory_qty), 0) as inventory_qty,
				coalesce(sum(prev_inv_qty), 0) as prev_inventory_qty,
				coalesce(sum(monthly_unit_change), 0) as monthly_unit_change,
				coalesce(sum(reported_inventory_valuation_rate), 0) as reported_inventory_valuation_rate,
				coalesce(sum(inventory_change_impact), 0) as inventory_change_impact,
				coalesce(sum(currency_impact_ch_inventory), 0) as currency_impact_ch_inventory,
				coalesce(sum(cc_inventory_impact), 0) as cc_inventory_impact,
				sum(adjusted_revenue) as adjusted_revenue,
				CAST(1 AS BOOLEAN) as official,
				(select load_date from prod.version where record = 'ACTUALS - ADJUSTED_REVENUE - SALES PRODUCT' 
					and load_date = (select max(load_date) from prod.version where record = 'ACTUALS - ADJUSTED_REVENUE - SALES PRODUCT')) as load_date,
				(select version from prod.version where record = 'ACTUALS - ADJUSTED_REVENUE - SALES PRODUCT'
					and version = (select max(version) from prod.version where record = 'ACTUALS - ADJUSTED_REVENUE - SALES PRODUCT')) as version,						
				coalesce(sum(monthly_inv_usd_change), 0) as monthly_inv_usd_change,
				coalesce(sum(previous_inventory_valuation_rate), 0) as previous_inventory_valuation_rate,
				coalesce(sum(period_price_change), 0) as implied_period_price_change,
				coalesce(sum(price_change_x_prev_inv_qty), 0) as implied_price_change_x_prev_inv_qty,
				coalesce(sum(monthly_unit_change_dollar_impact), 0) as monthly_unit_change_dollar_impact,
				coalesce(sum(proxy_change), 0) as proxy_change,
				coalesce(sum(monthly_inv_usd_change),0) - coalesce(sum(proxy_change), 0) - coalesce(sum(price_change_x_prev_inv_qty), 0) 
					- coalesce(sum(monthly_unit_change_dollar_impact), 0) as monthly_mix_change,
				coalesce(sum(monthly_inv_usd_change),0) - coalesce(sum(price_change_x_prev_inv_qty), 0) as alt_inventory_change_calc,
				coalesce(sum(inventory_change_impact), 0) - (coalesce(sum(monthly_inv_usd_change), 0) - coalesce(sum(price_change_x_prev_inv_qty), 0)) as inv_chg_calc_variance
			from adjusted_revenue4
			group by cal_date, country_alpha2, country, sales_product_number, pl, customer_engagement, 
			currency, geography, geography_grain, region_5, l5_description, net_currency

 """)

adj_rev_6.createOrReplaceTempView("adjusted_revenue_staging")
query_list.append(["fin_stage.adjusted_revenue_staging", adj_rev_6, "overwrite"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adjusted Revenue Salesprod

# COMMAND ----------

adj_rev_sales = spark.sql("""
select  record
      ,cal_date
      ,country_alpha2
      ,country
      ,geography
      ,geography_grain
      ,region_5
      ,sales_product_number
      ,pl
      ,l5_description
      ,customer_engagement
      ,currency
      ,net_revenue_before_hedge
      ,net_hedge_benefit
      ,net_revenue
      ,original_rate_usd
      ,current_rate_usd
      ,currency_rate_adjustment
      ,currency_impact
      ,cc_net_revenue
      ,inventory_usd
      ,prev_inv_usd
      ,inventory_qty
      ,prev_inventory_qty as prev_inv_qty
      ,monthly_unit_change
      ,reported_inventory_valuation_rate
      ,inventory_change_impact
      ,currency_impact_ch_inventory
      ,cc_inventory_impact
      ,adjusted_revenue
      ,official
      ,load_date
      ,version
      ,accounting_rate
	  ,monthly_inv_usd_change
      ,previous_inventory_valuation_rate
      ,implied_period_price_change
      ,implied_price_change_x_prev_inv_qty
	  ,monthly_unit_change_dollar_impact
	  ,proxy_change
	  ,monthly_mix_change
	  ,alt_inventory_change_calc
	  ,inv_chg_calc_variance
  from adjusted_revenue_staging
""")

query_list.append(["fin_prod.adjusted_revenue_salesprod", adj_rev_sales, "append"])

# COMMAND ----------

for t_name, df, mode in query_list:
    write_df_to_redshift(configs, df, t_name, mode)

# COMMAND ----------

tables = [
    ['fin_stage.adjusted_revenue_staging', adj_rev_6, 'overwrite'],
    ['fin_prod.adjusted_revenue_salesprod', adj_rev_sales, 'overwrite'],
    ['fin_stage.channel_inventory_prepped_ams_unadjusted', chann_inv_ams, 'overwrite'],
    ['fin_stage.adj_rev_supplies_history_constant_currency', supp_hist_5, 'overwrite'],
]

for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    mode = table[2]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = table[1]
    renamed_df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
    print(f'loading {table[0]}...')
    # Write the data to its target.
    renamed_df.write \
      .format(write_format) \
      .mode(mode) \
      .option("mergeSchema", "true")\
      .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')
