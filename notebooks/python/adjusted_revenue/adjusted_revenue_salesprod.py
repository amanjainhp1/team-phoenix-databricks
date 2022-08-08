# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Adjusted Revenue
# MAGIC 
# MAGIC  - Note: supp_history_3 has a variable date
# MAGIC  
# MAGIC  Where are these tables?:
# MAGIC  list_price_EU_CountryList (supp_hist_2)
# MAGIC  country_currency_map_staging -> mdm.country_currency_map?

# COMMAND ----------

## Global Variables
query_list = []

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Supplies History Constant Currency

# COMMAND ----------

supp_hist_1 = spark.sql("""
 -- start adjusted revenue computations
 -- bring in sales product actual information; select only the supplies PLs included in adjusted revenue reporting, i.e., just supplies
 -- bring in entire data set (i.e., all history) because all history restates to the current accounting rate
	WITH
		salesprod_raw AS
		(
			SELECT
				cal_date,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(net_revenue) AS net_revenue,
				SUM(total_cos) AS total_cos,
				SUM(gross_profit) AS gross_profit,
				SUM(revenue_units) AS revenue_units
			FROM fin_prod.actuals_supplies_salesprod s
			GROUP BY cal_date, country_alpha2, sales_product_number, pl, customer_engagement, currency
		),
		salesprod_actuals AS
		(
			SELECT cal_date,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(net_revenue) AS net_revenue,
				SUM(total_cos) AS total_cos,
				SUM(gross_profit) AS gross_profit,
				SUM(revenue_units) AS revenue_units
			FROM salesprod_raw
			WHERE pl IN
				(
					SELECT distinct pl
					FROM mdm.product_line_xref
					WHERE PL_category IN ('SUP')
						AND Technology IN ('Ink', 'Laser', 'PWA', 'LF')
						AND pl NOT IN ('GY', 'LZ') -- not included in the business drivers models
				)
			GROUP BY cal_date, country_alpha2, sales_product_number, pl, customer_engagement, currency
		)

			SELECT
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
			FROM salesprod_actuals
""")

supp_hist_1.createOrReplaceTempView("sales_prod_actuals")

# COMMAND ----------

supp_hist_2 = """
-- redundant code block from sales product sproc but needed for channel inventory that doesn't map to financial actuals
	WITH

		emea_currency_table AS
		(
			SELECT
				CASE
					WHEN country = 'Afghanistan' THEN 'AP'
					ELSE region_5
				END AS region_5,
				country_alpha2,
				country,
				CASE
					WHEN currency = 'Euro' THEN 'EUR'
					ELSE 'USD'
				END AS currency
			FROM IE2_Prod.dbo.list_price_EU_CountryList
			WHERE country_alpha2 NOT IN ('ZM', 'ZW')
		),
		row_currency_table AS
		(
			SELECT
				region_5,
				cmap.country_alpha2,
				cmap.country,
				currency_iso_code AS currency
			FROM mdm.country_currency_map cmap
			LEFT JOIN mdm.iso_country_code_xref iso ON cmap.country_alpha2 = iso.country_alpha2 AND cmap.country = iso.country
			WHERE cmap.country_alpha2 NOT IN (
				SELECT DISTINCT country_alpha2
				FROM emea_currency_table
				)
		),
		ALL_currencies AS
		(
			SELECT *
			FROM emea_currency_table
			UNION ALL
			SELECT *
			FROM row_currency_table
		)

			SELECT distinct
				region_5,
				country_alpha2,
				country,
				CASE
					WHEN currency IN ('ZMW', 'ZWD') THEN 'USD'
					ELSE currency
				END AS currency
			FROM ALL_currencies
"""

supp_hist_2.createOrReplaceTempView("currency")

# COMMAND ----------

supp_hist_3 = """
-- accounting rates

	--DECLARE @current_period Date = (SELECT MAX(EffectiveDate) AS current_period FROM IE2_Prod.dbo.acct_rates);
	DECLARE @current_period Date = (SELECT distinct '2022-07-01' AS current_period FROM prod.acct_rates); -- scenario; be careful of FY changes

	WITH

		accounting_rates_table AS
		(
			SELECT
				EffectiveDate,
				AccountingRate,
				IsoCurrCd
			FROM prod.acct_rates
			WHERE AccountingRate != 0
		)
			SELECT
				DISTINCT(IsoCurrCd),
				EffectiveDate as accounting_rate,
				AccountingRate
			FROM accounting_rates_table
			WHERE EffectiveDate = @current_period
"""

supp_hist_3.createOrReplaceTempView("current_accounting_rate")

# COMMAND ----------

supp_hist_4 = """
-- select * from #current_accounting_rate
	WITH

		accounting_rates_table AS
		(
			SELECT
				EffectiveDate,
				AccountingRate,
				IsoCurrCd
			FROM prod.acct_rates
			WHERE AccountingRate != 0
		)

			SELECT
				DISTINCT(IsoCurrCd),
				EffectiveDate,
				AccountingRate
			FROM accounting_rates_table
"""

supp_hist_4.createOrReplaceTempView("original_accounting_rate")

# COMMAND ----------

supp_hist_5 = """
-- select * from #original_accounting_rate
-- apply accounting rates to the financial actuals to determine currency adjustments to those actuals

	WITH

		salesprod_actuals_currency AS
		(
			SELECT cal_date,
				act.country_alpha2,
				iso.region_5,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(net_revenue) AS net_revenue,
				SUM(total_cos) AS total_cos,
				SUM(gross_profit) AS gross_profit,
				SUM(revenue_units) AS revenue_units
			FROM #salesproduct_actuals act
			LEFT JOIN mdm.iso_country_code_xref iso
				ON act.country_alpha2 = iso.country_alpha2
			GROUP BY cal_date, act.country_alpha2, iso.region_5, currency, sales_product_number, pl, customer_engagement
		),
		actuals_join_acct_rates AS
		(
			SELECT
				act.cal_date,
				region_5,
				country_alpha2,
				act.currency,
				sales_product_number,
				pl,
				customer_engagement,
				ISNULL(SUM(rate.AccountingRate), 1) AS original_rate,
				ISNULL(SUM(curr_rate.AccountingRate), 1) AS current_rate,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(net_revenue) AS net_revenue,
				SUM(total_cos) AS total_cos,
				SUM(gross_profit) AS gross_profit,
				SUM(gross_revenue) - SUM(contractual_discounts) - SUM(discretionary_discounts) AS net_revenue_before_hedge,
				SUM(revenue_units) AS revenue_units
			FROM salesprod_actuals_currency act
			LEFT JOIN #current_accounting_rate curr_rate
				ON curr_rate.IsoCurrCd = act.currency
			LEFT JOIN #original_accounting_rate rate
				ON act.currency = rate.IsoCurrCd AND act.cal_date = rate.EffectiveDate
			GROUP BY act.cal_date, country_alpha2, region_5, act.currency, sales_product_number, pl, customer_engagement, curr_rate.AccountingRate, rate.AccountingRate
		),
		actuals_join_acct_rates2 AS
		(
			SELECT
				cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				SUM(original_rate) AS original_rate,
				CASE
					WHEN currency = 'VEF'
					THEN
					(
						SELECT AccountingRate
						FROM IE2_Prod.dbo.acct_rates
						WHERE IsoCurrCd = 'VEF'
							AND EffectiveDate =
							(
							SELECT MAX(EffectiveDate) FROM IE2_Prod.dbo.acct_rates WHERE IsoCurrCd = 'VEF' AND AccountingRate != 0
							)
					)
					ELSE SUM(current_rate)
				END AS current_rate,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(net_revenue) AS net_revenue,
				SUM(total_cos) AS total_cos,
				SUM(gross_profit) AS gross_profit,
				SUM(net_revenue_before_hedge) AS net_revenue_before_hedge,
				SUM(revenue_units) AS revenue_units
			FROM actuals_join_acct_rates
			GROUP BY cal_date, country_alpha2, region_5, currency, sales_product_number, pl, customer_engagement
		),
		actuals_adjust_accounting_rates AS
		(
			SELECT
				cal_date,
				region_5,
				country_alpha2,
				currency,
				sales_product_number,
				pl,
				customer_engagement,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(net_revenue) AS net_revenue,
				SUM(total_cos) AS total_cos,
				SUM(gross_profit) AS gross_profit,
				SUM(net_revenue_before_hedge) AS net_revenue_before_hedge,
				SUM(revenue_units) AS revenue_units,
				-- USD is denominator of all accounting rates
				SUM(original_rate) AS original_rate,
				SUM(current_rate) AS current_rate,
				ISNULL((NULLIF(original_rate, 0) / NULLIF(current_rate, 0)), 1) AS currency_rate_adjustment,
				ISNULL((NULLIF(original_rate, 0) / NULLIF(current_rate, 0)), 1) * SUM(net_revenue_before_hedge) AS cc_net_revenue
			FROM actuals_join_acct_rates2
			GROUP BY cal_date, country_alpha2, region_5, currency, sales_product_number, pl, customer_engagement, original_rate, current_rate
		)


			SELECT
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
			FROM actuals_adjust_accounting_rates
"""

supp_hist_5.createOrReplaceTempView("supplies_history_constant_currency")
