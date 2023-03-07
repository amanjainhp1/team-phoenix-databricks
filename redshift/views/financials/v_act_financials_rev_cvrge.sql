CREATE OR REPLACE VIEW financials.v_act_financials_rev_cvrge AS 

(
--- open supplies actuals base product level of detail

WITH
	current_fiscal_yr AS
	(
		SELECT 
			CAST(fiscal_yr AS INT)-4 start_fiscal_yr
			, CAST(fiscal_yr AS INT)+5 end_fiscal_yr
		FROM
		mdm.calendar
		WHERE Date = cast(GETDATE() AS DATE)
	),
	supplies_baseprod_actuals AS
	(
		SELECT 
			'ACTUALS' AS record_type
			, actuals_supplies_baseprod.base_product_number
			, actuals_supplies_baseprod.platform_subset
			, actuals_supplies_baseprod.pl base_product_line_code
			, supplies_xref.Technology
			, CASE 
				WHEN (actuals_supplies_baseprod.pl in ('5T', 'GJ', 'GK', 'GP', 'IU', 'LS')) THEN 1
				ELSE 0
			  END AS IsCanon
			, iso_country_code_xref.region_5
			, iso_country_code_xref.country_alpha2
			, iso_country_code_xref.market10
			, cal_date
			, calendar.fiscal_year_qtr
			, calendar.fiscal_year_half
			, calendar.fiscal_yr
			, SUM(revenue_units) Units
			, SUM(gross_revenue) AS GrossRevenue
			, SUM(contractual_discounts) + SUM(discretionary_discounts) AS Contra
			, SUM(net_currency) AS Revenue_Currency
			, SUM(net_revenue) AS NetRevenue
			, SUM(total_cos) AS Total_COS
			, SUM(gross_profit) AS GrossProfit
		FROM fin_prod.actuals_supplies_baseprod actuals_supplies_baseprod
		LEFT JOIN
		mdm.supplies_xref supplies_xref
			ON supplies_xref.base_product_number = actuals_supplies_baseprod.base_product_number
		LEFT JOIN
		mdm.iso_country_code_xref iso_country_code_xref
			ON iso_country_code_xref.country_alpha2 = actuals_supplies_baseprod.country_alpha2
		LEFT JOIN
		mdm.calendar calendar
			ON calendar.Date = actuals_supplies_baseprod.cal_date
		CROSS JOIN
		current_fiscal_yr
		WHERE fiscal_yr >= start_fiscal_yr
		and customer_engagement <> 'EST_DIRECT_FULFILLMENT'
		and Day_of_Month = 1
		GROUP BY actuals_supplies_baseprod.record
			, actuals_supplies_baseprod.base_product_number
			, actuals_supplies_baseprod.platform_subset
			, actuals_supplies_baseprod.pl 
			, supplies_xref.Technology
			, iso_country_code_xref.region_5
			, iso_country_code_xref.country_alpha2
			, iso_country_code_xref.market10
			, cal_date
			, calendar.Fiscal_Year_Qtr
			, calendar.Fiscal_Year_Half
			, calendar.Fiscal_Yr
	)

		SELECT record_type
			, base_product_number
			, platform_subset
			, base_product_line_code
			, technology
			, iscanon
			, region_5
			, country_alpha2
			, market10
			, cal_date
			, fiscal_year_qtr
			, fiscal_year_half
			, fiscal_yr
			, units
			, grossrevenue
			, contra
			, revenue_currency
			, netrevenue
			, total_cos
			, grossprofit
		FROM supplies_baseprod_actuals
);

GRANT ALL ON TABLE financials.v_act_financials_rev_cvrge TO GROUP phoenix_dev;
