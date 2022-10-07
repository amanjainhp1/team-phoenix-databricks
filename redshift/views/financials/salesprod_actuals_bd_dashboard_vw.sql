CREATE OR REPLACE VIEW financials.salesprod_actuals_bd_dashboard_vw AS 

WITH
		supplies_llc_data As
		(
			SELECT
				calendar_yr_mo AS year_mon,
				pl,
				sales_product_number AS sales_prod_nbr,
				region_3,
				region_4,
				country,
				customer_engagement AS route_to_market,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(total_cos) AS total_cos,
				SUM(revenue_units) AS revenue_units
			FROM fin_prod.actuals_supplies_salesprod AS salesprod
			JOIN mdm.calendar AS cal ON salesprod.cal_date = cal.Date
			JOIN mdm.iso_country_code_xref geo ON geo.country_alpha2 = salesprod.country_alpha2
			WHERE Day_of_Month = 1
			-- exclude LF PL's for now (in development)
			and pl NOT IN ('IE', 'IX', 'UK', 'TX')
			GROUP BY
				calendar_Yr_Mo,
				pl,
				sales_product_number,
				region_3,
				region_4,
				country,
				customer_engagement
		),
		media AS
		(
			SELECT
				yearmon AS year_mon,
				pl,
				salesProdNbr AS sales_prod_nbr,
				Region_3 as region_3,
				Region_4 as region_4,
				Country as country,
				Route_to_market as route_to_market,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(total_cos) AS total_cos,
				SUM(revenue_units) AS revenue_units
			FROM fin_prod.edw_summary_actuals_plau
			GROUP BY yearmon, pl, salesProdNbr, Region_3, Region_4, Country, Route_to_market
		),
		supplies_llcs_media AS
		(
			SELECT * FROM supplies_llc_data
			UNION ALL
			SELECT * FROM media
		)
		
			SELECT
				year_mon,
				pl,
				sales_prod_nbr,
				region_3,
				region_4,
				country,
				route_to_market,
				SUM(gross_revenue) AS gross_revenue,
				SUM(net_currency) AS net_currency,
				SUM(contractual_discounts) AS contractual_discounts,
				SUM(discretionary_discounts) AS discretionary_discounts,
				SUM(total_cos) AS total_cos,
				SUM(revenue_units) AS revenue_units,
				--CONVERT(Date, GETDATE(), 9) AS version, -- what to do about version if anything?
				CAST(GETDATE() as DATE) as version,
				GETDATE() AS load_date
			FROM supplies_llcs_media
			GROUP BY year_mon, pl, sales_prod_nbr, region_3, region_4, country, route_to_market;