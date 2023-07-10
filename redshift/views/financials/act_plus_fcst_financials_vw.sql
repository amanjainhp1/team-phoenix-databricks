-- financials.act_plus_fcst_financials_vw source
CREATE OR REPLACE VIEW financials.act_plus_fcst_financials_vw AS --- open supplies actuals base product level of detail
    WITH current_fiscal_yr AS (
        SELECT CAST(fiscal_yr AS INTEGER) -4 Start_fiscal_yr,
             CAST(fiscal_yr AS INTEGER) +5 end_fiscal_yr
        FROM mdm.calendar
        WHERE Date = CAST(GETDATE() AS DATE)
    ),
    cat_group AS (
        SELECT distinct shm.base_product_number,
            --  hw.hw_product_family as tech_split,
            seg.technology_type
        FROM mdm.supplies_hw_mapping shm
            LEFT JOIN mdm.hardware_xref hw ON hw.platform_subset = shm.platform_subset
            LEFT JOIN mdm.hw_product_family_ink_business_segments seg on seg.hw_product_family = hw.hw_product_family
        WHERE hw.technology IN ('INK', 'PWA')
            AND shm.customer_engagement = 'TRAD'
            and shm.official = 1
            and seg.technology_type is not null
    ),
    supplies_baseprod_actuals AS (
        SELECT 'ACTUALS' AS record_type,
            actuals_supplies_baseprod.base_product_number,
            actuals_supplies_baseprod.pl base_product_line_code,
            supplies_xref.technology,
            CASE
                WHEN (
                    actuals_supplies_baseprod.pl in ('5T', 'GJ', 'GK', 'GP', 'IU', 'LS')
                ) THEN 1
                ELSE 0
            END AS is_canon,
            iso_country_code_xref.region_5,
            iso_country_code_xref.country_alpha2,
            iso_country_code_xref.market10,
            cal_date,
            calendar.fiscal_year_qtr,
            calendar.fiscal_year_half,
            calendar.fiscal_yr,
            supplies_xref.k_color,
            supplies_xref.size,
            supplies_xref.supplies_family,
            SUM(revenue_units) units,
            SUM(gross_revenue) AS gross_revenue,
            SUM(contractual_discounts) + SUM(discretionary_discounts) AS contra,
            SUM(net_currency) AS revenue_currency,
            SUM(net_revenue) AS net_revenue,
            SUM(total_cos) AS total_cos,
            SUM(gross_profit) AS gross_profit
        FROM fin_prod.actuals_supplies_baseprod actuals_supplies_baseprod
            LEFT JOIN mdm.supplies_xref supplies_xref ON supplies_xref.base_product_number = actuals_supplies_baseprod.base_product_number
            LEFT JOIN mdm.iso_country_code_xref iso_country_code_xref ON iso_country_code_xref.country_alpha2 = actuals_supplies_baseprod.country_alpha2
            LEFT JOIN mdm.calendar calendar ON calendar.Date = actuals_supplies_baseprod.cal_date
            CROSS JOIN current_fiscal_yr
        WHERE fiscal_yr >= Start_fiscal_yr
            and customer_engagement <> 'EST_DIRECT_FULFILLMENT'
            and day_of_month = 1
        GROUP BY actuals_supplies_baseprod.record,
            actuals_supplies_baseprod.base_product_number,
            actuals_supplies_baseprod.pl,
            supplies_xref.technology,
            iso_country_code_xref.region_5,
            iso_country_code_xref.country_alpha2,
            iso_country_code_xref.market10,
            cal_date,
            calendar.fiscal_year_qtr,
            calendar.fiscal_year_half,
            calendar.fiscal_yr,
            supplies_xref.k_color,
            supplies_xref.size,
            supplies_xref.supplies_family
    ) --select distinct cal_date, fiscal_yr from supplies_baseprod_actuals
,
    --- open supplies forecast at the base product level of detail, convert to total values from per unit entries
    supplies_baseprod_forecast AS --(from landing; after alpha release, update to financials db)
    (
        SELECT 'FORECAST' AS record_type,
            forecast_supplies_baseprod.base_product_number,
            base_product_line_code,
            supplies_xref.technology,
            CASE
                WHEN (
                    base_product_line_code in ('5T', 'GJ', 'GK', 'GP', 'IU', 'LS')
                ) THEN 1
                ELSE 0
            END AS is_canon,
            iso_country_code_xref.region_5,
            iso_country_code_xref.country_alpha2,
            iso_country_code_xref.market10,
            cal_date,
            calendar.fiscal_year_qtr,
            calendar.fiscal_year_half,
            calendar.fiscal_yr,
            supplies_xref.k_color,
            supplies_xref.size,
            supplies_xref.supplies_family,
            coalesce(insights_base_units, 0) units,
            coalesce(baseprod_gru, 0) * coalesce(insights_base_units, 0) gross_revenue,
            coalesce(baseprod_contra_per_unit, 0) * coalesce(insights_base_units, 0) contra,
            coalesce(baseprod_revenue_currency_hedge_unit, 0) * coalesce(insights_base_units, 0) revenue_currency,
            (
                (
                    coalesce(baseprod_gru, 0) - coalesce(baseprod_contra_per_unit, 0) + coalesce(baseprod_revenue_currency_hedge_unit, 0)
                ) * coalesce(insights_base_units, 0)
            ) net_revenue,
            (
                (
                    coalesce(baseprod_variable_cost_per_unit, 0) + coalesce(baseprod_fixed_cost_per_unit, 0)
                ) * coalesce(insights_base_units, 0)
            ) total_cos,
            (
                (
                    coalesce(baseprod_gru, 0) - coalesce(baseprod_contra_per_unit, 0) + coalesce(baseprod_revenue_currency_hedge_unit, 0) - coalesce(baseprod_variable_cost_per_unit, 0) - coalesce(baseprod_fixed_cost_per_unit, 0)
                ) * coalesce(insights_base_units, 0)
            ) gross_profit
        FROM fin_prod.forecast_supplies_baseprod forecast_supplies_baseprod
            left join mdm.iso_country_code_xref iso_country_code_xref on forecast_supplies_baseprod.country_alpha2 = iso_country_code_xref.country_alpha2
            left join mdm.calendar calendar on calendar.Date = forecast_supplies_baseprod.cal_date
            left join mdm.supplies_xref supplies_xref on supplies_xref.base_product_number = forecast_supplies_baseprod.base_product_number
            CROSS JOIN current_fiscal_yr
        WHERE fiscal_yr <= end_fiscal_yr
            and day_of_month = 1
            and forecast_supplies_baseprod.version = (
                select max(version)
                from fin_prod.forecast_supplies_baseprod
            )
            and forecast_supplies_baseprod.cal_date > (
                select max(cal_date)
                from fin_prod.actuals_supplies_baseprod
            )
    ),
    supplies_actuals_join_forecast AS (
        SELECT record_type,
            base_product_number,
            base_product_line_code,
            technology,
            is_canon,
            region_5,
            country_alpha2,
            market10,
            cal_date,
            fiscal_year_qtr,
            fiscal_year_half,
            fiscal_yr,
            k_color,
            size,
            supplies_family,
            units,
            gross_revenue,
            contra,
            revenue_currency,
            net_revenue,
            total_cos,
            gross_profit
        FROM supplies_baseprod_actuals
        UNION ALL
        SELECT record_type,
            base_product_number,
            base_product_line_code,
            technology,
            is_canon,
            region_5,
            country_alpha2,
            market10,
            cal_date,
            fiscal_year_qtr,
            fiscal_year_half,
            fiscal_yr,
            k_color,
            size,
            supplies_family,
            units,
            gross_revenue,
            contra,
            revenue_currency,
            net_revenue,
            total_cos,
            gross_profit
        FROM supplies_baseprod_forecast
    )
SELECT fcst.record_type,
    fcst.base_product_number,
    rdma.base_prod_desc,
    fcst.base_product_line_code,
    fcst.technology,
    fcst.is_canon,
    fcst.region_5,
    fcst.country_alpha2,
    fcst.market10,
    fcst.cal_date,
    fcst.fiscal_year_qtr,
    fcst.fiscal_year_half,
    fcst.fiscal_yr,
    fcst.k_color,
    fcst.size,
    fcst.supplies_family,
    fcst.units,
    fcst.gross_revenue,
    fcst.contra,
    fcst.revenue_currency,
    fcst.net_revenue,
    fcst.total_cos,
    fcst.gross_profit
FROM supplies_actuals_join_forecast fcst
    LEFT JOIN cat_group on cat_group.base_product_number = fcst.base_product_number
    LEFT JOIN mdm.rdma rdma on rdma.base_prod_number = fcst.base_product_number;
