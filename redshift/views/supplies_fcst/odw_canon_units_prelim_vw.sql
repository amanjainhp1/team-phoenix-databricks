CREATE OR REPLACE VIEW supplies_fcst.odw_canon_units_prelim_vw AS
with 
	odw_sales_product_units AS
            (
                  SELECT cal.Date AS cal_date
                  ,profit_center_code AS profit_center_code
                  ,material_number AS material_number
                  ,segment_code AS segment
                  ,isnull(SUM(unit_quantity_sign_flip), 0) AS units
                  FROM fin_prod.odw_revenue_units_sales_actuals_prelim w
                  LEFT JOIN mdm.calendar cal
               ON ms4_fiscal_Year_Period = fiscal_year_period
                  WHERE 1=1
                  AND day_of_month = 1
                  AND unit_reporting_code= 'S'
                  AND fiscal_year_period = (SELECT MAX(fiscal_year_period) FROM fin_prod.odw_revenue_units_sales_actuals_prelim)
                  GROUP BY 
                  cal.Date, 
                  profit_center_code, 
                  material_number, 
                  segment_code  
            ),
            
            change_profit_center_hierarchy AS
            (
                  SELECT
                  cal_date,
                  w.profit_center_code,
                  pl,
                  segment,
                  material_number,
                  plx.PLXX,
                  SUM(units) as units
                  FROM odw_sales_product_units w
                  LEFT JOIN mdm.product_line_xref plx 
               ON w.profit_center_code = plx.profit_center_code
                  WHERE 1=1
                  AND plx.plxx IN 
                  (
            SELECT distinct plxx
            FROM mdm.product_line_xref
            WHERE pl_category = 'SUP' 
            AND technology = 'LASER'
            AND plxx IN ('5T00', 'GJ00', 'GK00', 'GP00', 'IU00', 'LS00') -- Canon Only
            )
                  GROUP BY 
                  cal_date, 
                  pl, 
                  segment, 
                  material_number, 
                  w.profit_center_code,
                  plx.plxx
            ),
            
            --select * from change_profit_center_hierarchy where pl is null
      
      
      add_seg_hierarchy AS
            (
                  SELECT
                        cal_date,
                        pl,
                        country_alpha2,
                        region_3,
                        region_5,   
                        material_number,
                        SUM(units) as units
                  FROM change_profit_center_hierarchy w
                  LEFT JOIN mdm.profit_center_code_xref s
               ON w.segment = s.profit_center_code
                  GROUP BY 
                  cal_date, 
                  pl, 
                  country_alpha2, 
                  region_3, 
                  region_5, 
                  material_number
            ),
      odw_revenue_sales_translated AS 
            (
                        SELECT
                        cal_date,
                        pl,
                        country_alpha2,
                        region_3,
                        region_5,
                        material_number,
                        units
                       FROM add_seg_hierarchy
            ),
--select * from #odw_revenue_sales_translated
-- translate material number (sales product with option) to sales product number
      sales_material_number AS
            (
                  SELECT
                  cal_date,
                  pl,
                  country_alpha2,
                  region_3,
                  region_5,
                  material_number,
                  CASE
                        WHEN SUBSTRING(material_number,7,1) = '#' THEN SUBSTRING(material_number,1,6)
                        WHEN SUBSTRING(material_number,8,1) = '#' THEN SUBSTRING(material_number,1,7)
                        ELSE material_number
                  END AS sales_product_number,
                  SUM(units) AS units
            FROM odw_revenue_sales_translated
            GROUP BY cal_date,
                  pl,
                  country_alpha2,
                  region_3,
                  region_5,
                  material_number
            ),
            
            
      sales_product_number AS
                  (
            SELECT
                  cal_date,
                  pl,
                  country_alpha2,
                  region_3,
                  region_5,
                  sales_product_number,
                  sales_prod_nr as rdma_sales_product_number,
                  SUM(units) as units
            FROM sales_material_number sp
            LEFT JOIN mdm.rdma_sales_product rdma ON sales_product_number = sales_prod_nr
            GROUP BY cal_date,
                  pl,
                  country_alpha2,
                  region_3,
                  region_5,
                  sales_product_number,
                  sales_prod_nr
                  ),
                  
                  
      base_product_number AS
            (
                  SELECT
                        cal_date,
                        pl,
                        sales_product_line_code,
                        base_product_line_code,
                        country_alpha2,
                        region_3,
                        region_5,
                        sp.sales_product_number,
                        base_product_number,
                        SUM(units * isnull(base_prod_per_sales_prod_qty, 1)) AS units
                  FROM sales_product_number sp
                  LEFT JOIN mdm.rdma_base_to_sales_product_map rdma ON 
                        sp.sales_product_number = rdma.sales_product_number
                  GROUP BY cal_date,
                        pl,
                        country_alpha2,
                        region_3,
                        region_5,
                        sp.sales_product_number,
                        base_product_number,
                        sales_product_line_code,
                        base_product_line_code
            ),
            
            
      base_product_number2 AS
            (
                  SELECT
                  cal_date,
                  pl,
                  sales_product_line_code,
                  base_product_line_code,
                  country_alpha2,
                  region_3,
                  region_5,
                  sales_product_number,
                  CASE
                        WHEN base_product_line_code is null THEN 'UNKN' + pl
                        ELSE base_product_number
                  END AS base_product_number,
                  SUM(units) AS units
            FROM base_product_number sp
            GROUP BY cal_date,
                  pl,
                  country_alpha2,
                  region_3,
                  region_5,
                  sales_product_number,
                  base_product_number,
                  sales_product_line_code,
                  base_product_line_code
            ),
      odw_base_product_etl AS
            (
                  SELECT
                  cal_date,
                  pl,
                  country_alpha2,
                  region_3,
                  region_5,
                  base_product_number,
                  SUM(units) AS units
            FROM base_product_number2
            GROUP BY
                  cal_date,
                  pl,
                  country_alpha2,
                  region_3,
                  region_5,
                  base_product_number
            ),
      add_base_product_name AS
      (
            SELECT 
            cal_date,
            region_5,
            odw.pl,
            base_product_number,
            rdma.base_prod_name AS base_product_name,
            SUM(units) AS units
            FROM odw_base_product_etl odw
            LEFT JOIN mdm.rdma rdma ON odw.base_product_number = rdma.base_prod_number
            GROUP BY cal_date, region_5, odw.pl , base_product_number , rdma.base_prod_name
      )
SELECT
      cal_date,
      region_5,
      pl,
      base_product_number,
      base_product_name,
      SUM(units) as base_quantity
FROM add_base_product_name odw
GROUP BY cal_date,
      region_5,
      pl,
      base_product_number,
      base_product_name;