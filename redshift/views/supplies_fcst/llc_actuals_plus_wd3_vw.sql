-- supplies_fcst.llc_actuals_plus_wd3_vw source

--created 1/25/2022, Brent Merrick
--adapted to Redshift 2023-01-23, Matt Koson


CREATE OR REPLACE VIEW supplies_fcst.llc_actuals_plus_wd3_vw AS 
WITH base_data AS
(
--stitch actuals and wd3
SELECT
	record, 
	'ACTUALS' AS monthly_forecast, 
	geography AS region_5, 
	base_product_number AS base_prod_number, 
	base_product_number AS sku, 
	cal_date as month, 
	units, 
	version, 
	load_date 
FROM prod.actuals_llc 
WHERE 1=1
	AND cal_date > '2016-10-01' 

UNION ALL

SELECT
	record,
	record_name, 
	geo AS region_5, 
	bpn AS base_prod_number, 
	sku, 
	calendar_month AS month, 
	SUM(units) AS units, 
	version, 
	load_date 
FROM prod.wd3_llc 
where 1=1
	AND version in (SELECT DISTINCT MAX(version) over(partition by record_name) AS version from prod.wd3_llc WHERE record = 'WD3-LLC' ORDER BY version DESC LIMIT 12)
GROUP BY
	record, 
	record_name, 
	geo, 
	bpn, 
	sku, 
	calendar_month, 
	version, 
	load_date
),

--add RDMA attribute data
base_plus_rdma AS
(
SELECT 
	a.*,
	b.pl,
	b.base_prod_name,
	b.base_prod_desc,
	b.product_lab_name,
	b.product_class,
	b.fmc,
	b.platform_subset
FROM base_data a 
LEFT JOIN mdm.rdma b on a.base_prod_number=b.base_prod_number
)

--add calendar attribute data
SELECT
	c.*,
	d.fiscal_qtr,
	d.fiscal_year_half,
	d.fiscal_yr,
	d.calendar_qtr,
	d.calendar_yr
FROM base_plus_rdma c
LEFT JOIN mdm.calendar d on c.month=d.date;