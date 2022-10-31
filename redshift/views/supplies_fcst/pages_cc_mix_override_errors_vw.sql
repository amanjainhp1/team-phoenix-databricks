-- supplies_fcst.pages_cc_mix_override_errors_vw source

CREATE VIEW supplies_fcst.pages_cc_mix_override_errors_vw AS

WITH hw AS
(
	SELECT DISTINCT platform_subset
        , technology
        , hw_product_family
        , pl
	FROM mdm.hardware_xref
	WHERE 1=1
		AND product_lifecycle_status = 'N'
		AND technology IN ('INK', 'LASER', 'PWA')
)

, cmo AS
(
	SELECT DISTINCT platform_subset
	FROM prod.cartridge_mix_override
	WHERE 1=1
		AND official = 1
)

, shm AS
(
	SELECT DISTINCT platform_subset
	, base_product_number
	FROM mdm.supplies_hw_mapping
	WHERE 1=1
		AND official = 1
)

, actuals AS
(
    -- enriched actuals_supplies data ... feeds into pages_ccs_mix
	SELECT DISTINCT base_product_number
    FROM prod.cartridge_demand_volumes
    WHERE 1=1
        AND version = (SELECT max(version) FROM prod.cartridge_demand_volumes)
)

SELECT DISTINCT shm.platform_subset
	, h.technology
	, h.hw_product_family
	, h.pl
FROM shm  -- main table
join hw AS h
	ON h.platform_subset = shm.platform_subset
left join actuals AS a
    ON a.base_product_number = shm.base_product_number
left join cmo AS c
	ON c.platform_subset = shm.platform_subset
WHERE 1=1
	AND c.platform_subset IS NULL  -- these are platform_subsets that are missing
    AND a.base_product_number IS NULL  -- only returning platform_subsets w/o actuals;