-- supplies_fcst.missing_c2c_data_vw source
CREATE OR REPLACE VIEW supplies_fcst.missing_c2c_data_vw AS
WITH tbl_supplies_xref AS (
    /* base supplies xref info needed for each 'missing' query */
    SELECT DISTINCT base_product_number,
        crg_chrome,
        k_color,
        technology
    FROM mdm.supplies_xref
    WHERE 1 = 1
),
tbl_missing_supplies_xref_rows AS (
    /* set the flag if there is no entry in supplies_hw_mapping for this BPN. */
    SELECT DISTINCT xref.base_product_number,
        1 AS miss_shm_base_prod_number,
        0 AS miss_supplies_xref_crg_chrome,
        0 AS miss_k_color,
        0 AS miss_technology,
        0 AS miss_family,
        0 AS miss_yield,
        0 AS miss_mix
    FROM tbl_supplies_xref AS xref
        LEFT JOIN mdm.supplies_hw_mapping AS shm ON xref.base_product_number = shm.base_product_number
    WHERE 1 = 1
        AND shm.base_product_number IS NULL
),
tbl_missing_supplies_xref AS (
    /* set individual missing flags to 1 if there IS a row but a specific column is missing from supplies_xref */
    SELECT DISTINCT xref.base_product_number,
        0 AS miss_shm_base_prod_number,
        CASE
            WHEN crg_chrome = '' THEN 1
            WHEN crg_chrome IS NULL THEN 1
            ELSE 0
        END AS crg_chrome,
        CASE
            WHEN k_color = '' THEN 1
            WHEN k_color IS NULL THEN 1
            ELSE 0
        END AS k_color,
        CASE
            WHEN technology = '' THEN 1
            WHEN technology IS NULL THEN 1
            ELSE 0
        END AS miss_technology,
        0 AS miss_family,
        0 AS miss_yield,
        0 AS miss_mix
    FROM tbl_supplies_xref AS xref
        JOIN mdm.supplies_hw_mapping AS shm ON xref.base_product_number = shm.base_product_number
    WHERE 1 = 1
),
tbl_missing_family AS (
    SELECT DISTINCT xref.base_product_number,
        0 AS miss_shm_base_prod_number,
        0 AS miss_supplies_xref_Crg_chrome,
        0 AS miss_k_color,
        0 AS miss_technology,
        CASE
            WHEN hw_product_family IS NULL THEN 1
            WHEN hw_product_family = '' THEN 1
            WHEN hw_product_family = '(BLANK)' THEN 1
            ELSE 0
        END AS miss_family,
        0 AS miss_yield,
        0 AS miss_mix
    FROM tbl_supplies_xref AS xref
        JOIN mdm.supplies_hw_mapping AS shm ON shm.base_product_number = xref.base_product_number
        JOIN mdm.hardware_xref AS hw ON hw.platform_subset = shm.platform_subset
    WHERE 1 = 1
        AND shm.official = 1
        AND hw.technology in ('INK', 'PWA', 'LASER')
),
tbl_missing_yield AS (
    /* sets miss_yield to 1 if bpn missing */
    SELECT DISTINCT xref.base_product_number,
        0 AS miss_shm_base_prod_number,
        0 AS miss_supplies_xref_Crg_chrome,
        0 AS miss_k_color,
        0 AS miss_technology,
        0 AS miss_family,
        1 AS miss_yield,
        0 AS miss_mix
    FROM tbl_supplies_xref AS xref
        LEFT JOIN mdm.yield ON xref.base_product_number = yield.base_product_number
    WHERE 1 = 1
        AND yield.base_product_number IS NULL
),
tbl_union_missing AS (
    /* union all 'missing' rows into one dataset */
    SELECT *
    FROM tbl_missing_supplies_xref_rows
    WHERE 1 = 1
        AND miss_shm_base_prod_number > 0
    UNION ALL
    SELECT *
    FROM tbl_missing_supplies_xref
    WHERE 1 = 1
        AND miss_technology > 0
    UNION ALL
    SELECT *
    FROM tbl_missing_family
    WHERE 1 = 1
        AND miss_family > 0
    UNION ALL
    SELECT *
    FROM tbl_missing_yield
    WHERE 1 = 1
        AND miss_yield > 0
),
tbl_final AS (
    SELECT base_product_number, -- not in supplies_hw_mapping
        MAX(miss_shm_base_prod_number) AS miss_shm_base_prod_number, -- in shm - no crg_chrome or k_color
        MAX(miss_supplies_xref_Crg_chrome) AS miss_sup_xref_crg_chrome,
        MAX(miss_k_color) AS miss_sup_xref_k_color, -- in shm - no hw tech or hw_product_family
        MAX(miss_technology) AS miss_hw_tech,
        MAX(miss_family) AS miss_hw_family, -- not in yield
        MAX(miss_yield) AS miss_yield
    FROM tbl_union_missing
    GROUP BY base_product_number
)
SELECT *
FROM tbl_final;