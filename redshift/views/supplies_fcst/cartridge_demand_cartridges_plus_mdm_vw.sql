CREATE OR REPLACE VIEW supplies_fcst.cartridge_demand_cartridges_plus_mdm_vw AS
SELECT
    cdc.record,
    cdc.cal_date,
    cdc.geography_grain,
    cdc.geography,
    cdc.platform_subset,
    cdc.base_product_number,
    cdc.customer_engagement,
    cdc.cartridges,
    cdc.channel_fill,
    cdc.supplies_spares_cartridges,
    cdc.host_cartridges,
    cdc.welcome_kits,
    cdc.expected_cartridges,
    cdc.vtc,
    cdc.adjusted_cartridges,
    plx.l4_description,
    plx.l5_description,
    plx.l6_description,
    plx.pl,
    plx.technology,
    sx.cartridge_alias,
    sx.crg_chrome,
    sx.k_color,
    sx.single_multi,
    sx.size,
    sx.supplies_family,
    sx.type,
    cdc.load_date,
    cdc.version
FROM prod.cartridge_demand_cartridges cdc
LEFT JOIN mdm.supplies_xref sx
ON cdc.base_product_number = sx.base_product_number
LEFT JOIN mdm.product_line_xref plx
ON sx.pl = plx.pl
