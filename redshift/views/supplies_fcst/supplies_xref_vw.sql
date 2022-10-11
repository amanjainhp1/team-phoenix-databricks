CREATE OR REPLACE VIEW supplies_fcst.supplies_xref_vw
AS

SELECT 
    sf.record
    , sf.base_product_number
    , r.base_prod_desc
    , r.pl
    , sf.technology
    , sf.crg_intro_dt AS crg_intro_date
    , sf.crg_chrome AS crg_chrome
    , sf.k_color 
    , sf.size
    , sf.cartridge_alias
    , sf.single_multi
    , sf.type
    , COALESCE(sf.equivalents_multiplier,1) AS equivalents_multiplier
FROM mdm.supplies_xref sf
LEFT JOIN mdm.rdma r ON r.base_prod_number = sf.base_product_number
WHERE 1=1
    AND technology in ('INK', 'LASER', 'PWA');