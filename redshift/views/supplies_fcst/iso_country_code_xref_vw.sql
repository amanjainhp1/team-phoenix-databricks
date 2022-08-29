CREATE OR REPLACE VIEW supplies_fcst.iso_country_code_xref_vw
AS
SELECT DISTINCT
    region_5,
    market10,
    country_alpha2,
    country
FROM mdm.iso_country_code_xref;