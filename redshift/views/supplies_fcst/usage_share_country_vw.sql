CREATE OR REPLACE VIEW supplies_fcst.usage_share_country_vw
AS SELECT us.*, hw.technology
   FROM prod.usage_share_country us
   LEFT JOIN mdm.hardware_xref hw ON hw.platform_subset::text = us.platform_subset::text;
