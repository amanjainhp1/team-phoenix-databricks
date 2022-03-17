# Databricks notebook source
norm_ships_sql = """
SELECT 'dev.stage' AS variable
    , ns.cal_date
    , SUM(ns.units) AS units
FROM dev.stage.norm_ships AS ns
LEFT JOIN dev.mdm.hardware_xref AS hw
    ON hw.platform_subset = ns.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND ns.cal_date between '2019-03-01' and '2026-10-01'
GROUP BY ns.cal_date
UNION ALL
SELECT 'dev.prod' AS variable
    , ns.cal_date
    , SUM(ns.units) AS units
FROM dev.prod.norm_shipments AS ns
LEFT JOIN dev.mdm.hardware_xref AS hw
    ON hw.platform_subset = ns.platform_subset
WHERE 1=1
    AND hw.technology IN ('INK', 'LASER', 'PWA')
    AND ns.version = '2021.11.22.1'
    and ns.cal_date between '2019-03-01' and '2026-10-01'
GROUP BY ns.cal_date
"""
