# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Adjusted Revenue Flash
# MAGIC 
# MAGIC Tables Needed in Delta Lake:
# MAGIC - fin_prod.adjusted_revenue_salesprod
# MAGIC - fin_prod.supplies_finance_flash

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add Version

# COMMAND ----------

add_record = 'ADJ REV PLUS FLASH'
add_source = 'ADJREV PLUS SUPPLIES FLASH'

# COMMAND ----------

adj_flsh_add_version = call_redshift_addversion_sproc(configs, add_record, add_source)

version = adj_flsh_add_version[0]
print(version)

# COMMAND ----------

# Cells 7-8: Refreshes version table in delta lakes, to bring in new version from previous step, above.
version_df = read_redshift_to_df(configs) \
    .option("dbtable", "prod.version") \
    .load()

supplies_finance_flash = read_redshift_to_df(configs) \
    .option("dbtable", "fin_prod.supplies_finance_flash") \
    .load()

tables = [['prod.version', version_df, "overwrite"], ['fin_prod.supplies_finance_flash', supplies_finance_flash, "overwrite"]]

# COMMAND ----------

# MAGIC %run "../common/delta_lake_load_with_params" $tables=tables

# COMMAND ----------

adj_rev_data = spark.sql("""
with adjusted_revenue_staging as
         (select fiscal_year_qtr,
                 fiscal_yr,
                 geography,
                 geography_grain,
                 'NON-HQ'                          as hq_flag,
                 pl,
                 accounting_rate,
                 embargoed_sanctioned_flag,
                 sum(net_revenue)                  as reported_revenue,
                 sum(net_hedge_benefit)            as hedge,
                 sum(currency_impact)              as currency,
                 sum(cc_net_revenue)               as revenue_in_cc,
                 sum(inventory_change_impact)      as inventory_change,
                 sum(currency_impact_ch_inventory) as ci_currency_impact,
                 sum(cc_inventory_impact)          as total_inventory_impact,
                 sum(adjusted_revenue)             as adjusted_revenue
          from fin_prod.adjusted_revenue_salesprod ar
                   join mdm.calendar cal on cal.date = ar.cal_date -- add fiscal quarter
                   join mdm.iso_country_code_xref iso on iso.country_alpha2 = ar.country_alpha2
          where day_of_month = 1
            and ar.version = (select max(version) from fin_prod.adjusted_revenue_salesprod) -- e.g. '2021.06.08.2'
            -- not in any future quarter provided..
            and fiscal_year_qtr not in (select distinct fiscal_year_qtr
                                        from fin_prod.supplies_finance_flash
                                        where net_revenue <> 0
                                          and version = (select max(version) from fin_prod.supplies_finance_flash))
            and pl in (select distinct pl
                       from mdm.product_line_xref
                       where pl_category = 'SUP'
                         and technology in ('INK', 'PWA', 'LASER')
                         and pl not in ('GY', 'LZ'))
          group by fiscal_year_qtr,
                   fiscal_yr,
                   geography,
                   geography_grain,
                   pl,
                   embargoed_sanctioned_flag,
                   accounting_rate)

select 'ACTUALS'                   as record_description,
       fiscal_year_qtr,
       fiscal_yr,
       geography,
       geography_grain,
       hq_flag,
       pl,
       accounting_rate,
       embargoed_sanctioned_flag,
       sum(reported_revenue)       as reported_revenue,
       sum(hedge)                  as hedge,
       sum(currency)               as currency,
       sum(revenue_in_cc)          as revenue_in_cc,
       sum(inventory_change)       as inventory_change,
       sum(ci_currency_impact)     as ci_currency_impact,
       sum(total_inventory_impact) as total_inventory_impact,
       sum(adjusted_revenue)       as adjusted_revenue
from adjusted_revenue_staging
group by fiscal_year_qtr,
         fiscal_yr,
         geography,
         geography_grain,
         hq_flag,
         pl,
         embargoed_sanctioned_flag,
         accounting_rate
""")

adj_rev_data.createOrReplaceTempView("adjusted_revenue_data")

# COMMAND ----------

flash_data = spark.sql("""
with supplies_flash as
         (select fiscal_year_qtr,
                 fiscal_yr,
                 market                                                as geography,
                 hq_flag,
                 pl,
                 sum(net_revenue)                                      as reported_revenue,
                 sum(hedge)                                            as hedge,
                 sum(hedge) * -1                                       as currency, -- note: hedge * -1 = currency, so currency * -1 = hedge
                 sum(net_revenue) + (sum(hedge) * -1)                  as revenue_in_cc,
                 sum(channel_inventory)                                as ci_cbm_dollars,
                 sum(ci_change)                                        as inventory_change,
                 0                                                     as ci_currency_impact,
                 sum(ci_change)                                        as total_inventory_impact,
                 sum(net_revenue) + (sum(hedge) * -1) - sum(ci_change) as adjusted_revenue,
                 flash.version
          from fin_prod.supplies_finance_flash flash
          where 1 = 1
            and fiscal_year_qtr in (select distinct fiscal_year_qtr
                                    from fin_prod.supplies_finance_flash
                                    where net_revenue <> 0
                                      and version =
                                          (select max(version) from fin_prod.supplies_finance_flash))
            and flash.version = (select max(version) from fin_prod.supplies_finance_flash)
          group by fiscal_year_qtr,
                   fiscal_yr,
                   market,
                   hq_flag,
                   pl,
                   flash.version)

select 'FLASH' as record_description,
        fiscal_year_qtr,
        fiscal_yr,
        geography,
        hq_flag,
        pl,
        sum(reported_revenue)       as reported_revenue,
        sum(hedge)                  as hedge,
        sum(currency)               as currency,
        sum(revenue_in_cc)          as revenue_in_cc,
        sum(ci_cbm_dollars)         as cbm_ci_dollars,
        sum(inventory_change)       as inventory_change,
        sum(ci_currency_impact)     as ci_currency_impact,
        sum(total_inventory_impact) as total_inventory_impact,
        sum(adjusted_revenue)       as adjusted_revenue
from supplies_flash
group by fiscal_year_qtr,
    fiscal_yr,
    geography,
    hq_flag,
    pl
""")

flash_data.createOrReplaceTempView("flash_data")

# COMMAND ----------

zero_history_data = spark.sql("""
with quarters as
         (select distinct fiscal_year_qtr,
                          0 as net_revenue,
                          0 as hedge,
                          0 as channel_inventory,
                          0 as ci_change
          from mdm.calendar cal
          where day_of_month = 1
            and fiscal_yr > '2015')
        ,
     flash_markets as
         (select distinct fiscal_year_qtr,
                          market  as    geography,
                          case
                              when market = 'EMEA' then 'EMEA'
                              when market = 'APJ HQ' then 'APJ'
                              else 'AMS'
                              end as    region_3,
                          case
                              when market = 'EMEA' then 'EU'
                              when market = 'APJ HQ' then 'AP'
                              else 'NA'
                              end as    region_5,
                          pl,
                          case
                              when market in ('APJ HQ', 'AMS HQ') then 'HQ'
                              else 'NON-HQ'
                              end as    hq_flag,
                          max(cal_date) over (partition by market, pl order by pl) as max_cal_date
          from fin_prod.supplies_finance_flash
          where market in ('AMS HQ', 'APJ HQ', 'EMEA', 'WORLD WIDE')
            and version = (select max(version) from fin_prod.supplies_finance_flash))
        ,
     dummy_flash_mkt_history as
         (select distinct q.fiscal_year_qtr,
                          geography,
                          region_3,
                          region_5,
                          hq_flag,
                          pl,
                          net_revenue,
                          hedge,
                          channel_inventory,
                          ci_change
          from quarters q
                   cross join flash_markets f
          where q.fiscal_year_qtr <= f.fiscal_year_qtr),
     final_dummy_history as
         (select dmy.fiscal_year_qtr,
                 fiscal_yr,
                 geography,
                 region_3,
                 region_5,
                 hq_flag,
                 pl,
                 sum(net_revenue)                  as reported_revenue,
                 sum(hedge)                        as hedge,
                 0                                 as currency,
                 sum(net_revenue)                  as revenue_in_cc,
                 sum(ci_change)                    as inventory_change,
                 0                                 as ci_currency_impact,
                 sum(ci_change)                    as total_inventory_impact,
                 sum(net_revenue) - sum(ci_change) as adjusted_revenue
          from dummy_flash_mkt_history dmy
                   left join mdm.calendar cal on cal.fiscal_year_qtr = dmy.fiscal_year_qtr
          where dmy.fiscal_year_qtr not in (select distinct fiscal_year_qtr
                                            from fin_prod.supplies_finance_flash
                                            where net_revenue <> 0
                                              and version = (select max(version)
                                                             from fin_prod.supplies_finance_flash))
          group by dmy.fiscal_year_qtr, geography, region_3, region_5, hq_flag, dmy.pl, fiscal_yr)

select fiscal_year_qtr,
       fiscal_yr,
       geography,
       hq_flag,
       pl,
       'FLASH'                     as record_description,
       sum(reported_revenue)       as reported_revenue,
       sum(hedge)                  as hedge,
       sum(currency)               as currency,
       sum(revenue_in_cc)          as revenue_in_cc,
       sum(inventory_change)       as inventory_change,
       sum(ci_currency_impact)     as ci_currency_impact,
       sum(total_inventory_impact) as total_inventory_impact,
       sum(adjusted_revenue)       as adjusted_revenue
from final_dummy_history
group by fiscal_year_qtr,
         fiscal_yr,
         geography,
         hq_flag,
         pl
""")

zero_history_data.createOrReplaceTempView("zero_history_data")

# COMMAND ----------

cbm_ci_data = spark.sql("""
with adjusted_revenue_staging_ci_inventory_balance as -- ci is a balance sheet or "stock" item
(select fiscal_year_qtr,
                 ar.geography,
                 embargoed_sanctioned_flag,
                 pl,
                 sum(inventory_usd) as ci_dollars
          from fin_prod.adjusted_revenue_salesprod ar
                   join mdm.calendar cal
                        on ar.cal_date = cal.date
                    join mdm.iso_country_code_xref iso
                        on iso.country_alpha2 = ar.country_alpha2
          where 1 = 1
            and day_of_month = 1
            and ar.version = (select max(version) from fin_prod.adjusted_revenue_salesprod)
            and fiscal_month in ('3.0', '6.0', '9.0', '12.0') -- dropping to get end of quarter balance
          group by fiscal_year_qtr,
                   ar.geography,                   
                   embargoed_sanctioned_flag,
                   pl),
     ci_inventory_unadjusted as
         (select fiscal_year_qtr,
                 geography,
                 embargoed_sanctioned_flag,
                 pl,
                 sum(ci_dollars) as ci_dollars
          from adjusted_revenue_staging_ci_inventory_balance
          where pl not in (select distinct pl
                           from mdm.product_line_xref
                           where pl_category = 'SUP'
                             and technology in ('INK', 'PWA'))
             or geography not in ('LATIN AMERICA', 'NORTH AMERICA')
          group by fiscal_year_qtr, geography, embargoed_sanctioned_flag, pl),
     ci_inventory_ams_post_adustment_period as
         (select fiscal_year_qtr,
                 geography,
                 pl,
                 embargoed_sanctioned_flag,
                 sum(ci_dollars) as ci_dollars
          from adjusted_revenue_staging_ci_inventory_balance
          where pl in (select distinct pl
                       from mdm.product_line_xref
                       where pl_category = 'SUP'
                         and technology in ('INK', 'PWA'))
            and geography in ('LATIN AMERICA', 'NORTH AMERICA')
            and fiscal_year_qtr > '2021Q1'
          group by fiscal_year_qtr, embargoed_sanctioned_flag, geography, pl),
     cbm_st_database2 as
         (select case
                     when data_type = 'ACTUALS' then 'ACTUALS - CBM_ST_BASE_QTY'
                     when data_type = 'PROXY ADJUSTMENT' then 'PROXY_ADJUSTMENT'
                     end                                                        as record,
                 cast(month as date)                                            as cal_date,
                 case
                     when country_code = '0A' then 'XB'
                     when country_code = '0M' then 'XH'
                     when country_code = 'CS' then 'XA'
                     when country_code = 'KV' then 'XA'
                     else country_code
                     end                                                        as country_alpha2,
                 product_number                                                 as sales_product_number,
                 product_line_id                                                as pl,
                 partner,
                 rtm_2                                                          as rtm2,
                 sum(cast(coalesce(`sell_thru_usd`, 0) as float))               as sell_thru_usd,
                 sum(cast(coalesce(`sell_thru_qty`, 0) as float))               as sell_thru_qty,
                 sum(cast(coalesce(channel_inventory_usd, 0) as float))         as channel_inventory_usd,
                 (sum(cast(coalesce(channel_inventory_qty, 0) as float)) + 0.0) as channel_inventory_qty
          from fin_stage.cbm_st_data st
          where month > '2015-10-01'
    and (coalesce (`sell_thru_usd`
        , 0) + coalesce (`sell_thru_qty`
        , 0) + coalesce (channel_inventory_usd
        , 0) +
    coalesce (channel_inventory_qty
        , 0)) <> 0
    and product_line_id in
    (select distinct pl
    from mdm.product_line_xref
    where pl_category in ('SUP')
    and technology in ('INK'
        , 'PWA')
    or pl = 'AU')
group by data_type, month, country_code, product_number, product_line_id, partner, rtm_2),
    cbm_quarterly as
    (
select cal_date,
    market8 as geography,
    embargoed_sanctioned_flag,
    pl,
    fiscal_year_qtr,
    sum (channel_inventory_usd) as channel_inventory_usd
from cbm_st_database2 ci
    join mdm.calendar cal
on ci.cal_date = cal.date
    join mdm.iso_country_code_xref iso on iso.country_alpha2 = ci.country_alpha2
where day_of_month = 1
  and fiscal_month in ('3.0'
    , '6.0'
    , '9.0'
    , '12.0')
  and region_3 = 'AMS'
  and fiscal_year_qtr <= '2021Q1'
  and market8 is not null
group by fiscal_year_qtr, embargoed_sanctioned_flag, cal_date, market8, pl),
    americas_ink_media as
    (
select fiscal_year_qtr,
    cal_date,
    geography,
    embargoed_sanctioned_flag,
    pl,
    sum (channel_inventory_usd) as ci_usd,
    sum (channel_inventory_usd * 0.983) as finance_ink_ci
from cbm_quarterly
group by fiscal_year_qtr,
    cal_date,
    geography,
    embargoed_sanctioned_flag,
    pl),
    americas_ink_media2 as
    (
select fiscal_year_qtr,
    cal_date,
    geography,
    embargoed_sanctioned_flag,
    case
    when pl = 'AU' then '1N'
    else pl
    end as pl,
    sum (finance_ink_ci) as ci_dollars
from americas_ink_media
group by fiscal_year_qtr,
    cal_date,
    geography,
    embargoed_sanctioned_flag,
    pl),
    ci_inventory_adjusted as
    (select fiscal_year_qtr,
    geography,
    embargoed_sanctioned_flag,
    pl,
    coalesce (sum (ci_dollars), 0) as ci_dollars
from americas_ink_media2
group by fiscal_year_qtr, embargoed_sanctioned_flag, geography, pl),
    ci_inventory_fully_adjusted as
    (
select fiscal_year_qtr,
    geography,
    pl,
    embargoed_sanctioned_flag,
    sum (ci_dollars) as ci_dollars
from ci_inventory_unadjusted
group by fiscal_year_qtr,
    geography,
    pl,
    embargoed_sanctioned_flag

union all

select fiscal_year_qtr,
    geography,
    pl,
    embargoed_sanctioned_flag,
    sum (ci_dollars) as ci_dollars
from ci_inventory_ams_post_adustment_period
group by fiscal_year_qtr,
    geography,
    pl,
    embargoed_sanctioned_flag

union all

select fiscal_year_qtr,
    geography,
    pl,
    embargoed_sanctioned_flag,
    sum (ci_dollars) as ci_dollars
from ci_inventory_adjusted
group by fiscal_year_qtr,
    geography,
    pl,
    embargoed_sanctioned_flag),

    region_table as
    (
select distinct market8 as geography
, case
	when market8 IN ('CENTRAL & EASTERN EUROPE', 'NORTHWEST EUROPE', 'SOUTHERN EUROPE, ME & AFRICA') then 'EMEA'
	when market8 IN ('LATIN AMERICA', 'NORTH AMERICA') then 'AMS'
	when market8 IN ('GREATER ASIA', 'GREATER CHINA', 'INDIA SL & BL') then 'APJ'
	else 'WW'
 end as region_3
, region_5
from mdm.iso_country_code_xref
where 1 = 1
  and region_3 is not null
  and region_5 <> 'JP'
  and region_5 <> 'XU'
  and market8 is not null
   ), 
    ci_inventory_fully_adjusted2 as
    (
select fiscal_year_qtr,
    ci.geography,
    region_3,
    embargoed_sanctioned_flag,
    ci.pl,
    technology,
    coalesce (sum (ci_dollars), 0) as ci_dollars
from ci_inventory_fully_adjusted ci
    left join mdm.product_line_xref plx
on ci.pl = plx.pl
    left join region_table iso on iso.geography = ci.geography
group by fiscal_year_qtr,
    ci.geography,
    region_3,
    embargoed_sanctioned_flag,
    ci.pl,
    technology),
    ci_inventory_fully_adjusted3 as
    (
select fiscal_year_qtr,
    geography,
    region_3,
    embargoed_sanctioned_flag,
    pl,
    case
    when technology = 'PWA' then 'INK'
    else technology
    end as technology,
    sum (ci_dollars) as ci_dollars
from ci_inventory_fully_adjusted2
group by fiscal_year_qtr,
    geography,
    region_3,
    embargoed_sanctioned_flag,
    pl,
    technology),
    ci_inventory_fully_adjusted4 as
    (
select fiscal_year_qtr,
    geography,
    region_3,
    pl,
    embargoed_sanctioned_flag,
    technology,
    sum (ci_dollars) as ci_dollars
from ci_inventory_fully_adjusted3
where region_3 <> 'EMEA'
   or technology <> 'INK'
   or fiscal_year_qtr <> '2018Q2'
group by fiscal_year_qtr,
    geography,
    region_3,
    pl,
    embargoed_sanctioned_flag,
    technology),
    market_ci_mix as
    (
select fiscal_year_qtr,
    geography,
    region_3,
    pl,
    embargoed_sanctioned_flag,
    technology,
    case
    when sum (ci_dollars) over (partition by fiscal_year_qtr, region_3, technology) = 0 then null
    else ci_dollars / sum (ci_dollars) over (partition by fiscal_year_qtr, region_3, technology)
    end as market_mix
from ci_inventory_fully_adjusted3 ci
group by fiscal_year_qtr,
    geography,
    region_3,
    pl,
    embargoed_sanctioned_flag,
    technology,
    ci_dollars),
    market_ci_mix2 as
    (
select fiscal_year_qtr,
    geography,
    region_3,
    embargoed_sanctioned_flag,
    pl,
    technology,
    coalesce (sum (market_mix), 0) as market_mix
from market_ci_mix
group by fiscal_year_qtr, embargoed_sanctioned_flag, geography, pl, technology, region_3),
    finance_source_ci as
    (
select fiscal_year_qtr,
    region_3,
    case
    when technology = 'TONER' then 'LASER'
    else technology
    end as technology,
    sum (finance_reported_ci) as finance_reported_ci
from fin_prod.ci_history_supplies_finance_landing ci
where region_3 <> 'WW'
group by fiscal_year_qtr, region_3, technology),
    finance_official_ci as
    (
select ci.fiscal_year_qtr,
    geography,
    ci.region_3,
    pl,
    embargoed_sanctioned_flag,
    ci.technology,
    sum (finance_reported_ci * coalesce (market_mix, 0)) as ci_dollars
from finance_source_ci ci
    left join market_ci_mix2 mix
on
    ci.fiscal_year_qtr = mix.fiscal_year_qtr and
    ci.region_3 = mix.region_3 and
    ci.technology = mix.technology
where 1 = 1
  and geography is not null
group by ci.fiscal_year_qtr, ci.region_3, ci.technology, pl, embargoed_sanctioned_flag, geography),
    finance_official_ci2 as
    (
select fiscal_year_qtr,
    geography,
    region_3,
    embargoed_sanctioned_flag,
    pl,
    technology,
    sum (ci_dollars) as ci_dollars
from finance_official_ci
where 1 = 1
  and ci_dollars <> 0
group by fiscal_year_qtr, region_3, embargoed_sanctioned_flag,technology, pl, geography),
    final_official_ci as
    (
select fiscal_year_qtr,
    geography,
    region_3,
    pl,
    embargoed_sanctioned_flag,
    technology,
    sum (ci_dollars) as ci_dollars
from finance_official_ci2
group by fiscal_year_qtr, region_3, technology, embargoed_sanctioned_flag, pl, geography

union all

select fiscal_year_qtr,
    geography,
    region_3,
    pl,
    embargoed_sanctioned_flag,
    technology,
    sum (ci_dollars) as ci_dollars
from ci_inventory_fully_adjusted4
where fiscal_year_qtr > '2017Q4'
group by fiscal_year_qtr, region_3, technology, embargoed_sanctioned_flag, pl, geography),
    final_official_ci2 as
    (
select fiscal_year_qtr,
    geography,
    region_3,
    embargoed_sanctioned_flag,
    pl,
    technology,
    coalesce (sum (ci_dollars), 0) as ci_dollars
from final_official_ci
where fiscal_year_qtr not in (select distinct fiscal_year_qtr
    from fin_prod.supplies_finance_flash
    where net_revenue <> 0
  and version =
    (select max (version) from fin_prod.supplies_finance_flash))
group by fiscal_year_qtr, region_3, technology, pl, embargoed_sanctioned_flag, geography)
select fiscal_year_qtr,
       geography,
       embargoed_sanctioned_flag,
       pl,
       'ACTUALS'       as record_description,
       sum(ci_dollars) as cbm_ci_dollars
from final_official_ci2
group by fiscal_year_qtr, geography, pl, embargoed_sanctioned_flag
""")

cbm_ci_data.createOrReplaceTempView("cbm_ci_data")

# COMMAND ----------

combined_adj_rev_flash_data = spark.sql("""
with adjusted_revenue_flash as
         (select fiscal_year_qtr,
                 fiscal_yr,
                 geography,
                 hq_flag,
                 pl,
                 record_description,
                 embargoed_sanctioned_flag,
                 sum(reported_revenue)       as reported_revenue,
                 sum(hedge)                  as hedge,
                 sum(currency)               as currency,
                 sum(revenue_in_cc)          as revenue_in_cc,
                 0                           as cbm_ci_dollars,
                 sum(inventory_change)       as inventory_change,
                 sum(ci_currency_impact)     as ci_currency_impact,
                 sum(total_inventory_impact) as total_inventory_impact,
                 sum(adjusted_revenue)       as adjusted_revenue
          from adjusted_revenue_data
          group by fiscal_year_qtr,
                   fiscal_yr,
                   geography,
                   hq_flag,
                   record_description,
                   embargoed_sanctioned_flag,
                   pl

          union all

          select fiscal_year_qtr,
                 fiscal_yr,
                 geography,
                 hq_flag,
                 pl,
                 record_description,
                 'N' as embargoed_sanctioned_flag,
                 sum(reported_revenue)       as reported_revenue,
                 sum(hedge)                  as hedge,
                 sum(currency)               as currency,
                 sum(revenue_in_cc)          as revenue_in_cc,
                 sum(cbm_ci_dollars)         as cbm_ci_dollars,
                 sum(inventory_change)       as inventory_change,
                 sum(ci_currency_impact)     as ci_currency_impact,
                 sum(total_inventory_impact) as total_inventory_impact,
                 sum(adjusted_revenue)       as adjusted_revenue
          from flash_data
          group by fiscal_year_qtr,
                   fiscal_yr,
                   geography,
                   hq_flag,
                   record_description,
                   pl

          union all

          select fiscal_year_qtr,
                 fiscal_yr,
                 geography,
                 hq_flag,
                 pl,
                 record_description,
                 'N' as embargoed_sanctioned_flag,
                 sum(reported_revenue)       as reported_revenue,
                 sum(hedge)                  as hedge,
                 sum(currency)               as currency,
                 sum(revenue_in_cc)          as revenue_in_cc,
                 0                           as cbm_ci_dollars,
                 sum(inventory_change)       as inventory_change,
                 sum(ci_currency_impact)     as ci_currency_impact,
                 sum(total_inventory_impact) as total_inventory_impact,
                 sum(adjusted_revenue)       as adjusted_revenue
          from zero_history_data
          group by fiscal_year_qtr,
                   fiscal_yr,
                   geography,
                   hq_flag,
                   record_description,
                   pl

          union all

          select cbm.fiscal_year_qtr,
                 fiscal_yr,
                 geography,
                 'NON-HQ'            as hq_flag,
                 pl,
                 record_description,
                 embargoed_sanctioned_flag,
                 0                   as reported_revenue,
                 0                   as hedge,
                 0                   as currency,
                 0                   as revenue_in_cc,
                 sum(cbm_ci_dollars) as cbm_ci_dollars,
                 0                   as inventory_change,
                 0                   as ci_currency_impact,
                 0                   as total_inventory_impact,
                 0                   as adjusted_revenue
          from cbm_ci_data cbm
                   left join mdm.calendar cal
                             on cbm.fiscal_year_qtr = cal.fiscal_year_qtr
          where 1 = 1
            and day_of_month = 1
            and fiscal_month in ('3.0', '6.0', '9.0', '12.0')
          group by cbm.fiscal_year_qtr,
                   fiscal_yr,
                   geography,
                   record_description,
                   embargoed_sanctioned_flag,
                   pl),
     adjusted_revenue_flash2 as
         (select cal.date                                 as cal_date,
                 arf.fiscal_year_qtr,
                 arf.fiscal_yr,
                 geography,
                 hq_flag,
                 pl,
                 record_description,
                 embargoed_sanctioned_flag,
                 coalesce(sum(reported_revenue), 0)       as reported_revenue,
                 coalesce(sum(hedge), 0)                  as hedge,
                 coalesce(sum(currency), 0)               as currency,
                 coalesce(sum(revenue_in_cc), 0)          as revenue_in_cc,
                 coalesce(sum(cbm_ci_dollars), 0)         as cbm_ci_dollars,
                 coalesce(sum(inventory_change), 0)       as inventory_change,
                 coalesce(sum(ci_currency_impact), 0)     as ci_currency_impact,
                 coalesce(sum(total_inventory_impact), 0) as total_inventory_impact,
                 coalesce(sum(adjusted_revenue), 0)       as adjusted_revenue
          from adjusted_revenue_flash arf
                   left join mdm.calendar cal
                             on arf.fiscal_year_qtr = cal.fiscal_year_qtr
          where 1 = 1
            and day_of_month = 1
            and fiscal_month in ('3.0', '6.0', '9.0', '12.0')
          group by cal.date,
                   arf.fiscal_year_qtr,
                   arf.fiscal_yr,
                   geography,
                   hq_flag,
                   record_description,
                   embargoed_sanctioned_flag,
                   pl)

select cal_date,
       fiscal_year_qtr,
       fiscal_yr,
       geography,
       hq_flag,
       pl,
       record_description,
       embargoed_sanctioned_flag,
       sum(reported_revenue)       as reported_revenue,
       sum(hedge)                  as hedge,
       sum(currency)               as currency,
       sum(revenue_in_cc)          as revenue_in_cc,
       sum(cbm_ci_dollars)         as cbm_ci_dollars,
       sum(inventory_change)       as inventory_change,
       sum(ci_currency_impact)     as ci_currency_impact,
       sum(total_inventory_impact) as total_inventory_impact,
       sum(adjusted_revenue)       as adjusted_revenue
from adjusted_revenue_flash2
group by cal_date,
         fiscal_year_qtr,
         fiscal_yr,
         geography,
         hq_flag,
         record_description,
         embargoed_sanctioned_flag,
         pl
""")

combined_adj_rev_flash_data.createOrReplaceTempView("combined_adj_rev_flash_data")

# COMMAND ----------

adj_rev_flash_lagged_2 = spark.sql("""
with date_helper as
    (select date_key
    , date as cal_date
    , fiscal_year_qtr
from mdm.calendar
where day_of_month = 1
  and fiscal_month in ('3.0'
    , '6.0'
    , '9.0'
    , '12.0'))
    , yoy_analytic_setup as
    (
select cal_date,
    fiscal_year_qtr,
    geography,
    pl,
    record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    reported_revenue,
    hedge,
    currency,
    revenue_in_cc,
    cbm_ci_dollars,
    inventory_change,
    ci_currency_impact,
    total_inventory_impact,
    adjusted_revenue,
    (select min (cal_date) from combined_adj_rev_flash_data)
    as min_cal_date,
    (select max (cal_date) from combined_adj_rev_flash_data) as max_cal_date
from combined_adj_rev_flash_data)
        , yoy_full_cross_join as
    (
select distinct d.cal_date,
    d.fiscal_year_qtr,
    geography,
    pl,
    hq_flag,
    embargoed_sanctioned_flag
from date_helper d
    cross join yoy_analytic_setup ar
where d.cal_date between ar.min_cal_date
  and ar.max_cal_date)
    , fill_gap1 as
    (
select c.cal_date,
    coalesce (c.fiscal_year_qtr, s.fiscal_year_qtr) as fiscal_year_qtr,
    coalesce (c.geography, s.geography) as geography,
    coalesce (c.pl, s.pl) as pl,
    coalesce (c.hq_flag, s.hq_flag) as hq_flag,
    coalesce (c.embargoed_sanctioned_flag, s.embargoed_sanctioned_flag) as embargoed_sanctioned_flag,
    reported_revenue,
    hedge,
    currency,
    revenue_in_cc,
    cbm_ci_dollars,
    inventory_change,
    ci_currency_impact,
    total_inventory_impact,
    adjusted_revenue,
    record_description
from yoy_full_cross_join c
    left join yoy_analytic_setup s
on
    c.cal_date = s.cal_date and
    c.fiscal_year_qtr = s.fiscal_year_qtr and
    c.geography = s.geography and
    c.pl = s.pl and
    c.embargoed_sanctioned_flag = s.embargoed_sanctioned_flag and
    c.hq_flag = s.hq_flag)
    , fill_gap2 as
    (
select cal_date,
    fiscal_year_qtr,
    geography,
    pl,
    record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    coalesce (sum (reported_revenue), 0) as reported_revenue,
    coalesce (sum (hedge), 0) as hedge,
    coalesce (sum (currency), 0) as currency,
    coalesce (sum (revenue_in_cc), 0) as revenue_in_cc,
    coalesce (sum (cbm_ci_dollars), 0) as cbm_ci_dollars,
    coalesce (sum (inventory_change), 0) as inventory_change,
    coalesce (sum (ci_currency_impact), 0) as ci_currency_impact,
    coalesce (sum (total_inventory_impact), 0) as total_inventory_impact,
    coalesce (sum (adjusted_revenue), 0) as adjusted_revenue,
    min (cal_date) over (partition by geography, pl order by cal_date) as min_cal_date
from fill_gap1
where 1=1
and fiscal_year_qtr in (select distinct fiscal_year_qtr from adjusted_revenue_data)
group by cal_date, fiscal_year_qtr, geography, pl, record_description, hq_flag, embargoed_sanctioned_flag)
, fill_gap3 as
    (
select cal_date,
    fiscal_year_qtr,
    geography,
    pl,
    record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    coalesce (sum (reported_revenue), 0) as reported_revenue,
    coalesce (sum (hedge), 0) as hedge,
    coalesce (sum (currency), 0) as currency,
    coalesce (sum (revenue_in_cc), 0) as revenue_in_cc,
    coalesce (sum (cbm_ci_dollars), 0) as cbm_ci_dollars,
    coalesce (sum (inventory_change), 0) as inventory_change,
    coalesce (sum (ci_currency_impact), 0) as ci_currency_impact,
    coalesce (sum (total_inventory_impact), 0) as total_inventory_impact,
    coalesce (sum (adjusted_revenue), 0) as adjusted_revenue,
    min (cal_date) over (partition by geography, pl order by cal_date) as min_cal_date
from fill_gap1
where 1=1
and fiscal_year_qtr in (select distinct fiscal_year_qtr from flash_data)
group by cal_date, fiscal_year_qtr, geography, pl, record_description, embargoed_sanctioned_flag, hq_flag)
, fill_gap4 as
    (
select cal_date,
    fiscal_year_qtr,
    geography,
    pl,
    'ACTUALS' as record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    reported_revenue,
    hedge,
    currency,
    revenue_in_cc,
    cbm_ci_dollars,
    inventory_change,
    ci_currency_impact,
    total_inventory_impact,
    adjusted_revenue,
    min_cal_date
from fill_gap2

union all

select cal_date,
    fiscal_year_qtr,
    geography,
    pl,
    'FLASH' as record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    reported_revenue,
    hedge,
    currency,
    revenue_in_cc,
    cbm_ci_dollars,
    inventory_change,
    ci_currency_impact,
    total_inventory_impact,
    adjusted_revenue,
    min_cal_date
from fill_gap3)
        , adjusted_rev_staging_time_series as
    (
select cal_date,
    min_cal_date,
    fiscal_year_qtr,
    geography,
    pl,
    record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    reported_revenue,
    hedge,
    currency,
    revenue_in_cc,
    cbm_ci_dollars,
    inventory_change,
    ci_currency_impact,
    total_inventory_impact,
    adjusted_revenue,
    case when min_cal_date < cast (current_date () - day (current_date ()) + 1 as date) then 1 else 0 end as actuals_flag,
    count (cal_date) over (partition by geography, pl
    order by cal_date rows between unbounded preceding and current row) as running_count
from fill_gap4)
        , adjusted_revenue_full_calendar as
    (
select cal_date,
    fiscal_year_qtr,
    geography,
    pl,
    record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    coalesce (sum (reported_revenue), 0) as reported_revenue,
    coalesce (sum (hedge), 0) as hedge,
    coalesce (sum (currency), 0) as currency,
    coalesce (sum (revenue_in_cc), 0) as revenue_in_cc,
    coalesce (sum (cbm_ci_dollars), 0) as cbm_ci_dollars,
    coalesce (sum (inventory_change), 0) as inventory_change,
    coalesce (sum (ci_currency_impact), 0) as ci_currency_impact,
    coalesce (sum (total_inventory_impact), 0) as total_inventory_impact,
    coalesce (sum (adjusted_revenue), 0) as adjusted_revenue
from adjusted_rev_staging_time_series ar
group by cal_date, geography, pl, fiscal_year_qtr, record_description, hq_flag, embargoed_sanctioned_flag)
        , adjusted_revenue_quarterly as
    (
select fiscal_year_qtr,
    geography,
    pl,
    record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    sum (reported_revenue) as reported_revenue,
    sum (hedge) as hedge,
    sum (currency) as currency,
    sum (revenue_in_cc) as revenue_in_cc,
    sum (cbm_ci_dollars) as cbm_ci_dollars,
    sum (inventory_change) as inventory_change,
    sum (ci_currency_impact) as ci_currency_impact,
    sum (total_inventory_impact) as total_inventory_impact,
    sum (adjusted_revenue) as adjusted_revenue
from adjusted_revenue_full_calendar
group by fiscal_year_qtr, geography, pl, record_description, hq_flag, embargoed_sanctioned_flag)
        , adjusted_revenue_staging_lagged as
    (
select fiscal_year_qtr,
    geography,
    pl,
    record_description,
    hq_flag,
    embargoed_sanctioned_flag,
    sum (reported_revenue) as reported_revenue,
    sum (hedge) as hedge,
    sum (currency) as currency,
    sum (revenue_in_cc) as revenue_in_cc,
    sum (cbm_ci_dollars) as cbm_ci_dollars,
    sum (inventory_change) as inventory_change,
    sum (ci_currency_impact) as ci_currency_impact,
    sum (total_inventory_impact) as total_inventory_impact,
    sum (adjusted_revenue) as adjusted_revenue,
    lag(coalesce (sum (reported_revenue), 0), 4) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as prior_yr_rept_revenue,      -- needs to be a full year lag
    lag(coalesce (sum (hedge), 0), 4) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as prior_yr_hedge,
    lag(coalesce (sum (currency), 0), 4) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as prior_yr_currency,
    lag(coalesce (sum (revenue_in_cc), 0), 4) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as prior_yr_revenue_in_cc,
    lag(coalesce (sum (inventory_change), 0), 4) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as inventory_change_prior_year,
    lag(coalesce (sum (ci_currency_impact), 0), 4) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as ci_currency_impact_prior_year,
    lag(coalesce (sum (total_inventory_impact), 0), 4) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as total_inventory_impact_prior_year,
    lag(coalesce (sum (adjusted_revenue), 0), 4) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as adjusted_revenue_prior_year,
    lag(coalesce (sum (cbm_ci_dollars), 0), 1) over
    (partition by geography, pl, embargoed_sanctioned_flag
    order by fiscal_year_qtr) as cbm_ci_dollars_prior_period -- prior period (a.k.a., quarter) lag
from adjusted_revenue_quarterly
group by fiscal_year_qtr,
    geography,
    pl,
    record_description,
    embargoed_sanctioned_flag,
    hq_flag)

select fiscal_year_qtr,
       geography,
       ar.pl,
       ar.record_description,
       hq_flag,
       embargoed_sanctioned_flag,
       coalesce
           (sum(reported_revenue), 0)
                                                           as reported_revenue,
       coalesce(sum(hedge), 0)                             as hedge,
       coalesce(sum(currency), 0)                          as currency,
       coalesce(sum(revenue_in_cc), 0)                     as revenue_in_cc,
       coalesce(sum(cbm_ci_dollars), 0)                    as cbm_ci_dollars,
       coalesce(sum(inventory_change), 0)                  as inventory_change,
       coalesce(sum(ci_currency_impact), 0)                as ci_currency_impact,
       coalesce(sum(total_inventory_impact), 0)            as total_inventory_impact,
       coalesce(sum(adjusted_revenue), 0)                  as adjusted_revenue,
       coalesce(sum(prior_yr_rept_revenue), 0)             as prior_yr_rept_revenue,
       coalesce(sum(prior_yr_hedge), 0)                    as prior_yr_hedge,
       coalesce(sum(prior_yr_currency), 0)                 as prior_yr_currency,
       coalesce(sum(prior_yr_revenue_in_cc), 0)            as prior_yr_revenue_in_cc,
       coalesce(sum(inventory_change_prior_year), 0)       as inventory_change_prior_year,
       coalesce(sum(ci_currency_impact_prior_year), 0)     as ci_currency_impact_prior_year,
       coalesce(sum(total_inventory_impact_prior_year), 0) as total_inventory_impact_prior_year,
       coalesce(sum(adjusted_revenue_prior_year), 0)       as adjusted_revenue_prior_year,
       coalesce(sum(cbm_ci_dollars_prior_period), 0)       as cbm_ci_dollars_prior_period
from adjusted_revenue_staging_lagged ar
group by fiscal_year_qtr,
         geography,
         ar.pl,
         record_description,
         embargoed_sanctioned_flag,
         hq_flag
""")

adj_rev_flash_lagged_2.createOrReplaceTempView("adjusted_revenue_flash_lagged2")

# COMMAND ----------

adj_rev_flash_output = spark.sql("""
with tableau_helper as
             (select date as cal_date,
    fiscal_year_qtr,
    fiscal_yr
from mdm.calendar
where day_of_month = 1
  and fiscal_month in ('3.0'
    , '6.0'
    , '9.0'
    , '12.0'))
    , region_mappings as
    (
      
        
select distinct market8 as geography
, case
	when market8 IN ('CENTRAL & EASTERN EUROPE', 'NORTHWEST EUROPE', 'SOUTHERN EUROPE, ME & AFRICA') then 'EMEA'
	when market8 IN ('LATIN AMERICA', 'NORTH AMERICA') then 'AMS'
	when market8 IN ('GREATER ASIA', 'GREATER CHINA', 'INDIA SL & BL') then 'APJ'
	else 'WW'
 end as region_3
, region_5
from mdm.iso_country_code_xref
where 1 = 1
  and region_3 is not null
  and region_5 <> 'JP'
  and region_5 <> 'XU'
  and market8 is not null
    
    )
    , accounting_rate_applied as
    (
select distinct accounting_rate
from adjusted_revenue_data)
        , market_applied as
    (
select distinct geography_grain
from adjusted_revenue_data)
        ,
    adjusted_revenue_flash_full_data as
    (
select record_description,
    cal.date as cal_date,
    mdb.fiscal_year_qtr,
    fiscal_yr,
    mdb.geography,
    hq_flag,
    mdb.pl,
    l5_description,
    technology,
    embargoed_sanctioned_flag,
    sum (reported_revenue) as reported_revenue,
    sum (hedge) as hedge,
    sum (currency) as currency,
    sum (revenue_in_cc) as revenue_in_cc,
    sum (cbm_ci_dollars) as cbm_ci_dollars,
    sum (inventory_change) as inventory_change,
    sum (ci_currency_impact) as ci_currency_impact,
    sum (total_inventory_impact) as total_inventory_impact,
    sum (adjusted_revenue) as adjusted_revenue,
    sum (prior_yr_rept_revenue) as prior_yr_rept_revenue,
    sum (prior_yr_hedge) as prior_yr_hedge,
    sum (prior_yr_currency) as prior_yr_currency,
    sum (prior_yr_revenue_in_cc) as prior_yr_revenue_in_cc,
    sum (inventory_change_prior_year) as inventory_change_prior_year,
    sum (ci_currency_impact_prior_year) as ci_currency_impact_prior_year,
    sum (total_inventory_impact_prior_year) as total_inventory_impact_prior_year,
    sum (adjusted_revenue_prior_year) as adjusted_revenue_prior_year,
    sum (cbm_ci_dollars_prior_period) as cbm_ci_dollars_prior_quarter
from adjusted_revenue_flash_lagged2 mdb
    left join mdm.calendar cal
on mdb.fiscal_year_qtr = cal.fiscal_year_qtr
    left join region_mappings iso on mdb.geography = iso.geography
    left join mdm.product_line_xref plx on plx.pl = mdb.pl
where 1 = 1
  and day_of_month = 1
  and fiscal_month in ('3.0'
    , '6.0'
    , '9.0'
    , '12.0')
group by cal.date,
    mdb.fiscal_year_qtr,
    mdb.geography,
    hq_flag,
    mdb.pl,
    fiscal_yr,
    l5_description,
    record_description,
    embargoed_sanctioned_flag,
    technology)

select 'ADJUSTED REVENUE PLUS FLASH'                                               as record,
       record_description,
       cal_date,
       fiscal_year_qtr,
       fiscal_yr,
       case
           when geography = 'WORLD WIDE' then 'OTHER GEO'
           when geography = 'EMEA' then 'EMEA CO'
           else geography
           end                                                                     as geography,
       (select geography_grain from market_applied)                                as geography_grain,
       case
           when geography in ('EMEA', 'CENTRAL & EASTERN EUROPE', 'NORTHWEST EUROPE', 'SOUTHERN EUROPE, ME & AFRICA')
               then 'EMEA'
           when geography in ('APJ HQ', 'INDIA SL & BL', 'GREATER ASIA', 'GREATER CHINA') then 'APJ'
           when geography in ('AMS HQ', 'LATIN AMERICA', 'NORTH AMERICA') then 'AMS'
           else 'WW'
           end                                                                     as region_3,
       case
           when geography in ('EMEA', 'CENTRAL & EASTERN EUROPE', 'NORTHWEST EUROPE', 'SOUTHERN EUROPE, ME & AFRICA')
               then 'EU'
           when geography in ('APJ HQ', 'INDIA SL & BL', 'GREATER ASIA', 'GREATER CHINA') then 'AP'
           when geography in ('AMS HQ', 'NORTH AMERICA') then 'NA'
           when geography = 'LATIN AMERICA' then 'LA'
           else 'XW'
           end                                                                     as region_5,
       case
           when geography in ('APJ HQ', 'AMS HQ') then 'HQ'
           else 'NON-HQ'
           end                                                                     as hq_flag,
       pl,
       l5_description,
       technology,
       embargoed_sanctioned_flag,
       (select accounting_rate from accounting_rate_applied)                       as accounting_rate,
       sum(reported_revenue)                                                       as reported_revenue,
       sum(hedge)                                                                  as hedge,
       sum(currency)                                                               as currency,
       sum(revenue_in_cc)                                                          as revenue_in_cc,
       sum(cbm_ci_dollars)                                                         as cbm_ci_dollars,
       sum(inventory_change)                                                       as inventory_change,
       sum(ci_currency_impact)                                                     as ci_currency_impact,
       sum(total_inventory_impact)                                                 as total_inventory_change,
       sum(adjusted_revenue)                                                       as adjusted_revenue,
       sum(prior_yr_rept_revenue)                                                  as reported_revenue_prior_year,
       sum(prior_yr_hedge)                                                         as hedge_prior_year,
       sum(prior_yr_currency)                                                      as currency_prior_year,
       sum(prior_yr_revenue_in_cc)                                                 as cc_revenue_prior_year,
       sum(inventory_change_prior_year)                                            as ci_change_prior_year,
       sum(ci_currency_impact_prior_year)                                          as ci_currency_change_prior_year,
       sum(total_inventory_impact_prior_year)                                      as total_ci_change_prior_year,
       sum(adjusted_revenue_prior_year)                                            as adjusted_revenue_prior_year,
       sum(cbm_ci_dollars_prior_quarter)                                           as cbm_ci_dollars_prior_quarter,
       1                                                                           as official,
       (select load_date
        from prod.version
        where record = 'ADJ REV PLUS FLASH'
          and load_date = (select max(load_date)
                           from prod.version
                           where record = 'ADJ REV PLUS FLASH'))                   as load_date,
       (select distinct version
        from prod.version
        where record = 'ADJ REV PLUS FLASH'
          and version = (select max(version)
                         from prod.version
                         where record = 'ADJ REV PLUS FLASH'
                           and load_date = (select max(load_date)
                                            from prod.version
                                            where record = 'ADJ REV PLUS FLASH'))) as version
from adjusted_revenue_flash_full_data
     --where fiscal_year_qtr <> '2022q1'
group by record_description,
         cal_date,
         fiscal_year_qtr,
         geography,
         pl,
         l5_description,
         technology,
         embargoed_sanctioned_flag,
         fiscal_yr
""")

adj_rev_flash_output.createOrReplaceTempView("adjusted_revenue_flash_output")

# COMMAND ----------

adj_rev_flash = spark.sql("""
select record,
       record_description,
       cal_date,
       fiscal_year_qtr,
       fiscal_yr,
       geography,
       geography_grain,
       region_3,
       region_5,
       hq_flag,
       pl,
       l5_description,
       technology,
       accounting_rate,
      -- embargoed_sanctioned_flag,
       sum(reported_revenue)              as reported_revenue,
       sum(hedge)                         as hedge,
       sum(currency)                      as currency,
       sum(revenue_in_cc)                 as revenue_in_cc,
       sum(cbm_ci_dollars)                as cbm_ci_dollars,
       sum(inventory_change)              as inventory_change,
       sum(ci_currency_impact)            as ci_currency_impact,
       sum(total_inventory_change)        as total_inventory_change,
       sum(adjusted_revenue)              as adjusted_revenue,
       sum(reported_revenue_prior_year)   as reported_revenue_prior_year,
       sum(hedge_prior_year)              as hedge_prior_year,
       sum(currency_prior_year)           as currency_prior_year,
       sum(cc_revenue_prior_year)         as cc_revenue_prior_year,
       sum(ci_change_prior_year)          as ci_change_prior_year,
       sum(ci_currency_change_prior_year) as ci_currency_change_prior_year,
       sum(total_ci_change_prior_year)    as total_ci_change_prior_year,
       sum(adjusted_revenue_prior_year)   as adjusted_revenue_prior_year,
       sum(cbm_ci_dollars_prior_quarter)  as cbm_ci_dollars_prior_quarter,
       CAST(1 AS BOOLEAN)                 as official,
       load_date,
       version
from adjusted_revenue_flash_output
group by record,
         record_description,
         cal_date,
         fiscal_year_qtr,
         geography,
         geography_grain,
         region_3,
         region_5,
         hq_flag,
         pl,
         l5_description,
         technology,
         fiscal_yr,
         accounting_rate,
       --  embargoed_sanctioned_flag,
         official,
         load_date,
         version
""")

#adj_rev_flash.createOrReplaceTempView("adj_rev_flash")
write_df_to_redshift(configs, adj_rev_flash, "fin_prod.adjusted_revenue_flash", "append")

# COMMAND ----------

import re

tables = [
    ['fin_prod.adjusted_revenue_flash', adj_rev_flash, 'overwrite']
]

for table in tables:
    # Define the input and output formats and paths and the table name.
    schema = table[0].split(".")[0]
    table_name = table[0].split(".")[1]
    mode = table[2]
    write_format = 'delta'
    save_path = f'/tmp/delta/{schema}/{table_name}'
    
    # Load the data from its source.
    df = table[1]
    renamed_df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
    print(f'loading {table[0]}...')
    # Write the data to its target.
    renamed_df.write \
      .format(write_format) \
      .mode(mode) \
      .option("overwriteSchema", "true")\
      .save(save_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create the table.
    spark.sql("CREATE TABLE IF NOT EXISTS " + table[0] + " USING DELTA LOCATION '" + save_path + "'")
    
    spark.table(table[0]).createOrReplaceTempView(table_name)
    
    print(f'{table[0]} loaded')

# COMMAND ----------

#grant team access
query_access_grant = """
GRANT ALL ON TABLE fin_prod.adjusted_revenue_flash TO GROUP phoenix_dev;
"""

submit_remote_query(configs, query_access_grant)

# COMMAND ----------


