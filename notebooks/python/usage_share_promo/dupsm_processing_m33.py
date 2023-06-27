# Databricks notebook source
import os
import sys

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col

try:
    from notebooks.python.common.configs import analyst_constants, configs, constants, stack, secrets_get
    from notebooks.python.common.database_utils import read_redshift_to_df, write_df_to_s3
    from notebooks.python.common.datetime_utils import Date
except ModuleNotFoundError:
    print("running on Databricks")

# COMMAND ----------

# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# MAGIC %run ../common/datetime_utils

# COMMAND ----------

# COPIED FROM SOURCE
# Script info/instructions

# Name: DUPSM_M33_processing
# Author: Audrey Dickinson
# Date: 01/26/22

# The following code is intended to serve as the backbone for the 100 IB process.
# It pulls the necessary data from the Insights Environment, handles modeling
# selections, applies COVID impacts, and handles formatting.

# SUGGESTED USAGE
# connect to dbs and review available DUPSM and IB versions. Verify all settings
# in settings section including active time and max DUPSM date pulled. Run sections
# noted below one at a time and verify output after each section.

# SECTIONS:
# 1) Data - collect all input data including MPS, DUPSM, IB, and reference files
# 2) MPS additions to IB- integrates MPS IB with traditional IB (sourced from IE)
# 3) Proxy setting - sets proxy logic
# 4) DUPSM processing - locks current DUPSM to previous DUPSM where there is no telemetry
# 6) MPS additions - joins traditional IB and MPS IB after joining DUPSM
# 7) Fill logic - fills gaps in DUPSM data with info from EPA/NPI override table
# 8) COVID adjustments- calcs COVID impact and applies to 100% IB
# 9) Final Data Prep
# 10) Output data frames - creates summary tables to be included in output report
# 11) Final data polishing- modification of output report to desired specifications
# 12) Graphs - high level check of results
# 13) Create workbook - final product (this section has a code chunk to save environment data)

# MPS - What We are Doing:
  
# The purpose of addition of the MPS data to the dataset is to bring the data 100%IB
# processes into parity with the BD dashboards and what they present to executives.
# That means adding D and P MPS data to the dataset. cMPS data is not added for
# two reasons; the related IB is likely not mature enough for that application,
# and it is not included in what executives see in their weekly reports.

# The process for adding MPS data is subtraction. We take the MPS IB and subtract
# IE IB. The Transactional IB is then united with Usage and Share from the DUPSM.
# The MPS IB Usage and Share data is stacked onto the bottom of the dataset. Both
# datasets operate at the month-country-platform subset level. Because the MPS
# data is essentially a rider on the DUPSM dataset, you can check it by ensuring
# that IB and Pages at the input are the same as the IB and Pages at the output.
# SOURCE - Greg Paoli


# Covidization- What We are Doing:
  
#   Covid impact is modeled on BD to create covid impact coefficients at the region 5
# and business function (OPS/HPS) level. These coefficients are applied to modeled
# data. This process assumes modeled data is ignorant of the COVID crisis.
# This strategy is under review. Was originally applied at M33 level. Changed 2021 Q3 Pulse.


# Proxy Locking - What We are Doing:
  
#   Locks DUPSM modeled data to a previous DUPSM version to avoid instability in
# 100% IB report. Current Direct BD is used while any proxied data is not.


# Fill Logic - What We are Doing:
  
#   The 100% IB is built off of the IB- some matures have historically been missing. 
# Additionally, some NPIs are joined in during this step. This should be sunsetted 
# when IE provides NPIs and Matures. Fill logic is an attempt to remedy this. The
# source for fill logic is the NPI data and a matures overrides table.


# *for more info see DUPSM_logic_diagrams.pptx doc

# COMMAND ----------

def get_dupsm_configs(common_configs: dict) -> dict:
    """Extends configs

    :param common_configs: Dictionary of common configs
    :return: Extended dictionary of configs
    """
    extended_configs = common_configs
    # widget/input parameters
    extended_configs['usage_version'] = dbutils.widgets.get('usage_version')
    extended_configs['reporting_from'] = dbutils.widgets.get('reporting_from')
    extended_configs['reporting_to'] = dbutils.widgets.get('reporting_to')
    extended_configs['ib_version'] = dbutils.widgets.get('ib_version')
    extended_configs['covid_end_quarter'] = dbutils.widgets.get('covid_end_quarter')
    extended_configs['output_label'] = dbutils.widgets.get('output_label')

    # file location and save R data
    extended_configs['s3_base_dir'] = analyst_constants['S3_BASE_BUCKET'][stack]

    # Turn on/off effect of covid
    extended_configs['covid_on'] = "ON"

    # Turn on/off mps additions -
    # If off, will use transactional IB alone.
    # If on, will carve out MPS IB and join back in transactional IB
    extended_configs['mps_additions'] = "ON"

    extended_configs['output_path'] = f"{constants['S3_BASE_BUCKET'][stack]}cupsm_ites/"
    extended_configs['date'] = Date().getDatestamp("%Y-%m-%d")

    return extended_configs


# COMMAND ----------

def compare_demand_and_widget_ib_versions(demand: DataFrame, widget_ib_version: str) -> bool:
    demand_ib_version = demand.select(col('ib_version')).distinct().head()[0]
    if widget_ib_version != demand_ib_version:
        raise Exception("IB versions do not match")
    return True


# COMMAND ----------

def get_redshift_ref(query: str) -> DataFrame:
    df = read_redshift_to_df(configs) \
        .option('query', query) \
        .load()
    return df


# COMMAND ----------
def get_spectrum_ref(spark: SparkSession, bucket_prefix: str) -> DataFrame:
    df = spark.read.parquet(f'{constants["S3_BASE_BUCKET"][stack]}/spectrum/{bucket_prefix}')
    return df


# COMMAND ----------

def get_geo_ref(raw_data: dict) -> dict:
    query = """
    WITH iso_cc_rollup_xref AS (
        SELECT
            country_alpha2,
            country_level_2 AS market9
        FROM mdm.iso_cc_rollup_xref 
        WHERE country_scenario = 'MARKET9'
    )
    SELECT 
        iccx.country_alpha2,
        market8,
        market10,
        market9,
        region_5, 
        region_3,
        country as ie_country,
        developed_emerging,
        embargoed_sanctioned_flag
    FROM mdm.iso_country_code_xref iccx
    LEFT JOIN iso_cc_rollup_xref icrx
    ON iccx.country_alpha2 = icrx.country_alpha2
    WHERE region_5 not like 'X%' 
    """
    raw_data['geo_ref'] = get_redshift_ref(query)
    return raw_data


# COMMAND ----------

def get_hw_ref(raw_data: dict) -> dict:
    query = """
    SELECT *
    FROM mdm.hardware_xref
    WHERE technology='LASER'
    """
    raw_data['hw_ref'] = get_redshift_ref(query)
    return raw_data


# COMMAND ----------

def get_demand(spark: SparkSession, raw_data: dict, bucket_prefix: str, ib_version: str) -> dict:
    df = get_spectrum_ref(spark, bucket_prefix)
    if compare_demand_and_widget_ib_versions(df, ib_version):
        raw_data['demand'] = df
    return raw_data


# COMMAND ----------

def get_ib(raw_data: dict, ib_version: str, reporting_from: str, reporting_to: str) -> dict:
    query = f"""
    SELECT cal_date,
        country_alpha2, 
        platform_subset, 
        case  
            when platform_subset like '%STND%' THEN 'STD'
            when platform_subset like '%YET%' THEN 'HP+'
            ELSE customer_engagement 
        END as customer_engagement,
        sum(units) as units
    FROM prod.ib
    WHERE version = '{ib_version}'
        AND cal_date >= '{reporting_from}'
        AND cal_date <= '{reporting_to}'
        AND measure = 'IB'
    AND platform_subset in (SELECT distinct platform_subset from mdm.hardware_xref where technology='LASER')
    GROUP BY country_alpha2, customer_engagement, platform_subset, cal_date
    """
    raw_data['ib_pre'] = get_redshift_ref(query)
    return raw_data


# COMMAND ----------

def get_usage(spark: SparkSession, raw_data: dict, reporting_from: str, reporting_to: str) -> dict:
    raw_data['demand'].createOrReplaceTempView('demand')
    raw_data['hw_ref'].createOrReplaceTempView('hw_ref')
    usage = spark.sql(f"""
    SELECT cal_date, 
        geography AS country_alpha2, 
        platform_subset, 
        CASE 
        WHEN platform_subset LIKE '%STND%' THEN 'STD'
        WHEN platform_subset LIKE '%YET1%' THEN 'HP+'
        ELSE customer_engagement
        END AS customer_engagement,
        measure,
        sum(units) AS units,
        version,
        load_date,
        ib_version,
        source
    FROM demand
    WHERE measure IN ('K_USAGE','COLOR_USAGE','HP_SHARE')
        AND platform_subset  IN (SELECT platform_subset FROM hw_ref)
        AND cal_date >= '{reporting_from}'
        AND cal_date <= '{reporting_to}'
    GROUP BY country_alpha2, customer_engagement, platform_subset, cal_date, measure, version, load_date, ib_version, source 
    """)
    raw_data['usage'] = usage
    return raw_data


# COMMAND ----------

def get_ozzy_mps(spark: SparkSession, raw_data: dict) -> dict:
    # retrieve ozzy secrets
    ozzy_secret = secrets_get(constants["OZZY_SECRET_NAME"][stack], "us-west-2")

    # build query to pull data from OZZY server
    ozzy_mps_query = """
    SELECT 
        Country as MPS_country,
        Platform_Subset as platform_subset, 
        Month as cal_date, 
        sum(Pages_Rptd_Mono) AS Pages_Rptd_Mono, 
        sum(Pages_Rptd_Color) AS Pages_Rptd_Color, 
        sum(Pages_Rptd) AS Pages_Rptd,
        case  
            WHEN Platform_Subset like '%STND%' THEN 'STD'
            WHEN Platform_Subset like '%YET%' THEN 'HP+'
        ELSE 'TRAD' 
        END AS customer_engagement,
        sum(zDoNotUseDevIB) AS IB
    FROM ozzy.dbo.Planning_Install_Base_Details_to_ProductNbr_FY16_Forward
    WHERE Category_Grp NOT IN ('Ink', 'LF', 'Other', 'MV', 'Non-Printing')
      AND Is_This_a_Printer = 'Yes'
      AND Month >= '2015-01-01'
      AND BusinessTypeGrp NOT IN ('-', 'Other')
    GROUP BY Country, Platform_Subset, Month
    """

    # retrieve dataframe
    ozzy_mps_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://{ozzy_secret['host']}:{ozzy_secret['port']};trustServerCertificate=true;") \
        .option("user", ozzy_secret["username"]) \
        .option("password", ozzy_secret["password"]) \
        .option("query", ozzy_mps_query) \
        .load()

    raw_data['ozzy_mps'] = ozzy_mps_df
    return raw_data


# COMMAND ----------

def get_epa_published(spark: SparkSession, raw_data, file_ref):
    raw_data['epa_published'] = spark.read.csv(file_ref, header=True)
    return raw_data


# COMMAND ----------

def get_plat_family(spark: SparkSession, raw_data, file_ref):
    raw_data['plat_family'] = spark.read.csv(file_ref, header=True)
    return raw_data


# COMMAND ----------

def get_calendar(raw_data: dict) -> dict:
    calendar_query = """
    SELECT date, fiscal_yr, fiscal_year_qtr
    FROM mdm.calendar
    """
    calendar = read_redshift_to_df(configs) \
        .option('query', calendar_query) \
        .load()
    raw_data['calendar'] = calendar
    return raw_data


# COMMAND ----------
def get_mps_ib_tie_out(spark: SparkSession, raw_data: dict) -> dict:
    # source this from Rachel Lalley
    columns = ["FY", "mps_ib", "mps_pages", "A3_ib", "A4_ib"]
    data = [
        ("2023", 1484499, 36839022797, 220326, 1264083),
        ("2024", 1517288, 36063636495, 227333, 1289955),
        ("2025", 1657237, 36492403523, 256302, 1400934),
        ("2026", 1861436, 38503646838, 300944, 1560492),
        ("2027", 2107942, 41251124126, 360796, 1747146)
    ]
    raw_data['mps_ib_tie_out'] = spark.createDataFrame(data, columns)
    return raw_data


# COMMAND ----------

def extract_data(spark: SparkSession, extended_configs: dict) -> dict:
    """Load raw data from Redshift

    :param spark: SparkSession object
    :param extended_configs: Dictionary of configs
    :return: Dictionary of input DataFrames
    """
    raw_data = {}

    raw_data_w_geo_ref = get_geo_ref(raw_data=raw_data)

    raw_data_w_hw_ref = get_hw_ref(raw_data=raw_data_w_geo_ref)

    raw_data_w_demand = get_demand(
        spark=spark,
        raw_data=raw_data_w_hw_ref,
        bucket_prefix=f'demand/{extended_configs["usage_version"]}',
        ib_version=extended_configs["ib_version"]
    )

    raw_data_w_ib = get_ib(
        raw_data=raw_data_w_demand,
        ib_version=extended_configs["ib_version"],
        reporting_from=extended_configs["reporting_from"],
        reporting_to=extended_configs["reporting_to"]
    )

    raw_data_w_usage = get_usage(
        spark=spark,
        raw_data=raw_data_w_ib,
        reporting_from=extended_configs["reporting_from"],
        reporting_to=extended_configs["reporting_to"]
    )

    raw_data_w_ozzy_mps = get_ozzy_mps(
        spark=spark,
        raw_data=raw_data_w_usage
    )

    raw_data_w_epa_published = get_epa_published(
        spark=spark,
        raw_data=raw_data_w_ozzy_mps,
        file_ref=f'{extended_configs["s3_base_dir"]}/DUPSM_M33_processing_flat_files/epa_published.csv'
    )

    raw_data_w_calendar = get_calendar(raw_data=raw_data_w_epa_published)

    raw_data_w_mps_ib_tie_out = get_mps_ib_tie_out(spark=spark, raw_data=raw_data_w_calendar)

    return raw_data_w_mps_ib_tie_out


# COMMAND ----------

def transform_geo_ref(spark: SparkSession, geo_ref: DataFrame) -> DataFrame:
    # > GEO data
    # took out brazil and replaced with Chile 11/22
    # M34 addition
    geo_ref.createOrReplaceTempView('geo_ref')
    geo_ref = spark.sql("""
      SELECT geo.country_alpha2, geo.market10, geo.region_5, geo.region_3, geo.developed_emerging, geo.embargoed_sanctioned_flag,
          case 
            when geo.country_alpha2 in ("US", "CN", "DE", "CA", "FR", "GB", "AU", "IT", "ES", "CH", "IN", "MX", 'CL',"SE", "NL", "AE", "PL", "KR", "SA",  "TR", "TH", "HK", "TW", "BE") then geo.ie_country  --removed "RU"
            when geo.country_alpha2 in ('RU','BY') then 'RUSSIA/BELARUS'
            when geo.market8 == 'CENTRAL & EASTERN EUROPE' and geo.market9='ISE' then 'REST_OF_CENTRAL_EUROPE'
            when geo.market8 == 'SOUTHERN EUROPE, ME & AFRICA' and geo.market9='ISE' then 'REST_OF_SOUTHERN EUROPE'
            else 'REST_OF_'||geo.market9
          end as market_33,
          case 
            when geo.country_alpha2 in ("US", "CN", "DE", "CA", "FR", "GB", "AU", "IT", "ES", "CH", "IN", "MX", 'CL',"SE", "NL", "AE", "PL", "KR", "SA",  "TR", "TH", "HK", "TW", "BE") then geo.ie_country --removed "RU"
            when geo.country_alpha2 in ('RU','BY') then 'RUSSIA/BELARUS'
            else 'REST_OF_'||geo.market8
          end as m33_m8,
          case
            when geo.country_alpha2 in ('RU','BY') then 'RUSSIA/BELARUS'
            else market8
          end as market8r,
          market8,
          case
            when geo.country_alpha2 in ('RU','BY') then 'RUSSIA/BELARUS'
            else market9
          end as market9r,
          market9, 
          case
            when geo.ie_country="COTE D'IVOIRE" then "CÔTE D'IVOIRE"
            when geo.ie_country="MOLDOVA" then "MOLDOVA, REPUBLIC OF"
            when geo.ie_country="RUSSIAN FEDERATION" then "RUSSIA"
            when geo.ie_country="TANZANIA" then "TANZANIA, UNITED REPUBLIC OF"
            when geo.ie_country="UNITED STATES" then "USA"
            when geo.ie_country="UNITED KINGDOM" then "UK"
            when geo.ie_country="CZECHIA" then "CZECH REPUBLIC"
            else geo.ie_country
          end as mps_country
      FROM geo_ref geo
    """)
    return geo_ref


# COMMAND ----------

def transform_mps_ib_pre(spark: SparkSession, ozzy_mps: DataFrame, hw_ref: DataFrame) -> DataFrame:
    ozzy_mps.createOrReplaceTempView('ozzy_mps')
    hw_ref.createOrReplaceTempView('hw_ref')
    mps_ib_pre = spark.sql("""
    SELECT
      mi.MPS_country,
      mi.platform_subset,
      mi.cal_date,
      CASE 
        WHEN hx.mono_color='MONO' THEN mi.Pages_Rptd
        ELSE mi.Pages_Rptd_Mono
        END AS Pages_Rptd_Mono,
      CASE 
        WHEN hx.mono_color='MONO' THEN 0
        ELSE mi.Pages_Rptd_Color
        END AS Pages_Rptd_Color,
      Pages_Rptd,
      mi.customer_engagement,
      mi.IB
    FROM ozzy_mps mi
    LEFT JOIN hw_ref hx
    ON mi.platform_subset = hx.platform_subset
    """)
    return mps_ib_pre


# COMMAND ----------

def transform_plat_family_ref(spark: SparkSession, ib_pre: DataFrame, mps_ib_pre: DataFrame, hw_ref: DataFrame) -> DataFrame:
    ib_pre.createOrReplaceTempView('ib_pre')
    mps_ib_pre.createOrReplaceTempView('mps_ib_pre')
    hw_ref.createOrReplaceTempView('hw_ref')
    plat_family_ref = spark.sql("""
    WITH step1 AS (
        SELECT DISTINCT 
            platform_subset
        FROM ib_pre
        UNION 
        SELECT DISTINCT 
            platform_subset 
        FROM mps_ib_pre
    ), step2 AS (
        SELECT DISTINCT
            step1.platform_subset, 
            CASE 
              WHEN hw_ref.hw_product_family LIKE '% CN' THEN replace(hw_ref.hw_product_family,' CN','') 
              WHEN hw_ref.hw_product_family LIKE '% EPA' THEN replace(hw_ref.hw_product_family,' EPA','')
              WHEN hw_ref.hw_product_family LIKE '% EM' THEN replace(hw_ref.hw_product_family,' EM','')
              WHEN hw_ref.hw_product_family LIKE '% DM' THEN replace(hw_ref.hw_product_family,' DM','')
              WHEN hw_ref.hw_product_family LIKE '% MGD' THEN replace(hw_ref.hw_product_family,' MGD','')
              ELSE hw_ref.hw_product_family
            END AS IE_name_mod,
            hw_ref.epa_family as Family 
        FROM step1
        LEFT JOIN hw_ref
            ON step1.platform_subset=hw_ref.platform_subset
    )
    SELECT DISTINCT Family, platform_subset FROM step2
    """)
    return plat_family_ref


# COMMAND ----------

def transform_usage_share_ib_prep(spark: SparkSession, mps_trad_ib, dupsm_combined) -> DataFrame:
    mps_trad_ib.createOrReplaceTempView('mps_trad_ib')
    dupsm_combined.createOrReplaceTempView('dupsm_combined')
    usage_share_ib_prep = spark.sql("""
    WITH step1 AS (
        SELECT
            mps.cal_date
            , mps.country_alpha2
            , mps.platform_subset
            , mps.FYQTR
            , dum.customer_engagement
            , mps.route_to_market
            , dum.mono_color
            , dum.Family
            , CASE WHEN dum.measure = 'USAGE' AND mps.route_to_market='TRANSACTIONAL' THEN dum.units
                 WHEN mps.route_to_market='DPMPS' THEN mps.MPS_usage
                 ELSE 0 END AS units_USAGE
            , CASE WHEN dum.measure = 'COLOR_USAGE' AND mps.route_to_market='TRANSACTIONAL' THEN dum.units
                 WHEN mps.route_to_market='DPMPS' THEN mps.MPS_color_usage
                 ELSE 0 END AS units_COLOR_USAGE
            , CASE WHEN dum.measure = 'HP_SHARE' AND mps.route_to_market='TRANSACTIONAL' THEN dum.units
                 WHEN mps.route_to_market='DPMPS' THEN 1
                 ELSE 0 END AS units_HP_SHARE
            , CASE WHEN dum.measure='USAGE' AND mps.route_to_market='TRANSACTIONAL' THEN dum.source
                WHEN Family like 'SEG%' THEN 'PROXIED'
                WHEN mps.route_to_market='DPMPS' THEN 'DPMPS' 
                ELSE NULL END AS source_USAGE
            , CASE WHEN dum.measure='COLOR_USAGE' AND mps.route_to_market='TRANSACTIONAL' THEN dum.source
                WHEN Family like 'SEG%' THEN 'PROXIED'
                WHEN mps.route_to_market='DPMPS' THEN 'DPMPS' 
                ELSE NULL END AS source_COLOR_USAGE
            , CASE WHEN dum.measure='HP_SHARE' AND mps.route_to_market='TRANSACTIONAL' THEN dum.source
                WHEN Family like 'SEG%' THEN 'PROXIED'
                WHEN mps.route_to_market='DPMPS' THEN 'DPMPS' 
                ELSE NULL END AS source_HP_SHARE
        FROM mps_trad_ib mps
        LEFT JOIN dupsm_combined dum
        ON mps.cal_date=dum.cal_date
            AND mps.country_alpha2=dum.country_alpha2
            AND mps.platform_subset=dum.platform_subset
            AND mps.route_to_market=dum.route_to_market
    ) ,step2 AS (
        SELECT
            cal_date
            , country_alpha2
            , platform_subset
            , customer_engagement
            , route_to_market
            , mono_color
            , Family
            , FYQTR
            , sum(units_USAGE) as units_USAGE
            , sum(units_COLOR_USAGE) as units_COLOR_USAGE
            , sum(units_HP_SHARE) as units_HP_SHARE
            , max(source_USAGE) as source_USAGE
            , max(source_COLOR_USAGE) as source_COLOR_USAGE
            , max(source_HP_SHARE) as source_HP_SHARE
        FROM step1
        GROUP BY cal_date, country_alpha2, platform_subset, customer_engagement, route_to_market, mono_color, Family, FYQTR
    )
    SELECT 
        step2.cal_date
        , step2.country_alpha2
        , step2.platform_subset
        , CASE WHEN step2.customer_engagement IS NULL THEN 'TRAD' ELSE step2.customer_engagement END AS customer_engagement
        , step2.route_to_market, step2.mono_color, step2.Family
            , mps.Ib, step2.units_USAGE, step2.units_COLOR_USAGE, step2.units_HP_SHARE, step2.FYQTR, step2.source_USAGE, step2.source_COLOR_USAGE, step2.source_HP_SHARE 
        , CASE WHEN units_HP_SHARE IS NULL THEN 1
                    WHEN units_USAGE IS NULL THEN 1
                    WHEN mono_color='COLOR' AND units_COLOR_USAGE IS NULL THEN 1
                    ELSE 0 END AS missing_flag
    FROM step2
    LEFT JOIN mps_trad_ib mps
    ON step2.cal_date=mps.cal_date 
        AND step2.country_alpha2=mps.country_alpha2 
        AND step2.platform_subset=mps.platform_subset
        AND step2.route_to_market=mps.route_to_market
    """)
    return usage_share_ib_prep


# COMMAND ----------

def transform_final_dupsm(spark: SparkSession, usage_share_ib_prep: DataFrame) -> DataFrame:
    usage_share_ib_prep.createOrReplaceTempView('usage_share_ib_prep')
    final_dupsm = spark.sql("""
    SELECT 
        cal_date
        , country_alpha2
        , platform_subset
        , customer_engagement
        , route_to_market
        , mono_color
        , Family
        , source_USAGE
        , source_COLOR_USAGE
        , source_HP_SHARE
        , IB
        , units_USAGE
        , units_COLOR_USAGE
        , units_HP_SHARE
        , FYQTR  
    FROM usage_share_ib_prep
    WHERE missing_flag != 1
    """)
    return final_dupsm


# COMMAND ----------
def transform_ib_dupsm_covid_prep(spark: SparkSession, final_dupsm: DataFrame, geo_ref: DataFrame) -> DataFrame:
    final_dupsm.createOrReplaceTempView('final_dupsm')
    geo_ref.createOrReplaceTempView('geo_ref')
    ib_dupsm_covid_prep0 = spark.sql("""
    SELECT 
        final_dupsm.route_to_market
        ,final_dupsm.customer_engagement
        , final_dupsm.platform_subset
        , geo_ref.market9
        , final_dupsm.country_alpha2
        , final_dupsm.FYQTR
        , final_dupsm.cal_date
        --, final_dupsm.source_USAGE
        , final_dupsm.source_HP_SHARE
        , final_dupsm.IB
        , final_dupsm.source_USAGE
        , final_dupsm.source_COLOR_USAGE
        , final_dupsm.source_HP_SHARE
        , CASE WHEN final_dupsm.units_COLOR_USAGE IS NOT NULL THEN final_dupsm.units_COLOR_USAGE+final_dupsm.units_USAGE
              ELSE final_dupsm.units_USAGE END AS usage
        , final_dupsm.units_USAGE*final_dupsm.IB AS black_pages
        , final_dupsm.units_COLOR_USAGE*final_dupsm.IB AS color_pages
        , (final_dupsm.units_USAGE+final_dupsm.units_COLOR_USAGE)*final_dupsm.IB AS total_pages
        , (final_dupsm.units_USAGE+final_dupsm.units_COLOR_USAGE)*final_dupsm.IB*units_HP_SHARE AS hp_pages
        , CASE WHEN source_USAGE='TELEMETRY' THEN (final_dupsm.units_USAGE+final_dupsm.units_COLOR_USAGE)*final_dupsm.IB 
            ELSE NULL END AS bd_usage_pages
        , CASE WHEN source_HP_SHARE='TELEMETRY' THEN (final_dupsm.units_USAGE+final_dupsm.units_COLOR_USAGE)*final_dupsm.IB*units_HP_SHARE 
            ELSE NULL END AS bd_share_pages
    FROM final_dupsm
    LEFT JOIN geo_ref 
        ON final_dupsm.country_alpha2=geo_ref.country_alpha2                  
    """)
    return ib_dupsm_covid_prep0


# COMMAND ----------

def transform_ib_dupsm_covid(spark: SparkSession, ib_dupsm_covid_prep: DataFrame, geo_ref: DataFrame):
    ib_dupsm_covid_prep.createOrReplaceTempView('ib_dupsm_covid_prep')
    geo_ref.createOrReplaceTempView('geo_ref')
    ib_dupsm_covid = spark.sql("""
    WITH step1 AS (
        SELECT 
            ib.route_to_market
            , ib.customer_engagement
            , ib.platform_subset
            , gr.region_5
            , gr.market_33
            , gr.market9r as market9
            , gr.market8r as market8
            , gr.developed_emerging
            , ib.FYQTR
            , ib.total_pages
            , ib.hp_pages
            , ib.hp_pages/ib.total_pages as covid_share 
            , ib.covid_total_pages/ib.IB as covid_usage
            , ib.covid_total_pages*ib.hp_pages/ib.total_pages as covid_hp_pages
            , ib.covid_total_pages
            , ib.bd_usage_pages
            , ib.bd_share_pages
            , ib.color_pages
            , ib.black_pages
            , ib.IB
            , ib.color_pages/ib.total_pages*ib.covid_total_pages as covid_color_pages
            , ib.black_pages/ib.total_pages*ib.covid_total_pages as covid_black_pages
    FROM ib_dupsm_covid_prep ib
    LEFT JOIN geo_ref gr
        ON ib.country_alpha2=gr.country_alpha2
    ) SELECT 
        route_to_market
        , customer_engagement
        , platform_subset
        , region_5
        , market_33
        , market9
        , market8
        , developed_emerging
        , FYQTR
        , sum(hp_pages) as hp_pages
        , sum(total_pages) as total_pages
        , sum(covid_hp_pages) as covid_hp_pages
        , sum(covid_total_pages) as covid_total_pages
        , sum(covid_color_pages) as covid_color_pages
        , sum(covid_black_pages) as covid_black_pages
        , sum(color_pages) as color_pages
        , sum(black_pages) as black_pages
        , sum(IB) as IB
        , sum(bd_usage_pages) AS bd_usage_pages
        , sum(bd_share_pages) AS bd_share_pages
        , sum(bd_usage_pages)/sum(total_pages)*100 as bd_usage_telem_pct
        , sum(bd_share_pages)/sum(total_pages)*100 as bd_share_telem_pct
    FROM step1 
    GROUP BY 
        route_to_market
        , customer_engagement
        , platform_subset
        , region_5
        , market_33
        , market9
        , market8
        , developed_emerging
        , FYQTR
    """)
    return ib_dupsm_covid


# COMMAND ----------

def join_final_data(spark: SparkSession, ib_dupsm_covid: DataFrame, plat_family_ref: DataFrame, hw_ref: DataFrame, geo_ref: DataFrame):
    ib_dupsm_covid.createOrReplaceTempView('ib_dupsm_covid')
    plat_family_ref.createOrReplaceTempView('plat_family_ref')
    hw_ref.createOrReplaceTempView('hw_ref')
    geo_ref.createOrReplaceTempView('geo_ref')
    df = spark.sql("""
    SELECT idc.route_to_market as RTM, idc.customer_engagement, pfr.Family
        , CASE WHEN pfr.Family like 'PRNT CRTG CLJ%' THEN substr(pfr.Family,15,length(pfr.Family))
               WHEN pfr.Family like 'TONER CLJ%' THEN substr(pfr.Family,11,length(pfr.Family))
               WHEN pfr.Family like 'PRINT CRTG%' THEN substr(pfr.Family,12,length(pfr.Family))
               WHEN pfr.Family like 'TONER LJ%' THEN substr(pfr.Family,10,length(pfr.Family))
               WHEN pfr.Family like 'PRNT CRTG%' THEN substr(pfr.Family,11,length(pfr.Family))
               WHEN pfr.Family like 'TONER CLZ%' THEN substr(pfr.Family,11,length(pfr.Family))
               WHEN pfr.Family like 'TONER LZ%' THEN substr(pfr.Family,10,length(pfr.Family))
               ELSE pfr.Family
            END AS family_friendly
        , idc.platform_subset
        , gr.region_3 as region
        , idc.market_33, idc.market8, idc.market9, idc.developed_emerging
        , idc.FYQTR, substr(idc.FYQTR,1,4) as FY
        , 0 AS age
        , hw.business_feature
        , hw.category_feature
        , CASE WHEN Family like '%WOLVERINE%' THEN 'EXCLUDES'
               WHEN Family like '%UNASSIGNED%' THEN 'EXCLUDES'
               WHEN Family like 'SEG%' THEN 'SEG'
               ELSE 'OTHER' END AS publish
        , hw.vc_category
        , idc.covid_total_pages
        , idc.total_pages
        , idc.covid_hp_pages
        , idc.hp_pages
        , idc.IB
        , idc.BD_share_telem_pct
        , idc.BD_usage_telem_pct
        , idc.BD_share_pages/idc.BD_usage_pages as BD_share
        , idc.BD_usage_pages/idc.IB as BD_usage
        , idc.covid_color_pages
        , idc.color_pages
        , idc.covid_black_pages
        , idc.black_pages
        , hw.predecessor
        , hw.successor
        , hw.format
    FROM ib_dupsm_covid idc
    LEFT JOIN plat_family_ref pfr 
        ON idc.platform_subset=pfr.platform_subset 
    LEFT JOIN hw_ref hw
        ON idc.platform_subset=hw.platform_subset   
    LEFT JOIN (SELECT DISTINCT market_33, region_3 from geo_ref) gr      
        ON idc.market_33=gr.market_33     
    """)
    return df


# COMMAND ----------

def join_final_data_w_chroma(final_data_join: DataFrame, hw_ref: DataFrame) -> DataFrame:
    final_data_w_chroma = final_data_join \
        .join(
            other=hw_ref.select('platform_subset', 'mono_color', 'category_plus', 'managed_nonmanaged'),
            on='platform_subset',
            how='left'
        )
    return final_data_w_chroma


# COMMAND ----------

def transform_dupsm_long_current(spark: SparkSession, usage: DataFrame) -> DataFrame:
    usage.createOrReplaceTempView('usage')
    dupsm_long_current = spark.sql("""
    SELECT 
        cal_date
        , country_alpha2
        , platform_subset
        , customer_engagement
        , CASE
            WHEN measure='K_USAGE' THEN 'USAGE'
            ELSE measure
        END AS measure
        , CASE WHEN measure='COLOR_USAGE' THEN 3*units
            ELSE units 
        END AS units
        , source
        FROM usage
    """)
    return dupsm_long_current


# COMMAND ----------

def transform_mps_mm(spark: SparkSession, ib_pre: DataFrame, geo_ref: DataFrame, hw_ref: DataFrame, mps_ib_pre: DataFrame) -> DataFrame:
    # calculate monthly rate of change for trad platforms that have MPS counterparts at a business_feature, format,
    # market9 level. This will be used to extend the MPS IB (forecast).
    ib_pre.createOrReplaceTempView('ib_pre')
    geo_ref.createOrReplaceTempView('geo_ref')
    hw_ref.createOrReplaceTempView('hw_ref')
    mps_ib_pre.createOrReplaceTempView('mps_ib_pre')
    mps_mm = spark.sql("""
        WITH ib_pre1 AS (
            SELECT
                ibt.cal_date
                , ibt.country_alpha2
                , ibt.platform_subset
                , hw.business_feature
                , hw.format
                , geo.market9
                , ibt.units as tib
                , RANK() OVER (PARTITION BY ibt.country_alpha2, ibt.platform_subset ORDER BY ibt.cal_date) AS cal_num
            FROM ib_pre ibt
            LEFT JOIN geo_ref geo
                ON ibt.country_alpha2=geo.country_alpha2
            LEFT JOIN hw_ref hw
                ON ibt.platform_subset=hw.platform_subset
            LEFT JOIN (SELECT *, 1 as found FROM mps_ib_pre) mps
                ON geo.mps_country=UPPER(mps.MPS_country) and ibt.platform_subset=mps.platform_subset
            WHERE mps.found=1
            GROUP BY ibt.cal_date, ibt.country_alpha2, ibt.platform_subset, hw.business_feature, hw.format, geo.market9, ibt.units
        ) , ib_pre2 AS (
            SELECT business_feature, format, market9, cal_num, SUM(tib) as tib 
            FROM ib_pre1 
            GROUP BY business_feature, format, market9, cal_num
        ) , ib_pre3 AS (
            SELECT
                business_feature
                , format, market9, cal_num, tib
                , LAG(tib) OVER (PARTITION BY business_feature, format, market9 ORDER BY cal_num) AS lag_ib
                , 'DPMPS' AS route_to_market
            FROM ib_pre2
            GROUP BY business_feature, format, market9, cal_num, tib
        )
        SELECT 
        business_feature
        , format
        , market9
        , cal_num
        , tib/lag_ib as MM
        , route_to_market
        FROM ib_pre3
    """)
    return mps_mm


# COMMAND ----------

def transform_mps_override_pre(spark: SparkSession, ib_pre: DataFrame, geo_ref: DataFrame, hw_ref: DataFrame, mps_ib_pre: DataFrame, calendar: DataFrame, mps_mm: DataFrame) -> DataFrame:
    # extend IB forecasts.
    # Some MPS platforms have historical IB that decays out completely before the current quarter.
    # Due to a requirement to align to MPS actuals as much as possible,
    # but also to have continuous curves for platforms, the curve will be extended with an IB of 1.
    # Otherwise, if a platform has IB in the current quarter, it will calculate an IB based on
    # the monthly decay calculated in MPS_MM
    ib_pre.createOrReplaceTempView('ib_pre')
    geo_ref.createOrReplaceTempView('geo_ref')
    hw_ref.createOrReplaceTempView('hw_ref')
    mps_ib_pre.createOrReplaceTempView('mps_ib_pre')
    calendar.createOrReplaceTempView('calendar')
    mps_mm.createOrReplaceTempView('mps_mm')

    mps_override_pre = spark.sql("""
        WITH ibcl AS (
            SELECT 
                cal_date
                , country_alpha2
                , platform_subset
                , customer_engagement
                , RANK() OVER (PARTITION BY country_alpha2, platform_subset, customer_engagement ORDER BY cal_date) AS cal_num
            FROM ib_pre
            WHERE units>0
        ) , step2 AS (
            SELECT
                ib.cal_date
                , ib.country_alpha2
                , ib.platform_subset
                , ib.customer_engagement
                , ib.units as ib
                , geo_ref.mps_country
                , geo_ref.market9
                , mps.IB as MPS_IB
                , mps.Pages_Rptd_Mono
                , mps.Pages_Rptd_Color
                , mps.Pages_Rptd
                , mps2.max_mps_IB_date
                , cal2.fiscal_year_qtr AS max_mps_FYQTR_date
                , mps2.max_mps_IB
                , 'DPMPS' AS route_to_market
                , hw.business_feature
                , hw.format
                , ibcl.cal_num
                , cal.fiscal_year_qtr AS FYQTR
                , cal.fiscal_yr AS FY
            FROM ib_pre ib
            LEFT JOIN ibcl
                ON ib.cal_date=ibcl.cal_date 
                    AND ib.country_alpha2=ibcl.country_alpha2 
                    AND ib.platform_subset=ibcl.platform_subset 
                    AND ib.customer_engagement=ibcl.customer_engagement
            LEFT JOIN geo_ref
                ON ib.country_alpha2=geo_ref.country_alpha2
            LEFT JOIN hw_ref hw
                ON ib.platform_subset=hw.platform_subset
            LEFT JOIN (
                SELECT
                    MPS_country
                    , platform_subset
                    , customer_engagement
                    , cal_date
                    , SUM(IB) as IB
                    , SUM(Pages_Rptd_Mono) as Pages_Rptd_Mono
                    , SUM(Pages_Rptd_Color) AS Pages_Rptd_Color
                    , SUM(Pages_Rptd) as Pages_Rptd
                FROM mps_ib_pre
                GROUP BY MPS_country, platform_subset, customer_engagement, cal_date
            ) AS mps
            ON UPPER(geo_ref.mps_country)=UPPER(mps.MPS_country)
                AND ib.platform_subset=mps.platform_subset
                AND ib.cal_date=mps.cal_date
            LEFT JOIN (
                SELECT 
                    MPS_country
                    , platform_subset
                    , customer_engagement
                    , max(cal_date) AS max_mps_IB_date
                    , MAX(IB) AS max_mps_IB 
            FROM mps_ib_pre
            GROUP BY MPS_country, platform_subset, customer_engagement
            ) AS mps2
            ON geo_ref.mps_country=UPPER(mps2.MPS_country)
                AND ib.platform_subset=mps2.platform_subset
            LEFT JOIN calendar cal
                ON ib.cal_date=cal.date
            LEFT JOIN calendar cal2
                ON mps2.max_mps_IB_date=cal2.date
            WHERE ib.units>0
        )
        SELECT DISTINCT
            ib.cal_date
            , ib.country_alpha2
            , ib.platform_subset
            , ib.customer_engagement
            , ib.ib, ib.mps_country
            , ib.market9
            , ib.MPS_IB
            , ib.Pages_Rptd_Mono
            , ib.Pages_Rptd_Color
            , ib.Pages_Rptd
            , ib.max_mps_IB_date
            , ib.max_mps_FYQTR_date
            , ib.max_mps_IB
            , ib.route_to_market
            , ib.business_feature
            , ib.format, ib.cal_num
            , FYQTR
            , FY
            , CASE WHEN ib.cal_date > ib.max_mps_IB_date THEN mm.MM ELSE 1 END AS MM
        FROM step2 ib  
        LEFT JOIN mps_mm mm
            ON ib.cal_num=mm.cal_num 
                AND ib.business_feature=mm.business_feature
                AND ib.format=mm.format AND ib.market9=mm.market9      
        ORDER BY platform_subset, country_alpha2, cal_date
    """)
    return mps_override_pre


# COMMAND ----------

def transform_mps_override_pre_w_window_applied(spark: SparkSession, mps_override_pre: DataFrame) -> DataFrame:
    window_val = (
        Window
        .partitionBy('platform_subset', 'country_alpha2')
        .orderBy('cal_num')
        .rangeBetween(Window.unboundedPreceding, 0)
    )

    mps_override_pre_w_window_applied = mps_override_pre \
        .withColumn('multiplier', F.product('MM').over(window_val))
    mps_override_pre_w_window_applied.createOrReplaceTempView('mps_override_pre_w_window_applied')

    mps_override_pre_w_window_applied_final = spark.sql("""
        SELECT
            *,
            CASE WHEN max_mps_IB_date < add_months(current_date(),-4) AND cal_date > max_mps_IB_date THEN 0
                WHEN cal_date <= max_mps_IB_date THEN MPS_IB
                ELSE multiplier*max_mps_IB 
            END AS MPS_IB_extended
        FROM mps_override_pre_w_window_applied
    """)
    return mps_override_pre_w_window_applied_final


# COMMAND ----------

def transform_dupsm_combined(spark: SparkSession, dupsm_long_current: DataFrame, hw_ref: DataFrame, plat_family_ref: DataFrame) -> DataFrame:
    dupsm_long_current.createOrReplaceTempView('dupsm_long_current')
    hw_ref.createOrReplaceTempView('hw_ref')
    plat_family_ref.createOrReplaceTempView('plat_family_ref')
    dupsm_combined = spark.sql("""
        SELECT 
            dlc.cal_date
            , dlc.country_alpha2
            , dlc.platform_subset
            , dlc.customer_engagement
            , dlc.measure
            , dlc.units
            , dlc.source    
            , hw_ref.mono_color
            , pfr.Family 
        FROM dupsm_long_current dlc
        LEFT JOIN hw_ref    
            ON dlc.platform_subset=hw_ref.platform_subset  
        LEFT JOIN plat_family_ref pfr
            ON dlc.platform_subset=pfr.platform_subset
        """)

    dupsm_combined = dupsm_combined.selectExpr(
        "*",
        "max(source = 'TELEMETRY') over (partition by country_alpha2, platform_subset, customer_engagement, measure) as TFlag"
    )
    dupsm_combined.createOrReplaceTempView("dupsm_combined")

    dupsm_combined = spark.sql("""
        SELECT 
            cal_date
            , country_alpha2
            , platform_subset
            , customer_engagement
            , measure
            , units
            , mono_color
            , Family
            , CASE WHEN source='TELEMETRY' THEN 'TELEMETRY'
                    --TODO: SHOULD NPI be marked differntly?
                    WHEN TFlag='true' THEN 'MODELED'
                    ELSE 'PROXIED' END AS source
            , 'TRANSACTIONAL' AS route_to_market
        FROM dupsm_combined         
    """)
    return dupsm_combined


# COMMAND ----------

def transform_mps_trad_ib(spark: SparkSession, mps_trad_ib1: DataFrame) -> DataFrame:
    mps_trad_ib = mps_trad_ib1.selectExpr(
        'cal_date', 'country_alpha2', 'platform_subset', 'FYQTR',
        "stack(2,'DPMPS',DPMPS,'TRANSACTIONAL',TRANSACTIONAL) as (route_to_market,IB)"
    )

    mps_trad_ib.createOrReplaceTempView('mps_trad_ib')
    mps_trad_ib1.createOrReplaceTempView('mps_trad_ib1')

    mps_trad_ib = spark.sql("""
        SELECT
            a.cal_date
            , a.country_alpha2
            , a.platform_subset
            , a.FYQTR
            , a.route_to_market
            , a.IB 
            , a.FYQTR
            , CASE WHEN a.route_to_market='TRANSACTIONAL' THEN null
                WHEN a.IB is NULL THEN 0
                WHEN a.IB = 0 THEN 0
                ELSE b.Pages_Rptd_Mono/nullif(a.IB,0) END AS MPS_usage  
            , CASE WHEN a.route_to_market='TRANSACTIONAL' THEN null
                WHEN a.IB is NULL THEN 0
                WHEN a.IB = 0 THEN 0
                ELSE b.Pages_Rptd_Color/nullif(a.IB,0) END AS MPS_color_usage            
        FROM mps_trad_ib a
        LEFT JOIN mps_trad_ib1 b
        ON a.cal_date=b.cal_date 
            AND a.country_alpha2=b.country_alpha2 
            AND a.platform_subset=b.platform_subset 
            AND a.FYQTR=b.FYQTR
    """)
    return mps_trad_ib


# COMMAND ----------

def transform_mps_override(spark: SparkSession, mps_override_pre: DataFrame, mps_ib_tie_out: DataFrame) -> DataFrame:
    mps_override_pre.createOrReplaceTempView('mps_override_pre')
    mps_ib_tie_out.createOrReplaceTempView('mps_ib_tie_out')
    mps_override = spark.sql("""
        WITH step1 AS (
            SELECT 
                country_alpha2, 
                platform_subset, 
                customer_engagement, 
                mps_country, 
                market9, 
                FY, 
                FYQTR, 
                mean(MPS_IB_extended) AS mean_ib_extended
            FROM mps_override_pre 
            GROUP BY country_alpha2, platform_subset, customer_engagement, mps_country, market9, FY, FYQTR
        ) 
        , step2 AS (
            SELECT 
                country_alpha2,
                platform_subset, 
                customer_engagement,
                mps_country, 
                market9,
                FY, 
                mean(MPS_IB_extended) AS MPS_IB_extended_FY_avg
            FROM mps_override_pre 
            GROUP BY country_alpha2, platform_subset, customer_engagement, mps_country, market9, FY
        ) 
        , step3 AS (
            SELECT 
                FY
                , sum(MPS_IB_extended_FY_avg) as sum_MPS_IB_extended_FY_avg
            FROM step2
            GROUP BY FY
        )   
        , step4 AS (
            SELECT
                step2.country_alpha2
                , step2.platform_subset
                , step2.customer_engagement
                , step2.mps_country
                , step2.market9
                , step2.FY
                , step2.MPS_IB_extended_FY_avg
                , step2.MPS_IB_extended_FY_avg/step3.sum_MPS_IB_extended_FY_avg AS percentage_MPS_IB_FY
                , b.mps_ib
                , b.mps_pages
                , b.A3_ib
                , b.A4_ib
            FROM step2
            LEFT JOIN step3
            ON step2.FY=step3.FY
            LEFT JOIN mps_ib_tie_out b
            ON step2.FY=b.FY
        )
        , step5 AS (
            SELECT a.country_alpha2, a.platform_subset, a.customer_engagement, a.mps_country, a.market9, a.FY, a.FYQTR, b.mps_ib, b.mps_pages, b.A3_ib, b.A4_ib 
               , b.MPS_IB_extended_FY_avg, b.percentage_MPS_IB_FY
               , CASE WHEN b.mps_ib IS NOT null AND b.percentage_MPS_IB_FY IS NOT null THEN b.mps_ib*b.percentage_MPS_IB_FY
                ELSE b.MPS_IB_extended_FY_avg
               END AS MPS_IB_spread
               --mutate(MPS_IB_adjusted=ifelse(!is.na(mps_ib)& !is.na(ratio_mps_FYQTR) & !is.na(MPS_IB_spread), ratio_mps_FYQTR*MPS_IB_spread, MPS_IB_extended),
                --MPS_IB_adjusted=ifelse(route_to_market=='DPMPS' & cal_date>max_mps_IB_date & max_mps_FYQTR_date<active_time $FYQTR[[2]]-(quarter(1)/4) ,1,MPS_IB_adjusted))
            FROM step1 a 
           LEFT JOIN step4 b
                ON  a.country_alpha2=b.country_alpha2 and a.platform_subset=b.platform_subset and a.customer_engagement=b.customer_engagement and a.mps_country=b.mps_country and a.market9=b.market9 and a.FY=b.FY
        )
        , step6 AS (SELECT distinct pre.*, pre.MPS_IB_extended/step1.mean_ib_extended AS ratio_mps_FYQTR, step1.mean_ib_extended, step5.MPS_IB_extended_FY_avg, step5.mps_pages, step5.A3_ib, step5.A4_ib, step5.MPS_IB_spread, step5.percentage_MPS_IB_FY
        FROM mps_override_pre pre
        LEFT JOIN step5
            ON pre.country_alpha2=step5.country_alpha2 AND pre.platform_subset=step5.platform_subset AND pre.mps_country=step5.mps_country AND pre.market9=step5.market9 AND pre.FYQTR=step5.FYQTR
        LEFT JOIN step1
            ON pre.country_alpha2=step1.country_alpha2 AND pre.platform_subset=step1.platform_subset AND pre.mps_country=step1.mps_country AND pre.market9=step1.market9 AND pre.FYQTR=step1.FYQTR)
        , step7 AS ( SELECT cal_date, country_alpha2, platform_subset, customer_engagement, ib, mps_country, market9, Pages_Rptd_Mono, Pages_Rptd_Color, Pages_Rptd, FY, max_mps_IB_date, max_mps_FYQTR_date, max_mps_IB, route_to_market, business_feature, format, cal_num, MM
            , multiplier, MPS_IB_extended, FYQTR, FY, mean_ib_extended, ratio_mps_FYQTR, MPS_IB_extended_FY_avg, percentage_MPS_IB_FY, mps_ib, mps_pages, A3_ib, A4_ib, MPS_IB_spread
            , CASE WHEN mps_ib IS NOT null AND ratio_mps_FYQTR IS NOT null AND MPS_IB_spread IS NOT NULL THEN ratio_mps_FYQTR*MPS_IB_spread
               ELSE MPS_IB_extended 
               END AS MPS_IB_adjusted
        FROM step6)
        SELECT cal_date, country_alpha2, platform_subset, customer_engagement, ib, mps_country, market9, FY, Pages_Rptd_Mono, Pages_Rptd_Color, Pages_Rptd, max_mps_IB_date, max_mps_FYQTR_date, max_mps_IB, route_to_market, business_feature, format, cal_num, MM
            , multiplier, MPS_IB_extended, FYQTR, FY, mean_ib_extended, ratio_mps_FYQTR, MPS_IB_extended_FY_avg, percentage_MPS_IB_FY, mps_ib, mps_pages, A3_ib, A4_ib, MPS_IB_spread
            ,CASE WHEN cal_date > max_mps_IB_date THEN 1
                ELSE MPS_IB_adjusted
                END AS MPS_IB_adjusted
        FROM step7
    """)
    return mps_override


# COMMAND ----------

def transform_mps_trad_ib1(spark: SparkSession, mps_override: DataFrame) -> DataFrame:
    mps_override.createOrReplaceTempView('mps_override')
    # "carves out" MPS from trad and then formats for next steps, also calculates usage, color usage and extends them
    mps_trad_ib1 = spark.sql("""
        SELECT
            cal_date
            , country_alpha2
            , platform_subset, customer_engagement
            , FYQTR
            , Pages_Rptd
            , Pages_Rptd_Mono
            ,Pages_Rptd_Color
            , CASE WHEN MPS_IB_adjusted IS null THEN 0
                 ELSE MPS_IB_adjusted END AS DPMPS
            , CASE WHEN ib IS null THEN 0
                WHEN MPS_IB_adjusted IS null THEN ib
                WHEN ib - MPS_IB_adjusted <=0 THEN 1
                ELSE ib - MPS_IB_adjusted END AS TRANSACTIONAL
        FROM mps_override
    """)
    return mps_trad_ib1


# COMMAND ----------

def transform_data(spark: SparkSession, raw_data: dict) -> DataFrame:
    geo_ref = transform_geo_ref(
        spark=spark,
        geo_ref=raw_data['geo_ref']
    )

    mps_ib_pre = transform_mps_ib_pre(
        spark=spark,
        ozzy_mps=raw_data['ozzy_mps'],
        hw_ref=raw_data['hw_ref']
    )

    dupsm_long_current = transform_dupsm_long_current(
        spark=spark,
        usage=raw_data['usage']
    )

    mps_mm = transform_mps_mm(
        spark=spark,
        ib_pre=raw_data['ib_pre'],
        hw_ref=raw_data['hw_ref'],
        geo_ref=geo_ref,
        mps_ib_pre=mps_ib_pre
    )

    mps_override_pre = transform_mps_override_pre(
        spark=spark,
        ib_pre=raw_data['ib_pre'],
        hw_ref=raw_data['hw_ref'],
        geo_ref=geo_ref,
        mps_ib_pre=mps_ib_pre,
        calendar=raw_data['calendar'],
        mps_mm=mps_mm
    )

    mps_override_pre_w_window = transform_mps_override_pre_w_window_applied(
        spark=spark,
        mps_override_pre=mps_override_pre
    )

    plat_family_ref = transform_plat_family_ref(
        spark=spark,
        ib_pre=raw_data['ib_pre'],
        mps_ib_pre=mps_ib_pre,
        hw_ref=raw_data['hw_ref']
    )

    mps_override = transform_mps_override(
        spark=spark,
        mps_override_pre=mps_override_pre_w_window,
        mps_ib_tie_out=raw_data['mps_ib_tie_out']
    )

    mps_trad_ib1 = transform_mps_trad_ib1(
        spark=spark,
        mps_override=mps_override
    )

    mps_trad_ib = transform_mps_trad_ib(
        spark=spark,
        mps_trad_ib1=mps_trad_ib1
    )

    dupsm_combined = transform_dupsm_combined(
        spark=spark,
        dupsm_long_current=dupsm_long_current,
        hw_ref=raw_data['hw_ref'],
        plat_family_ref=plat_family_ref
    )

    usage_share_ib_prep = transform_usage_share_ib_prep(
        spark=spark,
        mps_trad_ib=mps_trad_ib,
        dupsm_combined=dupsm_combined
    )

    final_dupsm = transform_final_dupsm(
        spark=spark,
        usage_share_ib_prep=usage_share_ib_prep
    )

    ib_dupsm_covid_prep = transform_ib_dupsm_covid_prep(
        spark=spark,
        final_dupsm=final_dupsm,
        geo_ref=geo_ref
    )

    ib_dupsm_covid = transform_ib_dupsm_covid(
        spark=spark,
        ib_dupsm_covid_prep=ib_dupsm_covid_prep,
        geo_ref=geo_ref
    )

    final_data_join = join_final_data(
        spark=spark,
        ib_dupsm_covid=ib_dupsm_covid,
        plat_family_ref=plat_family_ref,
        hw_ref=raw_data['hw_ref'],
        geo_ref=geo_ref
    )
    
    final_data_w_chroma = join_final_data_w_chroma(
        final_data_join=final_data_join,
        hw_ref=raw_data['hw_ref']
    )

    return final_data_w_chroma


# COMMAND ----------

def write_final_data_to_xlsx(spark: SparkSession, extended_configs: dict, final_dataframe: DataFrame, s3_destination: str) -> None:
    comment = spark.createDataFrame(
        [
            (1, f"The Current data used hes is pulled from ::: usage_version_{extended_configs['usage_version']}"),
            (2, f"The IB post_applied to the data is ::: ib_version_{extended_configs['ib_version']}"),
            (3, f"COVID: ON ::: COVID END: {extended_configs['covid_end_quarter']}"),
        ],
        ["id", "Comment"])
    commentpd = comment.toPandas()
    fdjpd = final_dataframe.toPandas()
    with pd.ExcelWriter(path=s3_destination+".xlsx") as writer:
        commentpd.to_excel(writer, sheet_name="ABOUT", index=False)
        fdjpd.to_excel(writer, sheet_name='DATA', index=False)
    return None


# COMMAND ----------

def write_final_data_to_parquet(final_dataframe: DataFrame, s3_destination: str) -> None:
    write_df_to_s3(df=final_dataframe,
                   destination=s3_destination,
                   format="parquet",
                   mode="overwrite",
                   upper_strings=True)
    print("output file name: " + s3_destination)
    return None


# COMMAND ----------

def load_data(spark: SparkSession, extended_configs: dict, final_dataframe: DataFrame):
    s3_destination = f"{extended_configs['output_path']}/m33_cupsm_output_{extended_configs['date']}_{extended_configs['output_label']}"
    write_final_data_to_xlsx(
        spark=spark,
        extended_configs=extended_configs,
        final_dataframe=final_dataframe,
        s3_destination=s3_destination
    )
    write_final_data_to_parquet(
        final_dataframe=final_dataframe,
        s3_destination=s3_destination
    )
    return None


# COMMAND ----------

def get_spark_session() -> tuple:
    spark = SparkSession.builder.getOrCreate()
    db_session = "local" not in spark.conf.get("spark.master")
    if db_session == 'local':
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        spark = SparkSession.builder \
            .master('local[1]') \
            .config("spark.executor.cores", "1") \
            .config("spark.executor.instances", "1") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
    return spark, db_session


# COMMAND ----------

def main():
    spark, dbsession = get_spark_session()
    extended_configs = get_dupsm_configs(configs)
    raw_data = extract_data(spark, extended_configs)
    transformed_data = transform_data(spark, raw_data)
    load_data(spark, configs, transformed_data)


# COMMAND ----------

if __name__ == '__main__':
    main()
