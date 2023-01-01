# Databricks notebook source
# MAGIC %run ../common/configs

# COMMAND ----------

# MAGIC %run ../common/database_utils

# COMMAND ----------

# if table exists, truncate, else print exception message
try:
    row_count = read_redshift_to_df(configs).option("dbtable", "stage.i_ink_ltf_temp").load().count()
    if row_count > 0:
        submit_remote_query(configs, "TRUNCATE stage.i_ink_ltf_temp")
except Exception as error:
    print ("An exception has occured:", error)

# COMMAND ----------

submit_remote_query(configs, """UPDATE prod.instant_ink_enrollees_ltf
SET official = 0""")

# COMMAND ----------

df_iink_ltf = spark.read.format('csv').options(header='true', inferSchema='true').load('{}product/ib/instant_ink_enrollees_ltf/i_ink_ltf.csv'.format(constants['S3_BASE_BUCKET'][stack]))

# COMMAND ----------

add_version_sproc = """
call prod.addversion_sproc('IINK_LTF_IB', 'I-INK LTF FILE');  
"""

iink_proc = """
 ---------UNPIVOT DATA AND LOAD IN TEMP TABLE--------------------------------

 SELECT metric, region,FY,val*1000 val
 INTO #fiscal_unpvt
 FROM 
 (
  SELECT metric,region,FY20Q1,FY20Q2,FY20Q3,FY20Q4,FY21Q1,FY21Q2,FY21Q3,FY21Q4,FY22Q1,FY22Q2,FY22Q3,FY22Q4,FY23Q1,FY23Q2,FY23Q3,FY23Q4,FY24Q1,
		 FY24Q2,FY24Q3,FY24Q4,FY25Q1,FY25Q2,FY25Q3,FY25Q4,FY26Q1,FY26Q2,FY26Q3,FY26Q4,FY27Q1,FY27Q2,FY27Q3,FY27Q4
  FROM stage.i_ink_ltf_temp
  ) A
  UNPIVOT
  (val FOR FY IN (FY20Q1,FY20Q2,FY20Q3,FY20Q4,FY21Q1,FY21Q2,FY21Q3,FY21Q4,FY22Q1,FY22Q2,FY22Q3,FY22Q4,FY23Q1,FY23Q2,FY23Q3,FY23Q4,FY24Q1,
		 FY24Q2,FY24Q3,FY24Q4,FY25Q1,FY25Q2,FY25Q3,FY25Q4,FY26Q1,FY26Q2,FY26Q3,FY26Q4,FY27Q1,FY27Q2,FY27Q3,FY27Q4)
  ) AS Unpvt;


  --------------------FETCH NEXT ROW VALUES---------------------------

 SELECT  metric,region,FY,val,1 FiscalKey,LAG(val,1,1) OVER (PARTITION BY region ORDER BY region,FY)*(1 + POWER(val/NULLIF(LAG(val,1,1) OVER (PARTITION BY region ORDER BY region,FY),0),.33333)-1) FirstVal
  INTO #nextval 
  FROM #fiscal_unpvt 
  WHERE  metric = 'P2 Cumulative';

  -------------------------RETURN NEXT TWO ROW VALUES----------------------

  SELECT t.metric,t.region,c.cal_date,t.FirstVal val, Fiscal_Key,t.FY
  INTO #TEMP_LND
  FROM #nextval t
  LEFT JOIN mdm.fiscal_year_xref c on c.FY = t.FY and c.Fiscal_key = 1
  UNION
  SELECT t.metric,t.region,c.cal_date,t.FirstVal*(1 + POWER(val/NULLIF(LAG(val,1,1) OVER (PARTITION BY region ORDER BY region,t.FY),0),.33333)-1), Fiscal_Key,t.FY
  FROM #nextval t
  LEFT JOIN mdm.fiscal_year_xref c on c.FY = t.FY and c.Fiscal_key = 2
  UNION
  SELECT t.metric,t.region,c.cal_date, t.val, Fiscal_Key,t.FY
  FROM #nextval t
  LEFT JOIN mdm.fiscal_year_xref c on c.FY = t.FY and c.Fiscal_key = 3;



  -----------------------------POPULATE FUTURE DATA-----------------------------------------------

  	DECLARE @maxdate VARCHAR(20) = (SELECT MAX(cal_date) FROM #TEMP_LND);


SELECT DISTINCT date 
INTO #DATES
FROM mdm.calendar
WHERE 1=1
	AND date > @maxdate
	AND date < '2045-10-01'
	and Day_of_Month = 1;


SELECT  t.metric,t.region,cal_date,t.val
INTO #instant_ink_enrollees_LTF_stage
FROM #TEMP_LND  t
UNION ALL
SELECT  t.metric,t.region,date,t.val
FROM #TEMP_LND  t
CROSS JOIN #DATES
WHERE cal_date = @maxdate;

  ------------------UPDATE VERSION-----------------------------

UPDATE #instant_ink_enrollees_LTF_stage
SET load_date = (SELECT MAX(load_date) FROM prod.version WHERE record = 'IINK_LTF_IB'),
version = (SELECT MAX(version) FROM prod.version WHERE record = 'IINK_LTF_IB')
WHERE version IS NULL;

	-------------------LOAD PROD-------------------------

	INSERT INTO prod.instant_ink_enrollees_ltf(
	metric,
	region_5,
	cal_date,
	value,
	version,
	load_date
	)
	SELECT 
	metric,
	region_5,
	cal_date,
	value,
	version,
	load_date
	FROM #instant_ink_enrollees_LTF_stage;
"""

# COMMAND ----------

write_df_to_redshift(configs, df_iink_ltf, "stage.i_ink_ltf_temp", "append", add_version_sproc + "\n" + iink_proc)
