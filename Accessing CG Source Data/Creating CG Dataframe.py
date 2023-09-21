# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

display(dbutils.fs.ls('/mnt/'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/ci-carma/landing/'))

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

filePath = '/dbfs/mnt/ci-carma/initialSeed/Caregivers_23August2023.xlsx'
df = pd.read_excel(filePath,engine='openpyxl')

# COMMAND ----------

def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return DoubleType()
    elif f == 'float32': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

cg_df = pandas_to_spark(df)

# COMMAND ----------

display(cg_df)

# COMMAND ----------

from pyspark.sql.functions import to_date

# COMMAND ----------


cg_df = cg_df.select(
    cg_df["Person ICN"].alias("Full_Person_ICN"),
    cg_df["Applicant Type"].alias("Applicant _Type"),
    cg_df["Caregiver Status"].alias("Caregiver_Status"),
    to_date(cg_df["Dispositioned Date"], "MM/dd/yyyy").alias("Dispositioned_Date"),
    to_date(cg_df["Benefits End Date"], "MM/dd/yyyy").alias("Benefits_End_Date"),
    cg_df["CARMA Case Details: Veteran ICN"].alias("Full_Veteran_ICN"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Splitting Person ICN and CARMA Case Details: Veteran ICN

# COMMAND ----------

cg_df1 = cg_df.withColumn('CG_ICN', col("Full_Person_ICN").substr(1,10))\
    .withColumn('Veteran_ICN', col("Full_Veteran_ICN").substr(1,10))

# COMMAND ----------

display(cg_df1)

# COMMAND ----------

cg_df1.createOrReplaceTempView("cg_full_load_table")

# COMMAND ----------

spark.sql("SELECT * FROM cg_full_load_table").show(10)

# COMMAND ----------

#cg_csv_df = spark.read.format("csv").option("header","true").load('/mnt/ci-carma/landing/caregiverevent-6e110430-57ff-11ee-a720-0631cc63e406.csv')
                                                                    
dd =  spark.read.csv('/mnt/ci-carma/landing/caregiverevent-6e110430-57ff-11ee-a720-0631cc63e406.csv', header=True, inferSchema=True)

# COMMAND ----------

display(dd)

# COMMAND ----------

dd = dd.select(dd['Discharge_Revocation_Date__c'].alias('Discharge_Revocation_Date'),\
    dd['Caregiver_Status__c'].alias('Caregiver_Status'),\
    dd['Dispositioned_Date__c'].alias('Dispositioned_Date'),\
    dd['Applicant_Type__c'].alias('Applicant_Type'),\
    dd['CreatedDate'].alias('Created_Date'),\
    dd['Veteran_ICN__c'].alias('Full_Veteran_ICN'),\
    substring(dd['Veteran_ICN__c'],1,10).alias('Veteran_ICN'),\
    dd['Benefits_End_Date__c'].alias('Benefits_End_Date'),\
    dd['Caregiver_ICN__c'].alias('Full_Caregiver_ICN'),\
    substring(dd['Caregiver_ICN__c'],1,10).alias('CG_ICN'))

# COMMAND ----------

dd.createOrReplaceTempView("cg_csv_table")

# COMMAND ----------

display(spark.sql("SELECT * FROM cg_csv_table "))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For this data product, there are only five fields used to build the Caregiver data product: 
# MAGIC * Person ICN
# MAGIC * Applicant Type
# MAGIC * Caregiver Status
# MAGIC * Dispositioned Date 
# MAGIC * CARMA Case Details: Veteran ICN.
# MAGIC
# MAGIC Both the Person ICN and the CARMA Case Details: Veteran ICN were updated to remove the checksum part of the Full ICN given. The column names were also changed: CARMA Case Details: Veteran ICN column name was converted into the ICN_VETERAN column and Person ICN was converted into the ICN_CG column.
# MAGIC
# MAGIC The STATUS_BEGIN_DATE is created from the Dispositioned Date when a new caregiver is added to the Caregiver dataset.
# MAGIC
# MAGIC The STATUS_TERMINATION_DATE is created when a caregiver is deleted from the Caregiver dataset. When a new dataset is received, if the caregiver is no longer present in the new dataset, the STATUS_TERMINATION_DATE is set as the date the new dataset was processed.

# COMMAND ----------

display(dbutils.fs.ls('/mnt/ci-vadir-shared/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EDIPI Lookup
# MAGIC SELECT * FROM
# MAGIC IdentityData.PERSON_SITE_ASSOCIATIONS

# COMMAND ----------

file_path = '/mnt/ci-vadir-shared/VETPOP_MSTR_EXTRCT_202308.txt'
scd = spark.read.option("delimiter","^").csv(file_path, header=True)

# COMMAND ----------

display(scd)

# COMMAND ----------

file_path_dat = '/mnt/ci-vadir-shared/CPIDODIEX_#202307.dat'
scd_dat = spark.read.option("delimiter","^").csv(file_path_dat)

# COMMAND ----------

display(scd_dat)

# COMMAND ----------

file_path_dat = '/mnt/ci-vadir-shared/CPIDODIEX_#202203.dat'
with open(file_path_dat, "r") as file:
    data = file.read

# COMMAND ----------


