# Databricks notebook source
# MAGIC %md
# MAGIC ##### Locating the mount

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/mnt/'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/ci-carma/landing/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Installing openpyxl library to read excel file into a pandas dataframe

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Importing the necessary libraries

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Loading the full dataset into a pandas dataframe

# COMMAND ----------

filePath = '/dbfs/mnt/ci-carma/initialSeed/Caregivers_23August2023.xlsx'
df = pd.read_excel(filePath,engine='openpyxl')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Defining functions that accepts Pandas Dataframe and returns Spark Dataframe

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

display(cg_df.count())

# COMMAND ----------

null_person_icn_df = cg_df.where(cg_df['Person ICN'].isNull())

# COMMAND ----------

null_person_icn_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Loading only those records that are Not Null into cg_df dataframe

# COMMAND ----------

cg_df = cg_df.where(cg_df['Person ICN'].isNotNull())

# COMMAND ----------

display(cg_df.count())

# COMMAND ----------

cg_df = cg_df.select(
    cg_df["Person ICN"].alias("Full_Person_ICN"),
    cg_df["Applicant Type"].alias("Applicant_Type"),
    cg_df["Caregiver Status"].alias("Caregiver_Status"),
    to_date(cg_df["Dispositioned Date"], "yyyy-MM-dd").alias("Dispositioned_Date"),
    to_date(cg_df["Benefits End Date"], "yyyy-MM-dd").alias("Benefits_End_Date"),
    cg_df["CARMA Case Details: Veteran ICN"].alias("Full_Veteran_ICN"),
)

# COMMAND ----------

display(cg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Splitting Person ICN and CARMA Case Details: Veteran ICN and adding additional columns.

# COMMAND ----------

cg_df = cg_df.withColumn('CG_ICN', col("Full_Person_ICN").substr(1,10))\
    .withColumn('Veteran_ICN', col("Full_Veteran_ICN").substr(1,10))\
    .withColumn('Batch_CD',lit("CG"))\
    .withColumn("SC_COMBINED_DISABILITY_PERCENTAGE", lit(""))\
    .withColumn("P&T_INDICATOR", lit(""))\
    .withColumn("INDIVIDUAL_UNEMPLOYABILITY", lit(""))    

# COMMAND ----------

display(cg_df)

# COMMAND ----------

cg_df.createOrReplaceTempView("cg_full_load_table")

# COMMAND ----------

spark.sql("DESCRIBE TABLE cg_full_load_table").show()

# COMMAND ----------

spark.sql("select * from cg_full_load_table").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking Full_Person_ICN for NULL values

# COMMAND ----------

display(spark.sql("SELECT * FROM cg_full_load_table WHERE Full_Person_ICN IS NULL"))

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Joining IdentityData.Person_Site_Assoiciations table with cg_full_load_table to get *EDIPI*

# COMMAND ----------

spark.sql(
    "select mvi.EDIPI, cg.Full_Person_ICN, cg.Applicant_Type, cg.Caregiver_Status, cg.Dispositioned_Date, cg.Benefits_End_Date, cg.Full_Veteran_ICN, cg.CG_ICN, cg.Veteran_ICN from cg_full_load_table cg join IdentityData.PERSON_SITE_ASSOCIATIONS mvi on mvi.MVIPersonICN = cg.CG_ICN WHERE mvi.MVITreatingFacilityInstitutionSID = 5667"
)

# COMMAND ----------

spark.sql(
    "select mvi.EDIPI as EDIPI, CG as Batch_CD,  cg.Full_Person_ICN, cg.Applicant_Type, cg.Caregiver_Status, cg.Dispositioned_Date, cg.Benefits_End_Date, cg.Full_Veteran_ICN, cg.CG_ICN, cg.Veteran_ICN from cg_full_load_table cg join IdentityData.PERSON_SITE_ASSOCIATIONS mvi on mvi.MVIPersonICN = cg.CG_ICN WHERE mvi.MVITreatingFacilityInstitutionSID = 5667"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Fields required for the Patronage Schema
# MAGIC * EDIPI: string
# MAGIC * BATCH_CD: string
# MAGIC * SC_COMBINED_DISABILITY_PERCENTAGE: string
# MAGIC * P&T_INDICATOR: string
# MAGIC * INDIVIDUAL_UNEMPLOYABILITY: string
# MAGIC * STATUS_BEGIN_DATE: date
# MAGIC * STATUS_LAST_UPDATE_DATE: date
# MAGIC * STATUS_TERMINATION_DATE: date

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Loading updates (.csv) file into a dataframe

# COMMAND ----------

updates_df = spark.read.format("csv").option("header","true").load('/mnt/ci-carma/landing/caregiverevent-6e110430-57ff-11ee-a720-0631cc63e406.csv')

# COMMAND ----------

display(updates_df)

# COMMAND ----------

updates_df = updates_df.select(updates_df['Discharge_Revocation_Date__c'].alias('Discharge_Revocation_Date'),\
    updates_df['Caregiver_Status__c'].alias('Caregiver_Status'),\
    to_date(updates_df['Dispositioned_Date__c'], "MM/dd/yyyy").alias('Status_Begin_Date'),\
    updates_df['Applicant_Type__c'].alias('Applicant_Type'),\
    to_date(updates_df['CreatedDate'], "MM/dd/yyyy").alias('Created_Date'),\
    updates_df['Veteran_ICN__c'].alias('Full_Veteran_ICN'),\
    substring(updates_df['Veteran_ICN__c'],1,10).alias('Veteran_ICN'),\
    to_date(updates_df['Benefits_End_Date__c'], "MM/dd/yyyy").alias('Benefits_End_Date'),\
    updates_df['Caregiver_ICN__c'].alias('Full_Caregiver_ICN'),\
    substring(updates_df['Caregiver_ICN__c'],1,10).alias('CG_ICN'))

# COMMAND ----------

updates_df.createOrReplaceTempView("cg_csv_table")

# COMMAND ----------

display(spark.sql("SELECT * FROM cg_csv_table "))

# COMMAND ----------

spark.sql("DESCRIBE cg_csv_table").show()

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

# MAGIC %md
# MAGIC ##### Below sql script will help extracting EDIPI
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT 
# MAGIC [MVIPersonSiteAssociationSID]
# MAGIC ,[MVIPersonSiteAssociationIEN]
# MAGIC ,[MVIPersonICN]
# MAGIC ,[MVIPersonSID]
# MAGIC ,[MVITreatingFacilityInstitutionSID]
# MAGIC ,[TreatingFacilityPersonIdentifier]
# MAGIC ,[EDIPI]
# MAGIC ,[PersonSSN]
# MAGIC ,[SSNVerificationStatus]
# MAGIC ,[SSNVerificationStatusCode]
# MAGIC ,[VeteranFlag]
# MAGIC ,[LastName]
# MAGIC ,[FirstName]
# MAGIC ,[MiddleName]
# MAGIC ,[NamePrefix]
# MAGIC ,[NameSuffix]
# MAGIC ,[MotherMaidenName]
# MAGIC ,[Gender]
# MAGIC ,[BirthDateTime]
# MAGIC ,[BirthCity]
# MAGIC ,[MVIBirthStateSID]
# MAGIC ,[MVIBirthCountrySID]
# MAGIC ,[IsPatient]
# MAGIC ,[IsVeteran]
# MAGIC ,[IsAssociatedIndividual]
# MAGIC ,[IsEmployee]
# MAGIC ,[IsContractor]
# MAGIC ,[IsCaregiver]
# MAGIC ,[IsOther]
# MAGIC ,[calc_DOD_EDIPI]
# MAGIC FROM [IdentityData].[PERSON_SITE_ASSOCIATIONS]
# MAGIC WHERE MVIPersonICN = '1021034530' and MVITreatingFacilityInstitutionSID = 5667

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EDIPI Lookup
# MAGIC SELECT * FROM
# MAGIC IdentityData.PERSON_SITE_ASSOCIATIONS

# COMMAND ----------



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

import pyodbc

pyodbc.drivers()

# COMMAND ----------

PersonTable_df = (spark.read
  .format("jdbc")
  .option("url", "jdbc:sqlserver://vac21vdwaswdev.sql.azuresynapse.usgovcloudapi.net:1433;database=sqldbdevcxdw")
  .option("dbtable", "IdentityData.PERSON_SITE_ASSOCIATIONS")
  .option("user", "<username>")
  .option("password", "<password>")
  .load()
)

# COMMAND ----------

#copied from Azure Portal
jdbc:sqlserver://vac21vdwaswdev.sql.azuresynapse.usgovcloudapi.net:1433;database=sqldbdevcxdw;user=sqladminuser@vac21vdwaswdev;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.usgovcloudapi.net;loginTimeout=30;

# COMMAND ----------

#copied from Synapse

jdbc:sqlserver://vac20vdwasynprod.sql.azuresynapse.usgovcloudapi.net:1433;database=sqldbprodcxdw;user=undefined@vac20vdwasynprod;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;

# COMMAND ----------

mvi_site_df = spark.read.format("delta").load("/mnt/mviinstitution")#spark.sql("SELECT * FROM cdwwork.sveteran_smvipersonsiteassociation")
mvi_institution_df = spark.read.format("delta").load("/mnt/smvipersonsiteassociation")#spark.sql("SELECT * FROM cdwwork.ndim_mviinstitution")

# COMMAND ----------


