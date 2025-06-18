# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
import time
import os
import pandas as pd
from pyspark.storagelevel import *

# COMMAND ----------

# Validate dataframes with pre-defined conditions
def validate_data(df, checks):
    """
    Parameters: df-->Dataframe, checks-->List
    Validate data based on the given checks and returns an error message
    Returs custom list of error messages if condtion is met
    """
    # validation_results = []
    # for condition, error_message in checks:
    #     if df.filter(condition).limit(1).count() > 0:
    #         validation_results.append(error_message)
    # return validation_results
    return [error_message for condition, error_message in checks if df.filter(condition).limit(1).count()>0]


# Paths and reading source files
cg_file_path = "/mnt/ci-carma/landing"
scd_file_path = "/mnt/ci-vadir-shared/"
pt_indicator_path = "dbfs:/mnt/vac20sdpasa201vba/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/"

cg_files_df = (
    spark.read.format("binaryFile")
    .load(cg_file_path)
    .drop("content")
    .filter(col("path").contains('caregiverevent') & col("path").endswith(".csv"))
)
scd_files_df = (
    spark.read.format("binaryFile")
    .load(scd_file_path)
    .drop("content")
    .filter(col("path").contains("CPIDODIEX_") & col("path").endswith(".csv"))
)

pt_files = dbutils.fs.ls(pt_indicator_path)
pt_files_df = spark.createDataFrame(pt_files)
pt_files_df = pt_files_df.withColumn("modificationTime", to_timestamp(col("modificationTime") / 1000))

cg_max_mod_time = cg_files_df.select(max("modificationTime")).first()[0]
scd_max_mod_time = scd_files_df.select(max("modificationTime")).first()[0]
pt_max_mod_time = pt_files_df.select(max("modificationTime")).first()[0]

# Validation checks
scd_validation_checks = [
    (~col('ICN').rlike('^[0-9]'), "Invalid SCD ICN's"),
    (~col("Batch_CD").isin(['SCD']), "Invalid Batch Code"),
    (~col("PT_Indicator").isin(['Y', 'N']), "Invalid PT Indicator"),
    (col("Status_Begin_Date").isNull(), "Status_Begin_Date is Null"),
    (~col("SC_Combined_Disability_Percentage").isin(["000", "010", "020", "030", "040", "050", "060", "070", "080", "090", "100"]), 
     "Invalid SC_Combined_Disability_Percentage"),
]
cg_validation_checks = [
    (~col('ICN').rlike('^[0-9]'), "Invalid CG ICN's"),
    (~col("Batch_CD").isin(['CG']), "Invalid CG Batch Code"),
    (col("Status_Begin_Date").isNull(), "CG Status_Begin_Date is Null"),
    (~col("Applicant_Type").isin(['Primary Caregiver', 'General Caregiver', 'Secondary Caregiver']), "Invalid CG Applicant Type"),
    (~col("Status").isin(['Approved', 'Revoked/Discharged', 'Revoked', 'Pending Revocation/Discharge']), "Invalid CG Status"),
]

# Load tables
icn_relationship = (
    spark.read.format("delta")
    .load("/mnt/Patronage/identity_correlations")
    .filter(col("edipi").isNotNull())
    .selectExpr("MVIPersonICN AS lu_ICN", "participant_id AS lu_participant_id", "edipi AS lu_edipi")
)
scd_table_df = spark.table("DELTA.`/mnt/Patronage/SCD_Staging`")
cg_table_df = spark.table("DELTA.`/mnt/Patronage/Caregivers_Staging_New`")

# Null EDIPI join and counts
scd_null_edipi_df = scd_table_df.filter(col("edipi").isNull())
cg_null_edipi_df = cg_table_df.filter(col("edipi").isNull())

scd_join_df = scd_null_edipi_df.join(icn_relationship, col("lu_ICN") == col("ICN"), "inner")
cg_join_df = cg_null_edipi_df.join(icn_relationship, col("lu_ICN") == col("ICN"), "inner")

scd_new_edipi_cnt = scd_join_df.count()
cg_new_edipi_cnt = cg_join_df.count()

# Validation messages
error_messages = []
error_messages.extend(validate_data(scd_table_df, scd_validation_checks))
error_messages.extend(validate_data(cg_table_df, cg_validation_checks))

if scd_new_edipi_cnt > 0:
    error_messages.append(f"There are {scd_new_edipi_cnt} new EDIPI's in identity_correlations table that can be updated to SCD Staging table")

if cg_new_edipi_cnt > 0:
    error_messages.append(f"There are {cg_new_edipi_cnt} new EDIPI's in identity_correlations table that can be updated to CG Staging table")

# Modification time checks
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

if ((today - cg_max_mod_time).days) >= 1:
    error_messages.append(f"No new Caregivers files have landed since {((today - cg_max_mod_time).days)} day/s")
if ((today - scd_max_mod_time).days) >= 7:
    error_messages.append(f"No new SCD files have landed since {((today - scd_max_mod_time).days)} day/s")
if ((today - pt_max_mod_time).days) >= 30:
    error_messages.append(f"No new PT_Indicator files have landed since {((today - pt_max_mod_time).days)} day/s")

# Final status
if error_messages:
    dbutils.notebook.exit({"status": "Failed", "errors": error_messages})
else:
    dbutils.notebook.exit({"status": "Passed", "message": "No errors found"})

# COMMAND ----------

# DBTITLE 1,Create Temp View from scd_new_edipi dataframe
cg_join_df.createOrReplaceTempView("cg_join_df")
scd_join_df.createOrReplaceTempView("scd_join_df")


# COMMAND ----------

# DBTITLE 1,Transform data to DMDC required output
edipi_query = f"""
        select rpad(coalesce(lu_edipi,""),10," " ) ||
            rpad(coalesce(Batch_CD,""), 3," ") ||
            rpad(coalesce(SC_Combined_Disability_Percentage,""),3," ") ||
            rpad(coalesce(date_format(status_begin_date, 'yyyyMMdd'),""),8," ") ||
            rpad(coalesce(PT_Indicator,""),1," ") ||
            rpad(coalesce(Individual_Unemployability,""),1," ") ||  
            rpad(coalesce(date_format(Status_Last_Update, 'yyyyMMdd'),""),8," ") ||
            rpad(coalesce(date_format(Status_Termination_Date, 'yyyyMMdd'),""),8," ") as CG
        from cg_join_df
        union all
            select rpad(coalesce(lu_edipi,""),10," " ) ||
            rpad(coalesce(Batch_CD,""), 3," ") ||
            rpad(coalesce(SC_Combined_Disability_Percentage,""),3," ") ||
            rpad(coalesce(Status_Begin_Date,""),8," ") ||
            rpad(coalesce(PT_Indicator,""),1," ") ||
            rpad(coalesce(Individual_Unemployability,""),1," ") ||
            rpad(coalesce(Status_Last_Update,""),8," ") ||
            rpad(coalesce(Status_Termination_Date,""),8," ") as new_edipi
        from scd_join_df
        """


# COMMAND ----------

# DBTITLE 1,Write data to text file
date_format = today.strftime("%Y%m%d")
outuput_filename = f"LAGGED_EDIPI_PATRONAGE_{date_format}.txt"
outuput_path = f"/dbfs/mnt/ci-patronage/dmdc_extracts/combined_export/{outuput_filename}"
pandas_df = spark.sql(edipi_query).toPandas()

with open(outuput_path, "w", newline="\r\n") as f:            # DMDC requires output file as unix txt file
    f.write(pandas_df.to_string(index=False, header=False))

print(f'Data has been written to: {outuput_path}')
print(f'Number of records: {len(pandas_df)}')
print(pandas_df.to_string())

# COMMAND ----------

# DBTITLE 1,Update the delta table
targetTable = DeltaTable.forPath(spark, "/mnt/Patronage/SCD_Staging")

targetTable.alias("target").merge(
        scd_join_df.alias("source"),
        "target.ICN = source.ICN"
    ).whenMatchedUpdate(
        set={
            "edipi": "source.lu_edipi",
            # "SDP_Event_Created_Timestamp": "source.SDP_Event_Created_Timestamp",
        }
    ).execute()
