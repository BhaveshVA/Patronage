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

path = "/mnt/vac20sdpasa201vba/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/"
dbutils.fs.ls(path)

# COMMAND ----------

def validate_data(df: DataFrame, checks: List[Tuple[str, str]]) -> List[str]:
    """
    Parameters: df-->Dataframe, checks-->List
    Validate data based on the given checks and returns an error message
    Returs custom list of error messages if condtion is met
    """
    return [error_message for condition, error_message in checks if df.filter(condition).limit(1).count()>0]

def read_files(file_path: str, file_type: str, file_name: str) -> DataFrame:
    """
    Parameters: file_path-->str, file_type-->str, file_name-->str
    Read the files from the given path and returns the dataframe
    """
    files_df = (
        spark.read.format(file_type)
        .load(file_path)
        .drop("content")
        .filter(col("path").contains(file_name) & col("path").endswith(".csv"))
    )
    return files_df

def get_max_mod_time(df: DataFrame) -> datetime:
    """
    Parameters: df-->Dataframe
    Get the maximum modification time from the given dataframe
    """
    return df.select(max("modificationTime")).first()[0]

def get_files(path: str) -> DataFrame:
    """
    Parameters: path-->str
    Get the files from the given path and returns the dataframe
    """
    files = dbutils.fs.ls(path)
    files_df = spark.createDataFrame(files)
    files_df = files_df.withColumn("modificationTime", to_timestamp(col("modificationTime") / 1000))
    return files_df

def get_null_edipi(df: DataFrame, icn_relationship: DataFrame) -> DataFrame:
    """
    Parameters: df-->Dataframe, icn_relationship-->Dataframe
    Get the null edipi from the given dataframe and returns the dataframe
    """
    null_edipi_df = df.filter(col("edipi").isNull())
    join_df = null_edipi_df.join(icn_relationship, col("lu_ICN") == col("ICN"), "inner")
    return join_df

def get_new_edipi_cnt(df: DataFrame) -> int:
    """
    Parameters: df-->Dataframe
    Get the new edipi count from the given dataframe
    """
    return df.count()

def main():

    # Paths and reading source files
    cg_file_path = "/mnt/ci-carma/landing"
    scd_file_path = "/mnt/ci-vadir-shared/"
    pt_indicator_path = "/mnt/vac20sdpasa201vba/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/"

    cg_files_df = read_files(cg_file_path, "binaryFile", 'caregiverevent')
    scd_files_df = read_files(scd_file_path, "binaryFile", 'CPIDODIEX_')

    pt_files_df = get_files(pt_indicator_path)

    cg_max_mod_time = get_max_mod_time(cg_files_df)
    scd_max_mod_time = get_max_mod_time(scd_files_df)
    pt_max_mod_time = get_max_mod_time(pt_files_df)

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

    pt_indicator_checks = [
        (~col('PT_35_FLAG').isin(['Y']), "Invalid PT Indicator")
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

    pt_table_df = spark.table("DELTA.`/mnt/vac20sdpasa201vba/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/`")


    scd_null_edipi_df = get_null_edipi(scd_table_df, icn_relationship)
    cg_null_edipi_df = get_null_edipi(cg_table_df, icn_relationship)

    # Create Temp View 
    cg_null_edipi_df.createOrReplaceTempView("cg_null_df")
    scd_null_edipi_df.createOrReplaceTempView("scd_null_df")

    scd_new_edipi_cnt = get_new_edipi_cnt(scd_null_edipi_df)
    cg_new_edipi_cnt = get_new_edipi_cnt(cg_null_edipi_df)

    # Validation messages
    error_messages = []
    error_messages.extend(validate_data(scd_table_df, scd_validation_checks))
    error_messages.extend(validate_data(cg_table_df, cg_validation_checks))
    error_messages.extend(validate_data(pt_table_df, pt_indicator_checks))

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
    if error_messages and (cg_null_edipi_df or cg_null_edipi_df):
        status = "Failed"
        print({f"'Today': {today},  'status': {status}, 'errors': {error_messages}"})
    else:
        status = "Passed"
        dbutils.notebook.exit({f"'Today': {today},'status': {status}, 'message': 'No errors found'"})

main()

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
        from cg_null_df
        union all
            select rpad(coalesce(lu_edipi,""),10," " ) ||
            rpad(coalesce(Batch_CD,""), 3," ") ||
            rpad(coalesce(SC_Combined_Disability_Percentage,""),3," ") ||
            rpad(coalesce(Status_Begin_Date,""),8," ") ||
            rpad(coalesce(PT_Indicator,""),1," ") ||
            rpad(coalesce(Individual_Unemployability,""),1," ") ||
            rpad(coalesce(Status_Last_Update,""),8," ") ||
            rpad(coalesce(Status_Termination_Date,""),8," ") as new_edipi
        from scd_null_df
        """


# COMMAND ----------

# DBTITLE 1,Write data to text file
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

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
scd_join_df = spark.sql("select * from scd_null_df")
# scd_join_df.display()
targetTable.alias("target").merge(
        scd_join_df.alias("source"),
        "target.ICN = source.ICN"
    ).whenMatchedUpdate(
        set={
            "edipi": "source.lu_edipi",
            # "SDP_Event_Created_Timestamp": "source.SDP_Event_Created_Timestamp",
        }
    ).execute()
