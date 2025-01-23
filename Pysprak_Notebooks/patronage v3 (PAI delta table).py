# Databricks notebook source
# DBTITLE 1,Import Libraries
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
import time
import os
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Define Schemas
# old_cg_schema = StructType(
#     [
#         StructField("Discharge_Revocation_Date__c", StringType(), True),
#         StructField("Caregiver_Status__c", StringType(), True),
#         StructField("CreatedById", StringType(), True),
#         StructField("Dispositioned_Date__c", StringType(), True),
#         StructField("Applicant_Type__c", StringType(), True),
#         StructField("CreatedDate", StringType(), True),
#         StructField("Veteran_ICN__c", StringType(), True),
#         StructField("Benefits_End_Date__c", StringType(), True),
#         StructField("Caregiver_Id__c", StringType(), True),
#         StructField("Caregiver_ICN__c", StringType(), True),
#     ]
# )

new_cg_schema = StructType(
    [
        StructField("Discharge_Revocation_Date__c", StringType(), True),
        StructField("Caregiver_Status__c", StringType(), True),
        StructField("CreatedById", StringType(), True),
        StructField("Dispositioned_Date__c", StringType(), True),
        StructField("CARMA_Case_ID__c", StringType(), True),
        StructField("Applicant_Type__c", StringType(), True),
        StructField("CreatedDate", StringType(), True),
        StructField("Veteran_ICN__c", StringType(), True),
        StructField("Benefits_End_Date__c", StringType(), True),
        StructField("Caregiver_Id__c", StringType(), True),
        StructField("CARMA_Case_Number__c", StringType(), True),
        StructField("Caregiver_ICN__c", StringType(), True),
    ]
)

scd_schema = StructType(
    [
        StructField("PTCPNT_ID", StringType()),
        StructField("FILE_NBR", StringType()),
        StructField("LAST_NM", StringType()),
        StructField("FIRST_NM", StringType()),
        StructField("MIDDLE_NM", StringType()),
        StructField("SUFFIX_NM", StringType()),
        StructField("STA_NBR", StringType()),
        StructField("BRANCH_OF_SERV", StringType()),
        StructField("DATE_OF_BIRTH", StringType()),
        StructField("DATE_OF_DEATH", StringType()),
        StructField("VET_SSN_NBR", StringType()),
        StructField("SVC_NBR", StringType()),
        StructField("AMT_GROSS_OR_NET_AWARD", IntegerType()),
        StructField("AMT_NET_AWARD", IntegerType()),
        StructField("NET_AWARD_DATE", StringType()),
        StructField("SPECL_LAW_IND", IntegerType()),
        StructField("VET_SSN_VRFCTN_IND", IntegerType()),
        StructField("WIDOW_SSN_VRFCTN_IND", IntegerType()),
        StructField("PAYEE_SSN", StringType()),
        StructField("ADDRS_ONE_TEXT", StringType()),
        StructField("ADDRS_TWO_TEXT", StringType()),
        StructField("ADDRS_THREE_TEXT", StringType()),
        StructField("ADDRS_CITY_NM", StringType()),
        StructField("ADDRS_ST_CD", StringType()),
        StructField("ADDRS_ZIP_PREFIX_NBR", IntegerType()),
        StructField("MIL_POST_OFFICE_TYP_CD", StringType()),
        StructField("MIL_POSTAL_TYPE_CD", StringType()),
        StructField("COUNTRY_TYPE_CODE", IntegerType()),
        StructField("SUSPENSE_IND", IntegerType()),
        StructField("PAYEE_NBR", IntegerType()),
        StructField("EOD_DT", StringType()),
        StructField("RAD_DT", StringType()),
        StructField("ADDTNL_SVC_IND", StringType()),
        StructField("ENTLMT_CD", StringType()),
        StructField("DSCHRG_PAY_GRADE_NM", StringType()),
        StructField("AMT_OF_OTHER_RETIREMENT", IntegerType()),
        StructField("RSRVST_IND", StringType()),
        StructField("NBR_DAYS_ACTIVE_RESRV", IntegerType()),
        StructField("CMBNED_DEGREE_DSBLTY", StringType()),
        StructField("DSBL_DTR_DT", StringType()),
        StructField("DSBL_TYP_CD", StringType()),
        StructField("VA_SPCL_PROV_CD", IntegerType()),
    ]
)

scd_schema1 = StructType(
    [
        StructField("PTCPNT_ID", StringType()),
        StructField("CMBNED_DEGREE_DSBLTY", StringType()),
        StructField("DSBL_DTR_DT", StringType()),
    ]
)

pai_schema = StructType(
    [
        StructField("EDI_PI", StringType()),
        StructField("SSN_NBR", StringType()),
        StructField("FILE_NBR", StringType()),
        StructField("PTCPNT_VET_ID", StringType()),
        StructField("LAST_NM", StringType()),
        StructField("FIRST_NM", StringType()),
        StructField("MIDDLE_NM", StringType()),
        StructField("PT35_RATING_DT", TimestampType()),
        StructField("PT35_PRMLGN_DT", TimestampType()),
        StructField("PT35_EFFECTIVE_DATE", TimestampType()),
        StructField("PT35_END_DATE", TimestampType()),
        StructField("PT_35_FLAG", StringType()),
        StructField("COMBND_DEGREE_PCT", StringType()),
    ]
)

file_list_schema = StructType(
    [
        StructField("path", StringType()),
        StructField("name", StringType()),
        StructField("size", StringType()),
        StructField("modificationTime", StringType()),
    ]
)

# Loookup table for ICN, EDIPI
# icn_relationship = (spark.read.format("delta")
#                     .load("dbfs:/FileStore/test_correlations")
icn_relationship = (spark.read.format("delta")
                    .load("/mnt/Patronage/identity_correlations")
                    .withColumnRenamed('MVIPersonICN', 'ICN')).persist()


# COMMAND ----------

# DBTITLE 1,Various conditions to use in upsert.
# Define join conditions based on file type
join_conditions = {
    "CG": (
        (col("ICN") == col("target_ICN"))
        & (col("Veteran_ICN") == col("target_Veteran_ICN"))
        & (col("Batch_CD") == col("target_Batch_CD"))
        & (col("Applicant_Type") == col("target_Applicant_Type"))
        & (col("target_RecordStatus") == True)
    ),
    "SCD": (
        (col("ICN") == col("target_ICN"))
        & (col("target_RecordStatus") == True)
        & (col("Batch_CD") == col("target_Batch_CD"))
    ),
}

# Define delta condition based on file type
delta_conditions = {
    "CG": xxhash64(
        col("Status_Begin_Date"),
        col("Status_Termination_Date"),
        col("Applicant_Type"),
        col("Caregiver_Status")
    ) != xxhash64(
        col("target_Status_Begin_Date"),
        col("target_Status_Termination_Date"),
        col("target_Applicant_Type"),
        col("target_Caregiver_Status")
    ),
    "SCD": xxhash64(
        col("SC_Combined_Disability_Percentage")
    ) != xxhash64(col("target_SC_Combined_Disability_Percentage")),
}

# Track changes in specified columns and create a change log
columns_to_track = {
    "CG": [
        ("Status_Begin_Date", "target_Status_Begin_Date"),
        ("Status_Termination_Date", "target_Status_Termination_Date"),
        ("Applicant_Type", "target_Applicant_Type"),
        ("Caregiver_Status", "target_Caregiver_Status")
    ],
    "SCD": [(
        "SC_Combined_Disability_Percentage", "target_SC_Combined_Disability_Percentage"),
    ],
    "PAI": [
        ("PT_Indicator", "target_PT_Indicator")
    ]
}

# Define merge conditions and batch_cd
merge_conditions = {
    "CG": "concat(target.ICN, target.Veteran_ICN, target.Applicant_Type) = source.MERGEKEY and target.RecordStatus = True",
    "SCD": "((target.ICN = source.MERGEKEY) and (target.Batch_CD = source.Batch_CD) and (target.RecordStatus = True))"
}

# Define concat_column based on file type
concat_column = {
    "CG": concat(col("ICN"), col("Veteran_ICN"), col("Applicant_Type")),
    "SCD": col("ICN")
}

# COMMAND ----------

# DBTITLE 1,Helper function to list required source files (list_files_recursive(path))
def list_files_recursive(path, cg_unix_start_time):
    """
    Recursively lists files at any depth inside a directory based on filtering criteria.
    Parameters:
        path (str): Path of a directory.
        cg_unix_start_time (int): Start time in Unix format for filtering.
    Returns:
        List[Tuple]: List of files with metadata.
    """
    items = dbutils.fs.ls(path)
    files = [
        (item.path, item.name, item.size, item.modificationTime)
        for item in items
        if not item.isDir()
        and (
            (
                item.name.startswith("caregiverevent")
                and item.name.endswith(".csv")
                and item.modificationTime > cg_unix_start_time
            )
            or (
                item.name.startswith("CPIDODIEX_")
                and item.name.endswith(".csv")
                and "NEW" not in item.name
            )
            or (item.name.startswith("WRTS") and item.name.endswith(".txt"))
            or (
                item.path.startswith("dbfs:/mnt/vac20sdpasa201vba/ci-vba-edw-2/")
                and item.name.endswith("parquet")
            )
        )
    ]

    subdir_files = [
        list_files_recursive(item.path, cg_unix_start_time)
        for item in items
        if item.isDir()
    ]

    # Flatten nested lists from subdirectories
    return files + [file for sublist in subdir_files for file in sublist]

# COMMAND ----------

# DBTITLE 1,Helper function converts datetime to  Unix time
def get_unix_time(year, month, day, hour=0, minute=0, second=0):
    """
    Converts a datetime to Unix time in milliseconds.
    """
    dt = datetime(year, month, day, hour, minute, second)
    return int(time.mktime(dt.timetuple())) * 1000

# COMMAND ----------

# DBTITLE 1,New Logic Source Data Collection (collect_data_source())
def collect_data_source():
    """
    Collects files from directories, filters them based on modification time, and returns a DataFrame.
    Returns:
        DataFrame: PySpark DataFrame with files ready to process.
    """
    cg_unix_start_time = get_unix_time(2024, 12, 18, 23, 59, 59)
    others_unix_start_time = get_unix_time(2023, 11, 1)

    raw_file_folders = [
        "/mnt/ci-carma/landing/",
        "/mnt/ci-vadir-shared/",
        "/mnt/ci-patronage/pai_landing/",
        "/mnt/vac20sdpasa201vba/ci-vba-edw-2/",
    ]

    # Collect and flatten all files from directories
    master_file_list = [
        file for folder in raw_file_folders 
        for file in list_files_recursive(folder, cg_unix_start_time)
    ]

    file_list_df = spark.createDataFrame(master_file_list, file_list_schema)

    filtered_file_list_df = (
        file_list_df.filter(col("modificationTime") > others_unix_start_time)
        .withColumn("dateTime", to_timestamp(col("modificationTime") / 1000))
        .orderBy(col("modificationTime"))
    )

    no_of_files = spark.sql(
        "SELECT COALESCE(COUNT(DISTINCT filename), 0) AS count FROM mypatronage_test"
    ).collect()[0][0]

    query = (
        f"""
        SELECT COALESCE(MAX(SDP_Event_Created_Timestamp), TIMESTAMP('{others_unix_start_time}')) AS max_date from mypatronage_test
        """
    )

    max_processed_date = (
        datetime.fromtimestamp(others_unix_start_time/1000)
        if no_of_files == 1
        else spark.sql(query).collect()[0][0]
    )

    now = datetime.now()
    yesterday_end_time = (datetime(now.year, now.month, now.day) - timedelta(hours=4)) #Need to adjust to blob storage time
    yesterday_end_time_ts = int(yesterday_end_time.timestamp() * 1000)

    final_file_list_df = filtered_file_list_df.filter(
        (col("dateTime") > max_processed_date)
        & (col("modificationTime") <= yesterday_end_time_ts)
    )

    parquet_PT_flag = final_file_list_df.filter(final_file_list_df['path'].contains('parquet')).orderBy(desc(col("modificationTime"))).limit(1)

    all_other_files = final_file_list_df.filter(~final_file_list_df['path'].contains('parquet'))

    files_to_process_now = all_other_files.unionAll(parquet_PT_flag)
    print(
        f"Upserting all files landed between date: {max_processed_date} and date: {yesterday_end_time}"
    )

    if files_to_process_now.count() > 0:
        return files_to_process_now.orderBy(col("modificationTime"))
    else:
        dbutils.notebook.exit("Notebook exited because there are no files to upsert.")
        pass
        # return None

# COMMAND ----------

# MAGIC %md
# MAGIC This is the latest All Caregivers initial seed data
# MAGIC   
# MAGIC dbfs:/FileStore/All_Caregivers_InitialSeed_12182024_csv.csv

# COMMAND ----------

# DBTITLE 1,New: Load Caregivers seed file to a dataframe  (initialize_caregivers())
def initialize_caregivers():
        new_cg_df = spark.read.csv("dbfs:/FileStore/All_Caregivers_InitialSeed_12182024_csv.csv", header=True, inferSchema=True) #use this dbfs:/FileStore/All_Caregivers_InitialSeed_12182024_csv.csv
        transformed_cg_df = (new_cg_df
                             .select(substring("ICN", 1, 10).alias("ICN"),  
                                     "Applicant_Type", "Caregiver_Status", 
                                     date_format("Status_Begin_Date",'yyyyMMdd').alias("Status_Begin_Date"), 
                                     date_format("Status_Termination_Date", 'yyyyMMdd').alias("Status_Termination_Date"), 
                                     substring("Veteran_ICN", 1, 10).alias("Veteran_ICN"))
        )

        edipi_df = (
                broadcast(transformed_cg_df)
                .join(icn_relationship, ["ICN"], "left")
                .withColumn("filename", lit("dbfs:/FileStore/All_Caregivers_InitialSeed_12182024_csv.csv"))
                .withColumn("SDP_Event_Created_Timestamp", lit('2024-12-18T23:59:59.000+00:00').cast(TimestampType())) # lit('2024-12-18T23:59:59.000+00:00')
                .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
                .withColumn("PT_Indicator", lit(None).cast(StringType()))
                .withColumn("SC_Combined_Disability_Percentage", lit(None).cast(StringType()))
                .withColumn("RecordStatus", lit(True).cast(BooleanType()))
                .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
                .withColumn("Status_Last_Update", lit(None).cast(StringType()))
                .withColumn("sentToDoD", lit(False).cast(BooleanType()))
                .withColumn("Batch_CD", lit("CG").cast(StringType()))
        )
        return edipi_df

# COMMAND ----------

# DBTITLE 1,Upsert Caregivers, PAI and IF8 (process_updates())
def process_updates(edipi_df, file_type):
    
    """
    Upserts the input dataframe depending on input file type ('CG', 'PAI or 'SCD'). Uses Slowly Changing Dimensions type 2 logic that stores records that have been updated.
    Parameters: Pyspark dataframe and file type
    Returns: None
    """
    # Optimize the table based on the event timestamp
    spark.sql("OPTIMIZE mypatronage_test ZORDER BY (SDP_Event_Created_Timestamp)")

    # Load the target Delta table
    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_test")
    targetDF = targetTable.toDF().filter(
        (col("Batch_CD") == file_type) & (col("RecordStatus") == True)
    )
    # Rename columns in targetDF for clarity
    targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])

    # Perform the join based on file type
    if file_type in ["CG", "SCD"]:
        joinDF = broadcast(edipi_df).join(targetDF, join_conditions[file_type], "leftouter")

        # Handling logic for SCD file type
        if file_type == "SCD":
            joinDF = (
                joinDF.withColumn(
                    "Status_Last_Update",
                    col("DSBL_DTR_DT")
                )
                .withColumn(
                    "Status_Begin_Date",
                    coalesce(col("target_Status_Begin_Date"), col("DSBL_DTR_DT"))
                )
                .withColumn(
                    "PT_Indicator", coalesce(joinDF["target_PT_Indicator"], lit("N"))
                )
            )

        # Filter records that have changes based on delta condition
        filterDF = joinDF.filter(delta_conditions[file_type])

        # Handle dummy records with null MERGEKEY for unmatched records
        mergeDF = filterDF.withColumn("MERGEKEY", concat_column[file_type])
        dummyDF = filterDF.filter(col("target_ICN").isNotNull()).withColumn("MERGEKEY", lit(None))

        # Union the filtered and dummy DataFrames
        upsert_df = mergeDF.union(dummyDF)

    if file_type == "PAI":
        upsert_df = edipi_df 

    change_conditions = []
    for source_col, target_col in columns_to_track[file_type]:
        change_condition = when(
            xxhash64(coalesce(col(source_col), lit("Null"))) != xxhash64(coalesce(col(target_col), lit("Null"))),
            concat_ws(
                " ",
                lit(source_col),
                lit("old value:"),
                coalesce(col(target_col), lit("Null")),
                lit("changed to new value:"),
                coalesce(col(source_col), lit("Null"))
            )
        ).otherwise(lit(""))
        change_conditions.append(change_condition)

    new_record_condition = when(
        col("target_icn").isNull(), 
        lit("New Record")
        ).otherwise(lit("Updated Record")
        )

    upsert_df = upsert_df.withColumn("RecordChangeStatus", new_record_condition)

    if len(change_conditions) > 0:
        change_log_col = concat_ws(" ", *[coalesce(cond, lit("")) for cond in change_conditions])
    else:
        change_log_col = lit("")

    upsert_df = upsert_df.withColumn("change_log", change_log_col) 

    if file_type == "PAI":
        file_type = "SCD"
    # Perform the merge operation
    targetTable.alias("target").merge(
        upsert_df.alias("source"),
        merge_conditions[file_type]
    ).whenMatchedUpdate(
        set={
            "RecordStatus": "False",
            "RecordLastUpdated": "source.SDP_Event_Created_Timestamp",
            "sentToDoD": "true",
            "RecordChangeStatus": lit("Expired Record")
        }
    ).whenNotMatchedInsert(
        values={
            "edipi": "source.edipi",
            "ICN": "source.ICN",
            "Veteran_ICN": "source.Veteran_ICN",
            "Applicant_Type": "source.Applicant_Type",
            "Caregiver_Status": "source.Caregiver_Status",
            "participant_id": "source.participant_id",
            "Batch_CD": "source.Batch_CD",
            "SC_Combined_Disability_Percentage": "source.SC_Combined_Disability_Percentage",
            "PT_Indicator": "source.PT_Indicator",
            "Individual_Unemployability": "source.Individual_Unemployability",
            "Status_Begin_Date": "source.Status_Begin_Date",
            "Status_Last_Update": "source.Status_Last_Update",
            "Status_Termination_Date": "source.Status_Termination_Date",
            "SDP_Event_Created_Timestamp": "source.SDP_Event_Created_Timestamp",
            "RecordStatus": "true",
            "RecordLastUpdated": "source.RecordLastUpdated",
            "filename": "source.filename",
            "sentToDoD": "false",
            "change_log": "source.change_log",
            "RecordChangeStatus": "source.RecordChangeStatus"
        }
    ).execute()

# COMMAND ----------

def prepare_caregivers_data(cg_csv_files):
    """
    Filters caregivers filenames from input dataframe, aggregates data and returns dataframe
    Parameters: Pyspark dataframe with all filenames and metadata that are not processed (upsert)
    Returns: Dataframe: Dataframe with required column names and edipi of a caregiver ready for upsert
    """
    print(f"Upserting records from {cg_csv_files.count()} caregivers aggregated files")
    Window_Spec = Window.partitionBy("ICN", "Veteran_ICN", "Applicant_Type").orderBy(
        desc("Event_Created_Date")
    )

    cg_csv_files_to_process = cg_csv_files.select(collect_list('path')).first()[0]
    cg_df = (spark.read.schema(new_cg_schema).csv(cg_csv_files_to_process, header=True, inferSchema=False)
                        .withColumn("filename", col("_metadata.file_path"))
                        .withColumn("SDP_Event_Created_Timestamp", (col("_metadata.file_modification_time")).cast(TimestampType()))
    )

    combined_cg_df = (cg_df
                      .select(
                        substring("Caregiver_ICN__c", 1, 10).alias("ICN"),
                        substring("Veteran_ICN__c", 1, 10).alias("Veteran_ICN"),
                        date_format("Benefits_End_Date__c", "yyyyMMdd").alias("Status_Termination_Date").cast(StringType()),
                        col("Applicant_Type__c").alias("Applicant_Type"),
                        col("Caregiver_Status__c").alias("Caregiver_Status"),
                        date_format("Dispositioned_Date__c", "yyyyMMdd").alias("Status_Begin_Date").cast(StringType()),
                        col("CreatedDate").cast("timestamp").alias("Event_Created_Date"),
                        "filename",
                        "SDP_Event_Created_Timestamp",
                    )
                ).filter(col("Caregiver_ICN__c").isNotNull())

    edipi_df = (
        broadcast(combined_cg_df)
        .join(icn_relationship, ["ICN"], "left")
        .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
        .withColumn("PT_Indicator", lit(None).cast(StringType()))
        .withColumn("SC_Combined_Disability_Percentage", lit(None).cast(StringType()))
        .withColumn("RecordStatus", lit(True).cast(BooleanType()))
        .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
        .withColumn("Status_Last_Update", lit(None).cast(StringType()))
        .withColumn("sentToDoD", lit(False).cast(BooleanType()))
        .withColumn("Batch_CD", lit("CG").cast(StringType()))
        .withColumn("rank", rank().over(Window_Spec))
        .filter(col("rank") == 1)
        .dropDuplicates().drop("rank", "va_profile_id", "record_updated_date")
    ).orderBy(col("Event_Created_Date"))

    return edipi_df

# COMMAND ----------

# DBTITLE 1,Prepare SCD data (IF8) (prepare_scd_data())
def prepare_scd_data(row):
    """
    Prepares SCD data from the input row. This is the disability % data.
    Parameters: Row of data from pyspark dataframe with filenames and metadata that are not processed (upsert)
    Returns: Dataframe: Dataframe with required column names and edipi of a Veteran ready for upsert
    """

    file_name = row.path

    print(f"Upserting records from {file_name}")

    if len(spark.read.csv(file_name).columns) != 3:
        schema = scd_schema
    else:
        schema = scd_schema1  # the schema changed  to 3 columns after May

    scd_updates_df = (
        spark.read.csv(file_name, schema=schema, header=True, inferSchema=False)
        .selectExpr(
            "PTCPNT_ID as participant_id", "CMBNED_DEGREE_DSBLTY", "DSBL_DTR_DT"
        )
        .withColumn("filename", col("_metadata.file_path"))
        .withColumn("sentToDoD", lit(False).cast(BooleanType()))
        .withColumn("SDP_Event_Created_Timestamp", (col("_metadata.file_modification_time")).cast(TimestampType()),)
        .withColumn("SC_Combined_Disability_Percentage", 
                    lpad(coalesce( when(col("CMBNED_DEGREE_DSBLTY") == "", lit("000")).otherwise(col("CMBNED_DEGREE_DSBLTY"))), 3, "0", ), )
        .withColumn("DSBL_DTR_DT", when(col("DSBL_DTR_DT") == "", None).otherwise( date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), "yyyyMMdd")), )
    ).filter(col("DSBL_DTR_DT").isNotNull())

    Window_Spec = Window.partitionBy(scd_updates_df["participant_id"]).orderBy(
        desc("DSBL_DTR_DT"), desc("SC_Combined_Disability_Percentage")
    )

    edipi_df = (
        broadcast(scd_updates_df)
        .join(icn_relationship, ["participant_id"], "left")
        .withColumn("rank", rank().over(Window_Spec))
        .withColumn("Veteran_ICN", lit(None).cast(StringType()))
        .withColumn("Applicant_Type", lit(None).cast(StringType()))
        .withColumn("Caregiver_Status", lit(None).cast(StringType()))
        .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
        .withColumn("Status_Termination_Date", lit(None).cast(StringType()))
        .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
        .withColumn("Batch_CD", lit("SCD"))
        .withColumn("RecordStatus", lit(True).cast(BooleanType()))
        .filter(col("rank") == 1)
        .filter(col("ICN").isNotNull())
        .dropDuplicates()
        .drop("rank", "va_profile_id", "record_updated_date")
    )

    return edipi_df

# COMMAND ----------

# DBTITLE 1,Prepare PAI data (update_pai_data())
def update_pai_data(row, source_type):
    """
    Prepares PT Indicator from the input row, transforms and updates a Veteran's PT_Indicator column in delta table. 
    Parameters: Row of data from pyspark dataframe with filename and metadata that are not processed (upsert)
    Returns: Dataframe: Dataframe with required column names ready for upsert
    """

    if source_type == 'text':
        file_name = row.path
        file_creation_dateTime = row.dateTime 
        raw_pai_df = (
        spark.read.csv(file_name, header=True, inferSchema=True)
        .withColumn(
            "SDP_Event_Created_Timestamp",
            (col("_metadata.file_modification_time")).cast(TimestampType()),
        )
        .withColumn("filename", col("_metadata.file_path")))
    elif source_type == 'table':
        file_creation_dateTime = row.dateTime
        file_name = f"Updated from PA&I delta table on {file_creation_dateTime}"
        raw_pai_df = (spark.sql(""" SELECT * FROM DELTA.`/mnt/vac20sdpasa201vba/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/` """))

    print(f"Updating PT Indicator")

    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_test")
    targetDF = (
        targetTable.toDF().filter("Batch_CD == 'SCD'").filter("RecordStatus=='True'")
    )
    targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])

    pai_df = raw_pai_df.selectExpr(
        "PTCPNT_VET_ID as participant_id", "PT_35_FLAG as source_PT_Indicator"
    )
    existing_pai_data_df = spark.sql(
        "SELECT participant_id, PT_Indicator from mypatronage_test where RecordStatus is True and Batch_CD = 'SCD'"
    )

    joinDF = (
        pai_df.join(
            broadcast(targetDF),
            pai_df["participant_id"] == targetDF["target_participant_id"],
            "left",
        )
        .filter(targetDF["target_PT_Indicator"] == "N")
        .withColumn("filename", lit(file_name))
        .withColumn("SDP_Event_Created_Timestamp", lit(file_creation_dateTime))
    )

    filterDF = joinDF.filter(
        xxhash64(joinDF.source_PT_Indicator) != xxhash64(joinDF.target_PT_Indicator)
    )

    mergeDF = filterDF.withColumn("MERGEKEY", filterDF.target_ICN)

    dummyDF = filterDF.filter("target_ICN is not null").withColumn(
        "MERGEKEY", lit(None)
    )

    paiDF = mergeDF.union(dummyDF)

    edipi_df = (paiDF.selectExpr(
        "target_edipi as edipi",
        "participant_id",
        "target_ICN",
        "MERGEKEY",
        "target_SC_Combined_Disability_Percentage as SC_Combined_Disability_Percentage",
        "target_Status_Begin_Date as Status_Begin_Date",
        "target_Status_Last_Update as Status_Last_Update",
        "SDP_Event_Created_Timestamp",
        "filename","source_PT_Indicator", "target_PT_Indicator"
    )
    .withColumn("ICN", lit(col("target_ICN")))
    .withColumn("Veteran_ICN", lit(None))
    .withColumn("Applicant_Type", lit(None))
    .withColumn("Caregiver_Status", lit(None))
    .withColumn("Batch_CD", lit("SCD"))
    .withColumn("PT_Indicator", coalesce(col("source_PT_Indicator"), lit("N")))
    .withColumn("Individual_Unemployability", lit(None))
    .withColumn("Status_Termination_Date", lit(None))
    .withColumn("RecordLastUpdated", lit(None))
    )
    return(edipi_df)

# COMMAND ----------

# DBTITLE 1,Processing unprocessed files (process_files())
def process_files(files_to_process_now):
    """
    Seggregates files based on filename and calls required function to process these files
    Parameters: Pyspark dataframe with filenames and metadata that are not processed
    Returns: None
    """
    cg_csv_files = files_to_process_now.filter(files_to_process_now['path'].contains('caregiverevent'))
    edipi_df = prepare_caregivers_data(cg_csv_files)
    process_updates(edipi_df, "CG")
    # display(cg_csv_files)

    other_files = files_to_process_now.filter(~files_to_process_now['path'].contains('caregiverevent'))
    other_rows = other_files.collect()

    for row in other_rows:
        filename = row.path
        if "CPIDODIEX" in filename:
            print(f"Now processing: {row.path}")
            edipi_df = prepare_scd_data(row)
            process_updates(edipi_df, "SCD")
        elif "WRTS" in filename:
            print(f"Now processing: {row.path}")
            edipi_df = update_pai_data(row, "text")
            process_updates(edipi_df, "PAI")
        elif 'parquet' in filename:
            print(f"Now processing: {row.path}")
            edipi_df = update_pai_data(row, "table")
            process_updates(edipi_df, "PAI")
        else:
            pass

# COMMAND ----------

spark.sql(
    """ CREATE TABLE IF NOT EXISTS mypatronage_test (edipi string, ICN string, Veteran_ICN string, participant_id string, Batch_CD string,  Applicant_Type string, Caregiver_Status string, SC_Combined_Disability_Percentage string, PT_Indicator string, Individual_Unemployability string, Status_Begin_Date string, Status_Last_Update string, Status_Termination_Date string, SDP_Event_Created_Timestamp timestamp, filename string, RecordLastUpdated date, RecordStatus boolean, sentToDoD boolean, change_log string, RecordChangeStatus string) PARTITIONED BY(Batch_CD, RecordStatus ) LOCATION 'dbfs:/user/hive/warehouse/mypatronage_test' """
)

# COMMAND ----------

# DBTITLE 1,Bundling everything (patronage())
def patronage():


    file_counts = spark.sql(
        "SELECT COALESCE(COUNT(DISTINCT filename), 0) AS count from mypatronage_test"
    ).collect()[0][0]
    print(f"Total files processed till last run: {file_counts}")

    if file_counts == 0:
        print("Loading seed file into delta table")
        seed_df = initialize_caregivers()
        process_updates(seed_df, "CG")
        print("Loading required files for upsert")
        files_to_process_now = collect_data_source()
        print(f"Total files to process in this run: {files_to_process_now.count()}...")
        process_files(files_to_process_now)
    else:
        print("Loading required files for upsert")
        files_to_process_now = collect_data_source()
        print(f"Total files to process in this run: {files_to_process_now.count()}...")
        process_files(files_to_process_now)

# COMMAND ----------

# DBTITLE 1,patronage() function
patronage()

# COMMAND ----------

# DBTITLE 1,Displaying Files to be upserted since the last run (Testing Code)
files_to_process_now = collect_data_source()

display(files_to_process_now)

# COMMAND ----------

files_to_process_now = collect_data_source()

display(files_to_process_now)

# COMMAND ----------

remaining_files = files_to_process_now.filter(files_to_process_now['path'].contains('parquet'))
file_creation_dateTime = remaining_files.select(max("dateTime")).collect()[0][0]
print((file_creation_dateTime))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM mypatronage_test
# MAGIC where ICN NOT RLIKE '^[0-9]'

# COMMAND ----------

# DBTITLE 1,Max and Min file processed date
# MAGIC %sql
# MAGIC SELECT
# MAGIC   BATCH_CD,
# MAGIC   min(SDP_Event_Created_Timestamp) First_Processed_date,
# MAGIC   max(SDP_Event_Created_Timestamp) Last_Processed_date
# MAGIC FROM
# MAGIC   mypatronage_test
# MAGIC GROUP BY 1

# COMMAND ----------

# spark.sql("drop table mypatronage_test")
# dbutils.fs.rm('dbfs:/user/hive/warehouse/mypatronage_test', True)

# COMMAND ----------

# MAGIC %sql select date_format(current_date(), 'yyyyMMdd')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Batch_CD, count(*), (DATE(SDP_Event_Created_Timestamp)) 
# MAGIC FROM 
# MAGIC mypatronage_test
# MAGIC -- DELTA.`/mnt/Patronage/SCD_Staging`
# MAGIC -- DELTA.`/mnt/Patronage/Caregivers_Staging_New`
# MAGIC WHERE EDIPI IS NOT NULL
# MAGIC GROUP BY ALL
# MAGIC ORDER BY 3 DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), (DATE(SDP_Event_Created_Timestamp)) 
# MAGIC FROM DELTA.`/mnt/Patronage/SCD_Staging` 
# MAGIC GROUP BY ALL 
# MAGIC ORDER BY 2 DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT RecordChangeStatus, count(*), SDP_Event_Created_Timestamp
# MAGIC FROM   mypatronage_test
# MAGIC WHERE SDP_Event_Created_Timestamp =  (SELECT DISTINCT SDP_Event_Created_Timestamp FROM mypatronage_test where BATCH_CD = 'SCD' ORDER BY 1 desc LIMIT 1 OFFSET 0)
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Applicant_Type as distinct_cg, count(*) 
# MAGIC FROM mypatronage_test 
# MAGIC WHERE BATCH_CD = 'CG' 
# MAGIC AND recordstatus is TRUE 
# MAGIC GROUP BY ALL 
# MAGIC ORDER BY 2 desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM DELTA.`/mnt/Patronage/SCD_Staging`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DISTINCT SC_Combined_Disability_Percentage, PT_Indicator, count(*) as Total_Count FROM DELTA.`/mnt/Patronage/SCD_Staging` group BY ALL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  DISTINCT PT_Indicator, count(*) as Total_Count FROM DELTA.`/mnt/Patronage/SCD_Staging` group BY ALL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT caregiver_status FROM mypatronage_test

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/ci-patronage/pai_landing/pt-indicator-6-6-24", schema=pai_schema, header=True)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM DELTA.`/mnt/vac20sdpasa201vba/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/` where PTCPNT_VET_ID = 14337381

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (DESCRIBE HISTORY mypatronage_test)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RESTORE TABLE mypatronage_test VERSION AS OF 126

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM mypatronage_test
# MAGIC WHERE Batch_CD = 'SCD'
# MAGIC AND filename = "Updated from PA&I delta table on 2025-01-10 18:40:52"

# COMMAND ----------

raw_file_folders = [
    "/mnt/ci-carma/landing/",
    "/mnt/ci-vadir-shared/",
    "/mnt/ci-patronage/pai_landing/",
    "/mnt/vac20sdpasa201vba/ci-vba-edw-2/",
]
cg_unix_start_time = get_unix_time(2024, 12, 18, 23, 59, 59)

def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        (dir_path.path, dir_path.name, dir_path.size, dir_path.modificationTime)
        if dir_path.isFile():
            if (
                (
                    dir_path.name.startswith("caregiverevent")
                    and dir_path.name.endswith(".csv")
                    and dir_path.modificationTime > cg_unix_start_time
                )
                or (
                    dir_path.name.startswith("CPIDODIEX_")
                    and dir_path.name.endswith(".csv")
                    and "NEW" not in dir_path.name
                )
                or (dir_path.name.startswith("WRTS") and dir_path.name.endswith(".txt"))
                or (dir_path.path.startswith("dbfs:/mnt/vac20sdpasa201vba/ci-vba-edw-2/") and dir_path.name.endswith("parquet"))
            ):
                yield dir_path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)

# required_files = []
# for folders in raw_file_folders:
#     required_files += list(get_dir_content(folders))
# # print(required_files)
# test_df = spark.createDataFrame(data=required_files, schema=file_list_schema)

master_file_list = [
    file for folder in raw_file_folders 
    for file in get_dir_content(folder)
]

file_list_df = spark.createDataFrame(master_file_list, file_list_schema)

# COMMAND ----------

file_list_df.withColumn("dateTime", to_timestamp(col("modificationTime") / 1000)).orderBy(col("modificationTime")).display()
test_df.display()
