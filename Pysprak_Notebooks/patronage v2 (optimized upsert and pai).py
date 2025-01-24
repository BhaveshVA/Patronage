# Databricks notebook source
# DBTITLE 1,Install openpyxl to read .xlsx files
# MAGIC %pip install openpyxl;

# COMMAND ----------

# DBTITLE 1,Import Libraries
spark.conf.set("spark.databricks.io.cache.enabled", "true")
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
old_cg_schema = StructType(
    [
        StructField("Discharge_Revocation_Date__c", StringType(), True),
        StructField("Caregiver_Status__c", StringType(), True),
        StructField("CreatedById", StringType(), True),
        StructField("Dispositioned_Date__c", StringType(), True),
        StructField("Applicant_Type__c", StringType(), True),
        StructField("CreatedDate", StringType(), True),
        StructField("Veteran_ICN__c", StringType(), True),
        StructField("Benefits_End_Date__c", StringType(), True),
        StructField("Caregiver_Id__c", StringType(), True),
        StructField("Caregiver_ICN__c", StringType(), True),
    ]
)

cg_schema = StructType(
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
icn_relationship = (spark.read.format("delta")
                    .load("/mnt/Patronage/identity_correlations")
                    .withColumnRenamed('MVIPersonICN', 'ICN'))

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

# DBTITLE 1,Helper function to list required source files (all_file_list(path))
def all_file_list(path):
    """
    Creates a List of files that are at any depth inside a directory 
    Parameter: Path of a directory
    Returns: Flat List with file names and it's metadata
    """
    items = dbutils.fs.ls(path)
    files = []

    for item in items:
        if item.isDir():
            # If it's a directory, recursively list its contents
            files += all_file_list(item.path)
        elif (
            (item.name.startswith("caregiverevent") and item.name.endswith(".csv"))
            or (
                (item.name.startswith("CPIDODIEX_") and item.name.endswith(".csv"))
                and "NEW" not in item.name
            )
            or (item.name.startswith("WRTS") and item.name.endswith(".txt"))
        ):
            files.append((item.path, item.name, item.size, item.modificationTime))

    return files

# COMMAND ----------

# DBTITLE 1,Source Data Collection (collect_data_source())
def collect_data_source():
    """
    This is a helper function that will create a pyspark dataframe with all the files required to process
    Returns: Pyspark dataframe with files that are ready to process from all the 
    """
    start_time = datetime(2023, 10, 30, 23, 59, 59)
    unix_start_time = int(time.mktime(start_time.timetuple())) * 1000

    raw_file_folders = [
        "/mnt/ci-carma/landing/",
        "/mnt/ci-vadir-shared/",
        "/mnt/ci-patronage/pai_landing/",
    ]
    master_file_list = []

    for files in raw_file_folders:
        master_file_list.append(all_file_list(files))
    flattened_file_list = [item for sublist in master_file_list for item in sublist]

    file_list_df = spark.createDataFrame(flattened_file_list, file_list_schema)

    filtered_file_list_df = (
        file_list_df.filter(file_list_df["modificationTime"] > unix_start_time)
        .withColumn("dateTime", to_timestamp(col("modificationTime") / 1000))
        .orderBy(col("modificationTime"))
    )

    query = f"""SELECT COALESCE(MAX(SDP_Event_Created_Timestamp), TIMESTAMP('{start_time}')) AS max_date from mypatronage_new """

    max_processed_date = spark.sql(query).collect()[0][0]

    files_to_process_now = filtered_file_list_df.filter(
        filtered_file_list_df["dateTime"] > max_processed_date
    ).orderBy(col("modificationTime"))

    now = datetime.now()
    today_start_time = datetime(now.year, now.month, now.day)
    yesterday_end_time = today_start_time - timedelta(hours=4)  # To match the time of file creation date in blob storage
    yesterday_end_time_ts = int(yesterday_end_time.timestamp() * 1000)

    files_to_process_now = files_to_process_now.filter(
        files_to_process_now["modificationTime"] <= yesterday_end_time_ts
    )
    print(f"Last upsert file date: {max_processed_date}")

    if files_to_process_now.count() != 0:
        return files_to_process_now
    else:
        dbutils.notebook.exit("Notebook exited because there are no files to upsert.")

# COMMAND ----------

# DBTITLE 1,Load Caregivers seed file to a dataframe  (initialize_caregivers())
def initialize_caregivers():
    """
    Load one time initial seed excel file data into a spark dataframe
    Parameters: None
    Returns: Dataframe: Dataframe with required column names and edipi of a caregiver

    """
    file_path = "/dbfs/FileStore/CGs31October2023.xlsx"

    file_creation_time = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime(
        "%Y-%m-%d"
    )
    pandas_df = pd.read_excel(file_path)
    df = spark.createDataFrame(pandas_df)
    df = df.withColumn("filename", lit(file_path))
    df = df.withColumn(
        "SDP_Event_Created_Timestamp", lit(file_creation_time).cast(TimestampType())
    )
    initial_df = df.toDF(*(col.replace(" ", "_") for col in df.columns))

    cg_df = (
        (
            initial_df.withColumn(
                "Event_Created_Date", lit(col("SDP_Event_Created_Timestamp"))
            )
            .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
            .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
            .withColumn("PT_Indicator", lit(None).cast(StringType()))
            .withColumn(
                "SC_Combined_Disability_Percentage", lit(None).cast(StringType())
            )
            .withColumn("RecordStatus", lit(True).cast(BooleanType()))
            .withColumn("Status_Last_Update", lit(None).cast(StringType()))
            .withColumn("sentToDoD", lit(False).cast(BooleanType()))
            .select(
                substring("Person_ICN", 1, 10).alias("ICN"),
                substring("CARMA_Case_Details:_Veteran_ICN", 1, 10).alias(
                    "Veteran_ICN"
                ),
                date_format("Benefits_End_Date", "yyyyMMdd")
                .alias("Status_Termination_Date")
                .cast(StringType()),
                "Applicant_Type",
                "Caregiver_Status",
                date_format("Dispositioned_Date", "yyyyMMdd")
                .alias("Status_Begin_Date")
                .cast(StringType()),
                lit("CG").alias("Batch_CD"),
                "filename",
                "SDP_Event_Created_Timestamp",
                "Event_Created_Date",
                "RecordLastUpdated",
                "Individual_Unemployability",
                "PT_Indicator",
                "SC_Combined_Disability_Percentage",
                "RecordStatus",
                "Status_Last_Update",
                "sentToDoD",
            )
        )
        .filter(col("ICN").isNotNull())
        .dropDuplicates()
    )
    lookup_df = icn_relationship.selectExpr(
        "ICN as MVIPersonICN", "edipi", "participant_id"
    )
    edipi_df = cg_df.alias("source").join(
        lookup_df, lookup_df["MVIPersonICN"] == cg_df["ICN"], "left"
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
    spark.sql("OPTIMIZE mypatronage_new ZORDER BY (SDP_Event_Created_Timestamp)")

    # Load the target Delta table
    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_new")
    targetDF = targetTable.toDF().filter(
        (col("Batch_CD") == file_type) & (col("RecordStatus") == True)
    )
    # Rename columns in targetDF for clarity
    targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])

    # Perform the join based on file type
    if file_type in ["CG", "SCD"]:
        joinDF = edipi_df.join(targetDF, join_conditions[file_type], "leftouter")

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
        change_log_col = concat_ws(". ", *[coalesce(cond, lit("")) for cond in change_conditions])
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

# DBTITLE 1,Prepare CG data (prepare_caregivers_data())
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

    cg_csv_files_before_08082024 = cg_csv_files.filter(cg_csv_files["dateTime"] < '2024-08-08 19:51:00')
    filenames_before_08082024 = cg_csv_files_before_08082024.select(collect_list('path')).first()[0]
    df_before_08082024 = (spark.read.schema(old_cg_schema).csv(filenames_before_08082024, header=True, inferSchema=False)
                        .withColumn("filename", col("_metadata.file_path"))
                        .withColumn("SDP_Event_Created_Timestamp", (col("_metadata.file_modification_time")).cast(TimestampType()))
    )

    cg_csv_files_after_08082024 = cg_csv_files.filter(cg_csv_files["dateTime"] >= '2024-08-08 19:51:00')
    filenames_after_08082024 = cg_csv_files_after_08082024.select(collect_list('path')).first()[0]
    df_after_08082024 = (spark.read.schema(cg_schema).csv(filenames_after_08082024, header=True, inferSchema=False)
                        .withColumn("filename", col("_metadata.file_path"))
                        .withColumn("SDP_Event_Created_Timestamp", (col("_metadata.file_modification_time")).cast(TimestampType()))
    )

    for col1 in df_after_08082024.columns:
        if col1 not in df_before_08082024.columns:
            df_before_08082024 = df_before_08082024.withColumn(col1, lit(None))

    combined_cg_df = (df_before_08082024.unionByName(df_after_08082024)
                      .select(
                        substring("Caregiver_ICN__c", 1, 10).alias("ICN"),
                        substring("Veteran_ICN__c", 1, 10).alias("Veteran_ICN"),
                        date_format("Benefits_End_Date__c", "yyyyMMdd")
                        .alias("Status_Termination_Date")
                        .cast(StringType()),
                        col("Applicant_Type__c").alias("Applicant_Type"),
                        col("Caregiver_Status__c").alias("Caregiver_Status"),
                        date_format("Dispositioned_Date__c", "yyyyMMdd")
                        .alias("Status_Begin_Date").cast(StringType()),
                        col("CreatedDate").cast("timestamp").alias("Event_Created_Date"),
                        "filename",
                        "SDP_Event_Created_Timestamp",
                    )
                ).filter(col("Caregiver_ICN__c").isNotNull())

    edipi_df = (
        combined_cg_df
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
        .selectExpr( "PTCPNT_ID as participant_id", "CMBNED_DEGREE_DSBLTY", "DSBL_DTR_DT")
        .withColumn("filename", col("_metadata.file_path"))
        .withColumn("sentToDoD", lit(False).cast(BooleanType()))
        .withColumn("SDP_Event_Created_Timestamp", (col("_metadata.file_modification_time")).cast(TimestampType()))
        .withColumn("SC_Combined_Disability_Percentage",
                    lpad(coalesce(when(col("CMBNED_DEGREE_DSBLTY")=="", lit('000')).otherwise(col("CMBNED_DEGREE_DSBLTY"))), 3, "0"))
        .withColumn("Veteran_ICN", lit(None).cast(StringType()))
        .withColumn("Applicant_Type", lit(None).cast(StringType()))
        .withColumn("Caregiver_Status", lit(None).cast(StringType()))
        .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
        .withColumn("Status_Termination_Date", lit(None).cast(StringType()))
        .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
        .withColumn("Batch_CD", lit("SCD"))
        .withColumn("RecordStatus", lit(True).cast(BooleanType()))
        .withColumn("DSBL_DTR_DT",
                    when(col("DSBL_DTR_DT") == "", None)
                    .otherwise( date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), "yyyyMMdd")))
    ).filter(col("DSBL_DTR_DT").isNotNull())

    Window_Spec = Window.partitionBy(scd_updates_df["participant_id"]).orderBy(
        desc("DSBL_DTR_DT"), desc("SC_Combined_Disability_Percentage")
    )

    edipi_df = (scd_updates_df.join(
        icn_relationship, ["participant_id"], "left"
    ).withColumn("rank", rank().over(Window_Spec))
    .filter(col("rank") == 1)
    .filter(col("ICN").isNotNull())
    .dropDuplicates()
    .drop("rank", "va_profile_id", "record_updated_date"))

    return edipi_df

# COMMAND ----------

# DBTITLE 1,Prepare PAI data (update_pai_data())
def update_pai_data(row):
    """
    Prepares PT Indicator from the input row, transforms and updates a Veteran's PT_Indicator column in delta table. 
    Parameters: Row of data from pyspark dataframe with filename and metadata that are not processed (upsert)
    Returns: Dataframe: Dataframe with required column names ready for upsert
    """
    file_name = row.path
    file_creation_dateTime = row.dateTime
    print(f"Upserting records from {file_name}")

    raw_pai_df = (
        spark.read.csv(file_name, header=True, inferSchema=True)
        .withColumn(
            "SDP_Event_Created_Timestamp",
            (col("_metadata.file_modification_time")).cast(TimestampType()),
        )
        .withColumn("filename", col("_metadata.file_path"))
    )

    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_new")
    targetDF = (
        targetTable.toDF().filter("Batch_CD == 'SCD'").filter("RecordStatus=='True'")
    )
    targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])

    pai_df = raw_pai_df.selectExpr(
        "PTCPNT_VET_ID as participant_id", "PT_35_FLAG as source_PT_Indicator"
    )
    existing_pai_data_df = spark.sql(
        "SELECT participant_id, PT_Indicator from mypatronage_new where RecordStatus is True and Batch_CD = 'SCD'"
    )

    joinDF = (
        pai_df.join(
            targetDF,
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

    remaining_files = files_to_process_now.filter(~files_to_process_now['path'].contains('caregiverevent'))
    rows = remaining_files.collect()

    for row in rows:
        filename = row.path
        if "CPIDODIEX" in filename:
            edipi_df = prepare_scd_data(row)
            process_updates(edipi_df, "SCD")
        elif "WRTS" in filename:
            edipi_df = update_pai_data(row)
            process_updates(edipi_df, "PAI")
        else:
            pass

# COMMAND ----------

# DBTITLE 1,Bundling everything (patronage())
def patronage():
    spark.sql(
        """ CREATE TABLE IF NOT EXISTS mypatronage_new (edipi string, ICN string, Veteran_ICN string, participant_id string, Batch_CD string,  Applicant_Type string, Caregiver_Status string, SC_Combined_Disability_Percentage string, PT_Indicator string, Individual_Unemployability string, Status_Begin_Date string, Status_Last_Update string, Status_Termination_Date string, SDP_Event_Created_Timestamp timestamp, filename string, RecordLastUpdated date, RecordStatus boolean, sentToDoD boolean, change_log string, RecordChangeStatus string) PARTITIONED BY(Batch_CD, RecordStatus ) LOCATION 'dbfs:/user/hive/warehouse/mypatronage_new' """
    )

    file_counts = spark.sql(
        "select nvl(count(distinct filename),0) fileCount from mypatronage_new"
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

remaining_files = files_to_process_now.filter(~files_to_process_now['path'].contains('caregiverevent'))
display(remaining_files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mypatronage_new
# MAGIC where ICN not RLIKE '^[0-9]'

# COMMAND ----------

# DBTITLE 1,Max and Min file processed date
# MAGIC %sql
# MAGIC SELECT
# MAGIC   min(SDP_Event_Created_Timestamp) First_Processed_date,
# MAGIC   max(SDP_Event_Created_Timestamp) Last_Processed_date
# MAGIC FROM
# MAGIC   mypatronage_new

# COMMAND ----------

# spark.sql("drop table mypatronage_new")
# dbutils.fs.rm('dbfs:/user/hive/warehouse/mypatronage_new', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC caregiver_status, count(case when recordstatus IS TRUE and batch_cd = 'CG'  then 1 end)
# MAGIC from mypatronage_new WHERE batch_cd = 'CG'
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mypatronage_new where icn in 
# MAGIC (SELECT ICN FROM mypatronage_new
# MAGIC except
# MAGIC SElect ICN from DELTA.`/mnt/Patronage/SCD_Staging`)

# COMMAND ----------

# MAGIC %sql
# MAGIC SElect ICN from DELTA.`/mnt/Patronage/SCD_Staging`
# MAGIC except
# MAGIC SELECT ICN FROM mypatronage_new where batch_cd ='SCD'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select ICN from mypatronage_new where batch_cd ='SCD'
# MAGIC except
# MAGIC SElect ICN from DELTA.`/mnt/Patronage/SCD_Staging` 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SElect ICN from DELTA.`/mnt/Patronage/Caregivers_Staging_New`
# MAGIC except
# MAGIC SELECT ICN FROM mypatronage_new where batch_cd ='CG'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ICN FROM mypatronage_new where batch_cd ='CG'
# MAGIC except
# MAGIC SElect ICN from DELTA.`/mnt/Patronage/Caregivers_Staging_New`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mypatronage_new where icn = 1074917026

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC * from DELTA.`/mnt/Patronage/SCD_Staging` where icn = 1074917026

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*),'Josh_Count' FROM DELTA.`/mnt/Patronage/SCD_Staging`
# MAGIC UNION ALL
# MAGIC SELECT count(*), 'My_Count' FROM mypatronage_new where Batch_CD ='SCD' AND RecordStatus IS TRUE

# COMMAND ----------

6172610 - 6176073

# COMMAND ----------

6166338 - 6172146

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*),'Josh_Count' FROM DELTA.`/mnt/Patronage/Caregivers_Staging_New` where Status = 'Approved'
# MAGIC UNION ALL
# MAGIC SELECT count(*), 'My_Count' FROM mypatronage_new where Batch_CD ='CG' AND RecordStatus IS TRUE AND Caregiver_Status = 'Approved'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(SDP_Event_Created_Timestamp) FROM DELTA.`/mnt/Patronage/SCD_Staging` 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT RecordChangeStatus, count(*), SDP_Event_Created_Timestamp
# MAGIC FROM   mypatronage_new
# MAGIC WHERE SDP_Event_Created_Timestamp =  (SELECT DISTINCT SDP_Event_Created_Timestamp FROM mypatronage_new where BATCH_CD = 'SCD' ORDER BY 1 desc LIMIT 1 OFFSET 0)
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT SDP_Event_Created_Timestamp FROM mypatronage_new where BATCH_CD = 'SCD' --ORDER BY 1 desc LIMIT 1 OFFSET 1 ()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/mnt/Patronage/SCD_Staging` limit 1

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
# MAGIC SELECT date('20240524');
# MAGIC --where date_format(to_date(Status_Last_Update, 'yyyyMMdd'), 'MM-dd-yyyy') > to_date('01-01-2024', 'MM-dd-yyyy') 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT SC_Combined_Disability_Percentage, count(*) as Total_Count 
# MAGIC FROM DELTA.`/mnt/Patronage/SCD_Staging`
# MAGIC where Status_Last_Update >= 20240101
# MAGIC GROUP BY ALL
