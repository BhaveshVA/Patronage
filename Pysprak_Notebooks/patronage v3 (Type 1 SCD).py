# Databricks notebook source
# DBTITLE 1,Install openpyxl to read .xlsx files
# MAGIC %pip install openpyxl;

# COMMAND ----------

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

# COMMAND ----------

# DBTITLE 1,Lookup Table for EDIPI
icn_relationship = (
    spark.read.format("delta")
    .load("/mnt/ci-patronage/delta_tables/identity_correlations_table")
    .withColumnRenamed("MVIPersonICN", "ICN")
)  # Loookup table for ICN, EDIPI

# COMMAND ----------

# DBTITLE 1,Helper function to list required source files (all_filelist(path))
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

    query = f"""SELECT cast(coalesce(max(from_unixtime(unix_timestamp(cast(SDP_Event_Created_Timestamp AS TIMESTAMP)))), '{start_time}') as TIMESTAMP) max_date 
            FROM mypatronage_1 """

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
            .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
            .withColumn("PT_Indicator", lit(None).cast(StringType()))
            .withColumn(
                "SC_Combined_Disability_Percentage", lit(None).cast(StringType())
            )
            .withColumn("Status_Last_Update", lit(None).cast(StringType()))
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
                "Individual_Unemployability",
                "PT_Indicator",
                "SC_Combined_Disability_Percentage",
                "Status_Last_Update",
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

# DBTITLE 1,Upsert Caregivers and IF8 (process_updates())
def process_updates(source_df, file_type):
    """
    Upserts the input dataframe depending on input file type ('CG' or 'SCD')
    for Slowly Changing Dimension Type 1 logic.

    Parameters: Pyspark dataframe and file type
    Returns: None
    """
    # Optimize the table based on the event timestamp
    spark.sql("OPTIMIZE mypatronage_1 ZORDER BY (SDP_Event_Created_Timestamp)")

    # Load the target Delta table
    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_1")
    targetDF = targetTable.toDF()
    # Rename columns in targetDF for clarity
    targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])

    # Define join conditions based on file type
    join_conditions = {
        "CG": (
            (source_df.ICN == targetDF.target_ICN)
            & (source_df.Veteran_ICN == targetDF.target_Veteran_ICN)
            & (source_df.Batch_CD == targetDF.target_Batch_CD)
            & (source_df.Applicant_Type == targetDF.target_Applicant_Type)
        ),
        "SCD": (
            (source_df.ICN == targetDF.target_ICN)
            & (source_df.Batch_CD == targetDF.target_Batch_CD)
        ),
    }

    # Define delta condition for detecting changes
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

    # Define merge conditions as key-value pairs
    merge_conditions = {
        "CG": (
            "target.ICN = source.ICN "
            "AND target.Veteran_ICN = source.Veteran_ICN "
            "AND target.Batch_CD = source.Batch_CD "
            "AND target.Applicant_Type = source.Applicant_Type"
        ),
        "SCD": (
            "target.ICN = source.ICN "
            "AND target.Batch_CD = source.Batch_CD"
        ),
    }

    # Perform the join based on file type
    joinDF = source_df.join(targetDF, join_conditions[file_type], "leftouter")

    # Handling logic for SCD file type (Status_Begin_Date and Status_Last_Update remain intact)
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
    upsert_df = joinDF.filter(delta_conditions[file_type])

    # Perform the merge operation for SCD Type 1 logic (overwrite existing records)
    targetTable.alias("target").merge(
        upsert_df.alias("source"),
        merge_conditions[file_type]
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# DBTITLE 1,Prepare CG data (prepare_caregivers_data())
def prepare_caregivers_data(cg_csv_files):
    """
    Filters caregivers filenames from input dataframe, aggregates data and returns dataframe
    Parameters: Pyspark dataframe with all filenames and metadata that are not processed (upsert)
    Returns: Dataframe: Dataframe with required column names and edipi of a caregiver 
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
                        .alias("Status_Begin_Date")
                        .cast(StringType()),
                        col("CreatedDate").cast("timestamp").alias("Event_Created_Date"),
                        lit("CG").alias("Batch_CD"),
                        "filename",
                        "SDP_Event_Created_Timestamp",
                    )
                ).filter(col("Caregiver_ICN__c").isNotNull())

    edipi_df = (
        combined_cg_df.alias("source")
        .join(icn_relationship.alias("lookup"), "ICN", "left")
        .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
        .withColumn("PT_Indicator", lit(None).cast(StringType()))
        .withColumn("SC_Combined_Disability_Percentage", lit(None).cast(StringType()))
        .withColumn("Status_Last_Update", lit(None).cast(StringType()))
        .withColumn("rank", rank().over(Window_Spec))
        .filter(col("rank") == 1)
        .dropDuplicates()
        .select(
            "source.*",
            "lookup.EDIPI",
            "lookup.participant_id",
            "SC_Combined_Disability_Percentage",
            "Individual_Unemployability",
            "PT_Indicator",
            "Status_Last_Update",
        )
    ).orderBy(col("Event_Created_Date"))
    return edipi_df

# COMMAND ----------

# DBTITLE 1,Prepare SCD data (IF8) (prepare_scd_data())
def prepare_scd_data(row):
    """
    Prepares SCD data from the input row. This is the disability % data.
    Parameters: Row of data from pyspark dataframe with filenames and metadata that are not processed (upsert)
    Returns: Dataframe: Dataframe with required column names and edipi of a Veteran
    """

    file_name = row.path
    print(f"Upserting records from {file_name}")

    if len(spark.read.csv(file_name).columns) != 3:
        schema = scd_schema
    else:
        schema = scd_schema1  # the schema changed  to 3 columns after May

    scd_updates_df = (
        spark.read.csv(file_name, schema=schema, header=True)
        .withColumnRenamed("CMBNED_DEGREE_DSBLTY", "SC_Combined_Disability_Percentage")
        .withColumnRenamed("PTCPNT_ID", "participant_id")
        .withColumn("filename", col("_metadata.file_path"))
        .withColumn(
            "SDP_Event_Created_Timestamp",
            (col("_metadata.file_modification_time")).cast(TimestampType()),
        )
        .withColumn("Veteran_ICN", lit(None).cast(StringType()))
        .withColumn("Applicant_Type", lit(None).cast(StringType()))
        .withColumn("Caregiver_Status", lit(None).cast(StringType()))
        .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
        .withColumn("Status_Termination_Date", lit(None).cast(StringType()))
        .withColumn("Status_Last_Update", lit(None).cast(StringType()))
        .withColumn("Batch_CD", lit("SCD"))
        .withColumn(
            "Status_Begin_Date",
            date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), "yyyyMMdd"),
        )
        .filter(col("Status_Begin_Date").isNotNull())
        .selectExpr(
            "participant_id",
            "SC_Combined_Disability_Percentage",
            "filename",
            "SDP_Event_Created_Timestamp",
            "Veteran_ICN",
            "Applicant_Type",
            "Caregiver_Status",
            "Individual_Unemployability",
            "Status_Termination_Date",
            "Status_Last_Update",
            "Status_Begin_Date",
            "Batch_CD",
        )
    )

    Window_Spec = Window.partitionBy(scd_updates_df["participant_id"]).orderBy(
        desc("Status_Begin_Date"), desc("SC_Combined_Disability_Percentage")
    )

    lu_icn_relationship = icn_relationship.withColumnRenamed(
        "participant_id", "lu_participant_id"
    ).drop("va_profile_id", "record_updated_date")

    with_edipi_df = scd_updates_df.join(
        lu_icn_relationship,
        scd_updates_df.participant_id == lu_icn_relationship.lu_participant_id,
        "left",
    )

    with_edipi_df = (
        with_edipi_df.withColumn("rank", rank().over(Window_Spec))
        .filter(col("rank") == 1)
        .filter(col("ICN").isNotNull())
        .dropDuplicates()
    )

    with_edipi_df = with_edipi_df.drop("rank", "lu_participant_id")

    edipi_df = with_edipi_df.select(
        with_edipi_df.edipi,
        with_edipi_df.ICN,
        with_edipi_df.Veteran_ICN,
        with_edipi_df.participant_id,
        with_edipi_df.Batch_CD,
        with_edipi_df.Applicant_Type,
        with_edipi_df.Caregiver_Status,
        lpad(
            when(with_edipi_df.SC_Combined_Disability_Percentage == "", "000")
            .otherwise(with_edipi_df.SC_Combined_Disability_Percentage)
            .alias("SC_Combined_Disability_Percentage"),
            3,
            "0",
        ).alias("SC_Combined_Disability_Percentage"),
        with_edipi_df.Individual_Unemployability,
        with_edipi_df.Status_Last_Update,
        when(with_edipi_df.Status_Begin_Date == "", None)
        .otherwise(with_edipi_df.Status_Begin_Date)
        .alias("Status_Begin_Date"),
        with_edipi_df.Status_Termination_Date,
        with_edipi_df.SDP_Event_Created_Timestamp,
        with_edipi_df.filename,
    )
    return edipi_df

# COMMAND ----------

# DBTITLE 1,Prepare PAI data and Update (update_pai_data())
def update_pai_data(row):
    """
    Prepares PT Indicator from the input row, transforms and updates a Veteran's PT_Indicator column in delta table. 
    Parameters: Row of data from pyspark dataframe with filenames and metadata that are not processed (upsert)
    Returns: None
    """
    
    file_name = row.path
    print(f"Upserting records from {file_name}")

    raw_pai_df = (
        spark.read.csv(file_name, header=True, inferSchema=True)
        .withColumn(
            "SDP_Event_Created_Timestamp",
            (col("_metadata.file_modification_time")).cast(TimestampType()),
        )
        .withColumn("filename", col("_metadata.file_path"))
    )

    meta_raw_sdp = (
        raw_pai_df.select("SDP_Event_Created_Timestamp").distinct().collect()[0][0]
    )

    spark.sql("OPTIMIZE mypatronage_1 ZORDER BY (SDP_Event_Created_Timestamp)")
    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_1")
    targetDF = (
        targetTable.toDF().filter("Batch_CD == 'SCD'")
    )
    targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])

    pai_df = raw_pai_df.selectExpr(
        "PTCPNT_VET_ID as participant_id", "PT_35_FLAG as PT_Indicator"
    )

    joinDF = (
        pai_df.join(
            targetDF,
            pai_df["participant_id"] == targetDF["target_participant_id"],
            "left",
        )
        .filter(targetDF["target_PT_Indicator"] == "N")
        .withColumn("filename", lit(file_name))
        .withColumn("SDP_Event_Created_Timestamp", lit(meta_raw_sdp))
    )

    filterDF = joinDF.filter(
        xxhash64(joinDF.PT_Indicator) != xxhash64(joinDF.target_PT_Indicator)
    ).select(
    col("PT_Indicator").alias("PT_Indicator"),
    col("target_edipi").alias("edipi"),
    col("target_ICN").alias("ICN"),
    col("target_Veteran_ICN").alias("Veteran_ICN"),
    col("target_participant_id").alias("participant_id"),
    col("target_Batch_CD").alias("Batch_CD"),
    col("target_Applicant_Type").alias("Applicant_Type"),
    col("target_Caregiver_Status").alias("Caregiver_Status"),
    col("target_SC_Combined_Disability_Percentage").alias("SC_Combined_Disability_Percentage"),
    col("target_Individual_Unemployability").alias("Individual_Unemployability"),
    col("target_Status_Begin_Date").alias("Status_Begin_Date"),
    col("target_Status_Last_Update").alias("Status_Last_Update"),
    col("target_Status_Termination_Date").alias("Status_Termination_Date"),
    col("filename"),
    col("SDP_Event_Created_Timestamp"))

    targetTable.alias("target").merge(
        filterDF.alias("source"),
        condition=(
            "target.ICN = source.ICN and target.Batch_CD = 'SCD'"
        ),
    ).whenMatchedUpdate(
            set={
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
                "filename": "source.filename"
            }
        ) .execute()

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
            update_pai_data(row)
        else:
            pass

# COMMAND ----------

# %sql
# drop table mypatronage_1

# COMMAND ----------

# dbutils.fs.rm('dbfs:/user/hive/warehouse/mypatronage_1', True)

# COMMAND ----------

# DBTITLE 1,Bundling everything (patronage())
def patronage():
    spark.sql(
        """ CREATE TABLE IF NOT EXISTS mypatronage_1 (edipi string, ICN string, Veteran_ICN string, participant_id string, Batch_CD string,  Applicant_Type string, Caregiver_Status string, SC_Combined_Disability_Percentage string, PT_Indicator string, Individual_Unemployability string, Status_Begin_Date string, Status_Last_Update string, Status_Termination_Date string, SDP_Event_Created_Timestamp timestamp, filename string) PARTITIONED BY(Batch_CD ) LOCATION 'dbfs:/user/hive/warehouse/mypatronage_1' """
    )

    file_counts = spark.sql(
        "select nvl(count(distinct filename),0) fileCount from mypatronage_1"
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

temp_df =  prepare_caregivers_data(files_to_process_now)
display(temp_df)

# COMMAND ----------

files_to_process_now = collect_data_source()
display(files_to_process_now)
# df = prepare_caregivers_data(files_to_process_now)
# display(df)

# COMMAND ----------

remaining_files = files_to_process_now.filter(~files_to_process_now['path'].contains('caregiverevent'))
display(remaining_files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mypatronage_1
# MAGIC where ICN not RLIKE '^[0-9]'

# COMMAND ----------

# DBTITLE 1,Max and Min file processed date
# MAGIC %sql
# MAGIC SELECT
# MAGIC   min(SDP_Event_Created_Timestamp) First_Processed_date,
# MAGIC   max(SDP_Event_Created_Timestamp) Last_Processed_date
# MAGIC FROM
# MAGIC   mypatronage_1
