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
def all_filelist(path):
    from datetime import datetime

    files = []
    # List the files and subfolders in the current directory
    items = dbutils.fs.ls(path)
    for item in items:
        if item.isDir():
            # If it's a directory, recursively list its contents
            files += all_filelist(item.path)
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

    start_time = datetime(2023, 10, 30, 23, 59, 59)
    unix_start_time = int(time.mktime(start_time.timetuple())) * 1000

    file_folders = [
        "/mnt/ci-carma/landing/",
        "/mnt/ci-vadir-shared/",
        "/mnt/ci-patronage/pai_landing/",
    ]
    master_list = []

    for files in file_folders:
        master_list.append(all_filelist(files))
    flat_list = [item for sublist in master_list for item in sublist]

    file_list_df = spark.createDataFrame(flat_list, file_list_schema)

    filtered_file_list_df = (
        file_list_df.filter(file_list_df["modificationTime"] > unix_start_time)
        .withColumn("dateTime", to_timestamp(col("modificationTime") / 1000))
        .orderBy(col("modificationTime"))
    )

    query = f"""SELECT cast(coalesce(max(from_unixtime(unix_timestamp(cast(SDP_Event_Created_Timestamp AS TIMESTAMP)))), '{start_time}') as TIMESTAMP) max_date 
            FROM mypatronage_new """

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
    Parameters:
    None
    Returns:
    Dataframe: Dataframe with required column names

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

# DBTITLE 1,Upsert Caregivers and IF8 (process_updates())
def process_updates(edipi_df, file_type):
    spark.sql("OPTIMIZE mypatronage_new ZORDER BY (SDP_Event_Created_Timestamp)")

    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_new")
    targetDF = targetTable.toDF().filter(
        (col("Batch_CD") == file_type) & (col("RecordStatus") == True)
    )
    targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])

    if file_type == "CG":
        join_with_target_condition = (
            (edipi_df.ICN == targetDF.target_ICN)
            & (edipi_df.Veteran_ICN == targetDF.target_Veteran_ICN)
            & (edipi_df.Batch_CD == targetDF.target_Batch_CD)
            & (edipi_df.Applicant_Type == targetDF.target_Applicant_Type)
            & (targetDF.target_RecordStatus == "True")
        )
    elif file_type in ["SCD"]:
        join_with_target_condition = (
            (edipi_df.ICN == targetDF.target_ICN)
            & (
                targetDF.target_RecordStatus == "True"
            )  
            & (edipi_df.Batch_CD == targetDF.target_Batch_CD)
        )

    joinDF = edipi_df.join(targetDF, join_with_target_condition, "leftouter")

    if file_type == "SCD":
        joinDF = (
            joinDF.withColumn(
                "Status_Last_Update",
                when(
                    (
                        (col("target_participant_id") != (col("participant_id")))
                        & (col("Status_Last_Update").isNull())
                    ),
                    None,
                ).otherwise(col("Status_Begin_Date")),
            )
            .withColumn(
                "Updated_Status_Begin_Date",
                when(
                    (
                        (
                            xxhash64(col("Status_Begin_Date"))
                            != xxhash64(col("target_Status_Begin_Date"))
                        )
                        & col("target_Status_Begin_Date").isNotNull()
                    ),
                    col("target_Status_Begin_Date"),
                ).otherwise(col("Status_Begin_Date")),
            )
            .drop("Status_Begin_Date")
            .withColumnRenamed("Updated_Status_Begin_Date", "Status_Begin_Date")
            .withColumn(
                "PT_Indicator", coalesce(joinDF["target_PT_Indicator"], lit("N"))
            )
        )

    if file_type == "CG":
        delta_condition = xxhash64(
            joinDF.Status_Begin_Date,
            joinDF.Status_Termination_Date,
            joinDF.Applicant_Type,
            joinDF.Caregiver_Status,
        ) != xxhash64(
            joinDF.target_Status_Begin_Date,
            joinDF.target_Status_Termination_Date,
            joinDF.target_Applicant_Type,
            joinDF.target_Caregiver_Status,
        )
    elif file_type == "SCD":
        delta_condition = xxhash64(
            joinDF.SC_Combined_Disability_Percentage
        ) != xxhash64(joinDF.target_SC_Combined_Disability_Percentage)

    filterDF = joinDF.filter(delta_condition)

    if file_type == "CG":
        concat_column = concat(
            filterDF.ICN, filterDF.Veteran_ICN, filterDF.Applicant_Type
        )
    elif file_type == "SCD":
        concat_column = filterDF.ICN

    mergeDF = filterDF.withColumn("MERGEKEY", concat_column)

    dummyDF = filterDF.filter(col("target_ICN").isNotNull()).withColumn(
        "MERGEKEY", lit(None)
    )

    upsert_df = mergeDF.union(dummyDF)

    if file_type == "CG":
        batch_cd = "CG"
        merge_condition = "concat(target.ICN, target.Veteran_ICN, target.Applicant_Type) = source.MERGEKEY and target.RecordStatus = True"
    else:
        batch_cd = "SCD"
        merge_condition = "((target.ICN = source.MERGEKEY) and (target.Batch_CD = source.Batch_CD) and (target.RecordStatus = True))"

    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_new")

    targetDF = targetTable.toDF().filter(
        (col("Batch_CD") == batch_cd) & (col("RecordStatus") == True)
    )

    targetTable.alias("target").merge(
        upsert_df.alias("source"),
        condition=merge_condition,
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
        }
    ).execute()

# COMMAND ----------

# DBTITLE 1,Prepare CG data (prepare_caregivers_data())
def prepare_caregivers_data(row):
    # print(f"Process Caregivers File {row.path}: {row.dateTime} ")
    Window_Spec = Window.partitionBy("ICN", "Veteran_ICN", "Applicant_Type").orderBy(
        desc("Event_Created_Date")
    )

    file_name = row.path
    print(f"Upserting records from {file_name}")

    if len(spark.read.csv(file_name).columns) == 10:
        schema = old_cg_schema
    else:
        schema = cg_schema  # 08/08/2024 the schema changed from 10 to 12 columns

    cg_updates_df = (
        spark.read.csv(file_name, schema=schema, header=True)
        .withColumn("filename", col("_metadata.file_path"))
        .withColumn(
            "SDP_Event_Created_Timestamp",
            (col("_metadata.file_modification_time")).cast(TimestampType()),
        )
        .select(
            substring("Caregiver_ICN__c", 1, 10).alias("ICN"),
            substring("Veteran_ICN__c", 1, 10).alias("Veteran_ICN"),
            date_format("Benefits_End_Date__c", "yyyyMMdd")
            .alias("Status_Termination_Date")
            .cast(StringType()),
            (col("Applicant_Type__c")).alias("Applicant_Type"),
            (col("Caregiver_Status__c")).alias("Caregiver_Status"),
            date_format("Dispositioned_Date__c", "yyyyMMdd")
            .alias("Status_Begin_Date")
            .cast(StringType()),
            (col("CreatedDate").cast("timestamp").alias("Event_Created_Date")),
            lit(False).alias("sentToDoD").cast(BooleanType()),
            lit("CG").alias("Batch_CD"),
            "filename",
            "SDP_Event_Created_Timestamp",
        )
    ).filter(col("ICN").isNotNull())

    edipi_df = (
        cg_updates_df.alias("source")
        .join(icn_relationship.alias("lookup"), "ICN", "left")
        .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
        .withColumn("PT_Indicator", lit(None).cast(StringType()))
        .withColumn("SC_Combined_Disability_Percentage", lit(None).cast(StringType()))
        .withColumn("RecordStatus", lit(True).cast(BooleanType()))
        .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
        .withColumn("Status_Last_Update", lit(None).cast(StringType()))
        .withColumn("sentToDoD", lit(False).cast(BooleanType()))
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
            "RecordLastUpdated",
            "RecordStatus",
            "Status_Last_Update",
            "sentToDoD",
        )
    )
    return edipi_df

# COMMAND ----------

# DBTITLE 1,Prepare SCD data (IF8) (prepare_scd_data())
def prepare_scd_data(row):
    file_name = row.path
    file_month = file_name.split("_")[1]
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
        .withColumn("sentToDoD", lit(False).cast(BooleanType()))
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
        .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
        .withColumn("Batch_CD", lit("SCD"))
        .withColumn("RecordStatus", lit(True).cast(BooleanType()))
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
            "sentToDoD",
            "Veteran_ICN",
            "Applicant_Type",
            "Caregiver_Status",
            "Individual_Unemployability",
            "Status_Termination_Date",
            "Status_Last_Update",
            "RecordLastUpdated",
            "Status_Begin_Date",
            "RecordStatus",
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
        with_edipi_df.RecordLastUpdated,
        with_edipi_df.RecordStatus,
        with_edipi_df.sentToDoD,
    )
    return edipi_df

# COMMAND ----------

# DBTITLE 1,Prepare PAI data and Upsert (update_pai_data())
def update_pai_data(row):
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

    spark.sql("OPTIMIZE mypatronage_new ZORDER BY (SDP_Event_Created_Timestamp)")
    targetTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mypatronage_new")
    targetDF = (
        targetTable.toDF().filter("Batch_CD == 'SCD'").filter("RecordStatus=='True'")
    )
    targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])

    pai_df = raw_pai_df.selectExpr(
        "PTCPNT_VET_ID as participant_id", "PT_35_FLAG as PT_Indicator"
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
        .withColumn("SDP_Event_Created_Timestamp", lit(meta_raw_sdp))
    )

    filterDF = joinDF.filter(
        xxhash64(joinDF.PT_Indicator) != xxhash64(joinDF.target_PT_Indicator)
    )

    mergeDF = filterDF.withColumn("MERGEKEY", filterDF.target_ICN)

    dummyDF = filterDF.filter("target_ICN is not null").withColumn(
        "MERGEKEY", lit(None)
    )

    paiDF = mergeDF.union(dummyDF)

    final_df = paiDF.selectExpr(
        "target_edipi",
        "participant_id",
        "MERGEKEY",
        "target_ICN",
        "target_RecordStatus",
        "target_Batch_CD",
        "target_SC_Combined_Disability_Percentage",
        "PT_Indicator",
        "target_Status_Begin_Date",
        "target_Status_Last_Update",
        "SDP_Event_Created_Timestamp",
        "target_RecordStatus",
        "target_RecordLastUpdated",
        "target_sentToDoD",
        "filename",
    )

    targetTable.alias("target").merge(
        final_df.alias("source"),
        condition=(
            "target.ICN = source.MERGEKEY and target.Batch_CD = 'SCD' and target.RecordStatus = True"
        ),
    ).whenMatchedUpdate(
        set={
            "RecordStatus": "False",
            "RecordLastUpdated": "source.SDP_Event_Created_Timestamp",
            "sentToDoD": "True",
        }
    ).whenNotMatchedInsert(
        values={
            "edipi": "source.target_edipi",
            "ICN": "source.target_ICN",
            "Veteran_ICN": lit(None),
            "Applicant_Type": lit(None),
            "Caregiver_Status": lit(None),
            "participant_id": "source.participant_id",
            "Batch_CD": lit("SCD"),
            "SC_Combined_Disability_Percentage": "source.target_SC_Combined_Disability_Percentage",
            "PT_Indicator": when(col("source.PT_Indicator").isNull(), "N").otherwise(
                col("source.PT_Indicator")
            ),
            "Individual_Unemployability": lit(None),
            "Status_Begin_Date": "source.target_Status_Begin_Date",
            "Status_Last_Update": "source.target_Status_Last_Update",
            "Status_Termination_Date": lit(None),
            "SDP_Event_Created_Timestamp": "source.SDP_Event_Created_Timestamp",
            "RecordStatus": "true",
            "RecordLastUpdated": lit(None),
            "sentToDoD": "false",
            "filename": "source.filename",
        }
    ).execute()

# COMMAND ----------

# DBTITLE 1,Processing unprocessed files (process_files())
def process_files(files_to_process_now):
    rows = files_to_process_now.collect()
    for row in rows:
        filename = row.path

        if "caregiverevent" in filename:
            edipi_df = prepare_caregivers_data(row)
            process_updates(edipi_df, "CG")
        elif "CPIDODIEX" in filename:
            edipi_df = prepare_scd_data(row)
            process_updates(edipi_df, "SCD")
        elif "WRTS" in filename:
            update_pai_data(row)
        else:
            print("Check the filename")

# COMMAND ----------

# DBTITLE 1,Bundling everything (patronage())
def patronage():
    spark.sql(
        """ CREATE TABLE IF NOT EXISTS mypatronage_new (edipi string, ICN string, Veteran_ICN string, participant_id string, Batch_CD string,  Applicant_Type string, Caregiver_Status string, SC_Combined_Disability_Percentage string, PT_Indicator string, Individual_Unemployability string, Status_Begin_Date string, Status_Last_Update string, Status_Termination_Date string, SDP_Event_Created_Timestamp timestamp, filename string, RecordLastUpdated date, RecordStatus boolean, sentToDoD boolean) PARTITIONED BY(Batch_CD, RecordStatus ) LOCATION 'dbfs:/user/hive/warehouse/mypatronage_new' """
    )

    file_counts = spark.sql(
        "select nvl(count(distinct filename),0) fileCount from mypatronage_new"
    ).collect()[0][0]
    print(f"Total files processed till last run: {file_counts}")

    if file_counts == 0:
        print("Loading seed file into mypatronage_new table")
        seed_df = initialize_caregivers()
        process_updates(seed_df, "CG")
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

# DBTITLE 1,Max and Min file processed date
# MAGIC %sql
# MAGIC SELECT
# MAGIC   min(SDP_Event_Created_Timestamp) First_Processed_date,
# MAGIC   max(SDP_Event_Created_Timestamp) Last_Processed_date
# MAGIC FROM
# MAGIC   mypatronage_new

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct filename from mypatronage_new
