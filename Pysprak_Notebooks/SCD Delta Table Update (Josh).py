# Databricks notebook source
from pyspark.sql.functions import col, lit, when, desc, rank, lpad, to_date, date_format, from_unixtime
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime, timedelta, date
from delta.tables import *
from pyspark.sql.window import Window
import re

# COMMAND ----------

scd_schema = StructType([
    StructField('PTCPNT_ID', IntegerType()), 
    StructField('FILE_NBR', DoubleType()), 
    StructField('LAST_NM', StringType()), 
    StructField('FIRST_NM', StringType()), 
    StructField('MIDDLE_NM', StringType()), 
    StructField('SUFFIX_NM', StringType()), 
    StructField('STA_NBR', IntegerType()), 
    StructField('BRANCH_OF_SERV', StringType()), 
    StructField('DATE_OF_BIRTH', IntegerType()), 
    StructField('DATE_OF_DEATH', IntegerType()), 
    StructField('VET_SSN_NBR', IntegerType()), 
    StructField('SVC_NBR', StringType()), 
    StructField('AMT_GROSS_OR_NET_AWARD', IntegerType()),
    StructField('AMT_NET_AWARD', IntegerType()),
    StructField('NET_AWARD_DATE', IntegerType()), 
    StructField('SPECL_LAW_IND', IntegerType()), 
    StructField('VET_SSN_VRFCTN_IND', IntegerType()),
    StructField('WIDOW_SSN_VRFCTN_IND', IntegerType()), 
    StructField('PAYEE_SSN', IntegerType()), 
    StructField('ADDRS_ONE_TEXT', StringType()), 
    StructField('ADDRS_TWO_TEXT', StringType()),
    StructField('ADDRS_THREE_TEXT', StringType()), 
    StructField('ADDRS_CITY_NM', StringType()), 
    StructField('ADDRS_ST_CD', StringType()), 
    StructField('ADDRS_ZIP_PREFIX_NBR', IntegerType()), 
    StructField('MIL_POST_OFFICE_TYP_CD', StringType()), 
    StructField('MIL_POSTAL_TYPE_CD', StringType()), 
    StructField('COUNTRY_TYPE_CODE', IntegerType()), 
    StructField('SUSPENSE_IND', IntegerType()), 
    StructField('PAYEE_NBR', IntegerType()), 
    StructField('EOD_DT', IntegerType()), 
    StructField('RAD_DT', IntegerType()), 
    StructField('ADDTNL_SVC_IND', StringType()), 
    StructField('ENTLMT_CD', StringType()), 
    StructField('DSCHRG_PAY_GRADE_NM', StringType()), 
    StructField('AMT_OF_OTHER_RETIREMENT', IntegerType()), 
    StructField('RSRVST_IND', StringType()), 
    StructField('NBR_DAYS_ACTIVE_RESRV', IntegerType()), 
    StructField('CMBNED_DEGREE_DSBLTY', IntegerType()),
    StructField('DSBL_DTR_DT', StringType()), 
    StructField('DSBL_TYP_CD', StringType()),
    StructField('VA_SPCL_PROV_CD', IntegerType()),
  ])

vba_schema = StructType([
  StructField('EDI_PI', StringType()), 
  StructField('SSN_NBR', StringType()), 
  StructField('FILE_NBR', StringType()), 
  StructField('LAST_NM', StringType()), 
  StructField('FIRST_NM', IntegerType()), 
  StructField('MIDDLE_NM', StringType()),
  StructField('PAYEE_TYPE_CD', StringType()), 
  StructField('PT35_RATING_DT', StringType()), 
  StructField('PT35_PRMLGN_DT', StringType()), 
  StructField('EFFECTIVE_DATE', StringType()), 
  StructField('END_DT', StringType()), 
  StructField('PT_35_FLAG', StringType()), 
  StructField('COMBND_DEGREE_PCT', IntegerType()), 
  StructField('ICN', IntegerType()), 
  StructField('EDIPI', IntegerType()), 
  StructField('PARTICIPANT_ID', StringType()), 
])

# COMMAND ----------

# dbutils.fs.rm('/mnt/Patronage/SCD_Run_History_test', True)
# dbutils.fs.rm("/mnt/Patronage/SCD_Staging_test", True)

# COMMAND ----------

icn_relationship = spark.read.format("delta").load("/mnt/Patronage/identity_correlations").withColumnRenamed('MVIPersonICN', 'ICN')

# SCDRunUpdateTable = DeltaTable.forPath(spark, "/mnt/Patronage/SCD_Run_History_test")

if8_files = dbutils.fs.ls('/mnt/ci-vadir-shared')
scrubbed_if8 = [file for file in if8_files if ("CPIDODIEX" in file.name) & ("csv" in file.path) & ("NEW" not in file.path)]
 
pai_files = dbutils.fs.ls('/mnt/ci-patronage/pai_landing')
scrubbed_pai = [file for file in pai_files if ("5-22-24" not in file.path)]
 
delta_PT_path = dbutils.fs.ls('/mnt/vac20sdpasa201vba/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/')
scrubbed_PT = [file for file in delta_PT_path if ("parquet" in file.path)]

sorted_PT_file = sorted(scrubbed_PT, key=lambda x: x.modificationTime, reverse=True)
max_PT_file = sorted_PT_file[0]

max_PT_modificationTime = datetime.fromtimestamp(max_PT_file.modificationTime/1000.0)

files = scrubbed_if8 + scrubbed_pai + [max_PT_file]

sorted_files = sorted(files, key=lambda x: x.modificationTime)
file_dict = []
 
for file in sorted_files:
    file_dict.append({"file": file.path, "modification_time": datetime.fromtimestamp(file.modificationTime/1000.0).strftime("%Y-%m-%d %H:%M:%S")})
 
latest_modification_time = file_dict[-1]['modification_time']
 
print(file_dict)
print(latest_modification_time)

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, "/mnt/Patronage/SCD_Staging_test") is False:
    status = 'rebuild'
    files_to_process = file_dict
else:
    status = 'update'
    files_to_process = []

    SCDUpdateRuns = spark.read.format("delta").load("/mnt/Patronage/SCD_Run_History_test")
    SCDUpdateRuns.createOrReplaceTempView('SCDUpdateRuns_View')

    latest_run_modification = spark.sql('select modification_time from SCDUpdateRuns_View order by modification_time desc limit 1').collect()[0][0]     

    for file in file_dict:
        if datetime.strptime(file['modification_time'], "%Y-%m-%d %H:%M:%S") > datetime.strptime(latest_run_modification, "%Y-%m-%d %H:%M:%S"):
            files_to_process.append(file)
        else:
            pass

print(status)
print(files_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Update Processing

# COMMAND ----------

if len(files_to_process) == 0:
    dbutils.notebook.exit('No files to process')
else:
    Window_Spec = Window.partitionBy("participant_id").orderBy(
    desc("DSBL_DTR_DT"), desc("CMBNED_DEGREE_DSBLTY")
)

# COMMAND ----------

# a = [item for item in file_dict if datetime.strptime(item['modification_time'], "%Y-%m-%d %H:%M:%S" ) > datetime(2025, 1, 1)]
# print(a)

# COMMAND ----------

if status == "rebuild":
    initial_seed = files_to_process.pop(0)
    print(initial_seed)

    # date = initial_seed['modification_time']
    date = datetime.strptime(initial_seed['modification_time'], "%Y-%m-%d %H:%M:%S")
  
    # year = int(date[0:4])
    # month = int(date[5:7])
    # day = int(date[8:10])

    print(date)

    seed_file = (
        spark.read.csv(initial_seed['file'], schema=scd_schema)
        .withColumnRenamed("PTCPNT_ID", "participant_id")
        .withColumn("rank", rank().over(Window_Spec))
        .withColumn("Batch_CD", lit("SCD"))
        .withColumn("Status_Last_Update", date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), 'yyyyMMdd'))
        .withColumnRenamed("CMBNED_DEGREE_DSBLTY", "SC_Combined_Disability_Percentage")
        .withColumn("PT_Indicator", lit('N').cast("string"))
        .withColumn("Individual_Unemployability", lit(None).cast("string"))
        .withColumn("Status_Begin_Date",  date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), 'yyyyMMdd'))
        .withColumn("Status_Termination_Date", lit(None).cast("string"))
        .withColumn(
            "SDP_Event_Created_Timestamp", lit(date)
        )
        .filter((col("rank") == 1) & (col("DSBL_DTR_DT") > 1000))
        .select(
            "participant_id",
            "Batch_CD",
            "SC_Combined_Disability_Percentage",
            "PT_Indicator",
            "Individual_Unemployability",
            "Status_Begin_Date",
            "Status_Last_Update",
            "Status_Termination_Date",
            "SDP_Event_Created_Timestamp",
        )
        .distinct()
    )

    seed_dataframe = (
        seed_file.join(
            icn_relationship,
            seed_file.participant_id == icn_relationship.participant_id,
            "left",
        )
        .select(
            icn_relationship.ICN,
            icn_relationship.edipi,
            icn_relationship.participant_id,
            seed_file.Batch_CD,
            lpad(seed_file.SC_Combined_Disability_Percentage, 3, "0").alias(
                "SC_Combined_Disability_Percentage"
            ),
            seed_file.PT_Indicator,
            seed_file.Individual_Unemployability,
            seed_file.Status_Begin_Date,
            seed_file.Status_Last_Update,
            seed_file.Status_Termination_Date,
            seed_file.SDP_Event_Created_Timestamp,
        )
        .filter(col("ICN").isNotNull())
    )

    seed_dataframe.write.format("delta").mode("overwrite").save(
        "/mnt/Patronage/SCD_Staging_test"
    )
else:
    pass

# COMMAND ----------

# files_to_process = [item for item in file_dict if datetime.strptime(item['modification_time'], "%Y-%m-%d %H:%M:%S" )> datetime(2025, 1, 1)]
# print(a)

# COMMAND ----------

SCDDeltaTable = DeltaTable.forPath(spark, "/mnt/Patronage/SCD_Staging_test")

for file in files_to_process:

    scd_master = (
    spark.read.format("delta")
    .load("/mnt/Patronage/SCD_Staging_test")
    .withColumnRenamed("edipi", "master_edipi")
    .withColumnRenamed("ICN", "master_icn")
    .withColumnRenamed("PT_Indicator", "master_pt")
    .withColumnRenamed("SC_Combined_Disability_Percentage", "master_disability")
    .withColumnRenamed("SDP_Event_Created_Timestamp", "master_event_created")
)
    # date = file['modification_time']
    date = datetime.strptime(file['modification_time'], "%Y-%m-%d %H:%M:%S")

    # year = int(date[0:4])
    # month = int(date[5:7])
    # day = int(date[8:10])

    print(file['file'])
    # print(date, year, month, day)

    if "CPIDODIEX" in file['file']:
        if len(spark.read.csv(file['file']).columns) == 3:
            update_file = (
                spark.read.option("header", True)
                .csv(file['file'])
                .withColumnRenamed("PTCPNT_ID", "participant_id")
                .withColumn("rank", rank().over(Window_Spec))
                .withColumn("Batch_CD", lit("SCD"))
                .withColumnRenamed(
                    "CMBNED_DEGREE_DSBLTY", "SC_Combined_Disability_Percentage"
                )
                .withColumn("PT_Indicator", lit("N").cast("string"))
                .withColumn("Individual_Unemployability", lit(None).cast("string"))
                .withColumn(
                    "Status_Begin_Date",
                    date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), "yyyyMMdd"),
                )
                .withColumn(
                    "Status_Last_Update",
                    date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), "yyyyMMdd"),
                )
                .withColumn("Status_Termination_Date", lit(None).cast("string"))
                .withColumn(
                    "SDP_Event_Created_Timestamp",
                    lit(date),
                )
                .filter((col("rank") == 1) & (col("DSBL_DTR_DT").isNotNull()))
                .select(
                    "participant_id",
                    "Batch_CD",
                    "SC_Combined_Disability_Percentage",
                    "PT_Indicator",
                    "Individual_Unemployability",
                    "Status_Begin_Date",
                    "Status_Last_Update",
                    "Status_Termination_Date",
                    "SDP_Event_Created_Timestamp",
                )
                .distinct()
            )
        else:
            update_file = (
                spark.read.csv(file['file'], schema=scd_schema)
                .withColumnRenamed("PTCPNT_ID", "participant_id")
                .withColumn("rank", rank().over(Window_Spec))
                .withColumn("Batch_CD", lit("SCD"))
                .withColumnRenamed(
                    "CMBNED_DEGREE_DSBLTY", "SC_Combined_Disability_Percentage"
                )
                .withColumn("PT_Indicator", lit("N").cast("string"))
                .withColumn("Individual_Unemployability", lit(None).cast("string"))
                .withColumn(
                    "Status_Begin_Date",
                    date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), "yyyyMMdd"),
                )
                .withColumn(
                    "Status_Last_Update",
                    date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), "yyyyMMdd"),
                )
                .withColumn("Status_Termination_Date", lit(None).cast("string"))
                .withColumn(
                    "SDP_Event_Created_Timestamp",
                    lit(date),
                )
                .filter((col("rank") == 1) & (col("DSBL_DTR_DT") > 1000))
                .select(
                    "participant_id",
                    "Batch_CD",
                    "SC_Combined_Disability_Percentage",
                    "PT_Indicator",
                    "Individual_Unemployability",
                    "Status_Begin_Date",
                    "Status_Last_Update",
                    "Status_Termination_Date",
                    "SDP_Event_Created_Timestamp",
                )
                .distinct()
            )

        update_dataframe = (
            update_file.join(
                icn_relationship,
                update_file.participant_id == icn_relationship.participant_id,
                "left",
            )
            .select(
                icn_relationship.ICN,
                icn_relationship.edipi,
                icn_relationship.participant_id,
                update_file.Batch_CD,
                lpad(update_file.SC_Combined_Disability_Percentage, 3, "0").alias(
                    "SC_Combined_Disability_Percentage"
                ),
                update_file.PT_Indicator,
                update_file.Individual_Unemployability,
                update_file.Status_Begin_Date,
                update_file.Status_Last_Update,
                update_file.Status_Termination_Date,
                update_file.SDP_Event_Created_Timestamp,
            )
            .filter(col("ICN").isNotNull())
        )

        update_dataframe_v2 = (
            update_dataframe.join(
                scd_master,
                (update_dataframe.ICN == scd_master.master_icn),
                "left",
            )
            .select(
                update_dataframe.ICN,
                update_dataframe.edipi,
                update_dataframe.participant_id,
                update_dataframe.Batch_CD,
                update_dataframe.SC_Combined_Disability_Percentage,
                update_dataframe.PT_Indicator,
                update_dataframe.Individual_Unemployability,
                update_dataframe.Status_Begin_Date,
                update_dataframe.Status_Last_Update,
                update_dataframe.Status_Termination_Date,
                update_dataframe.SDP_Event_Created_Timestamp,
            )
            .filter(
                (col("master_icn").isNull())
                | (col("SC_Combined_Disability_Percentage") != col("master_disability"))
                | (
                    col("master_disability").isNull()
                    & (col("SC_Combined_Disability_Percentage").isNotNull())
                )
                | (
                    col("master_disability").isNotNull()
                    & (col("SC_Combined_Disability_Percentage").isNull())
                )
            )
        )

        SCDDeltaTable.alias("master").merge(
            update_dataframe_v2.alias("update"),
            "master.ICN = update.ICN",
        ).whenMatchedUpdate(
            set={
                "ICN": "update.ICN",
                "edipi": "update.edipi",
                "participant_id": "update.participant_id",
                "Batch_CD": "update.Batch_CD",
                "SC_Combined_Disability_Percentage": "update.SC_Combined_Disability_Percentage",
                "PT_Indicator": "master.PT_Indicator",
                "Individual_Unemployability": "update.Individual_Unemployability",
                "Status_Begin_Date": "master.Status_Begin_Date",
                "Status_Last_Update": "update.Status_Last_Update",
                "Status_Termination_Date": "update.Status_Termination_Date",
                "SDP_Event_Created_Timestamp": "update.SDP_Event_Created_Timestamp",
            }
        ).whenNotMatchedInsertAll().execute()

    elif "pt-indicator" in file['file']:

        pai_file = (
            spark.read.option("header", "true")
            .option("delimiter", ",")
            .option("inferSchema", "true")
            .csv(file['file'])
            .withColumnRenamed("PTCPNT_VET_ID", "participant_id")
            .withColumnRenamed("PT_35_FLAG", "PT_Indicator")
            .withColumn(
                "SDP_Event_Created_Timestamp", lit(date)
            )
            .select("participant_id", "PT_Indicator", "SDP_Event_Created_Timestamp")
        )

        scrubbed_pai = (
            pai_file.join(scd_master, "participant_id", "left")
            .filter((col("master_pt") == "N"))
            .select("participant_id", "PT_Indicator", "SDP_Event_Created_Timestamp")
        )

        SCDDeltaTable.alias("master").merge(
            scrubbed_pai.alias("update"),
            "master.participant_id = update.participant_id",
        ).whenMatchedUpdate(
            set={
                "ICN": "master.ICN",
                "edipi": "master.edipi",
                "participant_id": "master.participant_id",
                "Batch_CD": "master.Batch_CD",
                "SC_Combined_Disability_Percentage": "master.SC_Combined_Disability_Percentage",
                "PT_Indicator": "update.PT_Indicator",
                "Individual_Unemployability": "master.Individual_Unemployability",
                "Status_Begin_Date": "master.Status_Begin_Date",
                "Status_Last_Update": "master.Status_Last_Update",
                "Status_Termination_Date": "master.Status_Termination_Date",
                "SDP_Event_Created_Timestamp": "update.SDP_Event_Created_Timestamp",
            }
        ).execute()

    elif "DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT" in file['file']:
        pai_file = (spark.sql(""" SELECT * FROM DELTA.`/mnt/vac20sdpasa201vba/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/` """))
        pai_file = (
            pai_file
            .withColumnRenamed("PTCPNT_VET_ID", "participant_id")
            .withColumnRenamed("PT_35_FLAG", "PT_Indicator")
            .withColumn(
                "SDP_Event_Created_Timestamp", lit(date)
            )
            .select("participant_id", "PT_Indicator", "SDP_Event_Created_Timestamp")
        )

        scrubbed_pai = (
            pai_file.join(scd_master, "participant_id", "left")
            .filter((col("master_pt") == "N"))
            .select("participant_id", "PT_Indicator", "SDP_Event_Created_Timestamp")
        )

        SCDDeltaTable.alias("master").merge(
            scrubbed_pai.alias("update"),
            "master.participant_id = update.participant_id",
        ).whenMatchedUpdate(
            set={
                "ICN": "master.ICN",
                "edipi": "master.edipi",
                "participant_id": "master.participant_id",
                "Batch_CD": "master.Batch_CD",
                "SC_Combined_Disability_Percentage": "master.SC_Combined_Disability_Percentage",
                "PT_Indicator": "update.PT_Indicator",
                "Individual_Unemployability": "master.Individual_Unemployability",
                "Status_Begin_Date": "master.Status_Begin_Date",
                "Status_Last_Update": "master.Status_Last_Update",
                "Status_Termination_Date": "master.Status_Termination_Date",
                "SDP_Event_Created_Timestamp": "update.SDP_Event_Created_Timestamp",
            }
        ).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Update Run History

# COMMAND ----------

date_today = datetime.today().date()
print(date_today)

update_row = [
    Row(date_today, latest_modification_time)
]
update_columns = ["run_date", "modification_time"]

update_df = spark.createDataFrame(update_row).toDF(*update_columns)
display(update_df)
if status == 'rebuild':
    update_df.write.format("delta").mode("overwrite").save(
        "/mnt/Patronage/SCD_Run_History_test"
    )
else:
    SCDRunUpdateTable = DeltaTable.forPath(spark, "/mnt/Patronage/SCD_Run_History_test")
    SCDRunUpdateTable.alias("master").merge(update_df.alias("update"), "master.modification_time = update.modification_time").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------


scd = spark.read.format('delta').load("/mnt/Patronage/SCD_Staging_test")
scd.createOrReplaceTempView('scd')

scd_run = spark.read.format('delta').load("/mnt/Patronage/SCD_Run_History_test")
scd_run.createOrReplaceTempView('scd_run')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd where Status_Begin_Date is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SDP_Event_Created_Timestamp, count(*) from scd group by 1 order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd_run order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt DISTINCT SDP_Event_Created_Timestamp FROM scd
