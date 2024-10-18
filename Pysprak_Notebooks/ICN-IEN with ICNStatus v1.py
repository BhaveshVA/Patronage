# Databricks notebook source
import os
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from delta import DeltaTable
from pyspark.sql import functions as F
from functools import reduce


# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table test_correlations;

# COMMAND ----------

# dbutils.fs.rm("dbfs:/FileStore/test_correlations",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS test_correlations (MVIPersonICN string, EDIPI string, participant_id string, va_profile_id string, Last_updated_date TIMESTAMP, ICNStatus STRING  ) PARTITIONED BY (ICNStatus) LOCATION "dbfs:/FileStore/test_correlations"

# COMMAND ----------

Window_Spec = Window.partitionBy(
    "MVIPersonICN",
    "MVITreatingFacilityInstitutionSID",
    "TreatingFacilityPersonIdentifier",
).orderBy(desc("CorrelationModifiedDateTime"))

Window_Spec1 = Window.partitionBy(
    "Person_MVIPersonICN"
).orderBy(desc("calc_IngestionTimestamp"))

basepath = "dbfs:/FileStore/"

# COMMAND ----------

raw_psa_df = spark.read.parquet("/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/")

person = (spark.read.parquet("dbfs:/mnt/ci-mvi/Processed/SVeteran.SMVIPerson"))
person = (person
          .withColumnRenamed("MVIPersonICN", "Person_MVIPersonICN")
          .withColumn("rnk", rank().over(Window_Spec1)).filter(col("rnk")==1)
          .select("Person_MVIPersonICN","ICNStatus", "calc_IngestionTimestamp")
)

Institutions = spark.read.parquet("/mnt/ci-mvi/Raw/NDim.MVIInstitution/")

join_df = (
    raw_psa_df.filter(raw_psa_df["MVITreatingFacilityInstitutionSID"].isin([5667, 6061, 6722]))
    .filter((raw_psa_df["ActiveMergedIdentifier"] == "Active")
            | (raw_psa_df["ActiveMergedIdentifier"].isNull())
    )
    .filter(raw_psa_df["OpCode"] != 'D'
    )    
    .join(
        Institutions,
        raw_psa_df["MVITreatingFacilityInstitutionSID"]
        == Institutions["MVIInstitutionSID"],
        "left",
    )
    .join(person, raw_psa_df["MVIPersonICN"] == person["Person_MVIPersonICN"])
    .filter(raw_psa_df["MVIPersonICN"].isNotNull())
    .withColumn(
        "ranked_value",
        rank().over(Window_Spec)
    )
    .filter(col("ranked_value") == 1)
    .select(
        "MVITreatingFacilityInstitutionSID",
        "ActiveMergedIdentifier",
        raw_psa_df["OpCode"],
        "MVIPersonSiteAssociationSID",
        raw_psa_df["CorrelationModifiedDateTime"].cast("timestamp").alias("Last_updated"),
        "institutionCode",
        "TreatingFacilityPersonIdentifier",
        "MVIPersonICN",
        "MVIInstitutionSID", person["ICNStatus"]
    )
)


filtered_df = (
    join_df
    .selectExpr(
        "MVIInstitutionSID",
        "MVITreatingFacilityInstitutionSID",
        "institutionCode",
        "MVIPersonSiteAssociationSID",
        "TreatingFacilityPersonIdentifier",
        "MVIPersonICN",
        "Last_updated","ActiveMergedIdentifier", "ICNStatus"
    )
    .orderBy("MVIPersonICN")
    .distinct()
)

multiple_icn = (
    filtered_df.groupBy("TreatingFacilityPersonIdentifier", "MVIInstitutionSID")
    .count()
    .filter(col("count") > 1)
    .withColumnRenamed("count", "icn_count")
    .withColumnRenamed("TreatingFacilityPersonIdentifier", "TFPI")
    .withColumnRenamed("MVIInstitutionSID", "Inst")
)


multiple_tfpi = (
    filtered_df.groupBy("MVIPersonICN", "MVIInstitutionSID")
    .count()
    .filter(col("count") > 1)
    .withColumnRenamed("count", "tfpi_count")
    .withColumnRenamed("MVIInstitutionSID", "Inst")
    .withColumnRenamed("MVIPersonICN", "ICN")
)

non_duplicates = (
    filtered_df.join(
        multiple_icn,
        (filtered_df["TreatingFacilityPersonIdentifier"] == multiple_icn["TFPI"])
        & (filtered_df["MVIInstitutionSID"] == multiple_icn["Inst"]),
        "left",
    )
    .join(
        multiple_tfpi,
        (filtered_df["MVIPersonICN"] == multiple_tfpi["ICN"])
        & (filtered_df["MVIInstitutionSID"] == multiple_tfpi["Inst"]),
        "left",
    )
    .filter(filtered_df["TreatingFacilityPersonIdentifier"].isNotNull())
   .select(
        filtered_df["MVIPersonICN"],
        filtered_df["ActiveMergedIdentifier"],
        filtered_df["MVIInstitutionSID"],
        filtered_df["TreatingFacilityPersonIdentifier"]
    )
)

non_duplicates = non_duplicates.filter(col("icn_count").isNull()).filter(col("tfpi_count").isNull())

unique_icn = (filtered_df
              .groupBy(filtered_df["MVIPersonICN"], filtered_df["ICNStatus"])
              .agg(max("Last_updated").cast("date").alias("Last_updated"))
              .distinct()
              .selectExpr("MVIPersonICN as ICN", "Last_updated", "ICNStatus")
)

# COMMAND ----------

raw_psa_df.filter(col("TreatingFacilityPersonIdentifier")== '5348444').filter(raw_psa_df["MVITreatingFacilityInstitutionSID"].isin([5667, 6061, 6722])).display()

# COMMAND ----------

multiple_tfpi.filter(col("icn")=='1010124988').display()

# COMMAND ----------

dod_df = (non_duplicates
        .filter(
        (non_duplicates["MVIInstitutionSID"] == 5667)
                )
        .selectExpr("MVIPersonICN as dod_icn", "TreatingFacilityPersonIdentifier as DoD_facility"))


corp_df = (non_duplicates
        .filter(
        (non_duplicates["MVIInstitutionSID"] == 6061)
                )
        .selectExpr("MVIPersonICN as corp_icn", "TreatingFacilityPersonIdentifier as corp_facility"))


vets_df = (non_duplicates
        .filter((non_duplicates["MVIInstitutionSID"] == 6722)
                )
        .selectExpr("MVIPersonICN as vets_icn", "TreatingFacilityPersonIdentifier as vets_facility"))


# COMMAND ----------

master_df = (
    unique_icn
    .join(dod_df, unique_icn["ICN"] == dod_df["dod_icn"], "left")
    .join(corp_df, unique_icn["ICN"] == corp_df["corp_icn"], "left")
    .join(vets_df, unique_icn["ICN"] == vets_df["vets_icn"], "left")
    .select(
        unique_icn["ICN"].alias("MVIPersonICN"),
        dod_df["DoD_facility"].alias("EDIPI"),
        corp_df["corp_facility"].alias("participant_id"),
        vets_df["vets_facility"].alias("va_profile_id"),
        unique_icn["Last_updated"].alias("Last_Updated_Date"),
        unique_icn["ICNStatus"]
    )
)

# COMMAND ----------

master_df.display()

# COMMAND ----------

input_value = [5348444]
result_df = get_any_id(df, input_value)
display(result_df)

# COMMAND ----------

spark.sql("OPTIMIZE delta.`dbfs:/FileStore/test_correlations`")
target = DeltaTable.forPath(spark, basepath + "test_correlations")

target.alias("target").merge(
    source=master_df.alias("source"),
    condition="(target.MVIPersonICN = source.MVIPersonICN)",
).whenMatchedUpdate(
    condition=expr("xxhash64(target.participant_id, target.va_profile_id, target.EDIPI) != xxhash64(source.participant_id, source.va_profile_id, source.EDIPI)"),
    set={
        "participant_id": F.when(
            (F.col("source.participant_id") == F.col("target.participant_id"))
            | (F.col("source.participant_id").isNull()),
            F.col("target.participant_id"),
        ).otherwise(F.col("source.participant_id")),
        "va_profile_id": F.when(
            (F.col("source.va_profile_id") == F.col("target.va_profile_id"))
            | (F.col("source.va_profile_id").isNull()),
            F.col("target.va_profile_id"),
        ).otherwise(F.col("source.va_profile_id")),
        "EDIPI": F.when(
            (F.col("source.EDIPI") == F.col("target.EDIPI"))
            | (F.col("source.EDIPI").isNull()),
            F.col("target.EDIPI"),
        ).otherwise(F.col("source.EDIPI")),
        "Last_updated_date": "source.Last_updated_date",
        "ICNStatus": "source.ICNStatus"
    },
).whenNotMatchedInsert(
    values={
        "MVIPersonICN": "source.MVIPersonICN",
        "participant_id": "source.participant_id",
        "EDIPI": "source.EDIPI",
        "va_profile_id": "source.va_profile_id",
        "Last_updated_date": "source.Last_updated_date",
        "ICNStatus": "source.ICNStatus"
    }
).execute()

# COMMAND ----------

master_df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   version,
# MAGIC   operation,
# MAGIC   cast(operationMetrics.numTargetRowsInserted as INT) as Inserted,
# MAGIC   cast(operationMetrics.numTargetRowsUpdated as INT) as Updates  
# MAGIC FROM
# MAGIC   (describe history test_correlations)
# MAGIC where
# MAGIC   operation not in ("CREATE TABLE", "OPTIMIZE")
# MAGIC order by
# MAGIC   1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   version,
# MAGIC   operation,
# MAGIC   cast(operationMetrics.numTargetRowsInserted as INT) as Inserted,
# MAGIC   cast(operationMetrics.numTargetRowsUpdated as INT) as Updates  
# MAGIC FROM
# MAGIC   (describe history DELTA.`dbfs:/mnt/Patronage/identity_correlations`)
# MAGIC where
# MAGIC   operation not in ("CREATE TABLE", "OPTIMIZE")
# MAGIC order by
# MAGIC   1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)  from test_correlations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ICNStatus, count(*) FROM test_correlations
# MAGIC GROUP BY (ICNStatus)

# COMMAND ----------

master_df.groupBy("MVIPersonICN").count().orderBy("count", desc("count")).filter("count>1").display()

# COMMAND ----------

person.filter(person["Person_MVIPersonICN"] == 1000768382).display()

# COMMAND ----------

temp_person = spark.read.parquet("dbfs:/mnt/ci-mvi/Processed/SVeteran.SMVIPerson").withColumnRenamed("MVIPersonICN", "Person_MVIPersonICN")
temp_person.filter(temp_person["Person_MVIPersonICN"] == 1000768382).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/FileStore/test_correlations`

# COMMAND ----------

df = spark.table('test_correlations')

# COMMAND ----------


def get_summary(input_df):
    """
    Get summary statistics for a dataframe.
    Input: Spark dataframe
    Ouptput: Spark dataframe with summary statistics: Total Records, Nulls, Distincts, Dups, Non-Nulls, Max, Min and % of Nulls
    """
    # Total Records
    total_records = input_df.count()

    # Null Counts
    null_counts = input_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in input_df.columns])
    null_counts_dict = null_counts.first().asDict()

    # Total Non Null Counts
    counts = input_df.select([count(col(c)).alias(c) for c in input_df.columns])
    counts_dict = counts.first().asDict()

    # Count distinct
    distinct_counts_per_column = {}
    for column in input_df.columns:
        distinct_counts = input_df.select(countDistinct(col(column)).alias(column)).first()[column]
        distinct_counts_per_column[column] = distinct_counts

    total_dups_dict = {column: counts_dict[column] - distinct_counts_per_column[column] for column in input_df.columns}

    # max for each column
    max_value = {col: input_df.agg(max(col)).collect()[0][0] for col in input_df.columns}

    # min for each column
    min_value = {col: input_df.agg(min(col)).collect()[0][0] for col in input_df.columns}

    # % of nulls
    null_percent = {column: "{:.2f}".format((null_counts_dict[column] / total_records) * 100) for column in input_df.columns }
    # Create a summary DataFrame
    summary_data = [{"CountType": "Total Records", **{c: total_records for c in input_df.columns}},
                    {"CountType": "Total Nulls", **null_counts_dict},
                    {"CountType": "Null Percentage", **null_percent},
                    {"CountType": "Distinct Values", **distinct_counts_per_column},
                    {"CountType": "Total Dups", **total_dups_dict},
                    {"CountType": "Total Non-Nulls", **counts_dict},
                    {"CountType": "Max Value", **max_value},
                    {"CountType": "Min Value", **min_value},
                    ]


    summary_df = spark.createDataFrame(summary_data)

    # Reorder columns
    summary_df = summary_df.select("CountType", *input_df.columns)

    return summary_df

# COMMAND ----------

def get_any_id(df, input_value):
    conditions = [df[col].isin(input_value) for col in df.columns]
    df_filtered = df.filter(reduce(lambda a, b: a | b, conditions))

    return df_filtered

# COMMAND ----------

input_value = [5348444, 1010124988, 1019267760]
df = spark.table('test_correlations')
df = df.drop("Last_updated_date")
result_df = get_any_id(df, input_value)
display(result_df)

# COMMAND ----------

get_summary(df.drop("Last_Updated_Date")).display()

# COMMAND ----------

input_value = 4889737
result_df = get_any_id(df, input_value)
display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW not_in_josh as 
# MAGIC SELECT * FROM test_correlations
# MAGIC WHERE MVIPersonICN IN (
# MAGIC SELECT MVIPersonICN FROM DELTA.`dbfs:/FileStore/test_correlations`
# MAGIC MINUS
# MAGIC SELECT MVIPersonICN FROM DELTA.`dbfs:/mnt/Patronage/identity_correlations` )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`dbfs:/mnt/ci-patronage/delta_tables/identity_correlations_table` 
# MAGIC -- select * from not_in_josh

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(MVIPersonICN),"JoshDBFSCount" FROM DELTA.`dbfs:/mnt/Patronage/identity_correlations` 
# MAGIC UNION
# MAGIC SELECT count(MVIPersonICN), "myCount" FROM DELTA.`dbfs:/FileStore/test_correlations` 
# MAGIC UNION
# MAGIC SELECT count(MVIPersonICN), "patronageCount" FROM DELTA.`/mnt/ci-patronage/delta_tables/identity_correlations_table` 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DELTA.`/mnt/ci-patronage/delta_tables/identity_correlations_table`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from not_in_josh

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temp view temp_vw as 
# MAGIC select * from DELTA.`/mnt/ci-patronage/delta_tables/identity_correlations_table`
# MAGIC where participant_id is not null
# MAGIC qualify count(1) over (partition by participant_id) > 1
# MAGIC
# MAGIC -- select * from test_correlations
# MAGIC -- where EDIPI is not null
# MAGIC -- qualify count(1) over (partition by EDIPI) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from test_correlations
# MAGIC -- where MVIPersonICN in (select MVIPersonICN from temp_vw) and participant_id is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count( DISTINCT(participant_id)) FROM DELTA.`dbfs:/mnt/ci-patronage/delta_tables/identity_correlations_table/`
# MAGIC -- 57437759

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from test_correlations 
# MAGIC where participant_id in (Select participant_id  from  test_correlations
# MAGIC group by participant_id
# MAGIC having count(*) > 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct count(va_profile_id) from test_correlations

# COMMAND ----------

# MAGIC %sql
# MAGIC select 57617248 - 57596113

# COMMAND ----------

# MAGIC %sql
# MAGIC select MVIPersonICN, count(*) from test_correlations group by all having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_correlations where MVIPersonICN = 1052724750

# COMMAND ----------

Institutions.limit(1).display()

# COMMAND ----------

Institutions.select("InstitutionName", "MVIInstitutionSID").distinct().display()

# COMMAND ----------

Institutions.display()

# COMMAND ----------

Institutions.select("MVIInstitutionIEN", "MVIFacilityTypeIEN", "InstitutionName" ).distinct().orderBy("MVIFacilityTypeIEN","MVIInstitutionIEN").display()

# COMMAND ----------

multiple_tfpi.filter(col("icn")=='1010124988').display()
