# Databricks notebook source
import os
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from delta import DeltaTable
from pyspark.sql import functions as F
from functools import reduce


# COMMAND ----------

# %sql
# drop table test_correlations;

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
    "MVIPersonICN"
).orderBy(desc("calc_IngestionTimestamp"))

basepath = "dbfs:/FileStore/"

# COMMAND ----------

raw_psa_df = spark.read.parquet("/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/")

person_df = ((spark.read.parquet("dbfs:/mnt/ci-mvi/Processed/SVeteran.SMVIPerson"))
    .withColumn("rnk", rank().over(Window_Spec1))
    .filter(col("rnk") == 1)
    .select("MVIPersonICN", "ICNStatus")
)

institutions_df = spark.read.parquet("/mnt/ci-mvi/Raw/NDim.MVIInstitution/")

raw_psa_filtered_df = (raw_psa_df
                       .filter((col("MVITreatingFacilityInstitutionSID").isin([5667, 6061, 6722])) &
                               ((col("ActiveMergedIdentifier") == "Active") | col("ActiveMergedIdentifier").isNull()) &
                               (col("MVIPersonICN").isNotNull() & (col("OpCode") != 'D')))
                       .withColumn("rank", rank().over(Window_Spec))
                       .filter(col("rank") == 1)
                       .dropDuplicates(["MVIPersonICN", "MVITreatingFacilityInstitutionSID", "TreatingFacilityPersonIdentifier", "ActiveMergedIdentifier"])
                       .select("MVIPersonICN", "MVITreatingFacilityInstitutionSID", "TreatingFacilityPersonIdentifier", "ActiveMergedIdentifier", "CorrelationModifiedDateTime")
)

last_mod_status = (raw_psa_filtered_df
                   .groupBy("MVIPersonICN")
                   .agg(F.max("CorrelationModifiedDateTime").alias("Last_updated_date"))
                   .select("MVIPersonICN", "Last_updated_date")
)
# Step 2: Filter and join raw_psa_df with institution 
join_df = (raw_psa_filtered_df
           .join(institutions_df.select("MVIInstitutionSID"), raw_psa_df["MVITreatingFacilityInstitutionSID"] == institutions_df["MVIInstitutionSID"], 
          "left").orderBy("MVIPersonICN")
    .distinct()
)

multiple_icn = (
    join_df.groupBy("TreatingFacilityPersonIdentifier", "MVIInstitutionSID")
    .count()
    .filter(col("count") > 1)
    .withColumnRenamed("count", "icn_count")
    .withColumnRenamed("TreatingFacilityPersonIdentifier", "TFPI")
    .withColumnRenamed("MVIInstitutionSID", "Inst")
)


multiple_tfpi = (
    join_df.groupBy("MVIPersonICN", "MVIInstitutionSID")
    .count()
    .filter(col("count") > 1)
    .withColumnRenamed("count", "tfpi_count")
    .withColumnRenamed("MVIInstitutionSID", "Inst")
    .withColumnRenamed("MVIPersonICN", "ICN")
)

non_duplicates = (
    join_df.join(
        multiple_icn,
        (join_df["TreatingFacilityPersonIdentifier"] == multiple_icn["TFPI"])
        & (join_df["MVIInstitutionSID"] == multiple_icn["Inst"]),
        "left",
    )
    .join(
        multiple_tfpi,
        (join_df["MVIPersonICN"] == multiple_tfpi["ICN"])
        & (join_df["MVIInstitutionSID"] == multiple_tfpi["Inst"]),
        "left",
    )
    .filter(join_df["TreatingFacilityPersonIdentifier"].isNotNull())
#    .select(
#         join_df["MVIPersonICN"],
#         join_df["ActiveMergedIdentifier"],
#         join_df["MVIInstitutionSID"],
#         join_df["TreatingFacilityPersonIdentifier"]
#     )
)

non_duplicates = non_duplicates.filter(col("icn_count").isNull()).filter(col("tfpi_count").isNull())

pivoted_df = (
    non_duplicates
    .withColumn("MVIInstitutionSID", when(col("MVITreatingFacilityInstitutionSID") == 5667, "EDIPI")
                 .when(col("MVITreatingFacilityInstitutionSID") == 6061, "participant_id")
                 .when(col("MVITreatingFacilityInstitutionSID") == 6722, "va_profile_id"))
    .groupBy("MVIPersonICN")
    .pivot("MVIInstitutionSID", ["EDIPI", "participant_id", "va_profile_id"])
    .agg(first("TreatingFacilityPersonIdentifier"))
)

final_df = pivoted_df.join(last_mod_status, ["MVIPersonICN"], "left").join(person_df, ["MVIPersonICN"], "left")

# unique_icn = (filtered_df
#               .groupBy(filtered_df["MVIPersonICN"], filtered_df["ICNStatus"])
#               .agg(max("Last_updated").cast("date").alias("Last_updated"))
#               .distinct()
#               .selectExpr("MVIPersonICN as ICN", "Last_updated", "ICNStatus")
# )

# COMMAND ----------

final_df.groupBy(col("MVIPersonICN")).count().orderBy(col("count").desc()).display()

# COMMAND ----------

raw_psa_df.filter(col("TreatingFacilityPersonIdentifier")== '5348444').filter(raw_psa_df["MVITreatingFacilityInstitutionSID"].isin([5667, 6061, 6722])).display()

# COMMAND ----------

# dod_df = (non_duplicates
#         .filter(
#         (non_duplicates["MVIInstitutionSID"] == 5667)
#                 )
#         .selectExpr("MVIPersonICN as dod_icn", "TreatingFacilityPersonIdentifier as DoD_facility"))


# corp_df = (non_duplicates
#         .filter(
#         (non_duplicates["MVIInstitutionSID"] == 6061)
#                 )
#         .selectExpr("MVIPersonICN as corp_icn", "TreatingFacilityPersonIdentifier as corp_facility"))


# vets_df = (non_duplicates
#         .filter((non_duplicates["MVIInstitutionSID"] == 6722)
#                 )
#         .selectExpr("MVIPersonICN as vets_icn", "TreatingFacilityPersonIdentifier as vets_facility"))


# COMMAND ----------

# master_df = (
#     unique_icn
#     .join(dod_df, unique_icn["ICN"] == dod_df["dod_icn"], "left")
#     .join(corp_df, unique_icn["ICN"] == corp_df["corp_icn"], "left")
#     .join(vets_df, unique_icn["ICN"] == vets_df["vets_icn"], "left")
#     .select(
#         unique_icn["ICN"].alias("MVIPersonICN"),
#         dod_df["DoD_facility"].alias("EDIPI"),
#         corp_df["corp_facility"].alias("participant_id"),
#         vets_df["vets_facility"].alias("va_profile_id"),
#         unique_icn["Last_updated"].alias("Last_Updated_Date"),
#         unique_icn["ICNStatus"]
#     )
# )

# COMMAND ----------

master_df.display()

# COMMAND ----------

spark.sql("OPTIMIZE delta.`dbfs:/FileStore/test_correlations`")
target = DeltaTable.forPath(spark, basepath + "test_correlations")

target.alias("target").merge(
    source=final_df.alias("source"),
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

df = spark.table('test_correlations_1')

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

input_value = [47578199]
df = spark.table('test_correlations')
df = df.drop("Last_updated_date")
result_df = get_any_id(df, input_value)
display(result_df)

# COMMAND ----------

d1 = DeltaTable.forPath(spark, "dbfs:/mnt/Patronage/identity_correlations")
df1 = d1.toDF()

# COMMAND ----------

get_summary(df.drop("Last_updated_date")).display()

# COMMAND ----------

get_summary(df1.drop("record_updated_date")).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM test_correlations_1  WHERE MVIPersonICN in (1064103767,1077474889)--participant_id = 62329400 -- va_profile_id =32025585 
# MAGIC SELECT * FROM DELTA.`dbfs:/mnt/Patronage/identity_correlations` WHERE MVIPersonICN IN (1064103767,1077006882,1077474889)
# MAGIC -- not in (SELECT MVIPersonICN FROM DELTA.`dbfs:/mnt/Patronage/identity_correlations` )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_correlations_1  WHERE  MVIPersonICN IN (1064103767,1077006882,1077474889)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`dbfs:/mnt/Patronage/identity_correlations` WHERE MVIPersonICN not in (SELECT MVIPersonICN from test_correlations_1)

# COMMAND ----------

input_value = 4889737
result_df = get_any_id(df, input_value)
display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW not_in_josh as 
# MAGIC -- SELECT * FROM test_correlations
# MAGIC -- WHERE MVIPersonICN IN (
# MAGIC
# MAGIC
# MAGIC SELECT MVIPersonICN FROM DELTA.`dbfs:/mnt/Patronage/identity_correlations` MINUS
# MAGIC SELECT MVIPersonICN FROM DELTA.`dbfs:/FileStore/test_correlations`
# MAGIC -- )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM  DELTA.`dbfs:/mnt/Patronage/identity_correlations` 
# MAGIC select * from not_in_josh

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM  DELTA.`dbfs:/mnt/Patronage/identity_correlations` where participant_id is null and  edipi is null and va_profile_id is NULL
# MAGIC --  where MVIPersonICN = 1001851303

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM  test_correlations where participant_id is null and  edipi is null and va_profile_id is NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_correlations where MVIPersonICN = 1001851303

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(MVIPersonICN),"JoshDBFSCount" FROM DELTA.`dbfs:/mnt/Patronage/identity_correlations` where edipi is NULL and  participant_id is null AND va_profile_id IS NULL
# MAGIC -- # UNION
# MAGIC -- # SELECT count(MVIPersonICN), "myCount" FROM DELTA.`dbfs:/FileStore/test_correlations` 
# MAGIC -- # 59467388 - 59876763

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
# MAGIC select * from test_correlations where MVIPersonICN = 1052724750

# COMMAND ----------

multiple_tfpi.filter(col("icn")=='1047965857').display()

# COMMAND ----------

multiple_icn.filter(col("TFPI")=='1047965857').display()

# COMMAND ----------

join_df.filter(col("MVIPersonICN") == '1047965857').display()

# COMMAND ----------

raw_psa_df.createOrReplaceTempView("raw_psa")

# COMMAND ----------

# DBTITLE 1,Filtering by ICN
# MAGIC %sql
# MAGIC select 
# MAGIC   MVIPersonICN, MVITreatingFacilityInstitutionSID,
# MAGIC   TreatingFacilityPersonIdentifier, 
# MAGIC   ActiveMergedIdentifier,
# MAGIC   CorrelationModifiedDateTime,  
# MAGIC   rank() over (partition by MVIPersonICN, MVITreatingFacilityInstitutionSID, TreatingFacilityPersonIdentifier order by CorrelationModifiedDateTime desc  )  as rnk
# MAGIC from raw_psa 
# MAGIC where MVIPersonICN in (1029805160)                         -- (1018744003, 1045127634, 1023717356, 1077148299, 1024489160)
# MAGIC and MVITreatingFacilityInstitutionSID in (5667, 6061, 6722)
# MAGIC and ActiveMergedIdentifier = 'Active'
# MAGIC -- qualify rnk <= 2
# MAGIC order by 1, 2, 3, 5 desc

# COMMAND ----------

# DBTITLE 1,Filtering by TreatingFacilityPersonIdentifier
# MAGIC %sql
# MAGIC select 
# MAGIC   MVIPersonICN, MVITreatingFacilityInstitutionSID,
# MAGIC   TreatingFacilityPersonIdentifier, 
# MAGIC   ActiveMergedIdentifier,
# MAGIC   CorrelationModifiedDateTime,  
# MAGIC   rank() over (partition by MVIPersonICN, MVITreatingFacilityInstitutionSID, TreatingFacilityPersonIdentifier order by CorrelationModifiedDateTime desc  )  as rnk
# MAGIC from raw_psa 
# MAGIC where TreatingFacilityPersonIdentifier in (47578199, 47714383)                         -- (1018744003, 1045127634, 1023717356, 1077148299, 1024489160)
# MAGIC and MVITreatingFacilityInstitutionSID in (5667, 6061, 6722)
# MAGIC and ActiveMergedIdentifier = 'Active'
# MAGIC -- qualify rnk <= 2
# MAGIC order by 1, 2, 3, 5 desc

# COMMAND ----------

filtered_df.filter(col("TreatingFacilityPersonIdentifier").isin([47578199, 47714383])).filter(filtered_df["MVITreatingFacilityInstitutionSID"].isin([5667, 6061, 6722])).display()


# COMMAND ----------

join_df.filter(col("TreatingFacilityPersonIdentifier").isin([47578199, 47714383])).filter(join_df["MVITreatingFacilityInstitutionSID"].isin([5667, 6061, 6722])).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte1 as (
# MAGIC select 
# MAGIC   MVIPersonICN, 
# MAGIC   TreatingFacilityPersonIdentifier, 
# MAGIC   MVITreatingFacilityInstitutionSID,
# MAGIC   ActiveMergedIdentifier,
# MAGIC   CorrelationModifiedDateTime,  
# MAGIC   rank() over (partition by MVIPersonICN, MVITreatingFacilityInstitutionSID, TreatingFacilityPersonIdentifier order by CorrelationModifiedDateTime desc  ) as rnk
# MAGIC from raw_psa 
# MAGIC where MVIPersonICN in (1000971431, 1047965857)
# MAGIC and MVITreatingFacilityInstitutionSID in (5667, 6061, 6722)
# MAGIC and ActiveMergedIdentifier = 'Active'
# MAGIC qualify rnk <= 2)
# MAGIC select MVIPersonICN, from cte1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_correlations  where MVIPersonICN IN (1045127634,1023717356, 1077148299, 1024489160, 1029805160 )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`dbfs:/mnt/Patronage/identity_correlations`  where MVIPersonICN IN  (1045127634,1023717356, 1077148299, 1024489160, 1029805160)

# COMMAND ----------

df = spark.table('test_correlations')
df = df.drop("Last_updated_date")

# COMMAND ----------

input_value = [1029805160, 23224257]

result_df = get_any_id(df, input_value)
display(result_df)

# COMMAND ----------

psa.filter(col("MVIPersonICN").isin([1029805160])).filter(psa["MVITreatingFacilityInstitutionSID"].isin([5667, 6061, 6722])).display()

# COMMAND ----------

psa.filter(col("MVIPersonICN").isin([1029805160])).filter(psa["MVITreatingFacilityInstitutionSID"].isin([5667, 6061, 6722])).dropDuplicates(["MVIPersonICN", "MVITreatingFacilityInstitutionSID", "TreatingFacilityPersonIdentifier","Last_Modified"]).display()

