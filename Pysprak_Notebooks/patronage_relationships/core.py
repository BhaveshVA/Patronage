"""
patronage_relationships.core
Reusable, well-documented Spark functions for building and analyzing person-institution relationships.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, desc, rank, broadcast, max, collect_list, struct, first

# Configuration for data sources (can be overridden by user)
DEFAULT_CONFIG = {
    "psa": {
        "columns": [
            "MVIPersonICN",
            "MVITreatingFacilityInstitutionSID",
            "TreatingFacilityPersonIdentifier",
            "ActiveMergedIdentifier",
            "CorrelationModifiedDateTime",
        ],
        "filter_condition": (
            (col("MVIPersonICN").isNotNull()) &
            (col("TreatingFacilityPersonIdentifier").rlike("^[0-9]+$")) &
            ((col("ActiveMergedIdentifier") == "Active") | col("ActiveMergedIdentifier").isNull())
        ),
        "window_spec": Window.partitionBy('MVIPersonICN', 'MVITreatingFacilityInstitutionSID', 'TreatingFacilityPersonIdentifier').orderBy(desc('CorrelationModifiedDateTime'))
    },
    "person": {
        "columns": ["MVIPersonICN", "ICNStatus"],
        "filter_condition": (col("rnk") == 1),
        "window_spec": Window.partitionBy('MVIPersonICN').orderBy(desc('calc_IngestionTimestamp'))
    },
    "institution": {
        "columns": ["MVIInstitutionSID", "InstitutionCode"],
        "filter_condition": (col("MVIInstitutionSID").rlike("^[0-9]+$")),
        "window_spec": None
    }
}

def filter_psa(psa_df: DataFrame, config=DEFAULT_CONFIG) -> DataFrame:
    """Filter and rank the Person Site Association DataFrame."""
    src = config["psa"]
    return (
        psa_df.filter(src["filter_condition"])
              .withColumn("rnk", rank().over(src["window_spec"]))
              .filter(col("rnk") == 1)
              .select(src["columns"])
    )

def filter_person(person_df: DataFrame, config=DEFAULT_CONFIG) -> DataFrame:
    """Filter and rank the Person DataFrame."""
    src = config["person"]
    return (
        person_df.withColumn("rnk", rank().over(src["window_spec"]))
                 .filter(src["filter_condition"])
                 .select(src["columns"])
    )

def filter_institution(institution_df: DataFrame, config=DEFAULT_CONFIG) -> DataFrame:
    """Filter the Institution DataFrame."""
    src = config["institution"]
    return (
        institution_df.filter(src["filter_condition"])
                     .select(src["columns"])
    )

def join_psa_institution(psa_df: DataFrame, institution_df: DataFrame) -> DataFrame:
    """Join PSA and Institution DataFrames."""
    return (
        psa_df.join(
            broadcast(institution_df),
            psa_df["MVITreatingFacilityInstitutionSID"] == institution_df["MVIInstitutionSID"],
            "left"
        )
        .select(
            psa_df["MVIPersonICN"],
            institution_df["InstitutionCode"],
            institution_df["MVIInstitutionSID"],
            psa_df["TreatingFacilityPersonIdentifier"],
            psa_df["CorrelationModifiedDateTime"]
        )
        .distinct()
    )

def find_duplicate_relationships(joined_df: DataFrame):
    """Find duplicate person-institution and institution-identifier pairs."""
    person_institution_dups = (
        joined_df.groupBy("MVIPersonICN", "InstitutionCode")
        .count()
        .filter(col("count") > 1)
        .withColumnRenamed("count", "count_iens")
    )
    institution_tfpi_dups = (
        joined_df.groupBy("InstitutionCode", "TreatingFacilityPersonIdentifier")
        .count()
        .filter(col("count") > 1)
        .withColumnRenamed("count", "count_icns")
    )
    return person_institution_dups, institution_tfpi_dups

def remove_duplicate_relationships(joined_df: DataFrame, person_institution_dups: DataFrame, institution_tfpi_dups: DataFrame):
    """Remove duplicate correlations."""
    correlations = (
        joined_df
        .join(broadcast(person_institution_dups), ["MVIPersonICN", "InstitutionCode"], "left")
        .join(broadcast(institution_tfpi_dups), ["InstitutionCode", "TreatingFacilityPersonIdentifier"], "left")
    )
    duplicate_correlations = correlations.filter(col("count_iens").isNotNull() | col("count_icns").isNotNull())
    clean_correlations = (
        correlations
        .filter(col("count_iens").isNull())
        .filter(col("count_icns").isNull())
        .drop("count_iens", "count_icns")
        .select("MVIPersonICN", "CorrelationModifiedDateTime", "InstitutionCode", "TreatingFacilityPersonIdentifier")
    )
    return clean_correlations, duplicate_correlations

def get_latest_correlation_date(joined_df: DataFrame) -> DataFrame:
    """Get the max CorrelationModifiedDateTime for each MVIPersonICN."""
    return (
        joined_df
        .groupBy(col("MVIPersonICN"))
        .agg(max("CorrelationModifiedDateTime").alias("Last_Modified_Date"))
    )

def build_correlation_lookup_table(latest_date_df: DataFrame, clean_corr_df: DataFrame, person_df: DataFrame) -> DataFrame:
    """Build the final correlation lookup DataFrame."""
    return (
        latest_date_df.join(clean_corr_df, ['MVIPersonICN'], 'left')
        .join(person_df, ['MVIPersonICN'], "left")
        .groupBy("MVIPersonICN", "ICNStatus", "Last_Modified_Date")
        .pivot("InstitutionCode")
        .agg(first("TreatingFacilityPersonIdentifier"))
        .withColumnRenamed("200CORP", "participant_id")
        .withColumnRenamed("200DOD", "edipi")
        .withColumnRenamed("200VETS", "va_profile_id")
        .select("MVIPersonICN", "participant_id", "edipi", "va_profile_id", "ICNStatus", "Last_Modified_Date")
    )

def build_json_correlation_table(latest_date_df: DataFrame, clean_corr_df: DataFrame, person_df: DataFrame) -> DataFrame:
    """Build the JSON format DataFrame for all institutions."""
    return (
        latest_date_df.join(clean_corr_df, ['MVIPersonICN'], 'left')
        .join(person_df, ['MVIPersonICN'], "left")
        .groupBy("MVIPersonICN", "Last_Modified_Date", "ICNStatus")
        .agg(
            collect_list(
                struct(
                    col("InstitutionCode").alias("Institution"),
                    col("TreatingFacilityPersonIdentifier").alias("Identifier")
                )
            ).alias("InstitutionIDs")
        )
    )
