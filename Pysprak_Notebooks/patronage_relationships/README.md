# Patronage Relationships Package

![Data Processing Overview](https://raw.githubusercontent.com/databricks/tech-talks/main/assets/spark-modern-pipeline-diagram.png)

*Figure: Modern data pipeline in Databricks using the Patronage Relationships package. Data flows from raw sources, through modular transformation functions, to clean, analytics-ready Delta tables.*

---

A reusable, production-ready PySpark package for building, analyzing, and exporting person-institution relationship tables for healthcare and identity data pipelines.

## Directory Structure

```
VS_PATRONAGE/
  patronage_relationships/
    __init__.py
    core.py
    test_patronage_relationships.ipynb
    pipeline_patronage_relationships.ipynb
    README.md
```

## Installation & Usage

1. **Copy the `patronage_relationships` folder to your project or Databricks workspace.**
2. **Import the package in your notebook or script:**
   ```python
   from patronage_relationships.core import filter_psa, filter_person, filter_institution, join_psa_institution, find_duplicate_relationships, remove_duplicate_relationships, get_latest_correlation_date, build_correlation_lookup_table, build_json_correlation_table
   ```

## Example: End-to-End Usage

```python
from patronage_relationships.core import *

# Read data
psa_df = spark.read.parquet("/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/")
person_df = spark.read.parquet("/mnt/ci-mvi/Processed/SVeteran.SMVIPerson/")
institution_df = spark.read.parquet("/mnt/ci-mvi/Raw/NDim.MVIInstitution/")

# Transform
filtered_psa = filter_psa(psa_df)
filtered_person = filter_person(person_df)
filtered_institution = filter_institution(institution_df)
joined = join_psa_institution(filtered_psa, filtered_institution)
person_institution_dups, institution_tfpi_dups = find_duplicate_relationships(joined)
clean_corr, dup_corr = remove_duplicate_relationships(joined, person_institution_dups, institution_tfpi_dups)
latest_date = get_latest_correlation_date(joined)
lookup_table = build_correlation_lookup_table(latest_date, clean_corr, filtered_person)
json_table = build_json_correlation_table(latest_date, clean_corr, filtered_person)

# Save
lookup_table.write.saveAsTable("correlation_lookup")
```

## Function Reference

### 1. `filter_psa(psa_df, config=DEFAULT_CONFIG)`
Filters and ranks the Person Site Association DataFrame. Returns only the latest, valid records for each person-institution-identifier group.

### 2. `filter_person(person_df, config=DEFAULT_CONFIG)`
Filters and ranks the Person DataFrame to keep only the latest record for each person.

### 3. `filter_institution(institution_df, config=DEFAULT_CONFIG)`
Filters the Institution DataFrame to keep only valid institutions.

### 4. `join_psa_institution(psa_df, institution_df)`
Joins the filtered PSA and Institution DataFrames, returning a DataFrame with person, institution, and identifier columns.

### 5. `find_duplicate_relationships(joined_df)`
Finds duplicate person-institution and institution-identifier pairs. Returns two DataFrames: one for each type of duplicate.

### 6. `remove_duplicate_relationships(joined_df, person_institution_dups, institution_tfpi_dups)`
Removes duplicate correlations from the joined DataFrame. Returns two DataFrames: one with clean correlations, one with duplicates.

### 7. `get_latest_correlation_date(joined_df)`
Returns a DataFrame with the latest correlation modification date for each person.

### 8. `build_correlation_lookup_table(latest_date_df, clean_corr_df, person_df)`
Builds the final correlation lookup table, pivoting institution codes to columns (e.g., participant_id, edipi, va_profile_id).

### 9. `build_json_correlation_table(latest_date_df, clean_corr_df, person_df)`
Builds a JSON-style table with all institution identifiers for each person.

## Testing & Validation

- All functions are tested in `test_patronage_relationships.ipynb` using sample data.
- Each function is validated for correctness and edge cases before being used in production pipelines.
- The test notebook demonstrates expected input/output for each function and can be used as a template for onboarding or further development.

## Production Pipeline Example

See `pipeline_patronage_relationships.ipynb` for a real-world example of using the package in a Spark/Databricks pipeline, including reading from and writing to Delta tables.

## Support & Contribution

- For questions or improvements, please contact the package maintainer or submit a pull request.
- Contributions and suggestions are welcome!
