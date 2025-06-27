# Databricks notebook source
# import os
# from datetime import *
# from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Check today's Day
# todays_date = date.today()
# if todays_date.weekday() not in [2, 4]:
#     # print('Exiting this notebook because today is not Wednesday or Friday')
#     dbutils.notebook.exit(f"Notebook exited because today is {todays_date.strftime('%A')}.\nThis notebook runs only on Wednesday or Friday")
# else:
#     print(f"Today is {todays_date.strftime('%A')}.\nProceed to generate the required output file.")

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS dmdc_checkpoint;

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS dmdc_checkpoint (
#   checkpoint_timestamp TIMESTAMP,
#   filename STRING,
#   record_count INTEGER,
#   generate_query STRING
# )

# COMMAND ----------

# DBTITLE 1,Read data from the delta table to a dataframe
# cg_data = spark.read.format("delta").load("/mnt/Patronage/Caregivers_Staging_New")
# scd_data = spark.read.format("delta").load("/mnt/Patronage/SCD_Staging")

# cg_data.createOrReplaceTempView('cg_data')
# scd_data.createOrReplaceTempView('scd_data')


# COMMAND ----------

# DBTITLE 1,This function will pull data based on the last run date
# def get_data():
#     last_run_date = spark.sql(
#         'SELECT COALESCE(DATE(max(checkpoint_timestamp)), current_date()) AS last_run_date FROM dmdc_checkpoint'
#     ).first()[0]

#     now = datetime.now()
#     if todays_date.weekday() == 2:
#         today_start_time = todays_date - timedelta(days=1)
#     else:
#         today_start_time = datetime(now.year, now.month, now.day)

#     cg_query = f"""
#         select rpad(coalesce(EDIPI,''),10,' ' ) ||
#             rpad(coalesce(Batch_CD,''), 3,' ') ||
#             rpad(coalesce(SC_Combined_Disability_Percentage,''),3,' ') ||
#             rpad(coalesce(date_format(status_begin_date, 'yyyyMMdd'),''),8,' ') ||
#             rpad(coalesce(PT_Indicator,''),1,' ') ||
#             rpad(coalesce(Individual_Unemployability,''),1,' ') ||  
#             rpad(coalesce(date_format(Status_Last_Update, 'yyyyMMdd'),''),8,' ') ||
#             rpad(coalesce(date_format(Status_Termination_Date, 'yyyyMMdd'),''),8,' ') as CG
#         from cg_data
#         where SDP_Event_Created_Timestamp >= DATE('{last_run_date}')
#         and SDP_Event_Created_Timestamp <= DATE('{today_start_time}')
#         and EDIPI is not null
#         and Applicant_Type = 'Primary Caregiver'
#         and (Status_Termination_Date is NULL OR Status_Termination_Date >= curdate()
#             OR Status IN ('Approved', 'Pending Revocation/Discharge'))
#         """
#     scd_query = f"""
#         select rpad(coalesce(edipi,''),10,' ' ) ||
#             rpad(coalesce(Batch_CD,''), 3,' ') ||
#             rpad(coalesce(SC_Combined_Disability_Percentage,''),3,' ') ||
#             rpad(coalesce(Status_Begin_Date,''),8,' ') ||
#             rpad(coalesce(PT_Indicator,''),1,' ') ||
#             rpad(coalesce(Individual_Unemployability,''),1,' ') ||
#             rpad(coalesce(Status_Last_Update,''),8,' ') ||
#             rpad(coalesce(Status_Termination_Date,''),8,' ') as CG
#         from scd_data
#         where SDP_Event_Created_Timestamp >= DATE('{last_run_date}')
#         and SDP_Event_Created_Timestamp <= DATE('{today_start_time}')
#         and edipi is not null
#         """
#     combined_data = f"""
#         {cg_query}
#         union all
#         {scd_query}
#     """

#     return combined_data

# COMMAND ----------

# DBTITLE 1,This function will write the pandas dataframe to a blob storage
# def write_to_patronage(combined_data):

#     date_format = todays_date.strftime("%Y%m%d")
#     outuput_filename = f"PATRONAGE_{date_format}.txt"
#     outuput_path = f"/dbfs/mnt/ci-patronage/dmdc_extracts/combined_export/{outuput_filename}"
#     pandas_df = spark.sql(combined_data).toPandas()
#     df_record_count = len(pandas_df)
#     if df_record_count == 0:
#         dbutils.notebook.exit(f"Notebook exited because there is no data to write")

#     with open(outuput_path, "w", newline="\r\n") as f:            # DMDC requires output file as unix txt file
#         f.write(pandas_df.to_string(index=False, header=False))
#     # display(pandas_df)
#     now = datetime.now()
#     if todays_date.weekday() == 2:
#         today_start_time = todays_date - timedelta(days=1)
#     elif todays_date.weekday() == 4:
#         today_start_time = datetime(now.year, now.month, now.day)

#     spark.sql(f"""
#               INSERT INTO dmdc_checkpoint 
#               (checkpoint_timestamp, filename, record_count, generate_query)
#               VALUES ('{today_start_time}','{outuput_path}', {df_record_count}, "{combined_data}")
#               """
#     )

#     print(f'Data has been written to: {outuput_path}')
#     print(f'Number of records: {df_record_count}')
#     print(f"Current timestamp: {today_start_time}")
#     print(f"Generated Query:\n {combined_data}")
#     print(pandas_df.to_string())

# COMMAND ----------

# query = get_data()
# write_to_patronage(query)

# COMMAND ----------

# %sql
# SELECT * FROM dmdc_checkpoint ORDER BY 1 DESC LIMIT 5;

# COMMAND ----------

# query = get_data()
# print(query)

# COMMAND ----------

# from ftplib import FTP
# ip_add = '140.20.107.4'
# username = 'patelbn'
# password = 'w&zuyHqy_XX5zXe4'
# ftp_directory = '/ftp/data02/pftbat/incoming' 
# ftp = FTP(ip_add)
# ftp.login(user=username, passwd=password)
# ftp.cwd(ftp_directory)
# with open(outuput_path, "w", newline="\r\n") as f:            # DMDC requires output file as unix txt file
        # ftp.storbinary(pandas_df.to_string(index=False, header=False))

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Overview and Query Details
# MAGIC
# MAGIC This notebook orchestrates the extraction, transformation, and export of data for the DMDC (Defense Manpower Data Center) data pulls. It is designed to run only on Wednesdays and Fridays, ensuring that data is processed and exported on the required schedule. The workflow includes checkpointing, data extraction from two main sources, formatting, and output file generation.
# MAGIC
# MAGIC ## Workflow Summary
# MAGIC - **Execution Day Check:** The notebook runs only on Wednesdays and Fridays. If executed on any other day, it exits without processing.
# MAGIC - **Checkpoint Table:** Maintains a log of previous runs, including timestamps, output filenames, record counts, and the queries used.
# MAGIC - **Data Loading:** Reads data from two Delta tables: Caregivers (`cg_data`) and SCD (`scd_data`).
# MAGIC - **Query Construction:** Builds two SQL queries (`cg_query` and `scd_query`) to extract and format data from each source.
# MAGIC - **Data Combination:** Combines the results of both queries into a single dataset.
# MAGIC - **Output Generation:** Writes the combined data to a fixed-width text file and logs the run in the checkpoint table.
# MAGIC
# MAGIC ## What do `cg_query` and `scd_query` do?
# MAGIC
# MAGIC ### `cg_query` (Caregivers Data Query)
# MAGIC - **Purpose:** Extracts and formats records for primary caregivers from the `cg_data` table.
# MAGIC - **Logic:**
# MAGIC   - Selects only records where `EDIPI` is not null and `Applicant_Type` is 'Primary Caregiver'.
# MAGIC   - Filters records based on the event creation timestamp, ensuring only new or updated records since the last run are included.
# MAGIC   - Further restricts to records where the status is active or pending, or the termination date is null or in the future.
# MAGIC   - For each record, concatenates and right-pads several fields to fixed widths:
# MAGIC     - `EDIPI` (10 chars)
# MAGIC     - `Batch_CD` (3 chars)
# MAGIC     - `SC_Combined_Disability_Percentage` (3 chars)
# MAGIC     - `status_begin_date` (8 chars, formatted as YYYYMMDD)
# MAGIC     - `PT_Indicator` (1 char)
# MAGIC     - `Individual_Unemployability` (1 char)
# MAGIC     - `Status_Last_Update` (8 chars, formatted as YYYYMMDD)
# MAGIC     - `Status_Termination_Date` (8 chars, formatted as YYYYMMDD)
# MAGIC   - The result is a single fixed-width string per record, suitable for DMDC requirements.
# MAGIC
# MAGIC ### `scd_query` (SCD Data Query)
# MAGIC - **Purpose:** Extracts and formats records from the SCD (Service-Connected Disability) data source (`scd_data`).
# MAGIC - **Logic:**
# MAGIC   - Selects only records where `edipi` is not null.
# MAGIC   - Filters records based on the event creation timestamp, similar to `cg_query`.
# MAGIC   - For each record, concatenates and right-pads the same set of fields as in `cg_query`, but uses the SCD table's field names (note the lowercase `edipi` and some fields may not be date-formatted).
# MAGIC   - The result is a single fixed-width string per record, matching the format required for DMDC export.
# MAGIC
# MAGIC **Both queries ensure that the exported data is in a strict, fixed-width format, with all fields padded as required by the DMDC specification. The union of these queries provides a comprehensive export of both Caregivers and SCD data in a single output file.**

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Import Required Libraries

# COMMAND ----------

# Standard library imports
import os
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session if not already initialized
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Define Utility Functions

# COMMAND ----------

def is_valid_execution_day(current_date: date) -> bool:
    """
    Check if the current day is valid for notebook execution (Wednesday or Friday).
    
    Args:
        current_date (date): The date to check
        
    Returns:
        bool: True if it's Wednesday (2) or Friday (4), False otherwise
    """
    return current_date.weekday() in [2, 4]

def get_start_time(current_date: date) -> datetime:
    """
    Calculate the start time based on the current day.
    For Wednesday, it's previous day, for Friday it's current day midnight.
    
    Args:
        current_date (date): The current date
        
    Returns:
        datetime: The calculated start time
    """
    now = datetime.now()
    if current_date.weekday() == 2:  # Wednesday
        return current_date - timedelta(days=1)
    return datetime(now.year, now.month, now.day)

def format_fixed_width_field(value: Any, width: int) -> str:
    """
    Format a value as a fixed-width field with right padding.
    
    Args:
        value: The value to format
        width (int): The desired width of the field
        
    Returns:
        str: The formatted string, right-padded with spaces
    """
    return str(value or '').ljust(width)[:width]

def get_output_file_path(date_str: str) -> str:
    """
    Generate the output file path for the DMDC extract.
    
    Args:
        date_str (str): Date string in YYYYMMDD format
        
    Returns:
        str: The complete output file path
    """
    return f"/dbfs/mnt/ci-patronage/dmdc_extracts/combined_export/PATRONAGE_{date_str}.txt"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Data Loading and Processing Functions

# COMMAND ----------

def create_checkpoint_table():
    """
    Create the checkpoint table if it doesn't exist.
    This table tracks the execution history and output files.
    """
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dmdc_checkpoint (
            checkpoint_timestamp TIMESTAMP,
            filename STRING,
            record_count INTEGER,
            generate_query STRING
        )
    """)

def load_source_data():
    """
    Load the source data from Delta tables and create temporary views.
    
    Returns:
        tuple: (cg_data DataFrame, scd_data DataFrame)
    """
    cg_data = spark.read.format("delta").load("/mnt/Patronage/Caregivers_Staging_New")
    scd_data = spark.read.format("delta").load("/mnt/Patronage/SCD_Staging")
    
    # Create temporary views for SQL queries
    cg_data.createOrReplaceTempView('cg_data')
    scd_data.createOrReplaceTempView('scd_data')
    
    return cg_data, scd_data

def get_last_run_date() -> date:
    """
    Get the last checkpoint timestamp from the tracking table.
    
    Returns:
        date: The last run date or current date if no previous runs
    """
    return spark.sql(
        'SELECT COALESCE(DATE(max(checkpoint_timestamp)), current_date()) AS last_run_date FROM dmdc_checkpoint'
    ).first()[0]

def build_cg_query(last_run_date: date, today_start_time: datetime) -> str:
    """
    Build the query for Caregivers data with proper formatting.
    
    Args:
        last_run_date (date): The last execution date
        today_start_time (datetime): The start time for the current run
        
    Returns:
        str: The formatted SQL query
    """
    return f"""
        SELECT 
            rpad(coalesce(EDIPI,''),10,' ' ) ||
            rpad(coalesce(Batch_CD,''), 3,' ') ||
            rpad(coalesce(SC_Combined_Disability_Percentage,''),3,' ') ||
            rpad(coalesce(date_format(status_begin_date, 'yyyyMMdd'),''),8,' ') ||
            rpad(coalesce(PT_Indicator,''),1,' ') ||
            rpad(coalesce(Individual_Unemployability,''),1,' ') ||  
            rpad(coalesce(date_format(Status_Last_Update, 'yyyyMMdd'),''),8,' ') ||
            rpad(coalesce(date_format(Status_Termination_Date, 'yyyyMMdd'),''),8,' ') as CG
        FROM cg_data
        WHERE SDP_Event_Created_Timestamp >= DATE('{last_run_date}')
        AND SDP_Event_Created_Timestamp <= DATE('{today_start_time}')
        AND EDIPI IS NOT NULL
        AND Applicant_Type = 'Primary Caregiver'
        AND (Status_Termination_Date IS NULL 
             OR Status_Termination_Date >= curdate()
             OR Status IN ('Approved', 'Pending Revocation/Discharge'))
    """

def build_scd_query(last_run_date: date, today_start_time: datetime) -> str:
    """
    Build the query for SCD data with proper formatting.
    
    Args:
        last_run_date (date): The last execution date
        today_start_time (datetime): The start time for the current run
        
    Returns:
        str: The formatted SQL query
    """
    return f"""
        SELECT 
            rpad(coalesce(edipi,''),10,' ' ) ||
            rpad(coalesce(Batch_CD,''), 3,' ') ||
            rpad(coalesce(SC_Combined_Disability_Percentage,''),3,' ') ||
            rpad(coalesce(Status_Begin_Date,''),8,' ') ||
            rpad(coalesce(PT_Indicator,''),1,' ') ||
            rpad(coalesce(Individual_Unemployability,''),1,' ') ||
            rpad(coalesce(Status_Last_Update,''),8,' ') ||
            rpad(coalesce(Status_Termination_Date,''),8,' ') as CG
        FROM scd_data
        WHERE SDP_Event_Created_Timestamp >= DATE('{last_run_date}')
        AND SDP_Event_Created_Timestamp <= DATE('{today_start_time}')
        AND edipi IS NOT NULL
    """

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Output Processing and Checkpointing

# COMMAND ----------

def write_to_patronage(combined_data: str, current_date: date, today_start_time: datetime):
    """
    Write the combined data to a file and log the checkpoint.
    
    Args:
        combined_data (str): The SQL query for combined data
        current_date (date): The current date
        today_start_time (datetime): The start time for the current run
        
    Returns:
        None
        
    Raises:
        SystemExit: If there is no data to write
    """
    # Generate output file path
    date_format = current_date.strftime("%Y%m%d")
    output_path = get_output_file_path(date_format)
    
    # Convert to pandas and check if we have data
    pandas_df = spark.sql(combined_data).toPandas()
    df_record_count = len(pandas_df)
    
    if df_record_count == 0:
        dbutils.notebook.exit("Notebook exited because there is no data to write")
    
    # Write to file with Unix line endings (DMDC requirement)
    with open(output_path, "w", newline="\r\n") as f:
        f.write(pandas_df.to_string(index=False, header=False))
    
    # Log checkpoint
    spark.sql(f"""
        INSERT INTO dmdc_checkpoint 
        (checkpoint_timestamp, filename, record_count, generate_query)
        VALUES (
            '{today_start_time}',
            '{output_path}', 
            {df_record_count}, 
            "{combined_data}"
        )
    """)
    
    # Print summary
    print(f'Data has been written to: {output_path}')
    print(f'Number of records: {df_record_count}')
    print(f"Current timestamp: {today_start_time}")
    print(f"Generated Query:\n {combined_data}")
    print(pandas_df.to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Main Workflow

# COMMAND ----------

def main():
    """
    Main workflow function that orchestrates the DMDC data extraction process.
    """
    # Get current date and validate execution day
    current_date = date.today()
    if not is_valid_execution_day(current_date):
        dbutils.notebook.exit(
            f"Notebook exited because today is {current_date.strftime('%A')}.\n"
            f"This notebook runs only on Wednesday or Friday"
        )
    
    print(f"Today is {current_date.strftime('%A')}.\nProceed to generate the required output file.")
    
    # Initialize and load data
    create_checkpoint_table()
    load_source_data()
    
    # Get time range for data extraction
    last_run_date = get_last_run_date()
    today_start_time = get_start_time(current_date)
    
    # Build and combine queries
    cg_query = build_cg_query(last_run_date, today_start_time)
    scd_query = build_scd_query(last_run_date, today_start_time)
    combined_query = f"{cg_query}\nUNION ALL\n{scd_query}"
    
    # Process output
    write_to_patronage(combined_query, current_date, today_start_time)

# Execute main workflow
if __name__ == "__main__":
    main()

# COMMAND ----------


