# Databricks notebook source
import os
from datetime import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Check today's Day
todays_date = date.today()
if todays_date.weekday() not in [2, 4]:
    # print('Exiting this notebook because today is not Wednesday or Friday')
    dbutils.notebook.exit(f"Notebook exited because today is {todays_date.strftime('%A')}.\nThis notebook runs only on Wednesday or Friday")
else:
    print(f"Today is {todays_date.strftime('%A')}.\nProceed to generate the required output file.")

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS dmdc_checkpoint;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dmdc_checkpoint (
# MAGIC   checkpoint_timestamp TIMESTAMP,
# MAGIC   filename STRING,
# MAGIC   record_count INTEGER,
# MAGIC   generate_query STRING
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Read data from the delta table to a dataframe
cg_data = spark.read.format("delta").load("/mnt/Patronage/Caregivers_Staging_New")
scd_data = spark.read.format("delta").load("/mnt/Patronage/SCD_Staging")

cg_data.createOrReplaceTempView('cg_data')
scd_data.createOrReplaceTempView('scd_data')


# COMMAND ----------

# DBTITLE 1,This function will pull data based on the last run date
def get_data():
    last_run_date = spark.sql(
        'SELECT COALESCE(DATE(max(checkpoint_timestamp)), current_date()) AS last_run_date FROM dmdc_checkpoint'
    ).first()[0]

    now = datetime.now()
    if todays_date.weekday() == 2:
        today_start_time = todays_date - timedelta(days=1)
    else:
        today_start_time = datetime(now.year, now.month, now.day)

    cg_query = f"""
        select rpad(coalesce(EDIPI,''),10,' ' ) ||
            rpad(coalesce(Batch_CD,''), 3,' ') ||
            rpad(coalesce(SC_Combined_Disability_Percentage,''),3,' ') ||
            rpad(coalesce(date_format(status_begin_date, 'yyyyMMdd'),''),8,' ') ||
            rpad(coalesce(PT_Indicator,''),1,' ') ||
            rpad(coalesce(Individual_Unemployability,''),1,' ') ||  
            rpad(coalesce(date_format(Status_Last_Update, 'yyyyMMdd'),''),8,' ') ||
            rpad(coalesce(date_format(Status_Termination_Date, 'yyyyMMdd'),''),8,' ') as CG
        from cg_data
        where SDP_Event_Created_Timestamp >= DATE('{last_run_date}')
        and SDP_Event_Created_Timestamp <= DATE('{today_start_time}')
        and EDIPI is not null
        and Applicant_Type = 'Primary Caregiver'
        and (Status_Termination_Date is NULL OR Status_Termination_Date >= curdate()
            OR Status IN ('Approved', 'Pending Revocation/Discharge'))
        and EDIPI is not null
        """
    scd_query = f"""
        select rpad(coalesce(edipi,''),10,' ' ) ||
            rpad(coalesce(Batch_CD,''), 3,' ') ||
            rpad(coalesce(SC_Combined_Disability_Percentage,''),3,' ') ||
            rpad(coalesce(Status_Begin_Date,''),8,' ') ||
            rpad(coalesce(PT_Indicator,''),1,' ') ||
            rpad(coalesce(Individual_Unemployability,''),1,' ') ||
            rpad(coalesce(Status_Last_Update,''),8,' ') ||
            rpad(coalesce(Status_Termination_Date,''),8,' ') as CG
        from scd_data
        where SDP_Event_Created_Timestamp >= DATE('{last_run_date}')
        and SDP_Event_Created_Timestamp <= DATE('{today_start_time}')
        and edipi is not null
        """
    combined_data = f"""
        {cg_query}
        union all
        {scd_query}
    """

    return combined_data

# COMMAND ----------

# DBTITLE 1,This function will write the pandas dataframe to a blob storage
def write_to_patronage(combined_data):

    date_format = todays_date.strftime("%Y%m%d")
    outuput_filename = f"PATRONAGE_{date_format}.txt"
    outuput_path = f"/dbfs/mnt/ci-patronage/dmdc_extracts/combined_export/{outuput_filename}"
    pandas_df = spark.sql(combined_data).toPandas()
    df_record_count = len(pandas_df)
    if df_record_count == 0:
        dbutils.notebook.exit(f"Notebook exited because there is no data to write")

    with open(outuput_path, "w", newline="\r\n") as f:            # DMDC requires output file as unix txt file
        f.write(pandas_df.to_string(index=False, header=False))
    # display(pandas_df)
    now = datetime.now()
    if todays_date.weekday() == 2:
        today_start_time = todays_date - timedelta(days=1)
    elif todays_date.weekday() == 4:
        today_start_time = datetime(now.year, now.month, now.day)

    spark.sql(f"""
              INSERT INTO dmdc_checkpoint 
              (checkpoint_timestamp, filename, record_count, generate_query)
              VALUES ('{today_start_time}','{outuput_path}', {df_record_count}, "{combined_data}")
              """
    )

    print(f'Data has been written to: {outuput_path}')
    print(f'Number of records: {df_record_count}')
    print(f"Current timestamp: {today_start_time}")
    print(f"Generated Query:\n {combined_data}")
    print(pandas_df.to_string())

# COMMAND ----------

query = get_data()
write_to_patronage(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dmdc_checkpoint ORDER BY 1 DESC LIMIT 5;

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
