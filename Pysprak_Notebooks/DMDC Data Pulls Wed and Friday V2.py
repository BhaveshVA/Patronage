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

# DBTITLE 1,Read data from the delta table to a dataframe
cg_data = spark.read.format("delta").load("/mnt/Patronage/Caregivers_Staging_New")
scd_data = spark.read.format("delta").load("/mnt/Patronage/SCD_Staging")

cg_data.createOrReplaceTempView('cg_data')
scd_data.createOrReplaceTempView('scd_data')


# COMMAND ----------

# DBTITLE 1,This function will calculate last run date based on today's day of the week
def get_last_run_date():
    if todays_date.weekday() == 2: # Wednesday
        last_run_date = todays_date - timedelta(days=5) # last Friday
        last_run_date_str = last_run_date.strftime('%Y-%m-%d')
        # print("Last Run Date:", last_run_date_str)
    elif todays_date.weekday() == 4: # Friday
        last_run_date = todays_date - timedelta(days=2) # last Wednesday
        last_run_date_str = last_run_date.strftime('%Y-%m-%d')
        # print("Last Run Date:", last_run_date_str)
    else:
        raise ValueError("Todays_date is neither Wednesday nor Friday")
    # print("Last Run Date:", last_run_date_str)
    return last_run_date_str

# COMMAND ----------

# temp_dt = get_last_run_date()
# temp_dt

# COMMAND ----------

# %sql
# create or replace temp view ttemp_view as 
# select rpad(nvl(EDIPI,""),10," " ) ||
#             rpad(nvl(Batch_CD,""), 3," ") ||  
#             rpad(nvl(SC_Combined_Disability_Percentage,""),3," ") ||
#             rpad(nvl(date_format(status_begin_date, 'yyyyMMdd'),""),8," ") ||
#             rpad(nvl(PT_Indicator,""),1," ") ||
#             rpad(nvl(Individual_Unemployability,""),1," ") ||  
#             rpad(nvl(date_format(Status_Last_Update, 'yyyyMMdd'),""),8," ") ||
#             rpad(nvl(date_format(Status_Termination_Date, 'yyyyMMdd'),""),8," ") as CG  
#         from cg_data
#         where SDP_Event_Created_Timestamp >= '2024-10-11' --'yyyy-mm-dd'
#         and Applicant_Type = 'Primary Caregiver'
#         and (Status_Termination_Date is NULL OR Status_Termination_Date >= curdate()  OR Status IN ("Approved", "Pending Revocation/Discharge"))
#         and EDIPI is not null
# union all
#         select rpad(nvl(edipi,""),10," " ) ||
#             rpad(nvl(Batch_CD,""), 3," ") ||
#             rpad(nvl(SC_Combined_Disability_Percentage,""),3," ") ||
#             rpad(nvl(Status_Begin_Date,""),8," ") ||
#             rpad(nvl(PT_Indicator,""),1," ") ||
#             rpad(nvl(Individual_Unemployability,""),1," ") ||
#             rpad(nvl(Status_Last_Update,""),8," ") ||
#             rpad(nvl(Status_Termination_Date,""),8," ") as CG
#         from scd_data
#         where SDP_Event_Created_Timestamp >= '2024-10-11'
#         and edipi is not null

# COMMAND ----------

# %sql
# select count(*), 'not_42' from ttemp_view
# where len(cg) = 42

# COMMAND ----------

# %sql
# select * from ttemp_view



# COMMAND ----------

# DBTITLE 1,This function will pull data based on the last run date
def get_data(last_run_date_str):
    cg_query = f"""
        select rpad(nvl(EDIPI,""),10," " ) ||
            rpad(nvl(Batch_CD,""), 3," ") ||  
            rpad(nvl(SC_Combined_Disability_Percentage,""),3," ") ||
            rpad(nvl(date_format(status_begin_date, 'yyyyMMdd'),""),8," ") ||
            rpad(nvl(PT_Indicator,""),1," ") ||
            rpad(nvl(Individual_Unemployability,""),1," ") ||  
            rpad(nvl(date_format(Status_Last_Update, 'yyyyMMdd'),""),8," ") ||
            rpad(nvl(date_format(Status_Termination_Date, 'yyyyMMdd'),""),8," ") as CG  
        from cg_data
        where SDP_Event_Created_Timestamp >= DATE('{last_run_date_str}') --'yyyy-mm-dd'
        and Applicant_Type = 'Primary Caregiver'
        and (Status_Termination_Date is NULL OR Status_Termination_Date >= curdate()  OR Status IN ("Approved", "Pending Revocation/Discharge"))
        and EDIPI is not null
        """
    scd_query = f"""
        select rpad(nvl(edipi,""),10," " ) ||
            rpad(nvl(Batch_CD,""), 3," ") ||
            rpad(nvl(SC_Combined_Disability_Percentage,""),3," ") ||
            rpad(nvl(Status_Begin_Date,""),8," ") ||
            rpad(nvl(PT_Indicator,""),1," ") ||
            rpad(nvl(Individual_Unemployability,""),1," ") ||
            rpad(nvl(Status_Last_Update,""),8," ") ||
            rpad(nvl(Status_Termination_Date,""),8," ") as CG
        from scd_data
        where SDP_Event_Created_Timestamp >= DATE('{last_run_date_str}')
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

    with open(outuput_path, "w", newline="\r\n") as f:            # DMDC requires output file as unix txt file
        f.write(pandas_df.to_string(index=False, header=False))
    print(f'Data has been written to: {outuput_path}')
    print(f'Number of records: {len(pandas_df)}')
    # display(pandas_df)
    print(pandas_df.to_string())

# COMMAND ----------

last_run_date_str = get_last_run_date()
print(f"Last export on: {datetime.strptime(last_run_date_str, '%Y-%m-%d').strftime('%A')}, {last_run_date_str}")
sql_query = get_data(last_run_date_str)
write_to_patronage(sql_query)

# COMMAND ----------

last_run_date_str = get_last_run_date()
a = get_data(last_run_date_str)
print(a)
