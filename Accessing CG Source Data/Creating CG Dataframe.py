# Databricks notebook source
ContainerPath = 'ci-mvi@vac20dlsvdweastprod01.dfs.core.usgovcloudapi.net'


# COMMAND ----------

mostRecentRequestPath = None
for entity in dbutils.fs.ls('/mnt/ci-vadir-shared/'):
 print(entity.name) 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

display(dbutils.fs.ls('/mnt/'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/ci-carma/landing/'))

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import *

# COMMAND ----------

filePath = '/dbfs/mnt/ci-carma/initialSeed/Caregivers_23August2023.xlsx'
df = pd.read_excel(filePath,engine='openpyxl')

# COMMAND ----------

def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return DoubleType()
    elif f == 'float32': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

cg_df = pandas_to_spark(df)

# COMMAND ----------

display(cg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Person ICN
# MAGIC Applicant Type
# MAGIC Caregiver Status
# MAGIC Dispositioned Date
# MAGIC Benefits End Date
# MAGIC CARMA Case Details: Veteran ICN

# COMMAND ----------


