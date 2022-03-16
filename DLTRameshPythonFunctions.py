# Databricks notebook source
from datetime import date
def convertToEffMaturityBucketDateFormat(i: str) -> str:
  """
    :param i: column from Pyspark or SQL
    :return: correct date format for effMaturityBucketDateFormat.
  """
  return str(date.today())

spark.udf.register("effMaturityBucketDateFormat", convertToEffMaturityBucketDateFormat) # register the square udf for Spark SQL

# COMMAND ----------

import datetime
def validateEffMaturityBucketDateFormat(date_text: str) -> str:
    try:
        validDate = datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return validDate
    except ValueError:
        return None
        
spark.udf.register("validateEffMaturityBucketDateFormat", validateEffMaturityBucketDateFormat) # register the square udf for Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC silver = spark.sql("select effectiveMaturityBucket.* from reports.derivativesAndCollateralSupReport_raw")
# MAGIC #spark.table('reports.derivativesAndCollateralSupReport_raw').sql("select effectiveMaturityBucket.* from reports.derivativesAndCollateralSupReport_raw")
# MAGIC display(silver)

# COMMAND ----------

# MAGIC %md
# MAGIC SHOW CREATE TABLE reports.derivativesAndCollateralSupReport_clean
