# Databricks notebook source
# MAGIC %python
# MAGIC event_log = spark.read.format('delta').load("/Users/antonio.farias@databricks.com/data/system/events")
# MAGIC event_log.createOrReplaceTempView("event_log_raw")

# COMMAND ----------

latest_update_id = spark.sql("SELECT origin.update_id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1").collect()[0].update_id
spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM event_log_raw  where event_type = 'flow_progress' AND origin.update_id = '${latest_update.id}'
# MAGIC --GROUP BY
# MAGIC  -- event_log_raw.dataset,
# MAGIC  -- event_log_raw.name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       event_log_raw
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress' AND
# MAGIC       origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name
