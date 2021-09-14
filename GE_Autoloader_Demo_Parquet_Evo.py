# Databricks notebook source
# DBTITLE 1,Notebook Setup
# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.text("user", "ryan.kennedy@databricks.com")
# MAGIC user = dbutils.widgets.get("user")
# MAGIC 
# MAGIC streamSourcePath = f'/home/{user}/airbnb_small/streaming/parquet/'
# MAGIC print(streamSourcePath)
# MAGIC 
# MAGIC #One time set up for source table
# MAGIC df = spark.read.format("csv").option("header", True).option("inferSchema", True).load('/FileStore/tables/airbnb_clean.csv').repartition(5)
# MAGIC df.write.mode("overwrite").parquet(streamSourcePath)
# MAGIC 
# MAGIC 
# MAGIC dbutils.fs.ls(streamSourcePath)

# COMMAND ----------

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load('/FileStore/tables/airbnb_clean.csv').repartition(5)
display(df)

# COMMAND ----------

# DBTITLE 1,Set Variables and Initial Delta Table Creation
abnbDestPath = f'/tmp/{user}/delta/abnb_parquet/'
abnbDestTable = "abnb_autoload_parquet" #this is a Delta table

##One time Delta creation statement
#df=df.filter(df["latitude"] == "37.7743072106281").filter(df["longitude"] == "-122.42169305591644")
#display(df)
#df.write.mode("overwrite").format("delta").save(abnbDestPath)
#spark.sql("create table if not exists " + abnbDestTable + " using delta location '" + abnbDestPath + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM abnb_autoload_parquet

# COMMAND ----------

# DBTITLE 1,Step 1: Get the schema from the directory 
#note: We need this 'mergeSchema' option in order to get NEW columns which have been added to new files in the path
tableSchema = spark.read.format("parquet").option("mergeSchema","true").load(streamSourcePath).schema
print(tableSchema)

# COMMAND ----------

# DBTITLE 1,Step 2: Read the stream using the schema I just got
#we don't normally need maxFilesPerTrigger - this is just for demoing
events = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
  .option("cloudFiles.maxFilesPerTrigger", 1) \
  .option("optionanme", "triggerOnce") \
  .schema(tableSchema) \
  .load(streamSourcePath)

# COMMAND ----------

#events is a dataframe with binary data - how to parse binary data in a dataframe

#alternative - have a separate process that generates creates JSON, then drop JSON into bucket for Databricks autoloader to pick up. 



# COMMAND ----------

# DBTITLE 1,Step 3: Declare merge function - this remains unchanged.  NOTE: When we use the syntax  'WHEN MATCHED THEN UPDATE SET *', that tells Spark we want to do schema evolution
# MAGIC %python
# MAGIC from pyspark import Row
# MAGIC   
# MAGIC # Function to upsert `microBatchOutputDF` into Delta table using MERGE
# MAGIC def upsertToDelta(microBatchOutputDF, batchId): 
# MAGIC   # Set the dataframe to view name
# MAGIC   
# MAGIC   #add additional logic
# MAGIC   #microBatchOutputDF = microBatchOutputDF.filter('remove duplicates')
# MAGIC   
# MAGIC   #
# MAGIC   microBatchOutputDF.createOrReplaceTempView("updates")
# MAGIC 
# MAGIC   # ==============================
# MAGIC   # Supported in DBR 5.5 and above
# MAGIC   # ==============================
# MAGIC 
# MAGIC   # Use the view name to apply MERGE
# MAGIC   # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
# MAGIC   microBatchOutputDF._jdf.sparkSession().sql("""
# MAGIC     MERGE INTO abnb_autoload_parquet t
# MAGIC     USING updates s
# MAGIC     ON s.latitude = t.latitude AND s.longitude = t.longitude
# MAGIC     WHEN MATCHED THEN UPDATE SET *
# MAGIC     WHEN NOT MATCHED THEN INSERT *
# MAGIC   """)

# COMMAND ----------

# DBTITLE 1,Step 4: If we do not set this configuration, our target Delta table will not change
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True) 

# COMMAND ----------

# DBTITLE 1,Step 5: Write the stream out.  Only change here is to include the mergeSchema option
streamCheckpointPath = abnbDestPath + '_checkpoint' + '01'

events.writeStream \
  .format('delta') \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .option('checkpointLocation', streamCheckpointPath) \
  .option('mergeSchema', "true") \
  .start() 

# COMMAND ----------

# DBTITLE 1,Step 6: Prosper.  Our target table now has the new column!
# MAGIC %sql
# MAGIC SELECT * FROM abnb_autoload_parquet

# COMMAND ----------

# DBTITLE 1,This was the old table
# MAGIC %sql
# MAGIC SELECT * FROM abnb_autoload_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ##The rest of this notebook is the code to simulate new files coming in with a different schema

# COMMAND ----------

df_new = spark.read.format("parquet").load(streamSourcePath)



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC truncate table abnb_autoload_parquet

# COMMAND ----------

from pyspark.sql.functions import lit

df_new = df_new.withColumn("new_column", lit(50))

# COMMAND ----------

display(df_new)

# COMMAND ----------

df_new.write.mode("append").parquet(streamSourcePath)

# COMMAND ----------

df = spark.read.format("parquet").option("mergeSchema","true").load(streamSourcePath)

# COMMAND ----------


