# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/DeltaLake-logo.png" width="80px"/>
# MAGIC 
# MAGIC # Unifying Structured Streaming with Batch Jobs with Delta Lake
# MAGIC 
# MAGIC In this notebook, we will explore combining streaming and batch processing with a single pipeline. We will begin by defining the following logic:
# MAGIC 
# MAGIC - ingest streaming JSON data from disk and write it to a Delta Lake Table `/activity/Bronze`
# MAGIC - perform a Stream-Static Join on the streamed data to add additional geographic data
# MAGIC - transform and load the data, saving it out to our Delta Lake Table `/activity/Silver`
# MAGIC - summarize the data through aggregation into the Delta Lake Table `/activity/Gold/groupedCounts`
# MAGIC - materialize views of our gold table through streaming plots and static queries
# MAGIC 
# MAGIC We will then demonstrate that by writing batches of data back to our bronze table, we can trigger the same logic on newly loaded data and propagate our changes automatically.

# COMMAND ----------

#Athena - AWS' Presto
#EMR - just use Spark to write Parquet files
#Checkout prformance comparisons
#Hudi - look at performance - look at who uses in production
#TODO: Research EMR (Priyal)

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Set up relevant Delta Lake paths
# MAGIC 
# MAGIC These paths will serve as the file locations for our Delta Lake tables.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Each streaming write has its own checkpoint directory.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> You cannot write out new Delta files within a repository that contains Delta files. Note that our hierarchy here isolates each Delta table into its own directory.

# COMMAND ----------

activityPath = userhome + "/activity"

activityBronzePath = activityPath + "/Bronze"
activityBronzeCheckpoint = activityBronzePath + "/checkpoint"

activitySilverPath = activityPath + "/Silver"
activitySilverCheckpoint = activitySilverPath + "/checkpoint"

activityGoldPath = activityPath + "/Gold"
groupedCountPath = activityGoldPath + "/groupedCount"
groupedCountCheckpoint = groupedCountPath + "/checkpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Pipeline
# MAGIC 
# MAGIC To reset the pipeline, run the following:

# COMMAND ----------

dbutils.fs.rm(activityPath, True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Datasets Used
# MAGIC This notebook will consume cell phone accelerometer data. Records have been downsampled so that the streaming data represents less than 3% of the total data being produced. The remainder will be processed as batches.
# MAGIC 
# MAGIC The following fields are present:
# MAGIC 
# MAGIC - `Index`
# MAGIC - `Arrival_Time`
# MAGIC - `Creation_Time`
# MAGIC - `x`
# MAGIC - `y`
# MAGIC - `z`
# MAGIC - `User`
# MAGIC - `Model`
# MAGIC - `Device`
# MAGIC - `gt`
# MAGIC - `geolocation`
# MAGIC 
# MAGIC ## Define Schema
# MAGIC 
# MAGIC For streaming jobs, we need to define our schema before we start.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll reuse this same schema later in the notebook to define our batch processing, which will eliminate the jobs triggered by eliminating a file scan AND enforce the schema that we've defined here.

# COMMAND ----------

#TODO: see if can infer schema on json

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, LongType, StringType, DoubleType

schema = StructType([
  StructField("Arrival_Time",LongType()),
  StructField("Creation_Time",LongType()),
  StructField("Device",StringType()),
  StructField("Index",LongType()),
  StructField("Model",StringType()),
  StructField("User",StringType()),
  StructField("geolocation",StructType([
    StructField("city",StringType()),
    StructField("country",StringType())
  ])),
  StructField("gt",StringType()),
  StructField("id",LongType()),
  StructField("x",DoubleType()),
  StructField("y",DoubleType()),
  StructField("z",DoubleType())
])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Define Streaming Load from Files in Blob
# MAGIC 
# MAGIC Our streaming source directory has 36 JSON files of 5k records each saved in a repository. Here, we'll trigger processing on files one at a time. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In a production setting, this same logic would allow us to only read new files written to our source directory. We could define `maxFilesPerTrigger` to control the amount of data we consume with each load, or omit this option to consume all new data on disk since the last time the stream has processed.

# COMMAND ----------

rawEventsDF = (spark
  .readStream
  .format("json")
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .load("/mnt/training/definitive-guide/data/activity-json/streaming"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### WRITE Stream using Delta Lake

# COMMAND ----------

(rawEventsDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", activityBronzeCheckpoint)
  .outputMode("append")
  .start(activityBronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Static Lookup Table
# MAGIC 
# MAGIC Before enriching our bronze data, we will load a static lookup table for our country codes.
# MAGIC 
# MAGIC Here, we'll use a parquet file that contains countries and their associated codes and abbreviations.
# MAGIC 
# MAGIC While we can load this as a table (which will copy all files to the workspace and make it available to all users), here we'll manipulate it as a DataFrame.

# COMMAND ----------

from pyspark.sql.functions import col

geoForLookupDF = (spark
  .read
  .format("parquet")
  .load("/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet/")
  .select(col("EnglishShortName").alias("country"), col("alpha3Code").alias("countryCode3")))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Create QUERY tables (aka "silver tables")
# MAGIC 
# MAGIC Our current bronze table contains nested fields, as well as time data that has been encoded in non-standard unix time (`Arrival_Time` is encoded as milliseconds from epoch, while `Creation_Time` records nanoseconds between record creation and receipt). 
# MAGIC 
# MAGIC We also wish to enrich our data with 3 letter country codes for mapping purposes, which we'll obtain from a join with our `geoForLookupDF`.
# MAGIC 
# MAGIC In order to parse the data in human-readable form, we create query/silver tables out of the raw data.
# MAGIC 
# MAGIC We will stream from our previous file write, define transformations, and rewrite our data to disk.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we do not need to specify a schema when loading Delta files: it is inferred from the metadata!
# MAGIC 
# MAGIC The fields of a complex object can be referenced with a "dot" notation as in:
# MAGIC 
# MAGIC `col("geolocation.country")`
# MAGIC 
# MAGIC 
# MAGIC A large number of these fields/columns can become unwieldy.
# MAGIC 
# MAGIC For that reason, it is common to extract the sub-fields and represent them as first-level columns as seen below:

# COMMAND ----------

# MAGIC %md NOTE: You will not be able to run this command until the `rawEventsDF` has initialized.

# COMMAND ----------

import time
time.sleep(30)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

parsedEventsDF = (spark.readStream
  .format("delta")
  .load(activityBronzePath)
  .select(from_unixtime(col("Arrival_Time")/1000).alias("Arrival_Time").cast("timestamp"),
          (col("Creation_Time")/1E9).alias("Creation_Time").cast("timestamp"),
          col("Device"),
          col("Index"),
          col("Model"),
          col("User"),
          col("gt"),
          col("x"),
          col("y"),
          col("z"),
          col("geolocation.country").alias("country"),
          col("geolocation.city").alias("city"))
  .join(geoForLookupDF, ["country"], "left"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to QUERY Tables (aka "silver tables")

# COMMAND ----------

#TODO: consider hiding cell or doing another notebook

# COMMAND ----------

(parsedEventsDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", activitySilverCheckpoint)
  .outputMode("append")
  .start(activitySilverPath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC See contents of Silver directory.

# COMMAND ----------

dbutils.fs.ls(activitySilverPath)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### See list of active streams.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You should currently see two active streams, one for each streaming write that you've triggered. If you have called `display` on either of your streaming DataFrames, you will see an additional stream, as `display` writes the stream to memory.

# COMMAND ----------

for s in spark.streams.active:
  print(s.id)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Gold Table: Grouped Count of Events
# MAGIC 
# MAGIC Here we read a stream of data from `activitySilverPath` and write another stream to `activityGoldPath/groupedCount`.
# MAGIC 
# MAGIC The data consists of a total counts of all event, grouped by `hour`, `gt`, and `countryCode3`.
# MAGIC 
# MAGIC Performing this aggregation allows us to reduce the total number of rows in our table from hundreds of thousands (or millions, once we've loaded our batch data) to dozens.
# MAGIC 
# MAGIC In cell cmd25 this can be seen as a materialized view of the streaming data.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice that we're writing to a named directory within our gold path. If we wish to define additional aggregations, we would organize these parallel to thie directory to avoid metadata write conflicts.

# COMMAND ----------

import time
time.sleep(30)

# COMMAND ----------

from pyspark.sql.functions import window, hour

(spark.readStream
  .format("delta")
  .load(activitySilverPath)
  .groupBy(window("Arrival_Time", "60 minute"),"gt", "countryCode3")
  .count()
  .withColumn("hour",hour(col("window.start")))
  .drop("window")
  .writeStream
  .format("delta")
  .option("checkpointLocation", groupedCountCheckpoint)
  .outputMode("complete")
  .start(groupedCountPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE A Table Using Delta Lake
# MAGIC 
# MAGIC Create a table called `groupedCountPath` using `DELTA` out of the above data.
# MAGIC 
# MAGIC NOTE: You will not be able to run this command until the `groupedCountPath` has initialized.

# COMMAND ----------

import time
time.sleep(30)

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS grouped_count
""")
spark.sql("""
  CREATE TABLE grouped_count
  USING DELTA
  LOCATION '{}'
""".format(groupedCountPath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Important Considerations for `complete` Output with Delta
# MAGIC 
# MAGIC When using `complete` output mode, we rewrite the entire state of our table each time our logic runs. While this is ideal for calculating aggregates, we **cannot** read a stream from this directory, as Structured Streaming assumes data is only being appended in the upstream logic.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Certain options can be set to change this behavior, but have other limitations attached. For more details, refer to [Delta Streaming: Ignoring Updates and Deletes](https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes).
# MAGIC 
# MAGIC The gold Delta table we have just registered will perform a static read of the current state of the data each time we run the following query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM grouped_count

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Materialized View: Windowed Count of Hourly `gt` Events
# MAGIC 
# MAGIC Plot the occurrence of all events grouped by `gt`.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Because we're using `complete` output mode for our gold table write, we cannot define a streaming plot on these files.
# MAGIC 
# MAGIC Instead, we'll define a temp table based on the files written to our silver table as shown in the cell cmd cmd 31. We will them use this table to execute our streaming queries.
# MAGIC 
# MAGIC In order to create a LIVE bar chart of the data, you'll need to fill out the <b>Plot Options</b> as shown in cell cmd32 by clicking on the chart icon:
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/ch5-plot-options.png"/></div><br/>
# MAGIC 
# MAGIC ### Note on Gold Tables & Materialized Views
# MAGIC 
# MAGIC When we call `display` on a streaming DataFrame or execute a SQL query on a streaming view, we are using memory as our sink. 
# MAGIC 
# MAGIC In this case, we are executing a SQL query on a streaming view. We have already calculated all the values necessary to materialize our streaming view above in the gold table we've written to disk. 
# MAGIC 
# MAGIC **However**, we re-execute this logic on our silver table to generate streaming views, as structured streaming will not support reads from upstream files that have beem overwritten.

# COMMAND ----------

(spark.readStream
  .format("delta")
  .load(activitySilverPath)
  .createOrReplaceTempView("query_table")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT gt, HOUR(Arrival_Time) hour, COUNT(*) total_events
# MAGIC FROM query_table
# MAGIC GROUP BY gt, HOUR(Arrival_Time)
# MAGIC ORDER BY hour

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Load Data into Bronze Table - And It Flows Through The SAME Streaming LOGIC
# MAGIC 
# MAGIC We can use the same pipeline to process batch data.
# MAGIC 
# MAGIC By loading our raw data into our bronze table, we will push it through our already running streaming logic.
# MAGIC 
# MAGIC Here, we'll run 4 batches of around 170k records. We can track each batch through our streaming plots above.

# COMMAND ----------

for batch in range(4):
  (spark
    .read
    .format("json")
    .schema(schema)
    .load("/mnt/training/definitive-guide/data/activity-json/batch-{}".format(batch))
    .write
    .format("delta")
    .mode("append")
    .save(activityBronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that even on our small cluster, we can pass a batch of over 5 million records through our logic above without problems. 

# COMMAND ----------

(spark
  .read
  .format("json")
  .schema(schema)
  .load("/mnt/training/definitive-guide/data/activity-json/batch")
  .write
  .format("delta")
  .mode("append")
  .save(activityBronzePath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> While our streaming materialized view above updates as data flows in, we can also easily generate this view from our `grouped_count` table. 
# MAGIC 
# MAGIC We will need to re-run this query each time we wish to update the data. Run the below query now, and then after your batch has finished processing.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The state reflected in a query on a registered Delta table will always reflect the most recent valid state of the files.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM grouped_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Finally, make sure all streams are stopped.

# COMMAND ----------

#check for spark.streams.pending

# COMMAND ----------

#for s in spark.streams.active:
    #s.stop()
