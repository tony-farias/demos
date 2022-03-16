-- Databricks notebook source
-- MAGIC %md # Delta Live Tables - 2052a Example (SQL)
-- MAGIC 
-- MAGIC We can tackle data discovery and quality with our medallion architecture.
-- MAGIC 
-- MAGIC 
-- MAGIC ![test](/files/tmp/2052a/Screen_Shot_2022_01_25_at_07_54_51.png)

-- COMMAND ----------

-- DBTITLE 1,Ingest raw 2052a extracts
CREATE INCREMENTAL LIVE TABLE derivativesAndCollateralSupReport_raw
COMMENT "Raw JSON parse"
AS SELECT * FROM cloud_files("dbfs:/FileStore/tmp/2052a/", "json", map("cloudFiles.inferColumnTypes", "true"))
-- AS SELECT * FROM json.`dbfs:/FileStore/tmp/2052a/testXml2052a.json`

-- COMMAND ----------

CREATE  LIVE TABLE derivativesAndCollateralSupReport_raw2
COMMENT "Raw JSON parse"
--AS SELECT * FROM cloud_files("dbfs:/FileStore/tmp/2052a_v2/", "json")
AS SELECT * FROM json.`dbfs:/FileStore/tmp/2052a_v2/testXml2052a_2.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can write constraints to make sure our data conforms to a standard

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE LIVE TABLE derivativesAndCollateralSupReport_clean (
-- MAGIC )
-- MAGIC COMMENT "Valid data cleaned and prepared for analysis."
-- MAGIC AS
-- MAGIC -- transformation, flatten out rows for tables
-- MAGIC select * from live.derivativesAndCollateralSupReport_raw

-- COMMAND ----------

CREATE LIVE TABLE derivativesAndCollateralSupReport_clean (
  CONSTRAINT validMaturityStart EXPECT (
    validateEffMaturityBucketDateFormat(effectiveMaturityBucket_maturityEnd) IS NOT NULL
  ) ON VIOLATION DROP ROW,
  CONSTRAINT validMaturityEnd EXPECT (
    validateEffMaturityBucketDateFormat(effectiveMaturityBucket_maturityStart) IS NOT NULL
  ) ON VIOLATION DROP ROW
) COMMENT "Valid data cleaned and prepared for analysis." AS -- transformation, flatten out rows for tables


select
  live.derivativesAndCollateralSupReport_raw.*,
  /*effMaturityBucketDateFormat(maturityEnd)*/
  maturityEnd as effectiveMaturityBucket_maturityEnd,
  /*effMaturityBucketDateFormat(maturityStart)*/
  maturityStart as effectiveMaturityBucket_maturityStart,
  `#text` as marketValue_VALUE,
  _converted as marketValue_converted,
  _unit as marketValue_unit,
  _unitMultiplier as marketValue_unitMultiplier
from
  (
    select
      _reportID,
      effectiveMaturityBucket.*,
      marketValue.*
    from
      live.derivativesAndCollateralSupReport_raw
  ) as corrected
  join live.derivativesAndCollateralSupReport_raw on corrected._reportID = live.derivativesAndCollateralSupReport_raw._reportID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Woops! Looks like our date constraints are failing! Let's write a piece of Python code to change the date to what the schema expects. SQL show to do

-- COMMAND ----------

CREATE LIVE TABLE derivativesAndCollateralSupReport_gold (
  `_internal` STRING NOT NULL,
  `_nettingEligible` STRING NOT NULL,
  `_noNamespaceSchemaLocation` STRING,
  `_reportID` STRING NOT NULL,
  `_treasuryControl` STRING NOT NULL,
  `businessLine` STRING,
  `collateralClass` STRING,
  `collateralLevel` STRING,
  `comment` STRING,
  `counterpartySector` STRING,
  `derivativesAndCollateralSupProduct` STRING,
  `effectiveMaturityBucket` STRUCT<`maturityEnd`: STRING, `maturityStart`: STRING>,
  `encumbranceType` STRING,
  `executionAndClearingMechanism` STRING,
  `gsib` STRING,
  `internalCounterparty` STRING,
  `marketValue` STRUCT<`#text`: STRING, `_converted`: STRING, `_unit`: STRING, `_unitMultiplier`: STRING>,
  `reportScope` STRING,
  `segregationCategory` STRING,
  `effectiveMaturityBucket_maturityEnd` STRING,
  `effectiveMaturityBucket_maturityStart` STRING,
  `marketValue_VALUE` STRING,
  `marketValue_converted` STRING,
  `marketValue_unit` STRING,
  `marketValue_unitMultiplier` STRING,
  `_rescued_data` STRING
  ) AS
  SELECT * FROM live.derivativesAndCollateralSupReport_clean

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE LIVE TABLE derivativesAndCollateralSupReport_gold (
-- MAGIC   `reportScope` STRING NOT NULL,
-- MAGIC   `reportID` INT NOT NULL,
-- MAGIC   `comment` STRING,
-- MAGIC   `reporter_listingExchange` STRING NOT NULL,
-- MAGIC   `reportingDate` DATE NOT NULL,
-- MAGIC   `version` INT NOT NULL,
-- MAGIC   `reportingFrequency` STRING NOT NULL,
-- MAGIC   `assetProduct` STRING NOT NULL,
-- MAGIC   `assetSource` STRING,
-- MAGIC   `marketValue_converted` BOOLEAN NOT NULL,
-- MAGIC   `marketValue_unit` STRING NOT NULL,
-- MAGIC   `marketValue_unitMultiplier` INT NOT NULL,
-- MAGIC   `marketValue_valueBase` BIGINT NOT NULL,
-- MAGIC   `lendableValue_converted` BOOLEAN NOT NULL,
-- MAGIC   `lendableValue_unit` STRING NOT NULL,
-- MAGIC   `lendableValue_unitMultiplier` INT NOT NULL,
-- MAGIC   `lendableValue_valueBase` BIGINT NOT NULL,
-- MAGIC   `maturityBucket_maturityStart` STRING NOT NULL,
-- MAGIC   `maturityBucket_maturityEnd` STRING,
-- MAGIC   `forwardStartValue_converted` BOOLEAN NOT NULL,
-- MAGIC   `forwardStartValue_unit` STRING NOT NULL,
-- MAGIC   `forwardStartValue_unitMultiplier` INT NOT NULL,
-- MAGIC   `forwardStartValue_valueBase` BIGINT NOT NULL,
-- MAGIC   `forwardStartBucket_maturityStart` STRING NOT NULL,
-- MAGIC   `forwardStartBucket_maturityEnd` STRING,
-- MAGIC   `collateralClass` STRING NOT NULL,
-- MAGIC   `accountingDesignation` STRING NOT NULL,
-- MAGIC   `effectiveMaturityBucket_maturityStart` STRING NOT NULL,
-- MAGIC   `effectiveMaturityBucket_maturityEnd` STRING,
-- MAGIC   `encumbranceType` STRING,
-- MAGIC   `internalCounterparty` STRING,
-- MAGIC   `businessLine` STRING,
-- MAGIC   `treasuryControl` BOOLEAN NOT NULL
-- MAGIC ) AS
-- MAGIC SELECT * FROM live.derivativesAndCollateralSupReport_clean
