# Databricks notebook source
# MAGIC %run ./esg_config

# COMMAND ----------

import pandas as pd
from io import StringIO

esg_df = pd.read_csv(StringIO("""organisation|description|ticker|url|logo
Alliance Data Systems|Alliance Data Systems Corporation, together with its subsidiaries, provides data-driven and transaction-based marketing and customer loyalty solutions primarily in the United States and Canada|ADS|https://www.responsibilityreports.com/HostedData/ResponsibilityReportArchive/a/NYSE_ADS_2018.pdf|https://www.responsibilityreports.com/HostedData/CompanyLogos/NYSE_ADS.png
Nasdaq|Nasdaq Exchange|NDAQ|https://www.responsibilityreports.com/HostedData/ResponsibilityReports/PDF/NASDAQ_NDAQ_2019.pdf|https://www.responsibilityreports.com/HostedData/CompanyLogos/NASDAQ_NDAQ.png
London Stock Exchange|London Stock Exchange|LSE|https://www.responsibilityreports.com/HostedData/ResponsibilityReports/PDF/LSE_LSE_2020.pdf|https://www.responsibilityreports.com/HostedData/CompanyLogos/London%20Stock%20Exchange%20Group%20PLC_a.png
CBOE Global Markets|CBOE Exchange|CBOE|https://www.responsibilityreports.com/HostedData/ResponsibilityReportArchive/c/NASDAQ_CBOE_2019.pdf|https://www.responsibilityreports.com/HostedData/CompanyLogos/NASDAQ_CBOE.png
American Express|American Express Company provides charge and credit payment card products, and travel-related services worldwide.|AXP|https://www.responsibilityreports.com/HostedData/ResponsibilityReportArchive/a/NYSE_AXP_2018.pdf|https://www.responsibilityreports.com/HostedData/CompanyLogos/NYSE_AXP.png
Capital One|Capital One Financial Corporation operates as the bank holding company for the Capital One Bank (USA), National Association and Capital One, National Association, which provide various financial products and services in the United States, Canada, and the United Kingdom|COF|https://www.responsibilityreports.com/Click/1640|https://www.responsibilityreports.com/HostedData/CompanyLogos/untitledgildfsp.bmp
Discover Financial|Discover Financial Services operates as a credit card issuer and electronic payment services company primarily in the United States. The company offers Discover Card-branded credit cards to individuals and small businesses over the Discover Network|DFS|https://www.responsibilityreports.com/Click/2357|https://www.responsibilityreports.com/HostedData/CompanyLogos/NYSE_DFS.png
Equifax|Equifax Inc. collects, organizes, and manages various financial, demographic, employment, and marketing information solutions for businesses and consumers|EFX|https://www.responsibilityreports.com/Click/1346|https://www.responsibilityreports.com/HostedData/CompanyLogos/NYSE_EFX.png
PayPal|PayPal is a leading technology platform company that enables digital and mobile payments on behalf of consumers and merchants worldwide. They put their customers at the center of everything they do. They strive to increase our relevance for consumers, merchants, friends and family to access and move their money anywhere in the world, anytime, on any platform and through any device.|PYPL|https://www.responsibilityreports.com/HostedData/ResponsibilityReportArchive/p/NASDAQ_PYPL_2018.pdf|https://www.responsibilityreports.com/HostedData/CompanyLogos/NASDAQ_PYPL.png
Provident Financial|Provident Financial plc provides personal credit products to non-standard lending market in the United Kingdom and Ireland.|PFG|https://www.responsibilityreports.com/HostedData/ResponsibilityReportArchive/p/LSE_PFG_2017.pdf|https://www.responsibilityreports.com/HostedData/CompanyLogos/NFJHGHJ.PNG"""),sep='|')

# COMMAND ----------

import requests
from PyPDF2 import PdfFileReader
from pyspark.sql import functions as F
from io import BytesIO

def _extract_content(url):
  response = requests.get(url)
  open_pdf_file = BytesIO(response.content)
  pdf = PdfFileReader(open_pdf_file, strict=False)  
  text = [pdf.getPage(i).extractText() for i in range(0, pdf.getNumPages())]
  return "\n".join(text)

esg_df['organisation'] = esg_df['organisation'].apply(lambda x: x.lower())
esg_df['content']  = esg_df['url'].apply(_extract_content)

_ = (
  spark
    .createDataFrame(esg_df)
    .write
    .mode('overwrite')
    .format('delta')
    .save(getParam('csr_statements_path'))
)

# COMMAND ----------

_ = sql(
  'OPTIMIZE delta.`{}` ZORDER BY(ticker)'
    .format(getParam('csr_statements_path'))
)

# COMMAND ----------

display(spark.read.format('delta').load(getParam('csr_statements_path')))

# COMMAND ----------

orgs = spark.createDataFrame(esg_df[['ticker']])
gdelt = spark.read.table(getParam('gdelt_silver_table'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re

# only pre-filter articles tagged as united nation guiding principles
@F.udf('boolean')
def filterTheme(xs):
  return len([x for x in xs if x.startswith('UNGP')]) > 0

@F.udf('string')
def extract_title(xml):
  if xml:
    m = re.search('<PAGE_TITLE>(.+?)</PAGE_TITLE>', xml)
    if m:
      return str(m.group(1))
  return ''

window = Window.partitionBy('ticker').orderBy(F.desc('date'))

_ = (
  gdelt
    .join(orgs, 'ticker')
    .filter(filterTheme(F.col('themes')))
    .withColumn('title', extract_title('extrasXML'))
    .filter(F.length('title') > 0)
    .select(
      F.col('publishDate').alias('date'),
      F.col('ticker'),
      F.col('documentIdentifier').alias('url'),
      F.col('tone.tone'),
      F.col('sourceCommonName').alias('source'),
      F.col('title')
    )
    .distinct()
    .withColumn('rank', F.row_number().over(window))
    .filter(F.col('rank') < 100)
    .write
    .mode('overwrite')
    .format('delta')
    .save(getParam('gdelt_silver_path'))
)

# COMMAND ----------

_ = sql(
  'OPTIMIZE delta.`{}` ZORDER BY(ticker)'
    .format(getParam('gdelt_silver_path'))
)

# COMMAND ----------

display(
    spark
    .read
    .format('delta')
    .load(getParam('gdelt_silver_path'))
    .groupBy('ticker')
    .count()
)

# COMMAND ----------


