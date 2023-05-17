# Databricks notebook source
dbutils.widgets.text('SOURCE_URL', 'dbfs:/tmp/fhir-export-01')
dbutils.widgets.text('DESTINATION_SCHEMA', 'devdays_fhir')

# COMMAND ----------

SOURCE_URL=dbutils.widgets.get('SOURCE_URL')
DESTINATION_SCHEMA=dbutils.widgets.get('DESTINATION_SCHEMA')

print(f"""Loading and merging data
 from: `{SOURCE_URL}`
 to schema: `{DESTINATION_SCHEMA}`
 """)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DESTINATION_SCHEMA}")

# COMMAND ----------

from pathling import PathlingContext
from pathling.datasink import ImportMode

pc = PathlingContext.create(spark, enable_extensions=True)
resources = pc.read.ndjson(SOURCE_URL)
resources.write.tables(DESTINATION_SCHEMA, ImportMode.MERGE)
