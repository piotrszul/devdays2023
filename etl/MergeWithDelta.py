# Databricks notebook source
"""
Merge resource data form ndjson files into a delta lake schema.
Encodes the ndjson files as SparkSQL datasets and merges them with the delta tables.

:param SOURCE_URL: the URL to the directory with ndjson encoded resouce files
:param DESTINATION_SCHEMA: the name of the data lake schema to merge the resource data into
"""

dbutils.widgets.text('SOURCE_URL', 'dbfs:/tmp/DevDays/demo-etl')
dbutils.widgets.text('DESTINATION_SCHEMA', 'devdays_fhir')

SOURCE_URL=dbutils.widgets.get('SOURCE_URL')
DESTINATION_SCHEMA=dbutils.widgets.get('DESTINATION_SCHEMA')

print(f"""Loading and merging data:
 from: `{SOURCE_URL}`
 to schema: `{DESTINATION_SCHEMA}`
 """)

# COMMAND ----------

from pathling import PathlingContext
from pathling.datasink import ImportMode

# Initialize Pathling context
pc = PathlingContext.create(spark)

# Load resources data from njdson files. Resource types are infered from file names 
# e.g.: 'Observation.0003.ndjson' -> Observation.
# Creates a `DataSource` instance, 
# see: https://pathling.csiro.au/docs/python/pathling.html#pathling.datasource.DataSource
resources = pc.read.ndjson(SOURCE_URL)

# Create the destination schema and merge the new data into it
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DESTINATION_SCHEMA}")
resources.write.tables(DESTINATION_SCHEMA, ImportMode.MERGE)

#DEBUG: Dislay created/existing tables in the destination schema
print(f"Tables in schema {DESTINATION_SCHEMA}:")
for table in spark.catalog.listTables(DESTINATION_SCHEMA):
    print(table.name)
