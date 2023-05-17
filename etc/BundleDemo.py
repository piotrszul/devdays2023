# Databricks notebook source
# MAGIC %sql
# MAGIC -- DROP  SCHEMA IF EXISTS devdays_bundles CASCADE;
# MAGIC CREATE SCHEMA IF NOT EXISTS devdays_bundles;
# MAGIC USE devdays_bundles;

# COMMAND ----------

from pathling import PathlingContext
from pathling import Expression as fpe
from pathling.datasink import ImportMode
pc = PathlingContext.create(spark)

# COMMAND ----------

fhir_ds = pc.read.bundles('s3://pathling-demo/staging/devdays/bundles/', ['Patient', 'Condition', 'Observation'])
fhir_ds.write.tables(schema='devdays_bundles', import_mode = ImportMode.MERGE)

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, gender FROM patient;
