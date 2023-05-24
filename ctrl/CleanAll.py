# Databricks notebook source
# MAGIC %sql
# MAGIC -- DROP all schemas
# MAGIC DROP SCHEMA IF EXISTS devdays_fhir CASCADE;
# MAGIC DROP SCHEMA IF EXISTS devdays_sql CASCADE;
# MAGIC DROP SCHEMA IF EXISTS devdays_bundles CASCADE;

# COMMAND ----------

dbutils.fs.rm('/tmp/DevDays', True)
