# Databricks notebook source
# MAGIC %sql
# MAGIC DROP  SCHEMA IF EXISTS devdays_bundles CASCADE;
# MAGIC CREATE SCHEMA IF NOT EXISTS devdays_bundles;
# MAGIC USE devdays_bundles;

# COMMAND ----------

from pyspark.sql.functions import *
from pathling import PathlingContext
from pathling import Expression as fpe
from pathling.datasink import ImportMode
pc = PathlingContext.create(spark)

# COMMAND ----------

bundles_stream = spark.readStream.text('s3://pathling-demo/staging/devdays/bundles/', wholetext=True)
patient_stream  = pc.encode_bundle(bundles_stream, 'Patient')
condition_stream  = pc.encode_bundle(bundles_stream, 'Condition')

# COMMAND ----------

display(patient_stream.groupBy(col('gender')).agg(count("*")))

# COMMAND ----------

condition_stream.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/FileStore/checkpoints/Conditions") \
  .toTable("Conditions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM conditions;
