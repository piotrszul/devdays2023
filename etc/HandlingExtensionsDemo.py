# Databricks notebook source
from pathling import PathlingContext
from pathling import Expression as fpe
from pyspark.sql.functions import col

pc = PathlingContext.create(spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE devdays_fhir;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, explode_outer(_extension[_fid]) FROM patient;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, filter(_extension[_fid], e -> e.url='http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal[0] FROM patient;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ptn.id, ptn.gender, birthDate, 
# MAGIC   name.family[0] as familyName, name.given[0][0] as givenName, 
# MAGIC   filter(_extension[_fid], e -> e.url='http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal[0] > 60 as isAdjustedAgeOver60
# MAGIC FROM  devdays_fhir.patient as ptn; 

# COMMAND ----------

#TODO: add the extract example
