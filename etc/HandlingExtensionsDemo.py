# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Extensions on `Patient` resource:
# MAGIC
# MAGIC - string: http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName
# MAGIC - Address: http://hl7.org/fhir/StructureDefinition/patient-birthPlace
# MAGIC - decimal: http://synthetichealth.github.io/synthea/quality-adjusted-life-years 
# MAGIC - decimal: http://synthetichealth.github.io/synthea/disability-adjusted-life-years
# MAGIC
# MAGIC
# MAGIC Extensions on `address` element:
# MAGIC
# MAGIC - complex: http://hl7.org/fhir/StructureDefinition/geolocation
# MAGIC     - decimal: latitude
# MAGIC     - decimal: longitude
# MAGIC
# MAGIC
# MAGIC
# MAGIC Select the `quality_adjusted_life_years`, `address_latitude`, `address_longitude` for each Patient, using both SQL and `extract` operation.

# COMMAND ----------

from pathling import PathlingContext
from pathling import Expression as fpe
from pyspark.sql.functions import col
pc = PathlingContext.create(spark)
spark.catalog.setCurrentDatabase('devdays_fhir')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, 
# MAGIC   filter(_extension[_fid], e -> e.url='http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress[0].city as city_of_birth,
# MAGIC   filter(_extension[_fid], e -> e.url='http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress[0].country as country_of_birth,
# MAGIC   filter(_extension[_fid], e -> e.url='http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal[0] AS quality_adjusted_life_years, 
# MAGIC   address[0].city AS city, 
# MAGIC   address[0].state AS state, 
# MAGIC   address[0].country AS country, 
# MAGIC   filter(_extension[filter(_extension[address[0]._fid], e -> e.url='http://hl7.org/fhir/StructureDefinition/geolocation')[0]._fid], e -> e.url='latitude')[0].valueDecimal AS address_latitude, 
# MAGIC   filter(_extension[filter(_extension[address[0]._fid], e -> e.url='http://hl7.org/fhir/StructureDefinition/geolocation')[0]._fid], e -> e.url='longitude')[0].valueDecimal AS address_longitude 
# MAGIC FROM patient 
# MAGIC ORDER BY id;

# COMMAND ----------

ds = pc.read.tables()
result = ds.extract('Patient', [
    fpe("id"), 
    fpe("extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress.city.first()").alias("city_of_birth"),
    fpe("extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress.country.first()").alias("country_of_birth"),
    fpe("extension('http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal").alias("quality_adjusted_life_years"),
    fpe("address.city.first()", "city"),
    fpe("address.state.first()", "state"),
    fpe("address.country.first()", "country"),
    fpe("address.extension('http://hl7.org/fhir/StructureDefinition/geolocation').extension('latitude').valueDecimal.first()").alias("address_latitude"),
    fpe("address.extension('http://hl7.org/fhir/StructureDefinition/geolocation').extension('longitude').valueDecimal.first()").alias("address_longitude"),
])
display(result.orderBy('id'))
