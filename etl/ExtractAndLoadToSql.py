# Databricks notebook source
dbutils.widgets.text('INPUT_FHIR_SCHEMA', 'devdays_fhir')
dbutils.widgets.text('OUTPUT_SQL_SCHEMA', 'devdays_sql')

# COMMAND ----------

INPUT_FHIR_SCHEMA=dbutils.widgets.get('INPUT_FHIR_SCHEMA')
OUTPUT_SQL_SCHEMA=dbutils.widgets.get('OUTPUT_SQL_SCHEMA')

print(f"""Extracting encounters
 from: `{INPUT_FHIR_SCHEMA}`
 to: `{OUTPUT_SQL_SCHEMA}`
 """)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SQL_SCHEMA}")

# COMMAND ----------

from pathling import PathlingContext
from pathling import Expression as fpx
from pyspark.sql.functions import col

pc = PathlingContext.create(spark)
fhir_ds = pc.read.tables(INPUT_FHIR_SCHEMA)

def encounter(path):
    return fpx("%s.%s" % ('reverseResolve(Encounter.subject)', path))

encounter_raw_ds = fhir_ds.extract('Patient', [
    fpx('id').alias('patient_id'), 
    fpx('gender').alias('gender'), 
    fpx("extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress.country").alias('birth_place_country'),
    fpx("extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress.state").alias('birth_place_state'),
    fpx("extension('http://synthetichealth.github.io/synthea/disability-adjusted-life-years').valueDecimal").alias('disability_adjusted_life_years'),
    fpx("extension('http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal").alias('quality_adjusted_life_years'),
    encounter('id').alias('encounter_id'),
    encounter("period.start").alias('encounter_start'),
    encounter("period.end").alias('encounter_end'),
    #exp("period.start.until(%resource.period.end, 'minutes')").alias('duration'),
    encounter('status').alias('encounter_status'),
    encounter('class').alias('encounter_class_code'),
    encounter('type.text').alias('encounter_type'),
    encounter("reasonCode.coding.where(system='http://snomed.info/sct').display.first()").alias('encounter_reason'),
])


encounter_ds = encounter_raw_ds \
    .withColumn('encounter_start', col('encounter_start').cast('TIMESTAMP')) \
    .withColumn('encounter_end', col('encounter_end').cast('TIMESTAMP'))


encounter_ds.printSchema()
encounter_ds.show(5)

encounter_ds.write.saveAsTable(f"{OUTPUT_SQL_SCHEMA}.encounter_view", mode='overwrite')
