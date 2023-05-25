# Databricks notebook source
"""
Demonstrates how to use Pathling `extract()` operation to  to extract data from the FHIR 
resources in the form of delta lake tables, 
for the pupose of COVID-19 risk factor self-service analytics.

The operation uses data across mutliple resources: Patient, Condition, Observation, 
and  Immunization to produce a flat table with the following data:

  id: patient's id
  gender: patient's gender
  birthDate: patients' date of birth
  postalCode: patient's address postal code
  hasCHC: has patient been even disgnosed with a heart disease
  hasCKD: has patient been even diagnosed with a chronic kidney disease
  hasBMIOver30: has the patient even had BMI over 30
  isCovidVaccinated: has the patient been vaccianed with any of the COVID-19 vaccines
"""

# Initialise pathling context connected 
# the default terminology server

from pathling import PathlingContext
pc = PathlingContext.create(spark)

# Set the current schema to the FHIR delta lake.
spark.catalog.setCurrentDatabase('devdays_fhir')


# COMMAND ----------

# MAGIC %sql
# MAGIC -- 
# MAGIC -- The SQL based query
# MAGIC -- 
# MAGIC SELECT 
# MAGIC   patient.id, patient.gender, patient.birthDate, 
# MAGIC   patient.address[0].postalCode AS postalCode,
# MAGIC   NOT ISNULL(chc.ref) as hasCHC,
# MAGIC   NOT ISNULL(ckd.ref) as hasCKD,
# MAGIC   NOT ISNULL(bmi.ref) as hasBMIOver30,
# MAGIC   NOT ISNULL(covid.ref) as isCovidVaccinated
# MAGIC FROM  patient
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT DISTINCT subject.reference AS ref 
# MAGIC         FROM condition WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '56265001', NULL, NULL), TRUE)) 
# MAGIC     AS chc ON patient.id_versioned = chc.ref
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT DISTINCT subject.reference AS ref
# MAGIC         FROM condition WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '709044004', NULL, NULL), TRUE)) 
# MAGIC     AS ckd ON patient.id_versioned = ckd.ref
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT DISTINCT subject.reference AS ref
# MAGIC         FROM observation WHERE subsumes(code.coding, struct(NULL, 'http://loinc.org', NULL, '39156-5', NULL, NULL), TRUE) AND valueQuantity.value > 30) 
# MAGIC     AS bmi ON patient.id_versioned = bmi.ref
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT DISTINCT patient.reference AS ref  FROM immunization WHERE member_of(vaccineCode.coding, 'https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines'))
# MAGIC     AS covid ON patient.id_versioned = covid.ref
# MAGIC LIMIT 5;    

# COMMAND ----------

from pathling import Expression as exp

#
# Create a FHIR data source on the table form the current database.
# see: https://pathling.csiro.au/docs/python/pathling.html#pathling.datasource.DataSources.tables
#

fhir_ds = pc.read.tables()

#
# Apply the `extract()` operation to define the ouput view using fhirpath expressions 
# to defined the values of the output columns.
# 
# see: https://pathling.csiro.au/docs/python/pathling.html#pathling.datasource.DataSource.extract
#
# Uses:
#  - `Patient` as the main resource
#  - `reverseResolve()` to "join" data from other resources
#  - `subsumedBy()` and `memberOf()` terminology functions
#  -  `Quantity` literals and unit aware `Quantity` comparison 
# 
# For supported fhirpath subset see: https://pathling.csiro.au/docs/fhirpath 
#

covid19_view_df = fhir_ds.extract('Patient',
    columns= [
        exp("id"),
        exp("gender"),
        exp("birthDate"),
        exp("address.postalCode.first()").alias("postalCode"),
        exp("reverseResolve(Condition.subject).exists($this.code.subsumedBy(http://snomed.info/sct|709044004))").alias("hasCKD"),
        exp("reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|56265001))").alias("hasCHC"),
        exp("reverseResolve(Observation.subject).where(code.subsumedBy(http://loinc.org|39156-5)).exists(valueQuantity > 30 'kg/m2')").alias("hasBMIOver30"),
        exp("reverseResolve(Immunization.patient).vaccineCode.memberOf('https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines').anyTrue()").alias("isCovidVaccinated"),
    ],
    filters = [
        "address.country.first() = 'US'"
    ]
)

display(covid19_view_df)
