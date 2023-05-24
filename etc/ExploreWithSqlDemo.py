# Databricks notebook source
"""
Demonstrates how to use SQL to extract data from the FHIR resources in the form of delta lake tables, 
for the pupose of COVID-19 risk factor self-service analytics.

The query uses data across mutliple resources: Patient, Condition, Observation, 
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

# Initialise pathling context to register terminology UDFs and connect 
# the default terminology server

from pathling import PathlingContext
pc = PathlingContext.create(spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current schema to the FHIR delta lake.
# MAGIC USE devdays_fhir

# COMMAND ----------

# MAGIC %sql
# MAGIC --
# MAGIC -- Select basic patient information
# MAGIC -- Also include the `id_versioned` column that can be used for effciently resolve references 
# MAGIC -- to patients in other resources 
# MAGIC -- 
# MAGIC SELECT 
# MAGIC     id, gender, birthDate, 
# MAGIC     address[0].postalCode AS postalCode, address[0].country  AS country, 
# MAGIC     id_versioned 
# MAGIC FROM  patient 
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC --
# MAGIC -- Find conditions related to heart diseases.
# MAGIC -- Condition code is subsumed by SNOMED concept `56265001`.
# MAGIC -- 
# MAGIC SELECT id, subject.reference, code.text FROM condition
# MAGIC WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '56265001', NULL, NULL, NULL), TRUE)
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC --
# MAGIC -- Find conditions related to  chronic kidney disease.
# MAGIC -- Condition code is subsumed by SNOMED concept `709044004``.
# MAGIC -- 
# MAGIC SELECT id, subject.reference, code.text FROM condition
# MAGIC WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '709044004', NULL, NULL, NULL), TRUE)
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 
# MAGIC -- Find Body Mass Index observations
# MAGIC -- Observation code is subsubed by LONIC code `39156-5`
# MAGIC -- Note, that the value of the observation is a quantity usually expressed in `kg/m2`. 
# MAGIC -- `valueQuantity._value_canonicalized.value` can be used to obtaint the value always
# MAGIC -- expressed in UCUM canonical units, in our case `g/m2`.
# MAGIC -- 
# MAGIC SELECT 
# MAGIC     id, subject.reference, valueQuantity.value, valueQuantity.unit, 
# MAGIC     valueQuantity._value_canonicalized.value AS canonical_value, valueQuantity._code_canonicalized AS canonical_unit, 
# MAGIC     valueQuantity._value_canonicalized.value / 1000 as bmiValue
# MAGIC FROM observation 
# MAGIC WHERE subsumes(code.coding, struct(NULL, 'http://loinc.org', NULL, '39156-5', NULL, NULL, NULL), TRUE)
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC --
# MAGIC -- Find immunization with a COVIV-19 vaccine.
# MAGIC -- Immunization vaccineCode is member of `https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines`
# MAGIC -- value set.
# MAGIC -- 
# MAGIC SELECT id, patient.reference, vaccineCode.text 
# MAGIC FROM immunization 
# MAGIC WHERE member_of(vaccineCode.coding, 'https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines')
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC --  
# MAGIC -- Select the patient data and join with other resources 
# MAGIC -- to obtain risk factors and vaccination status.
# MAGIC -- 
# MAGIC
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
# MAGIC         FROM condition WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '56265001', NULL, NULL, NULL), TRUE)) 
# MAGIC     AS chc ON patient.id_versioned = chc.ref
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT DISTINCT subject.reference AS ref
# MAGIC         FROM condition WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '709044004', NULL, NULL, NULL), TRUE)) 
# MAGIC     AS ckd ON patient.id_versioned = ckd.ref
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT DISTINCT subject.reference AS ref
# MAGIC         FROM observation WHERE subsumes(code.coding, struct(NULL, 'http://loinc.org', NULL, '39156-5', NULL, NULL, NULL), TRUE) AND valueQuantity.value > 30) 
# MAGIC     AS bmi ON patient.id_versioned = bmi.ref
# MAGIC LEFT OUTER JOIN (
# MAGIC         SELECT DISTINCT patient.reference AS ref  FROM immunization WHERE member_of(vaccineCode.coding, 'https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines'))
# MAGIC     AS covid ON patient.id_versioned = covid.ref;
