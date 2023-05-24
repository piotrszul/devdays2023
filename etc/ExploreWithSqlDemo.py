# Databricks notebook source
from pathling import PathlingContext
from pathling import Expression as fpe
from pyspark.sql.functions import col

pc = PathlingContext.create(spark)

# COMMAND ----------

INPUT_FHIR_SCHEMA='devdays_fhir'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE devdays_fhir

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, gender, birthDate, 
# MAGIC name.family[0] as familyName, name.given[0][0] as givenName, 
# MAGIC filter(telecom, t -> t.system='phone').value[0] telephone, id_versioned from patient LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, subject.reference, valueQuantity, issued FROM observation where code.text =  'Body Mass Index'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, subject, code FROM condition
# MAGIC WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '56265001', NULL, NULL, NULL), TRUE);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ptn.id, ptn.gender, birthDate, 
# MAGIC   name.family[0] as familyName, name.given[0][0] as givenName, 
# MAGIC   filter(telecom, t -> t.system='phone').value[0] telephone,
# MAGIC   obs.valueQuantity.value, obs.valueQuantity.unit, obs.issued
# MAGIC FROM  devdays_fhir.patient as ptn LEFT OUTER JOIN devdays_fhir.observation as obs ON ptn.id_versioned = obs.subject.reference 
# MAGIC WHERE obs.code.text =  'Body Mass Index'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW covid_factor AS
# MAGIC SELECT ptn.id, ptn.gender, birthDate, 
# MAGIC   name.family[0] as familyName, name.given[0][0] as givenName, 
# MAGIC   filter(telecom, t -> t.system='phone').value[0] telephone,
# MAGIC   NOT ISNULL(chc.ref) as hasCHC,
# MAGIC   NOT ISNULL(ckd.ref) as hasCKD,
# MAGIC   NOT ISNULL(bmi.ref) as hasBMIOver30,
# MAGIC   NOT ISNULL(covid.ref) as isCovidVaccinated,
# MAGIC   filter(_extension[_fid], e -> e.url='http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal[0] > 60 as isAdjustedAgeOver60
# MAGIC FROM  devdays_fhir.patient as ptn 
# MAGIC LEFT OUTER JOIN (SELECT DISTINCT subject.reference AS ref FROM condition WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '56265001', NULL, NULL, NULL), TRUE)) as chc ON ptn.id_versioned = chc.ref
# MAGIC LEFT OUTER JOIN (SELECT DISTINCT subject.reference AS ref  FROM condition WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '709044004', NULL, NULL, NULL), TRUE)) as ckd ON ptn.id_versioned = ckd.ref
# MAGIC LEFT OUTER JOIN (SELECT DISTINCT subject.reference AS ref  FROM observation WHERE subsumes(code.coding, struct(NULL, 'http://loinc.org', NULL, '39156-5', NULL, NULL, NULL), TRUE) AND valueQuantity.value > 30) as bmi ON ptn.id_versioned = bmi.ref
# MAGIC LEFT OUTER JOIN (SELECT DISTINCT patient.reference AS ref  FROM immunization WHERE member_of(vaccineCode.coding, 'https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines')) as covid ON ptn.id_versioned = covid.ref;
# MAGIC
# MAGIC SELECT * FROM covid_factor;
