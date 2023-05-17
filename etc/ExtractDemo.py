# Databricks notebook source
from pathling import PathlingContext
spark.sql("USE devdays_fhir")
pc = PathlingContext.create(spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ptn.id, ptn.gender, birthDate, 
# MAGIC   name.family[0] as familyName, name.given[0][0] as givenName, 
# MAGIC   filter(telecom, t -> t.system='phone').value[0] telephone,
# MAGIC   filter(_extension[_fid], e -> e.url='http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal[0] > 60 as isAdjustedAgeOver60,
# MAGIC   NOT ISNULL(chc.ref) as hasCHC,
# MAGIC   NOT ISNULL(ckd.ref) as hasCKD,
# MAGIC   NOT ISNULL(bmi.ref) as hasBMIOver30,
# MAGIC   NOT ISNULL(covid.ref) as isCovidVaccinated
# MAGIC FROM  devdays_fhir.patient as ptn 
# MAGIC LEFT OUTER JOIN (SELECT DISTINCT subject.reference AS ref FROM condition WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '56265001', NULL, NULL, NULL), TRUE)) as chc ON ptn.id_versioned = chc.ref
# MAGIC LEFT OUTER JOIN (SELECT DISTINCT subject.reference AS ref  FROM condition WHERE subsumes(code.coding, struct(NULL, 'http://snomed.info/sct', NULL, '709044004', NULL, NULL, NULL), TRUE)) as ckd ON ptn.id_versioned = ckd.ref
# MAGIC LEFT OUTER JOIN (SELECT DISTINCT subject.reference AS ref  FROM observation WHERE subsumes(code.coding, struct(NULL, 'http://loinc.org', NULL, '39156-5', NULL, NULL, NULL), TRUE) AND valueQuantity.value > 30) as bmi ON ptn.id_versioned = bmi.ref
# MAGIC LEFT OUTER JOIN (SELECT DISTINCT patient.reference AS ref  FROM immunization WHERE member_of(vaccineCode.coding, 'https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines')) as covid ON ptn.id_versioned = covid.ref;

# COMMAND ----------

from pathling import Expression as fpe
fhir_ds = pc.read.tables()
view_df = fhir_ds.extract('Patient',
[
    fpe("id"),
    fpe("gender"),
    fpe("birthDate"),
    fpe("name.family.first()").alias('familyName'),
    fpe("name.given.first()").alias('firstName'),
    fpe("telecom.where(system='phone').value.first()", "phoneNumber"),
    fpe("extension('http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal.first() > 60").alias("isAdjustedAgeOver60"),
    fpe("reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|709044004))", 'hasCKD'),
    fpe("reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|56265001))", 'hasCHC'),
    fpe("reverseResolve(Observation.subject).where(code.subsumedBy(http://loinc.org|39156-5)).exists(valueQuantity > 30 'kg/m2')", "hasBMIOver30"),
    fpe("(reverseResolve(Immunization.patient).vaccineCode.memberOf('https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines') contains true)").alias("isCovidVaccinated"),
])
display(view_df)
