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
from pathling import Expression as fpe

pc = PathlingContext.create(spark)
fhir_ds = pc.read.tables(INPUT_FHIR_SCHEMA)

coivd19_view_df = fhir_ds.extract('Patient',
    columns= [
        fpe("id"),
        fpe("gender"),
        fpe("birthDate"),
        fpe("name.family.first()").alias('familyName'),
        fpe("name.given.first()").alias('firstName'),
        fpe("telecom.where(system='phone').value.first()", "phoneNumber"),
        fpe("address.postalCode.first()").alias("postalCode"),
        fpe("reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|709044004))", 'hasCKD'),
        fpe("reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|56265001))", 'hasCHC'),
        fpe("reverseResolve(Observation.subject).where(code.subsumedBy(http://loinc.org|39156-5)).exists(valueQuantity > 30 'kg/m2')", "hasBMIOver30"),
        fpe("(reverseResolve(Immunization.patient).vaccineCode.memberOf('https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines') contains true)").alias("isCovidVaccinated"),
    ],
    filters = [
        "address.country.first() = 'US'"
    ]
)

coivd19_view_df.printSchema()
coivd19_view_df.show(5)

coivd19_view_df.write.saveAsTable(f"{OUTPUT_SQL_SCHEMA}.covid19_view", mode='overwrite')
