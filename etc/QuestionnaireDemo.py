# Databricks notebook source
from pathling import PathlingContext
from pathling import Expression as fpe
from pyspark.sql.functions import col, explode, explode_outer, first

pc = PathlingContext.create(spark, max_nesting_level=5)

# COMMAND ----------

ds = pc.read.ndjson("s3://pathling-demo/staging/devdays/questionnaire")

# COMMAND ----------

questionnaire_ds = ds.read('Questionnaire')
item_ds = questionnaire_ds\
    .withColumn('item0', explode_outer('item')) \
    .withColumn('item1', explode_outer('item0.item')) \
    .withColumn('item2', explode_outer('item1.item')) \
    .withColumn('item3', explode_outer('item2.item')) \
    .withColumn('item4', explode_outer('item3.item')) \
    .withColumn('item5', explode_outer('item4.item')) \
    .select(col('id'), col('item0.linkid'), col('item0.text'),  col('item1.linkid'), col('item1.text'), col('item2.linkid'), col('item3.linkid'), col('item4.linkid'), col('item5.linkid'))

display(item_ds) 

# COMMAND ----------

response_ds = ds.read('QuestionnaireResponse').cache()
ritem_ds = response_ds \
    .where(col('id') == 'f201') \
    .withColumn('item0', explode_outer('item')) \
    .withColumn('item1', explode_outer('item0.item')) \
    .withColumn('item2', explode_outer('item1.item')) \
    .select(col('id'), col('subject.reference').alias('subject'), col('item0.linkid').alias('link_id0'), col('item1.linkid').alias('link_id1'), col('item1.text'), col('item1.answer.valueString').getItem(0).alias('answer'))
display(ritem_ds) 

# COMMAND ----------

fitems = ritem_ds.groupBy(col('id'), col('subject')).pivot("link_id1", ['2.1', '2.3', '2.4', '3.1']).agg(first('answer').alias('answer')) \
    .withColumnRenamed('2.1', 'gender') \
    .withColumnRenamed('2.3', 'country_of_birth') \
    .withColumnRenamed('2.4', 'marital_status') \
    .withColumnRenamed('3.1', 'smoker') 
display(fitems)

# COMMAND ----------

x = ds.extract('QuestionnaireResponse', 
    [
        'id',
        fpe("subject.reference").alias("subject"),
        fpe("item.item.where(linkId='2.1').answer.valueString.first()").alias("gender"), 
        fpe("item.item.where(linkId='2.3').answer.valueString.first()").alias("country_of_birth"), 
        fpe("item.item.where(linkId='2.4').answer.valueString.first()").alias("maritial_status"), 
        fpe("item.item.where(linkId='3.1').answer.valueString.first()").alias("smoker"), 
    ], 
    filters=["id = 'f201'"])
display(x)
