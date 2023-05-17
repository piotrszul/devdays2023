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
    .select(col('id'), col('subject'), col('item0.linkid').alias('link_id0'), col('item1.linkid').alias('link_id1'), col('item1.text'), col('item1.answer.valueString').getItem(0).alias('answer'))
display(ritem_ds) 

#BUG: Why subject is NULL ???

# COMMAND ----------

display(ritem_ds.groupBy(col('id')).pivot("link_id1", ['2.1', '2.3', '2.4', '3.1']).agg(first('text').alias('question'), first('answer').alias('answer')))

# COMMAND ----------

fitems = ritem_ds.groupBy(col('id')).pivot("link_id1", ['2.1', '2.3', '2.4', '3.1']).agg(first('answer').alias('answer')) \
    .withColumnRenamed('2.1', 'gender') \
    .withColumnRenamed('2.3', 'country_of_birth') \
    .withColumnRenamed('2.4', 'marital_status') \
    .withColumnRenamed('3.1', 'smoker') 
display(fitems)

# COMMAND ----------

x = ds.extract('QuestionnaireResponse', 
    [
        'id',
        fpe("item.item.where(linkId='2.4').answer.valueString").alias("maritalStatus"), 
        fpe("item.item.where(linkId='3.1').answer.valueString").alias("smoker"), 
    ], 
    filters=["id = 'f201'"])
display(x)
#BUG: should be unnested differently ??? and takes 1.11 minutes to RUN !!!

# COMMAND ----------

x = ds.extract('QuestionnaireResponse', 
    [
        'id',
        fpe("item.item.where(linkId='2.4').answer.valueString contains 'married'").alias("isMarried"), 
        fpe("item.item.where(linkId='3.1').answer.valueString contains 'No'").alias("nonSmoker"), 
    ], 
    filters=["id = 'f201'"])
display(x)
