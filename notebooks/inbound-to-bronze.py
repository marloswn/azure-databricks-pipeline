# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Checking the data mount and access to them
dbutils.fs.ls('/mnt/data/inbound')

# COMMAND ----------

# DBTITLE 1,Reading the data from the inbound layer
df = spark.read.json('dbfs:/mnt/data/inbound/dados_brutos_imoveis.json')

# COMMAND ----------

# DBTITLE 1,Reading the loaded data
display(df)

# COMMAND ----------

# DBTITLE 1,Cleaning the data
df = df.drop('imagens', 'usuario')

# COMMAND ----------

# DBTITLE 1,Checking the cleaning
display(df).limit(5)

# COMMAND ----------

# DBTITLE 1,Creating an ID column
df = (
    df
    .withColumn('id', col('anuncio.id'))
    #ou: .withColumn('id', df.anuncio.id)
)

# COMMAND ----------

# DBTITLE 1,Saving the data to the bronze layer
(
    df
    .write
    .format('delta')
    .mode('overwrite')
    .save('dbfs:/mnt/data/bronze/dataset_imoveis')
)
