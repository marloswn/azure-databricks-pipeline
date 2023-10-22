# Databricks notebook source
# DBTITLE 1,Reading the data from the bronze layer
df = spark.read.format('delta').load('dbfs:/mnt/data/bronze/dataset_imoveis/')

# COMMAND ----------

# DBTITLE 1,Visualizing the JSON in columns
display(df.select('anuncio.*', 'anuncio.endereco.*')).limit(5)

# COMMAND ----------

# DBTITLE 1,Converting JSON fields into columns
df = df.select('anuncio.*', 'anuncio.endereco.*').drop('caracteristicas', 'endereco')

# COMMAND ----------

# DBTITLE 1,Viewing the result
df.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Saving the data to the silver layer
df.write.format('delta').mode('overwrite').save('dbfs:/mnt/data/silver/dataset_imoveis/')
