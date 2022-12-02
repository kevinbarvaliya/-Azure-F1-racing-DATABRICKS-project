# Databricks notebook source
# MAGIC %md
# MAGIC ##Read constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the JSON Filw using the spark dataframe

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

    constructors_df = spark.read.schema(constructors_schema).json("/mnt/formula1dalalake1/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3 -- Rename column & Add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("/mnt/formula1dalalake1/processed/constructors")
