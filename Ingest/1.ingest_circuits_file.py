# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Circuits File

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the CSV using the spark data frame reader

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType

# COMMAND ----------

circuits_sceama = StructType(fields=[StructField("circuitId",IntegerType(), False),
                                     StructField("circuitRef",StringType(), True),
                                     StructField("name",StringType(), True),
                                     StructField("location",StringType(), True),
                                     StructField("country",StringType(), True),
                                     StructField("lat",DoubleType(), True),
                                     StructField("lng",DoubleType(), True),
                                     StructField("alt",IntegerType(), True),
                                     StructField("url",StringType(), True)])

# COMMAND ----------

 circuits_df =spark.read.option("header",True).schema(circuits_sceama).csv("/mnt/formula1dalalake1/raw/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Rename the columns as required 

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref" ) \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

 from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 - Wtrite data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dalalake1/processed/circuits")

# COMMAND ----------


