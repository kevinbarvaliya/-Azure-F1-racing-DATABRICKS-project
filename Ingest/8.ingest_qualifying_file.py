# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read multiple multiline JSON files using the spark data frame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType, FloatType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId",IntegerType(),False),
                                         StructField("raceId",IntegerType(),True),
                                         StructField("driverId",IntegerType(),True),
                                         StructField("constructorId",IntegerType(),True),
                                         StructField("number", IntegerType(),True),
                                         StructField("position",IntegerType(),True),
                                         StructField("q1",StringType(),True),
                                         StructField("q2",StringType(),True),
                                         StructField("q3",StringType(),True)])
                                  

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("/mnt/formula1dalalake1/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename columns & Add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Wtrite data to datalake as parquet

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/formula1dalalake1/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dalalake1/processed/qualifying"))
