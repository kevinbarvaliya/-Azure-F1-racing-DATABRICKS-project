# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest pit stops file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the JSON file using the spark data frame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields = [StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("stop",StringType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("time", StringType(),True),
                                      StructField("duration",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)])
                                  

# COMMAND ----------

pit_stops_df = spark.read \
.option("header",True) \
.schema(pit_stops_schema) \
.option("multiline",True) \
.json("/mnt/formula1dalalake1/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename columns & Add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Wtrite data to datalake as parquet

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/formula1dalalake1/processed/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dalalake1/processed/pit_stops"))

# COMMAND ----------


