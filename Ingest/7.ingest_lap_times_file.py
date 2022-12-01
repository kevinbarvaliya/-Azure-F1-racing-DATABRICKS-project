# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest lap times file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read multiple CSV files using the spark data frame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType, FloatType

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("time", StringType(),True),
                                      StructField("milliseconds",IntegerType(),True),])
                                  

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/formula1dalalake1/raw/lap_times/*")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename columns & Add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Wtrite data to datalake as parquet

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/formula1dalalake1/processed/lap_times")
