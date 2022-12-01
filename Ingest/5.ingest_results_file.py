# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest results file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the JSON file using the spark data frame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId",IntegerType(),False),
                                      StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),False),
                                      StructField("constructorId",IntegerType(),False),
                                      StructField("number", IntegerType(),True),
                                      StructField("grid",IntegerType(),False),
                                      StructField("position",IntegerType(),True),
                                      StructField("positionText",StringType(),False),
                                      StructField("positionOrder",IntegerType(),False),
                                      StructField("points",FloatType(),False),
                                      StructField("laps",IntegerType(),False),
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True),
                                      StructField("fastestLap",IntegerType(),True),
                                      StructField("rank",IntegerType(),True),
                                      StructField("fastestLapTime",StringType(),True),
                                      StructField("fastestLapSpeed",StringType(),True),
                                      StructField("statusId",IntegerType(),False)])

# COMMAND ----------

results_df = spark.read \
.option("header",True) \
.schema(results_schema) \
.json("/mnt/formula1dalalake1/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

results_renamed_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumnRenamed("statusId", "status_id") 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Remove unwanted columns & Add ingestion date 

# COMMAND ----------

results_final_df =  results_renamed_columns_df.withColumn("ingestion_date", current_timestamp()) \
.drop("status_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Wtrite data to datalake as parquet

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1dalalake1/processed/results")

# COMMAND ----------


