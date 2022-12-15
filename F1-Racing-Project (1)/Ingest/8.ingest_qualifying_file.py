# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source") 

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

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
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename columns & Add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import lit


# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Wtrite data to datalake as parquet

# COMMAND ----------

# overwrite_partition(qualifying_final_df, "f1_processed", "qualifying", "race_id")



from delta.tables import DeltaTable

merge_condition = 'trg.qualify_id = src.qualify_id and trg.race_id = src.race_id'

merge_delta_data(qualifying_final_df , "f1_processed", "qualifying", processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC 
# MAGIC %sql 
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.qualifying
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------


