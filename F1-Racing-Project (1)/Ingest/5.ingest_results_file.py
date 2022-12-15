# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest results file

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
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename columns

# COMMAND ----------

from pyspark.sql.functions import lit


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
.withColumnRenamed("statusId", "status_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Remove unwanted columns & Add ingestion date 

# COMMAND ----------

results_final_df =  results_renamed_columns_df.withColumn("ingestion_date", current_timestamp()) \
.drop("status_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####De-Dupe Dataframe

# COMMAND ----------

result_deduplicated_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Wtrite data to datalake as parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")
       

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 2

# COMMAND ----------

# overwrite_partition(results_final_df, "f1_processed", "results", "race_id")


# COMMAND ----------

display(results_final_df)

# COMMAND ----------




# COMMAND ----------

# overwrite_partition(qualifying_final_df, "f1_processed", "results", "race_id")

from delta.tables import DeltaTable

merge_condition = 'trg.result_id = src.result_id and trg.race_id = src.race_id'

merge_delta_data(result_deduplicated_df , "f1_processed", "results", processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, driver_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC order by race_id, driver_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_processed.results
