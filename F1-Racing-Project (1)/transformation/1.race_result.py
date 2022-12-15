# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1 - Read all the data as required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuits_df =spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuits_location") 

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality","driver_nationality")



# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "races_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'")\
.withColumnRenamed("time", "race_time")\
.withColumnRenamed("race_id", "result_race_id")\
.withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Join circuites to races

# COMMAND ----------

races_circuites_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id,"inner") \
    .select(races_df.race_id,races_df.race_year, races_df.races_name, races_df.race_date, circuits_df.circuits_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Join Result to Driver, Constructor and races_circuits

# COMMAND ----------

race_result_df = results_df.join(races_circuites_df, results_df.result_race_id == races_circuites_df.race_id) \
                           .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                           .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_id","race_year", "races_name", "race_date", "circuits_location", "driver_name", "driver_number",
                                 "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points","position", "result_file_date") \
.withColumn("created_date", current_timestamp())\
.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Filter data to view race result

# COMMAND ----------

display(final_df.where("race_year == 2021 and races_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 - Writing joined and filtered data into datalake as parquer file

# COMMAND ----------

# overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")

from delta.tables import DeltaTable

merge_condition = 'trg.driver_name = src.driver_name and trg.race_id = src.race_id'

merge_delta_data(final_df , "f1_presentation", "race_results", presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results order by race_year desc

# COMMAND ----------


