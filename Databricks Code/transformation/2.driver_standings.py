# Databricks notebook source
# MAGIC %md
# MAGIC ##Produce driver standings

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Reading the race_result parquet 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_result_list = df_column_to_list(race_result_df,"race_year")

# COMMAND ----------

from pyspark.sql.functions import col
race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_result_list))

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Grouping the data

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

driver_standing_df = race_result_df \
.groupBy("race_year","driver_name","driver_nationality") \
.agg(sum("points").alias("total_points"), 
    count(when(col("position")==1, True)).alias("wins"))


# COMMAND ----------

display(driver_standing_df.filter("race_year = 2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Applying window finction rank 

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write Transformed data in to datalake

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standing")


from delta.tables import DeltaTable

merge_condition = 'trg.driver_name = src.driver_name and trg.race_year = src.race_year'

merge_delta_data(final_df , "f1_presentation", "driver_standing", presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql select *from f1_presentation.driver_standing order by race_year desc

# COMMAND ----------


