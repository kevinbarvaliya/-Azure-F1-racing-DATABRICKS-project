# Databricks notebook source
# MAGIC %md
# MAGIC ##Produce constructor standings

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



# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Grouping the data

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

constructor_standing_df = race_result_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"), 
    count(when(col("position")==1, True)).alias("wins"))


# COMMAND ----------

display(constructor_standing_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Applying window finction rank 

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write Transformed data in to datalake

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings ")

from delta.tables import DeltaTable

merge_condition = 'trg.team = src.team and trg.race_year = src.race_year'

merge_delta_data(final_df , "f1_presentation", "constructor_standings", presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_presentation.constructor_standings

# COMMAND ----------


