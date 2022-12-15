# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Races File 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the CSV using the spark data frame reader

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True),
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitid",IntegerType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("date",StringType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("url",StringType(),True)
                                  ])

# COMMAND ----------

races_df = spark.read \
.option("header",True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Add Ingestion date and race_timestamp to the date
# MAGIC Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, concat, lit

# COMMAND ----------

races_timestamp_df = races_df.withColumn("ingestion_date",current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(" "), col('time')),'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Select only the required columns & Rename as required

# COMMAND ----------

races_selected_df = races_timestamp_df.select(col("raceid").alias("race_id"),col("year").alias("race_year"),                                  col("round"),col("circuitid").alias("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"),col("data_source"),col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Wtrite data to datalake as parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")
                                                                  

# COMMAND ----------



# COMMAND ----------

dbutils.notebook.exit("Success")
