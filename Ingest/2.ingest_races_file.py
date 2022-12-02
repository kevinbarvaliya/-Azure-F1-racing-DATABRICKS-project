# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Races File 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the CSV using the spark data frame reader

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType,

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
.csv("/mnt/formula1dalalake1/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Add Ingestion date and race_timestamp to the date
# MAGIC Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, col, concat, lit

# COMMAND ----------

races_timestamp_df = races_df.withColumn("ingestion_date",current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(" "), col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Select only the required columns & Rename as required

# COMMAND ----------

races_selected_df = races_timestamp_df.select(col("raceid").alias("race_id"),col("year").alias("race_year"),
                                              col("round"),col("circuitid").alias("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Wtrite data to datalake as parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1dalalake1/processed/races")
