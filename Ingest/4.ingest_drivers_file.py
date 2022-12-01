# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest drivers file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the JSON file using the spark data frame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename",StringType(),True),
                                 StructField("surname",StringType(),True)])

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId",IntegerType(),False),
                                      StructField("driverRef",StringType(),True),
                                      StructField("number",IntegerType(),True),
                                      StructField("code",StringType(),True),
                                      StructField("name",name_schema,True),
                                      StructField("dob",StringType(),True),
                                      StructField("nationality",StringType(),True),
                                      StructField("url",StringType(),True)
                                     ])

# COMMAND ----------

drivers_df = spark.read \
.option("header",True) \
.schema(drivers_schema) \
.json("/mnt/formula1dalalake1/raw/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename columns & Add ingestion date columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit


# COMMAND ----------

drivers_updated_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Remove unwanted columns

# COMMAND ----------

drivers_final_df =  drivers_updated_columns_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Wtrite data to datalake as parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1dalalake1/processed/drivers")
