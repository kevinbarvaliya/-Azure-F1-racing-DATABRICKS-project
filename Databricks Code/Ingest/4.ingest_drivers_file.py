# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest drivers file

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
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename columns & Add ingestion date columns

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit


# COMMAND ----------

drivers_updated_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Remove unwanted columns

# COMMAND ----------

drivers_final_df =  drivers_updated_columns_df.drop("url")

# COMMAND ----------

spark.conf.set("spark.databricks.delta.defaults.columnMapping.mode", "name")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Wtrite data to datalake as parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").option("delta.columnMapping.mode", "name").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# %sql drop table f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.drivers

# COMMAND ----------


