# Databricks notebook source
# MAGIC %sql
# MAGIC create database if  not exists f1_demo
# MAGIC location "/mnt/formula1dalalake1/demo"

# COMMAND ----------



# COMMAND ----------

result_df = spark.read.option("inferschema",True)\
.json("/mnt/formula1dalalake1/raw/2021-03-28/results.json")

# COMMAND ----------

display(result_df)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").save("/mnt/formula1dalalake1/demo/result_external")


# COMMAND ----------

# MAGIC %sql  
# MAGIC CREATE TABLE f1_demo.result_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1dalalake1/demo/result_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed 
# MAGIC set points = 11 - position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dalalake1/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(condition = "position <= 10", set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql delete from f1_demo.results_managed
# MAGIC where position > 10

# COMMAND ----------

# MAGIC %sql  select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dalalake1/demo/results_managed')

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql  select * from f1_demo.results_managed

# COMMAND ----------

driver_day1_df = spark.read.option("inferSchema",True)\
.json('/mnt/formula1dalalake1/raw/2021-03-28/drivers.json')\
.filter("driverId<=10")\
.select("driverId","dob","name.forename", "name.surname")

# COMMAND ----------

display(driver_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day2_df = spark.read.option("inferSchema",True)\
.json('/mnt/formula1dalalake1/raw/2021-03-28/drivers.json')\
.filter("driverId between 6 and 15")\
.select("driverId","dob",upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(driver_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day3_df = spark.read.option("inferSchema",True)\
.json('/mnt/formula1dalalake1/raw/2021-03-28/drivers.json')\
.filter("driverId between 1 and 5 or driverId between 16 and 20")\
.select("driverId","dob",upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(driver_day3_df)

# COMMAND ----------

driver_day1_df.createOrReplaceTempView("driver_day1")

# COMMAND ----------

driver_day2_df.createOrReplaceTempView("driver_day2")

# COMMAND ----------

driver_day3_df.createOrReplaceTempView("driver_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge(
# MAGIC driverId Int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# %sql 
# drop table f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO f1_demo.drivers_merge mrg
# MAGIC USING driver_day1 d1
# MAGIC ON mrg.driverId = d1.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     mrg.driverId = d1.driverId,
# MAGIC     mrg.dob = d1.dob,
# MAGIC     mrg.forename = d1.forename,
# MAGIC     mrg.surname = d1.surname,
# MAGIC     mrg.updatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,dob,forename, surname,createdDate)
# MAGIC   VALUES(
# MAGIC     driverId,dob,forename, surname,CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO f1_demo.drivers_merge mrg
# MAGIC USING driver_day2 d1
# MAGIC ON mrg.driverId = d1.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     mrg.driverId = d1.driverId,
# MAGIC     mrg.dob = d1.dob,
# MAGIC     mrg.forename = d1.forename,
# MAGIC     mrg.surname = d1.surname,
# MAGIC     mrg.updatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,dob,forename, surname,createdDate)
# MAGIC   VALUES(
# MAGIC     driverId,dob,forename, surname,CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge%sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO f1_demo.drivers_merge mrg
# MAGIC USING driver_day3 d1
# MAGIC ON mrg.driverId = d1.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     mrg.driverId = d1.driverId,
# MAGIC     mrg.dob = d1.dob,
# MAGIC     mrg.forename = d1.forename,
# MAGIC     mrg.surname = d1.surname,
# MAGIC     mrg.updatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,dob,forename, surname,createdDate)
# MAGIC   VALUES(
# MAGIC     driverId,dob,forename, surname,CURRENT_TIMESTAMP)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable= DeltaTable.forPath(spark, '/mnt/formula1dalalake1/demo/drivers_merge')


deltaTable.alias('mrg') \
  .merge(
    driver_day3_df.alias('updates'),
    'mrg.driverId = updates.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
        "mrg.driverId" : "updates.driverId",
        "mrg.dob" : "updates.dob",
        "mrg.forename" : "updates.forename",
        "mrg.surname" : "updates.surname",
        "mrg.updatedDate" : "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
        "mrg.driverId" : "updates.driverId",
        "mrg.dob" : "updates.dob",
        "mrg.forename" : "updates.forename",
        "mrg.surname" : "updates.surname",
        "mrg.createdDate" : "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge VERSION as of 1;

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf","1").load("/mnt/formula1dalalake1/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------


