# Databricks notebook source
reault = dbutils.notebook.run("1.ingest_circuits_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
reault

# COMMAND ----------

reault = dbutils.notebook.run("2.ingest_races_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})
reault

# COMMAND ----------

reault = dbutils.notebook.run("3.ingest_constructors_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})
reault

# COMMAND ----------

reault = dbutils.notebook.run("4.ingest_drivers_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})
reault

# COMMAND ----------

reault = dbutils.notebook.run("5.ingest_results_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})
reault

# COMMAND ----------

reault = dbutils.notebook.run("6.ingest_pit_stops_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})
reault

# COMMAND ----------

reault = dbutils.notebook.run("7.ingest_lap_times_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})
reault

# COMMAND ----------

reault = dbutils.notebook.run("8.ingest_qualifying_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})
reault

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,
# MAGIC        count(1)
# MAGIC        from f1_processed.races
# MAGIC        GROUP BY race_id
# MAGIC        ORDER BY race_id

# COMMAND ----------


