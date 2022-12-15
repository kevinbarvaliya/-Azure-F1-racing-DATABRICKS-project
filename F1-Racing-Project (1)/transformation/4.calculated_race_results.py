# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_resuls(
          race_year INT,
          team_name STRING,
          race_id INT,
          driver_id INT,  
          driver_name STRING,
          position INT, 
          points INT,
          calculated_points INT,
          created_date TIMESTAMP,
          updated_date TIMESTAMP
          )
          USING DELTA
          """)

# COMMAND ----------

spark.sql(f"""
        create or replace temp view race_result_updated
        as
        select races.race_year,
               constructors.name as team_name,
               drivers.driver_id,
               drivers.name as driver_name,
               races.race_id,
               results.position,
               results.points,
               11 - results.position as calculated_points
        from f1_processed.results 
        join f1_processed.races on results.race_id = races.race_id
        join f1_processed.drivers on results.driver_id = drivers.driver_id
        join f1_processed.constructors on results.constructor_id = constructors.constructor_id
        where results.position <= 10
          and results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO f1_presentation.calculated_race_resuls trg
    USING race_result_updated upd
    ON (trg.driver_id = upd.driver_id and trg.race_id = upd.race_id)
    WHEN MATCHED THEN
      UPDATE SET
        trg.position = upd.position,
        trg.points = upd.points,
        trg.calculated_points = upd.calculated_points,
        trg.updated_date = CURRENT_TIMESTAMP
    WHEN NOT MATCHED
      THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date)
            VALUES(race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, CURRENT_TIMESTAMP)
""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- drop table f1_presentation.calculated_race_resuls

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(1)
# MAGIC from race_result_updated

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(1)
# MAGIC from f1_presentation.calculated_race_resuls
