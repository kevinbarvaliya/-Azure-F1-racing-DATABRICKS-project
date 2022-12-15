# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_columns(input_df, partition_column):
    columns_list = []
    for column_name in input_df.columns:
        if column_name != partition_column:
            columns_list.append(column_name)

    columns_list.append(partition_column)

    return input_df.select(columns_list)


# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_columns(input_df, partition_column)
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")    
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")    
    

# COMMAND ----------


def merge_delta_data(input_df , db_name, table_name, folder_path, merge_condition, partition_column):
    
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")


    if (spark._jsparkSession.catalog().tableExists(F"{db_name}.{table_name}")):
        deltatable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")

        deltatable.alias('trg') \
          .merge(input_df.alias('src'),
                 merge_condition ) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()

    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(F"{db_name}.{table_name}")     

