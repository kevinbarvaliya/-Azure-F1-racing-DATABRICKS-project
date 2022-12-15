# Databricks notebook source
dbutils.secrets.list("f1-project-scope")

# COMMAND ----------

storage_account_name = "formula1dalalake1"
client_id            = dbutils.secrets.get(scope="f1-project-scope",key="databricks-app-client-id")
tenant_id            = dbutils.secrets.get(scope="f1-project-scope",key="databricks-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="f1-project-scope",key="databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

def mount_adl(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

mount_adl("raw")


# COMMAND ----------

mount_adl("processed")

# COMMAND ----------

mount_adl("presentation")

# COMMAND ----------

mount_adl("demo")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dalalake1/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dalalake1/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dalalake1/presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dalalake1/demo")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


