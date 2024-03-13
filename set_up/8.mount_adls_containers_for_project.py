# Databricks notebook source
# MAGIC %md
# MAGIC * Access ADL using Access Keys

# COMMAND ----------



# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret-id')

    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")   

    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
         mount_point = f"/mnt/{storage_account_name}/{container_name}",
         extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula1dl612', 'raw')

# COMMAND ----------

mount_adls('formula1dl612', 'processed')

# COMMAND ----------

mount_adls('formula1dl612', 'presentation')

# COMMAND ----------

# MAGIC %md
# MAGIC ### stop

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret-id')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1dl612.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl612.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl612.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl612.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl612.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl612.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl612/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl612.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl612.dfs.core.windows.net"))

# COMMAND ----------

display( spark.read.csv("/mnt/formula1dl612/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())
