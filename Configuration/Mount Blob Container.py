# Databricks notebook source
# Azure Storage Account Name
storage_account_name = "rmcelhinnystorage"

# Azure Storage Account Key
storage_account_key = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# Azure Storage Account Source Container
container = "azureefileextract"

# Set the configuration details to read/write
spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

# COMMAND ----------

  dbutils.fs.mount(
   source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
   mount_point = "/mnt/rmcelhinnystorage",
   extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
  )

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/rmcelhinnystorage")