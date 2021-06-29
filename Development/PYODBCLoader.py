# Databricks notebook source
# MAGIC %sql
# MAGIC create temporary table Titles
# MAGIC (
# MAGIC RID string,
# MAGIC Title string,
# MAGIC Price string,
# MAGIC PurchaseDate string,
# MAGIC RowCreated string
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (Path "/mnt/rmcelhinnystorage/Titles/Titles.txt", delimiter "", header "false" )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Titles