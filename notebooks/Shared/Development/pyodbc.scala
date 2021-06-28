// Databricks notebook source
// MAGIC %sh
// MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
// MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
// MAGIC apt-get update
// MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
// MAGIC apt-get -y install unixodbc-dev
// MAGIC sudo apt-get install python3-pip -y
// MAGIC pip3 install --upgrade pyodbc

// COMMAND ----------

// MAGIC %sh
// MAGIC pip install --upgrade pip

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.put("/databricks/scripts/pyodbc-install.sh","""
// MAGIC #!/bin/bash
// MAGIC 
// MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
// MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
// MAGIC apt-get update
// MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
// MAGIC apt-get -y install unixodbc-dev
// MAGIC sudo apt-get install python3-pip -y
// MAGIC pip3 install --upgrade pyodbc""", True)

// COMMAND ----------

dbutils.fs.head("/databricks/scripts/pyodbc-install.sh")

// COMMAND ----------

// MAGIC %python
// MAGIC pip install azure-storage-blob