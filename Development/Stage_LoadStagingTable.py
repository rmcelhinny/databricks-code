# Databricks notebook source
# MAGIC %py
# MAGIC #FileName = dbutils.widgets.get("FileName")
# MAGIC #ControlTableKey = dbutils.widgets.get("ControlTableKey")
# MAGIC #ProcessName = dbutils.widgets.get("ProcessName")
# MAGIC 
# MAGIC FileName = "Titles.txt"
# MAGIC ControlTableKey = "1"
# MAGIC RunKey = "2"
# MAGIC SourceTableName = "Titles"

# COMMAND ----------

# MAGIC %py
# MAGIC # Connection prelim work
# MAGIC from azure.storage.blob import ContainerClient
# MAGIC from azure.storage.blob import BlobServiceClient
# MAGIC from pyspark.sql import SQLContext
# MAGIC sqlContext = SQLContext(sc)
# MAGIC import pyodbc
# MAGIC import pandas as pd
# MAGIC import sys
# MAGIC import numpy as np
# MAGIC import databricks.koalas as ks
# MAGIC import time
# MAGIC import datetime
# MAGIC #Hyperscale/Blob Connection Info
# MAGIC Username = "rmcelhinny"
# MAGIC Password = "XXXXXX"
# MAGIC Sever = "rmcelhinny-db.database.windows.net"
# MAGIC Databasename = "rmcelhinny-db"
# MAGIC driver = '{ODBC Driver 17 for SQL Server}'
# MAGIC 
# MAGIC #Connect to Hyperscale
# MAGIC ConnectionAttempts = 0
# MAGIC MaxRetry = 3
# MAGIC for i in range(ConnectionAttempts,MaxRetry):
# MAGIC   try:
# MAGIC     conn = pyodbc.connect(f"driver={driver};server={Sever};database={Databasename};uid={Username};pwd={Password}")
# MAGIC     cursor = conn.cursor()
# MAGIC     break
# MAGIC   except:
# MAGIC     exc_type, exc_value, exc_traceback = sys.exc_info()
# MAGIC     errorinfo = str(sys.exc_info())
# MAGIC     ConnectionAttempts = ConnectionAttempts+1
# MAGIC     print('Error connecting: '  + errorinfo + '.  Will Retry in 60 seconds')
# MAGIC     if ConnectionAttempts <= 3:
# MAGIC       time.sleep(60)
# MAGIC       continue
# MAGIC     else:
# MAGIC       break

# COMMAND ----------


#Gets FileMetadata
FileMetaData = f"""select ControlTableKey, ProcessName, SourceSchemaName, SourceTableName, TargetTableSchema, TargetTableName, ContainerName, Delimiter from  ProcessMetadata.ControlTable where ControlTableKey = {ControlTableKey} and SourceTableName = '{SourceTableName}'"""
FileMetaData = pd.read_sql(FileMetaData,conn)


SourceSchemaName = FileMetaData.loc[FileMetaData.index[0], 'SourceSchemaName']
SourceTableName = FileMetaData.loc[FileMetaData.index[0], 'SourceTableName']
TargetTableSchema = FileMetaData.loc[FileMetaData.index[0], 'TargetTableSchema']
TargetTableName = FileMetaData.loc[FileMetaData.index[0], 'TargetTableName']
Delimiter = FileMetaData.loc[FileMetaData.index[0], 'Delimiter']
ContainerName = FileMetaData.loc[FileMetaData.index[0], 'ContainerName']
CSV_Delimiter = ''
TempTableName = SourceTableName
StorageAccount = "rmcelhinnystorage"

#FileColumnData
TableColumns = f"select OrdinalPosition, SourceColumn, SourceColumnDataType,Length, Scale, Nullable, DatabricksDataTypeExternal,DatabricksDataType,SQLDataType from ProcessMetadata.SourceTableColumns where ControlTableKey = {ControlTableKey} and SourceTableName = '{SourceTableName}'"
TableColumns = pd.read_sql(TableColumns,conn)
#TableColumns.display()

#min/max ordinal position
MaxOrdinalPosition = TableColumns["OrdinalPosition"].max()
MinOrdinalPosition = TableColumns["OrdinalPosition"].min()

#Generate TempTable Code to Execute
TempTableColumns ="("
for value in range(0,MaxOrdinalPosition):
  if TableColumns.loc[TableColumns.index[value],"SourceColumnDataType"] == 'decimal':
    TempTableColumns += (TableColumns.loc[TableColumns.index[value],"SourceColumn"]) + ' '

    TempTableColumns += TableColumns.loc[TableColumns.index[value],"DatabricksDataTypeExternal"] + '(' + str(TableColumns.loc[TableColumns.index[value],"Length"]) + ',' + str(TableColumns.loc[TableColumns.index[value],"Scale"]) + ')'
    if value != MaxOrdinalPosition-1:
      TempTableColumns += ', \n'
    else:
      TempTableColumns += ') \n'
  else:
    TempTableColumns += (TableColumns.loc[TableColumns.index[value],"SourceColumn"]) + ' '
    TempTableColumns += TableColumns.loc[TableColumns.index[value],"DatabricksDataTypeExternal"] 
    if value != MaxOrdinalPosition-1:
      TempTableColumns += ', \n'
    else:
      TempTableColumns += ') \n'
  
#TempTable Statements
DropTempTable = f"Drop table if exists {TempTableName};"
CreateTempTable = f'Create Temporary Table {TempTableName}\n{TempTableColumns} USING CSV \nOPTIONS(Path "/mnt/{StorageAccount}/{SourceTableName}/{FileName}",delimiter "{Delimiter}", quotes"", header "false", encoding "cp1252")'   
#print(CreateTempTable)

#Generate staging code to execute
#Add more conversions later
StagingTableColumns ="select \n"
for value in range(0,MaxOrdinalPosition):
  if TableColumns.loc[TableColumns.index[value],"DatabricksDataTypeExternal"] != TableColumns.loc[TableColumns.index[value],"DatabricksDataType"] and TableColumns.loc[TableColumns.index[value],"DatabricksDataType"] == 'timestamp':
    StagingTableColumns += "TO_TIMESTAMP(" + (TableColumns.loc[TableColumns.index[value],"SourceColumn"]) + ", 'yyyy-MM-dd HH:mm:ss') AS " + TableColumns.loc[TableColumns.index[value],"SourceColumn"]
    if value != MaxOrdinalPosition-1:
      StagingTableColumns += ',\n'
    else:
      StagingTableColumns += '\n'
  else:
    StagingTableColumns += TableColumns.loc[TableColumns.index[value],"SourceColumn"]
    if value != MaxOrdinalPosition-1:
      StagingTableColumns += ',\n'
    else:
      StagingTableColumns += '\n'
#print(StagingTableColumns)

CreateSchema = f"create schema if not exists {TargetTableSchema};" 
DropStagingTable = f"Drop table if exists {TargetTableSchema}.{TargetTableName};"
CreateStagingTable = f"Create table {TargetTableSchema}.{TargetTableName} \nUsing Delta as\n{StagingTableColumns}from {TempTableName};"
#print(CreateStagingTable)

# COMMAND ----------

#Create TempTable from File
spark.sql(DropTempTable)
spark.sql(CreateTempTable)

#Create Staging Table
spark.sql(CreateSchema)
spark.sql(DropStagingTable)
spark.sql(CreateStagingTable)

#Close Connection
cursor.close()
conn.close()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM STG_Movies.Titles

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.exit("Success");