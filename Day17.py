# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

sch = StructType().add("ID",IntegerType(),True)\
                  .add("Name",StringType(),True)\
                  .add("Age", IntegerType(),True)

# COMMAND ----------

df = spark.read.option("header",True).option("mode","pervasive").schema(sch).format("csv").load("/FileStore/simple.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = spark.read.option("header",True).option("mode","dropmalformed").schema(sch).format("csv").load("/FileStore/simple.csv")

# COMMAND ----------

df1.display()

# COMMAND ----------

df2 = spark.read.option("header",True).option("mode","failfast").schema(sch).format("csv").load("/FileStore/simple.csv")

# COMMAND ----------

df2.display()

# COMMAND ----------

dbutils.fs.mount(source = "wasbs://input1@adlsgdme2344.blob.core.windows.net",mount_point ="/mnt/input_03",extra_configs = {"fs.azure.account.key.adlsgdme2344.blob.core.windows.net":dbutils.secrets.get(scope = "adb_scope", key = "adlskeyergdme")})

# COMMAND ----------

dbutils.fs.ls("/mnt/input_03")

# COMMAND ----------

dbutils.fs.ls("/mnt/input_03")

# COMMAND ----------

schema = StructType().add("TransactionID",IntegerType(),True)\
                  .add("SalesRepID",StringType(),True)\
                  .add("TransactionDate",DateType(),True)\
                  .add("TransactionAmount", DoubleType(),True)

# COMMAND ----------

df3 = spark.read.option("header",True).schema(schema).format("csv").load("/mnt/input_03/transaction_emea_26_07_2024.csv")

# COMMAND ----------

df3.display()

# COMMAND ----------


result_df = (df3.groupBy("TransactionID").agg(sum("TransactionAmount").alias("TotalFareAmount")))

# COMMAND ----------

result_df.display()

# COMMAND ----------

result_df.write.parquet("/mnt/input_03/parquet1/")

# COMMAND ----------

result_df.write.format("delta").saveAsTable("stor")

# COMMAND ----------

# MAGIC %md 
# MAGIC Creating external table and dropping managed and external tables

# COMMAND ----------

result_df.write.format("delta").save("/mnt/input_03/store_trans2")

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table ext_store_trans3
# MAGIC USING delta
# MAGIC OPTIONS (path 'abfss://input1@adlsgdme2344.dfs.core.windows.net/store_trans2')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe stored
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail stored

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stored

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from stored where TransactionID=12;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended stored

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history stored

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into stored values(40,50.88);

# COMMAND ----------

# MAGIC %sql
# MAGIC update stored set TotalFareAmount=34.56 where TransactionID=13;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE stored TO version as of 0

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC optimize stored;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history stored

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table stored
# MAGIC cluster by (TransactionID);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history stored

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize stored
# MAGIC zorder by (TransactionID);

# COMMAND ----------

df3.createOrReplaceGlobalTempView("gun_tem_table")
