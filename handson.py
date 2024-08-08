# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("start_date", "2008-06-01", "Start Date")
dbutils.widgets.text("end_date", "2008-06-01", "End Date")

# COMMAND ----------

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://gmdeserverdb004.database.windows.net:1433;databaseName=gmdedb002"
jdbc_properties = {
    "user": "sqladmin",
    "password": "Gundappa@2001",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

header_df = spark.read.jdbc(url=jdbc_url, table="saleslt.salesorderheader", properties=jdbc_properties)
detail_df = spark.read.jdbc(url=jdbc_url, table="saleslt.salesorderdetail", properties=jdbc_properties)
customer_df = spark.read.jdbc(url=jdbc_url, table="saleslt.customer", properties=jdbc_properties)

# COMMAND ----------

sales_df = header_df.join(detail_df, on="SalesOrderID", how="inner")

# COMMAND ----------

sales_df.display()

# COMMAND ----------

sales_df = sales_df.withColumn("TotalSales1", col("UnitPrice") * col("OrderQty"))

# COMMAND ----------

filtered_sales_df = sales_df.filter(
    (col("OrderDate") >= start_date) & (col("OrderDate") <= end_date)
)

# COMMAND ----------

filtered_sales_df.display()

# COMMAND ----------

sales_customer_df = filtered_sales_df.join(customer_df, on="CustomerID", how="inner")

# COMMAND ----------

sales_customer_df.display()

# COMMAND ----------


cleaned_sales_df = sales_customer_df \
    .fillna({'TotalSales1': 0, 'OrderQty': 0}) \
    .withColumn("TotalSales1", when(col("TotalSales1").isNull(), 0).otherwise(col("TotalSales1")))


aggregated_df = cleaned_sales_df.groupBy("CustomerID", "FirstName","LastName") \
    .agg(
        sum("TotalSales1").alias("TotalSales1"),
        sum("OrderQty").alias("TotalQuantity"),
        count("SalesOrderID").alias("NumberOfOrders")
    )


# COMMAND ----------

cleaned_sales_df.display()

# COMMAND ----------

aggregated_df.display()

# COMMAND ----------

aggregated_df.write.format("delta").save("/mnt/input_03/store_trans5")

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table ext_store_trans6
# MAGIC USING delta
# MAGIC OPTIONS (path 'abfss://input1@adlsgdme2344.dfs.core.windows.net/store_trans5')

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ext_store_trans6 values(29667,"Arun","Swamy",68142.6132,284,42)

# COMMAND ----------

# MAGIC %sql
# MAGIC update ext_store_trans6 set NumberOfOrders=33 where CustomerID=29929;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from ext_store_trans6 where CustomerID=30050;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history ext_store_trans6

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE ext_store_trans6  TO version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ext_store_trans6;

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize ext_store_trans6
# MAGIC zorder by (CustomerID);

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table ext_store_trans6
# MAGIC cluster by (CustomerID);
