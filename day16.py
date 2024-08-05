# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df_001 =  spark.read.csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------

df_001.show()

# COMMAND ----------

df_001.display(5)

# COMMAND ----------

df1 = spark.read.option("header",True).csv("/FileStore/gmde/googleplaystore-2.csv")

# COMMAND ----------

df1.display(5)

# COMMAND ----------

df2 = spark.read.option("header",True).option("inferschema",True).csv("/FileStore/gmde/googleplaystore-2.csv")

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

sch = StructType().add("App",StringType(),True)\
                  .add("Category",StringType(),True)\
                  .add("Rating",DoubleType(),True)\
                  .add("Reviews",IntegerType(),True)\
                  .add("Size",StringType(),True)\
                  .add("Installs",StringType(),True)\
                  .add("Type",StringType(),True)\
                  .add("Price",StringType(),True)\
                  .add("Content Rating",StringType(),True)\
                  .add("Genre",StringType(),True)\
                  .add("Last Updated",StringType(),True)\
                  .add("Current Ver",StringType(),True)\
                  .add("Android Ver",StringType(),True)
                           



# COMMAND ----------

df1 = spark.read.option("header",True).schema(sch).csv("/FileStore/gmde/googleplaystore-2.csv")

# COMMAND ----------

df1.display()

# COMMAND ----------

zip_df_final= df1.withColumn("appgc",concat(col("App"),col("Genre"))).withColumn("newcol",col("Category")).withColumn("owner",lit("Gunesh"))

# COMMAND ----------

zip_df_final.display()

# COMMAND ----------

zip_df_02 = df1.withColumn("newcol",col("Category"))

# COMMAND ----------

zip_df_02.display()

# COMMAND ----------

zip_df_03 = df1.withColumn("owner",lit("Gunesh"))

# COMMAND ----------

zip_df_03.display()

# COMMAND ----------

zip_df_05 = df1.withColumnRenamed("Rating","Ratings")

# COMMAND ----------

zip_df_05.display()

# COMMAND ----------

df_gm_final = df1.select("App","Category","Rating")

# COMMAND ----------

df_gm_final.display()

# COMMAND ----------

gm_final2_de = df_gm_final.selectExpr('cast(Rating as Integer) as int_rating')

# COMMAND ----------

gm_final2_de.display()

# COMMAND ----------

new_df = df1.sort(col('Rating').desc())
new_df.display()

# COMMAND ----------

dis_df = df1.distinct()
dis_df.display()

# COMMAND ----------

df_ord = df1.orderBy(col("Category").desc())
df_ord.display()

# COMMAND ----------

dropdub = dis_df.dropDuplicates(["App","Last Updated"])
dropdub.display()
dropdub.count()

# COMMAND ----------

nnew = dropdub.withColumn(
    "Ratings",
    when((col("Rating") > 4.3) & (col("Rating") < 4.5), "Average")
    .when((col("Rating") > 4.5) & (col("Rating") < 4.8), "Good")
    .when(col("Rating") > 4.8, "Excellent")
    .otherwise("not at all satisfied")
)

# COMMAND ----------

nnew.display()

# COMMAND ----------

sch1 = StructType().add("App",StringType(),True)\
                  .add("Translated_Review",StringType(),True)\
                  .add("Sentiment",StringType(),True)\
                  .add("Reviews",StringType(),True)\
                  .add("Sentiment_Polarity",StringType(),True)\
                  .add("Sentiment_Subjectivity",DoubleType(),True)

# COMMAND ----------

df2 = spark.read.option("header",True).schema(sch1).csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------

df2.display()

# COMMAND ----------

df_final = df1.join(df2,df1.App==df2.App,how="left").select(df1['*'],df2["Sentiment"])

# COMMAND ----------

df_final.display()

# COMMAND ----------

df_final_group  = df_final.groupBy("App").avg("Rating")

# COMMAND ----------

df_final_group.display()
