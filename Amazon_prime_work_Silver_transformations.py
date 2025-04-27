# Databricks notebook source
spark.conf.set(
    "fs.azure.account.auth.type.storageaccountforamzn.dfs.core.windows.net", "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.storageaccountforamzn.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.storageaccountforamzn.dfs.core.windows.net", ""
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.storageaccountforamzn.dfs.core.windows.net",
    "",
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.storageaccountforamzn.dfs.core.windows.net",
    "https://login.microsoftonline.com/e2e892af-9dfb-466c-ae3c-78849084b334/oauth2/token",
)

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@storageaccountforamzn.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Profiling

# COMMAND ----------

# MAGIC %md
# MAGIC Importing/loading data

# COMMAND ----------

df_silver = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        "abfss://bronze@storageaccountforamzn.dfs.core.windows.net/amazon_prime_titles.csv"
    )
)

df_silver.show()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

# summary statistics
df_silver.describe().display()


# COMMAND ----------

total_rows = df_silver.count()
print(total_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preprocessing

# COMMAND ----------

# check for duplicate rows
# Check for duplicate rows
duplicates = df_silver.groupBy(df_silver.columns).count().filter("count > 1")

# Display duplicates
duplicates.display()

# COMMAND ----------

# dropping the duplicate rows
df_silver = df_silver.dropDuplicates()
print(df_silver.count())

# COMMAND ----------

# remnaming the column title
df_silver = df_silver.withColumnRenamed("listed_in", "genre")

# COMMAND ----------

display(df_silver.limit(10))


# COMMAND ----------

df_silver.write.format("parquet").mode("append").option(
    "path",
    "abfss://silver@storageaccountforamzn.dfs.core.windows.net/amazon_prime_title_silver.csv",
).save()
