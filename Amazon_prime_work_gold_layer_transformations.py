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
    "https://login.microsoftonline.com//oauth2/token",
)

# COMMAND ----------

dbutils.fs.ls("abfss://silver@storageaccountforamzn.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# reading the parquet file in the silver container
df_gold = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        "abfss://silver@storageaccountforamzn.dfs.core.windows.net/amazon_prime_title_silver.csv"
    )
)


# COMMAND ----------

df_gold.display()

# COMMAND ----------

# changing the date format to a cohesive date type
df_gold = df_gold.withColumn("date_added", to_date(df_gold["date_added"], "MM/dd/yyyy"))


# COMMAND ----------

# breaking down the genre column delimited values to seperate columns
df_gold = df_gold.withColumn("genre1", split(df_gold["genre"], ",").getItem(0))
df_gold = df_gold.withColumn("genre2", split(df_gold["genre"], ",").getItem(1))

display(df_gold)

df_gold.display()

# COMMAND ----------

df_gold.write.format("delta").mode("append").option(
    "path",
    "abfss://gold@storageaccountforamzn.dfs.core.windows.net/amazon_prime_title_gold",
).save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database gold_layer;

# COMMAND ----------

df_gold.write.format("delta").mode("append").option(
    "path", "abfss://gold@storageaccountforamzn.dfs.core.windows.net/prime_gold"
).saveAsTable("gold_layer.prime_gold")
