# Databricks notebook source
# MAGIC %sql
# MAGIC vacuum p1_crypto.bronze_layer.bronze_crypto_streamtable retain 168 hours;

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum p1_crypto.silver_layer.silver_crypto_streamtable retain 168 hours;