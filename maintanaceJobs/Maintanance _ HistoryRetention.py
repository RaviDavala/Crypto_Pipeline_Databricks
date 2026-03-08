# Databricks notebook source
# MAGIC %sql
# MAGIC delete from p1_crypto.bronze_layer.bronze_crypto_streamtable
# MAGIC where ingested_at < current_timestamp() - INTERVAL 60 DAYS;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from p1_crypto.silver_layer.silver_crypto_streamtable
# MAGIC where ingested_at < current_timestamp() - INTERVAL 90 DAYS;

# COMMAND ----------

from datetime import datetime, timedelta

# The json files from the API location in databricks:
path = "/Volumes/p1_crypto/bronze_layer/crypto_files_volume/raw/"

# 60 days retention logic
threshold_date = datetime.now() - timedelta(days=60)

# ls will get the list of all files in the path
files = dbutils.fs.ls(path)

#loop iterates for each file to check whether to remove or keep it
total_files = 0
for file in files:
    file_mod_time = datetime.fromtimestamp(file.modificationTime / 1000)
    total_files += 1
    if file_mod_time < threshold_date:
        print(f"Deleting: {file.path}")
        dbutils.fs.rm(file.path)
    else:
        print(f"Keeping: {file.path}")
print(f"Total files: {total_files}")