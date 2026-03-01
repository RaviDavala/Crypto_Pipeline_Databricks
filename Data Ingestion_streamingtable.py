import dlt
from pyspark.sql.functions import *

bronze_catalog = spark.conf.get("catalog_name")
bronze_schema = spark.conf.get("bronze_schema_name")

landing_path = "/Volumes/p1_crypto/bronze_layer/crypto_files_volume/raw"


@dlt.table(
    name=f"{bronze_catalog}.{bronze_schema}.bronze_crypto_streamtable",
    comment="Raw crypto API snapshots exploded"
)
def bronze_crypto_streamtable():

    df = spark.readStream.format("cloudFiles") \
         .option("cloudFiles.format", "json") \
         .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
         .option("cloudFiles.inferColumnTypes", "true") \
         .load(landing_path)

    # data already ARRAY → explode directly
    exploded = df.select(
        explode(col("data")).alias("coin")
    )

    # flatten
    final = exploded.select("coin.*")

    return final.withColumn("ingested_at", current_timestamp())