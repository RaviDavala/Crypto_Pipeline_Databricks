import dlt
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog_name")
bronze_schema = spark.conf.get("bronze_schema_name")
silver_schema = spark.conf.get("silver_schema_name")

@dlt.table(
    name=f"{catalog}.{silver_schema}.silver_crypto_streamtable",
    comment="cleaned data from bronze layer"
)
@dlt.expect_or_drop("id_not_null", "id IS NOT NULL")
@dlt.expect_or_drop("symbol_not_null", "symbol IS NOT NULL")
@dlt.expect_or_drop("name_not_null", "name IS NOT NULL")
@dlt.expect_or_drop("last_updated_not_null", "last_updated IS NOT NULL")
@dlt.expect_or_drop("price_positive", "current_price > 0")
@dlt.expect_or_drop("total_volume_positive", "total_volume > 0")
@dlt.expect_or_drop("marketCapRank_not_null", "market_cap_rank IS NOT NULL")
@dlt.expect_or_drop("marketCapRank_<_1000, >_0", "market_cap_rank <= 1000 and market_cap_rank > 0")

def silver_crypto_streamtable():

    df = spark.readStream.table(f"{catalog}.{bronze_schema}.bronze_crypto_streamtable")

    # Dropping unwanted columns from bronze table
    df = df.drop(
        "image","ath","roi","atl","fully_diluted_valuation","circulating_supply",
        "max_supply","ath_change_percentage","atl_change_percentage","ath_date","atl_date"
    )

    # Casting timestamp
    df = df.withColumn("last_updated", col("last_updated").cast("timestamp"))

    # Fill numeric nulls
    df = df.fillna(0, subset=[
        "market_cap","market_cap_rank","total_volume","high_24h","low_24h",
        "price_change_24h","price_change_percentage_24h","market_cap_change_24h",
        "market_cap_change_percentage_24h","total_supply"
    ])

    # Rounding values
    numerical_columns = [
        'market_cap','total_volume','high_24h','low_24h','price_change_24h',
        'price_change_percentage_24h','market_cap_change_24h',
        'market_cap_change_percentage_24h','current_price'
    ]

    for c in numerical_columns:
        df = df.withColumn(c, round(col(c),2))

    # Normalize strings
    for c in ["id","symbol","name"]:
        df = df.withColumn(c, lower(trim(col(c))))

    df.dropDuplicates()

    return df