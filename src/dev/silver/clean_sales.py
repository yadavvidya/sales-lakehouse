import dlt
from pyspark.sql.functions import current_timestamp, col, window, expr

@dlt.table(
    name="sales_clean",
    comment="Cleaned and typed sales data",
    table_properties={"quality": "silver",
                      "delta.enableChangeDataFeed": "true"
                      },
)
def sales_clean():
    return (
        dlt.read_stream("sales_raw_v2")
        .withWatermark("ingest_time", "10 minutes")
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("country", col("country").cast("string"))
        .withColumn("ingest_time", col("ingest_time").cast("date"))
    )

