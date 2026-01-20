import dlt
from pyspark.sql.functions import current_timestamp, col, window, expr

@dlt.table(
    name="sales_clean",
    comment="Cleaned and typed sales data",
    table_properties={"quality": "silver"},
)
def sales_clean():
    return (
        dlt.read_stream("sales_raw_v2")
        .option("watermark", "ingest_time", "10 minutes")
        .groupBy(window(col("ingest_time"), "5 minutes", "10 minutes", "5 minutes"), col("country"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("country", col("country").cast("string"))
        .withColumn("ingest_time", col("ingest_time").cast("date"))
        .filter(
            col("order_time") >= current_timestamp() - expr("INTERVAL 2 DAYS")
                )
    )

