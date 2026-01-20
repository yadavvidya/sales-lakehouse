import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="sales_clean",
    comment="Cleaned and typed sales data",
    table_properties={"quality": "silver"},
)
def sales_clean():
    return (
        dlt.read_stream("sales_raw_v2")
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("ingest_time", col("ingest_time").cast("date"))
    )

