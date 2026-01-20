import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="sales_raw_v2",
    comment="Raw sales data ingested from S3 using Auto Loader",
    table_properties={"quality": "bronze",
                      "delta.autoOptimize.optimizeWrite":"true",
                      "delta.autoOptimize.autoCompact":"true"
                      },
)
@dlt.expect_all_or_drop({
    "amount_not_null": "amount IS NOT NULL",
    "amount_positve": "amount >= 0"
})
def sales_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "dbfs:/Volumes/sales/sales_dev/_schemas")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://sales-lakehouse-vidya/raw/sales/")
        .withColumn("ingest_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )

@dlt.table(
    name="sales_quarantine",
    comment="Bad records with rejection reason"
)
def sales_quarantine():
    return dlt.read_stream("sales_raw_v2").filter("_rescued_data IS NOT NULL")

