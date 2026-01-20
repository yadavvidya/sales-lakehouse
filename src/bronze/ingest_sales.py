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
def sales_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/sales/sales_bronze/_schemas")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://sales-lakehouse-vidya/raw/sales/")
        .withColumn("ingest_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )