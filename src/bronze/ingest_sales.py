import dlt
from pyspark.sql.functions import current_timestamp, col

ENV = spark.conf.get("env")

RAW_PATH = spark.conf.get("s3.raw.path")

SCHEMA_LOCATION = f"/Volumes/sales/sales_{ENV}/_schemas"


@dlt.table(
    name="sales_bronze",
    comment="Raw sales data ingested from S3 using Auto Loader",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["ingest_date"]
)
@dlt.expect_or_drop("valid_units_sold", "`Units Sold` IS NOT NULL AND `Units Sold` >=0")
@dlt.expect_or_drop("valid_sales_channel", "`Sales Channel` IN ('Online', 'Offline')")
@dlt.expect_or_drop("valid_priority","`Order Priority` IN ('L','M','H','C')")
@dlt.expect_or_drop("valid_order_date","`Order Date` IS NOT NULL")
@dlt.expect_or_drop("valid_ship_date","`Ship Date` IS NOT NULL")
@dlt.expect_or_drop("valid_order_id","`Order ID` IS NOT NULL") 
def sales_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(RAW_PATH)
        .withColumn("ingest_date", current_timestamp().cast("date"))
        .withColumn("ingest_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withWatermark("ingest_time", "10 minutes")
    )

@dlt.table(
    name="sales_quarantine",
    comment="Records failing quality checks or schema validation"
)
def sales_quarantine():
    return (
        dlt.read_stream("sales_bronze")
        .filter(
            "_rescued_data IS NOT NULL OR `Units Sold` IS NULL OR `Units Sold` < 0"
        )
    )
