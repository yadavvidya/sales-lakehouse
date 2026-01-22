import dlt
from pyspark.sql.functions import current_timestamp, col, window, expr

@dlt.table(
    name="sales_silver",
    comment="Cleaned and typed sales data",
    table_properties={"quality": "silver",
                      "delta.enableChangeDataFeed": "true"
                      },
)
def sales_clean():
    return (
        dlt.read_stream("sales_bronze")
        .withWatermark("ingest_time", "10 minutes")
        .withColumn("Region", col("Region").cast("string"))
        .withColumn("Country", col("Country").cast("string"))
        .withColumn("Item Type", col("Item Type").cast("string"))
        .withColumn("Sales Channel", col("Sales Channel").cast("string"))
        .withColumn("Order Priority", col("Order Priority").cast("string"))
        .withColumn("Order Date", expr("to_date(`Order Date`, 'MM/dd/yyyy')"))
        .withColumn("Ship Date", expr("to_date(`Ship Date`, 'MM/dd/yyyy')"))
        .withColumn("Units Sold", col("Units Sold").cast("integer"))
        .withColumn("Unit Price", col("Unit Price").cast("double"))
        .withColumn("Unit Cost", col("Unit Cost").cast("double"))
        .withColumn("Total Revenue", col("Total Revenue").cast("double"))
        .withColumn("Total Cost", col("Total Cost").cast("double"))
        .withColumn("Total Profit", col("Total Profit").cast("double"))
        .withColumn("Order ID", col("Order ID").cast("string"))
    )

