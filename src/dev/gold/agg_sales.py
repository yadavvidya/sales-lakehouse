import dlt
from pyspark.sql.functions import col, sum, to_date

@dlt.table(
    name="sales_daily_metrics",
    table_properties={
        "quality": "gold"
    }
)
def sales_daily_metrics():
    return (
        dlt.read("sales_clean")
        .withColumn("sales_date", to_date(col("ingest_time")))
        .groupBy(
            "sales_date",
            "country",
        )
        .agg(
            sum("amount").alias("total_sales")
        )
    )