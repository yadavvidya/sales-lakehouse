import dlt
from pyspark.sql.functions import col, sum, to_date

@dlt.table(
    name="sales_gold",
    table_properties={
        "quality": "gold"
    }
)
def sales_daily_metrics():
    return (
        dlt.read("sales_silver")
        .withColumn("sales_date", to_date(col("Order Date")))
        .groupBy(
            "Items Type",
        )
        .agg(
            sum("Total Revenue").alias("Total Revenue of the product")
        )
    )