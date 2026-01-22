import dlt
from pyspark.sql.functions import col, sum, avg, count, max as spark_max, min as spark_min

@dlt.table(
    name="sales_gold_by_region_item",
    comment="Aggregated sales metrics by region and item type",
    table_properties={
        "quality": "gold"
    }
)
def sales_by_region_item():
    return (
        dlt.read("sales_silver")
        .groupBy(
            "Region",
            "Item Type",
            "Sales Channel"
        )
        .agg(
            sum("Total Revenue").alias("total_revenue"),
            sum("Total Profit").alias("total_profit"),
            sum("Total Cost").alias("total_cost"),
            sum("Units Sold").alias("total_units_sold"),
            avg("Unit Price").alias("avg_unit_price"),
            count("Order ID").alias("order_count"),
            spark_max("Order Date").alias("latest_order_date"),
            spark_min("Order Date").alias("earliest_order_date")
        )
    )

@dlt.table(
    name="sales_gold_by_country",
    comment="Aggregated sales metrics by country",
    table_properties={
        "quality": "gold"
    }
)
def sales_by_country():
    return (
        dlt.read("sales_silver")
        .groupBy(
            "Region",
            "Country",
            "Sales Channel"
        )
        .agg(
            sum("Total Revenue").alias("total_revenue"),
            sum("Total Profit").alias("total_profit"),
            sum("Units Sold").alias("total_units_sold"),
            count("Order ID").alias("order_count")
        )
    )