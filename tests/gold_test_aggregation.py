from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max, min as spark_min, expr


def test_aggregation_logic():
    """Test gold layer aggregation logic on sample data"""
    # Read and transform sample data
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("tests/data/sample_sales_data.csv")
    
    # Apply silver transformations
    silver_df = df \
        .withColumn("Order ID", col("Order ID").cast("long")) \
        .withColumn("Order Date", expr("to_date(`Order Date`, 'MM/dd/yyyy')")) \
        .withColumn("Ship Date", expr("to_date(`Ship Date`, 'MM/dd/yyyy')")) \
        .withColumn("Units Sold", col("Units Sold").cast("integer")) \
        .withColumn("Unit Price", col("Unit Price").cast("double")) \
        .withColumn("Total Revenue", col("Total Revenue").cast("double")) \
        .withColumn("Total Profit", col("Total Profit").cast("double")) \
        .withColumn("Total Cost", col("Total Cost").cast("double"))
    
    # Apply gold aggregation (sales_gold_by_region_item logic)
    gold_df = silver_df.groupBy("Region", "Item Type", "Sales Channel").agg(
        spark_sum("Total Revenue").alias("total_revenue"),
        spark_sum("Total Profit").alias("total_profit"),
        spark_sum("Total Cost").alias("total_cost"),
        spark_sum("Units Sold").alias("total_units_sold"),
        avg("Unit Price").alias("avg_unit_price"),
        count("Order ID").alias("order_count"),
        spark_max("Order Date").alias("latest_order_date"),
        spark_min("Order Date").alias("earliest_order_date")
    )
    
    # Assertions
    assert gold_df.count() > 0, "Aggregation produced no results"
    
    # Check all aggregation columns exist
    expected_cols = [
        "total_revenue", "total_profit", "total_cost", "total_units_sold", 
        "avg_unit_price", "order_count", "latest_order_date", "earliest_order_date"
    ]
    for col_name in expected_cols:
        assert col_name in gold_df.columns, f"Missing column: {col_name}"
    
    # Check no nulls in aggregations
    for col_name in ["total_revenue", "total_profit", "order_count"]:
        assert gold_df.filter(f"{col_name} IS NULL").count() == 0, f"{col_name} has null values"
    
    # Verify totals match
    silver_total = silver_df.agg(spark_sum("Total Revenue")).collect()[0][0]
    gold_total = gold_df.agg(spark_sum("total_revenue")).collect()[0][0]
    assert abs(silver_total - gold_total) < 0.01, "Revenue totals don't match between silver and gold"
    
    print("Gold aggregation logic test passed!")


# Run the test
test_aggregation_logic()
print("All aggregation tests passed!")


def test_gold_no_duplicate_groups(spark):
    """Test gold tables have no duplicate group combinations"""
    # Test region/item table
    df_region = spark.read.table("sales_dev.sales_gold_by_region_item")
    total_count = df_region.count()
    distinct_count = df_region.select("Region", "Item Type", "Sales Channel").distinct().count()
    assert total_count == distinct_count, \
        f"Duplicate groups in sales_gold_by_region_item: {total_count} rows but {distinct_count} distinct groups"
    
    # Test country table
    df_country = spark.read.table("sales_dev.sales_gold_by_country")
    total_count = df_country.count()
    distinct_count = df_country.select("Region", "Country", "Sales Channel").distinct().count()
    assert total_count == distinct_count, \
        f"Duplicate groups in sales_gold_by_country: {total_count} rows but {distinct_count} distinct groups"
    
    print("Gold no duplicate groups test passed!")


def test_gold_positive_metrics(spark):
    """Test gold aggregation metrics are positive or zero"""
    df_gold = spark.read.table("sales_dev.sales_gold_by_region_item")
    
    # Assert no negative revenues
    negative_revenue = df_gold.filter("total_revenue < 0")
    assert negative_revenue.count() == 0, f"Found {negative_revenue.count()} records with negative revenue"
    
    # Assert no negative units
    negative_units = df_gold.filter("total_units_sold < 0")
    assert negative_units.count() == 0, f"Found {negative_units.count()} records with negative units"
    
    # Assert order count is positive
    zero_orders = df_gold.filter("order_count <= 0")
    assert zero_orders.count() == 0, f"Found {zero_orders.count()} groups with zero or negative orders"
    
    print(" Gold positive metrics test passed!")