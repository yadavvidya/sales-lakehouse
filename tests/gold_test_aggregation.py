"""Gold layer aggregation tests"""
import pytest
from pyspark.sql.functions import col, sum as spark_sum


def test_agg_by_region_item(spark):
    """Test sales_gold_by_region_item aggregations are correct"""
    df_gold = spark.read.table("sales_dev.sales_gold_by_region_item")
    df_silver = spark.read.table("sales_dev.sales_silver")
    
    # Assert gold table has data
    assert df_gold.count() > 0, "Gold table sales_gold_by_region_item is empty"
    
    # Assert grouping columns exist
    required_group_cols = ["Region", "Item Type", "Sales Channel"]
    for col_name in required_group_cols:
        assert col_name in df_gold.columns, f"Grouping column '{col_name}' missing"
    
    # Assert aggregation columns exist and are numeric
    agg_columns = ["total_revenue", "total_profit", "total_cost", 
                   "total_units_sold", "avg_unit_price", "order_count"]
    for col_name in agg_columns:
        assert col_name in df_gold.columns, f"Aggregation column '{col_name}' missing"
    
    # Assert no null values in aggregations
    for col_name in agg_columns:
        null_count = df_gold.filter(f"{col_name} IS NULL").count()
        assert null_count == 0, f"Column '{col_name}' has {null_count} null values"
    
    # Assert date columns exist
    assert "latest_order_date" in df_gold.columns, "latest_order_date missing"
    assert "earliest_order_date" in df_gold.columns, "earliest_order_date missing"
    
    # Verify aggregation logic for a sample group
    sample_group = df_gold.first()
    if sample_group:
        region = sample_group["Region"]
        item_type = sample_group["Item Type"]
        channel = sample_group["Sales Channel"]
        
        # Calculate expected values from silver
        silver_subset = df_silver.filter(
            (col("Region") == region) & 
            (col("Item Type") == item_type) & 
            (col("Sales Channel") == channel)
        )
        
        expected_revenue = silver_subset.agg(spark_sum("Total Revenue")).collect()[0][0]
        actual_revenue = sample_group["total_revenue"]
        
        # Assert values match (with floating point tolerance)
        assert abs(expected_revenue - actual_revenue) < 0.01, \
            f"Revenue mismatch for {region}/{item_type}/{channel}: expected {expected_revenue}, got {actual_revenue}"
    
    print("✅ Gold aggregation by region/item test passed!")


def test_agg_by_country(spark):
    """Test sales_gold_by_country aggregations are correct"""
    df_gold = spark.read.table("sales_dev.sales_gold_by_country")
    
    # Assert gold table has data
    assert df_gold.count() > 0, "Gold table sales_gold_by_country is empty"
    
    # Assert grouping columns
    required_group_cols = ["Region", "Country", "Sales Channel"]
    for col_name in required_group_cols:
        assert col_name in df_gold.columns, f"Grouping column '{col_name}' missing"
    
    # Assert aggregation columns
    agg_columns = ["total_revenue", "total_profit", "total_units_sold", "order_count"]
    for col_name in agg_columns:
        assert col_name in df_gold.columns, f"Aggregation column '{col_name}' missing"
        null_count = df_gold.filter(f"{col_name} IS NULL").count()
        assert null_count == 0, f"Column '{col_name}' has {null_count} null values"
    
    print("✅ Gold aggregation by country test passed!")


def test_gold_aggregation_totals(spark):
    """Test gold aggregations match silver totals"""
    df_gold_region = spark.read.table("sales_dev.sales_gold_by_region_item")
    df_silver = spark.read.table("sales_dev.sales_silver")
    
    # Calculate totals from gold
    gold_total_revenue = df_gold_region.agg(spark_sum("total_revenue")).collect()[0][0]
    gold_total_profit = df_gold_region.agg(spark_sum("total_profit")).collect()[0][0]
    
    # Calculate totals from silver
    silver_total_revenue = df_silver.agg(spark_sum("Total Revenue")).collect()[0][0]
    silver_total_profit = df_silver.agg(spark_sum("Total Profit")).collect()[0][0]
    
    # Assert totals match (with floating point tolerance)
    assert abs(gold_total_revenue - silver_total_revenue) < 0.01, \
        f"Total revenue mismatch: Gold={gold_total_revenue}, Silver={silver_total_revenue}"
    
    assert abs(gold_total_profit - silver_total_profit) < 0.01, \
        f"Total profit mismatch: Gold={gold_total_profit}, Silver={silver_total_profit}"
    
    print("✅ Gold aggregation totals test passed!")


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
    
    print("✅ Gold no duplicate groups test passed!")


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
    
    print("✅ Gold positive metrics test passed!")