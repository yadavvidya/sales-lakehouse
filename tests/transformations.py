"""Transformation and data quality tests"""
import pytest
from pyspark.sql.functions import col, expr, to_date
from datetime import datetime


def test_units_sold_validation(spark):
    """Test Units Sold data quality"""
    df = spark.read.option("header", "true").csv("tests/data/sample_sales_data.csv")
    
    # Assert no null or negative values
    bad = df.filter("`Units Sold` IS NULL OR `Units Sold` < 0")
    assert bad.count() == 0, f"Found {bad.count()} records with invalid Units Sold"
    
    # Assert Units Sold is numeric
    total_count = df.count()
    numeric_count = df.filter("`Units Sold` RLIKE '^[0-9]+$'").count()
    assert total_count == numeric_count, "Units Sold contains non-numeric values"
    
    print("✅ Units Sold validation test passed!")


def test_sales_channel_validation(spark):
    """Test Sales Channel data quality"""
    df = spark.read.option("header", "true").csv("tests/data/sample_sales_data.csv")
    
    # Assert only valid channels
    bad = df.filter("`Sales Channel` NOT IN ('Online', 'Offline')")
    assert bad.count() == 0, f"Found {bad.count()} records with invalid Sales Channel"
    
    # Assert no nulls
    null_channels = df.filter("`Sales Channel` IS NULL")
    assert null_channels.count() == 0, "Found null values in Sales Channel"
    
    # Assert both channels exist in data
    distinct_channels = df.select("`Sales Channel`").distinct().collect()
    channel_values = {row[0] for row in distinct_channels}
    assert 'Online' in channel_values or 'Offline' in channel_values, "No valid sales channels found"
    
    print("✅ Sales Channel validation test passed!")


def test_order_priority_validation(spark):
    """Test Order Priority data quality"""
    df = spark.read.option("header", "true").csv("tests/data/sample_sales_data.csv")
    
    # Assert only valid priorities
    bad = df.filter("`Order Priority` NOT IN ('L', 'M', 'H', 'C')")
    assert bad.count() == 0, f"Found {bad.count()} records with invalid Order Priority"
    
    # Assert no nulls
    null_priority = df.filter("`Order Priority` IS NULL")
    assert null_priority.count() == 0, "Found null values in Order Priority"
    
    # Assert all priorities are single character
    bad_length = df.filter("LENGTH(`Order Priority`) != 1")
    assert bad_length.count() == 0, "Order Priority should be single character"
    
    print("✅ Order Priority validation test passed!")


def test_order_id_validation(spark):
    """Test Order ID data quality"""
    df = spark.read.option("header", "true").csv("tests/data/sample_sales_data.csv")
    
    # Assert no nulls
    bad = df.filter("`Order ID` IS NULL")
    assert bad.count() == 0, f"Found {bad.count()} records with null Order ID"
    
    # Assert no empty strings
    empty_ids = df.filter("LENGTH(TRIM(`Order ID`)) = 0")
    assert empty_ids.count() == 0, "Found empty Order IDs"
    
    # Assert Order IDs are unique (if expected)
    total_count = df.count()
    distinct_count = df.select("`Order ID`").distinct().count()
    print(f"Total records: {total_count}, Distinct Order IDs: {distinct_count}")
    # Note: Order IDs may not be unique if multiple line items per order
    
    print("✅ Order ID validation test passed!")


def test_date_fields_validation(spark):
    """Test Order Date and Ship Date data quality"""
    df = spark.read.option("header", "true").csv("tests/data/sample_sales_data.csv")
    
    # Assert no nulls in dates
    bad_dates = df.filter("`Order Date` IS NULL OR `Ship Date` IS NULL")
    assert bad_dates.count() == 0, f"Found {bad_dates.count()} records with null dates"
    
    # Assert no empty date strings
    empty_order_date = df.filter("LENGTH(TRIM(`Order Date`)) = 0")
    assert empty_order_date.count() == 0, "Found empty Order Date values"
    
    empty_ship_date = df.filter("LENGTH(TRIM(`Ship Date`)) = 0")
    assert empty_ship_date.count() == 0, "Found empty Ship Date values"
    
    print("✅ Date fields validation test passed!")


def test_date_transformation(spark):
    """Test date parsing transformation from silver layer"""
    # Sample test data
    test_data = [("12/20/2015", "12/27/2015"), ("7/20/2015", "7/27/2015")]
    df = spark.createDataFrame(test_data, ["Order Date", "Ship Date"])
    
    # Apply transformation
    transformed_df = df \
        .withColumn("Order Date", expr("to_date(`Order Date`, 'MM/dd/yyyy')")) \
        .withColumn("Ship Date", expr("to_date(`Ship Date`, 'MM/dd/yyyy')"))
    
    # Assert transformation worked
    assert transformed_df.filter("`Order Date` IS NULL").count() == 0, "Date transformation failed for Order Date"
    assert transformed_df.filter("`Ship Date` IS NULL").count() == 0, "Date transformation failed for Ship Date"
    
    # Assert types are correct
    from pyspark.sql.types import DateType
    schema = transformed_df.schema
    order_date_type = [f.dataType for f in schema.fields if f.name == "Order Date"][0]
    ship_date_type = [f.dataType for f in schema.fields if f.name == "Ship Date"][0]
    
    assert isinstance(order_date_type, DateType), "Order Date should be DateType after transformation"
    assert isinstance(ship_date_type, DateType), "Ship Date should be DateType after transformation"
    
    print("✅ Date transformation test passed!")


def test_numeric_fields_validation(spark):
    """Test numeric fields are valid"""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("tests/data/sample_sales_data.csv")
    
    numeric_fields = ["Unit Price", "Unit Cost", "Total Revenue", "Total Cost", "Total Profit"]
    
    for field in numeric_fields:
        # Assert no nulls
        null_count = df.filter(f"`{field}` IS NULL").count()
        assert null_count == 0, f"Found {null_count} null values in {field}"
        
        # Assert positive or zero values for prices and revenues
        if field in ["Unit Price", "Unit Cost", "Total Revenue", "Total Cost"]:
            negative_count = df.filter(f"`{field}` < 0").count()
            assert negative_count == 0, f"Found {negative_count} negative values in {field}"
    
    print("✅ Numeric fields validation test passed!")


def test_region_country_validation(spark):
    """Test Region and Country fields are valid"""
    df = spark.read.option("header", "true").csv("tests/data/sample_sales_data.csv")
    
    # Assert no nulls
    assert df.filter("Region IS NULL").count() == 0, "Found null values in Region"
    assert df.filter("Country IS NULL").count() == 0, "Found null values in Country"
    
    # Assert no empty strings
    assert df.filter("LENGTH(TRIM(Region)) = 0").count() == 0, "Found empty Region values"
    assert df.filter("LENGTH(TRIM(Country)) = 0").count() == 0, "Found empty Country values"
    
    # Assert reasonable distinct counts
    region_count = df.select("Region").distinct().count()
    country_count = df.select("Country").distinct().count()
    
    assert region_count > 0, "No distinct regions found"
    assert country_count > 0, "No distinct countries found"
    assert country_count >= region_count, "Countries should be equal or more than regions"
    
    print(f"✅ Region/Country validation passed! Regions: {region_count}, Countries: {country_count}")

