"""Schema validation tests for sales pipeline - All layers"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
import pytest


def test_bronze_schema_structure(spark):
    """Test bronze layer has all required columns with correct types"""
    expected_columns = {
        "Region", "Country", "Item Type", "Sales Channel", "Order Priority",
        "Order Date", "Order ID", "Ship Date", "Units Sold", "Unit Price",
        "Unit Cost", "Total Revenue", "Total Cost", "Total Profit",
        "ingest_date", "ingest_time", "source_file"
    }
    
    df = spark.read.table("sales_dev.sales_bronze")
    actual_columns = set(df.columns)
    
    # Assert all expected columns exist
    missing_columns = expected_columns - actual_columns
    assert not missing_columns, f"Missing columns in bronze: {missing_columns}"
    
    # Assert metadata columns exist
    assert "ingest_date" in actual_columns, "ingest_date column missing"
    assert "ingest_time" in actual_columns, "ingest_time column missing"
    assert "source_file" in actual_columns, "source_file column missing"
    
    # Assert no null values in required columns
    assert df.filter("`Order ID` IS NULL").count() == 0, "Order ID has null values"
    assert df.filter("`Order Date` IS NULL").count() == 0, "Order Date has null values"
    assert df.filter("`Ship Date` IS NULL").count() == 0, "Ship Date has null values"
    
    print("✅ Bronze schema structure validation passed!")


def test_silver_schema_structure(spark):
    """Test silver layer schema with proper data types"""
    df = spark.read.table("sales_dev.sales_silver")
    schema = df.schema
    
    # Assert column count
    assert len(schema.fields) >= 14, f"Expected at least 14 columns, got {len(schema.fields)}"
    
    # Build field type map
    field_types = {field.name: field.dataType for field in schema.fields}
    
    # Assert specific type requirements
    assert isinstance(field_types.get("Order Date"), DateType), "Order Date should be DateType"
    assert isinstance(field_types.get("Ship Date"), DateType), "Ship Date should be DateType"
    assert isinstance(field_types.get("Units Sold"), IntegerType), "Units Sold should be IntegerType"
    assert isinstance(field_types.get("Unit Price"), DoubleType), "Unit Price should be DoubleType"
    assert isinstance(field_types.get("Unit Cost"), DoubleType), "Unit Cost should be DoubleType"
    assert isinstance(field_types.get("Total Revenue"), DoubleType), "Total Revenue should be DoubleType"
    assert isinstance(field_types.get("Total Cost"), DoubleType), "Total Cost should be DoubleType"
    assert isinstance(field_types.get("Total Profit"), DoubleType), "Total Profit should be DoubleType"
    assert isinstance(field_types.get("Order ID"), StringType), "Order ID should be StringType"
    
    # Assert no null values in critical business columns
    assert df.filter("Region IS NULL").count() == 0, "Region has null values"
    assert df.filter("Country IS NULL").count() == 0, "Country has null values"
    assert df.filter("`Item Type` IS NULL").count() == 0, "Item Type has null values"
    
    print("✅ Silver schema structure validation passed!")


def test_gold_schema_structure(spark):
    """Test gold layer aggregation tables have correct structure"""
    # Test sales_gold_by_region_item
    df_region_item = spark.read.table("sales_dev.sales_gold_by_region_item")
    required_cols_region = {
        "Region", "Item Type", "Sales Channel", "total_revenue", 
        "total_profit", "total_cost", "total_units_sold", 
        "avg_unit_price", "order_count", "latest_order_date", "earliest_order_date"
    }
    
    actual_cols_region = set(df_region_item.columns)
    missing_cols = required_cols_region - actual_cols_region
    assert not missing_cols, f"Missing columns in sales_gold_by_region_item: {missing_cols}"
    
    # Assert aggregation columns are numeric
    schema = df_region_item.schema
    field_types = {field.name: field.dataType for field in schema.fields}
    
    assert isinstance(field_types.get("total_revenue"), DoubleType), "total_revenue should be DoubleType"
    assert isinstance(field_types.get("total_profit"), DoubleType), "total_profit should be DoubleType"
    assert isinstance(field_types.get("total_units_sold"), IntegerType), "total_units_sold should be IntegerType"
    assert isinstance(field_types.get("order_count"), IntegerType), "order_count should be IntegerType"
    
    # Test sales_gold_by_country
    df_country = spark.read.table("sales_dev.sales_gold_by_country")
    required_cols_country = {
        "Region", "Country", "Sales Channel", "total_revenue",
        "total_profit", "total_units_sold", "order_count"
    }
    
    actual_cols_country = set(df_country.columns)
    missing_cols = required_cols_country - actual_cols_country
    assert not missing_cols, f"Missing columns in sales_gold_by_country: {missing_cols}"
    
    print("✅ Gold schema structure validation passed!")

