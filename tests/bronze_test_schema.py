"""Bronze layer schema and data quality tests"""
import pytest
from pyspark.sql.types import DateType, TimestampType, StringType, IntegerType, DoubleType


def test_bronze_required_columns(spark):
    """Test all required columns exist in bronze layer"""
    df = spark.read.table("sales_dev.sales_bronze")

    required_columns = [
        "Order ID",
        "Region",
        "Country",
        "Item Type",
        "Sales Channel",
        "Order Priority",
        "Order Date",
        "Ship Date",
        "Units Sold",
        "Unit Price",
        "Unit Cost",
        "Total Revenue",
        "Total Cost",
        "Total Profit",
        "ingest_date",
        "ingest_time",
        "source_file"
    ]

    for col in required_columns:
        assert col in df.columns, f"Required column '{col}' missing from bronze layer"
    
    # Assert column count
    assert len(df.columns) >= len(required_columns), f"Expected at least {len(required_columns)} columns"
    
    print("✅ Bronze required columns test passed!")


def test_bronze_metadata_columns(spark):
    """Test bronze layer metadata columns have correct types and values"""
    df = spark.read.table("sales_dev.sales_bronze")
    schema = df.schema
    field_types = {field.name: field.dataType for field in schema.fields}
    
    # Assert metadata column types
    assert isinstance(field_types.get("ingest_date"), DateType), "ingest_date should be DateType"
    assert isinstance(field_types.get("ingest_time"), TimestampType), "ingest_time should be TimestampType"
    assert isinstance(field_types.get("source_file"), StringType), "source_file should be StringType"
    
    # Assert no nulls in metadata
    assert df.filter("ingest_date IS NULL").count() == 0, "ingest_date has null values"
    assert df.filter("ingest_time IS NULL").count() == 0, "ingest_time has null values"
    assert df.filter("source_file IS NULL").count() == 0, "source_file has null values"
    
    # Assert source_file contains valid path
    source_files = df.select("source_file").distinct().collect()
    for row in source_files:
        assert row[0] is not None and len(row[0]) > 0, "source_file should not be empty"
    
    print("✅ Bronze metadata columns test passed!")


def test_bronze_data_quality_expectations(spark):
    """Test bronze layer enforces data quality expectations"""
    df = spark.read.table("sales_dev.sales_bronze")
    
    # Assert DLT expectations are enforced
    # No null or negative Units Sold
    bad_units = df.filter("`Units Sold` IS NULL OR `Units Sold` < 0")
    assert bad_units.count() == 0, f"Found {bad_units.count()} records with invalid Units Sold"
    
    # Valid Sales Channel
    bad_channel = df.filter("`Sales Channel` NOT IN ('Online', 'Offline')")
    assert bad_channel.count() == 0, f"Found {bad_channel.count()} records with invalid Sales Channel"
    
    # Valid Order Priority
    bad_priority = df.filter("`Order Priority` NOT IN ('L', 'M', 'H', 'C')")
    assert bad_priority.count() == 0, f"Found {bad_priority.count()} records with invalid Order Priority"
    
    # No null Order Date
    bad_order_date = df.filter("`Order Date` IS NULL")
    assert bad_order_date.count() == 0, f"Found {bad_order_date.count()} records with null Order Date"
    
    # No null Ship Date
    bad_ship_date = df.filter("`Ship Date` IS NULL")
    assert bad_ship_date.count() == 0, f"Found {bad_ship_date.count()} records with null Ship Date"
    
    # No null Order ID
    bad_order_id = df.filter("`Order ID` IS NULL")
    assert bad_order_id.count() == 0, f"Found {bad_order_id.count()} records with null Order ID"
    
    print("✅ Bronze data quality expectations test passed!")


def test_bronze_partition_columns(spark):
    """Test bronze layer is properly partitioned"""
    df = spark.read.table("sales_dev.sales_bronze")
    
    # Assert partition column exists
    assert "ingest_date" in df.columns, "Partition column 'ingest_date' missing"
    
    # Assert partition column has values
    partition_count = df.select("ingest_date").distinct().count()
    assert partition_count > 0, "No partitions found in bronze layer"
    
    print(f"✅ Bronze partition test passed! Found {partition_count} partition(s)")
    