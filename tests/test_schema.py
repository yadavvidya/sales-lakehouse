"""
Schema validation tests for sales pipeline
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

def get_expected_bronze_schema():
    """Expected schema for bronze layer"""
    return StructType([
        StructField("Region", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Item Type", StringType(), True),
        StructField("Sales Channel", StringType(), True),
        StructField("Order Priority", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Ship Date", StringType(), True),
        StructField("Units Sold", IntegerType(), True),
        StructField("Unit Price", DoubleType(), True),
        StructField("Unit Cost", DoubleType(), True),
        StructField("Total Revenue", DoubleType(), True),
        StructField("Total Cost", DoubleType(), True),
        StructField("Total Profit", DoubleType(), True),
        StructField("ingest_date", DateType(), True),
        StructField("ingest_time", TimestampType(), True),
        StructField("source_file", StringType(), True)
    ])

def get_expected_silver_schema():
    """Expected schema for silver layer"""
    return StructType([
        StructField("Region", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Item Type", StringType(), True),
        StructField("Sales Channel", StringType(), True),
        StructField("Order Priority", StringType(), True),
        StructField("Order Date", DateType(), True),
        StructField("Ship Date", DateType(), True),
        StructField("Units Sold", IntegerType(), True),
        StructField("Unit Price", DoubleType(), True),
        StructField("Unit Cost", DoubleType(), True),
        StructField("Total Revenue", DoubleType(), True),
        StructField("Total Cost", DoubleType(), True),
        StructField("Total Profit", DoubleType(), True),
        StructField("Order ID", StringType(), True)
    ])

def test_bronze_schema(spark_session, bronze_df):
    """Test bronze layer schema matches expected"""
    expected_schema = get_expected_bronze_schema()
    assert bronze_df.schema == expected_schema, "Bronze schema mismatch"

def test_silver_schema(spark_session, silver_df):
    """Test silver layer schema matches expected"""
    expected_schema = get_expected_silver_schema()
    assert silver_df.schema == expected_schema, "Silver schema mismatch"
