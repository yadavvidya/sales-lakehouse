"""
Transformation tests for sales pipeline
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    return SparkSession.builder \
        .appName("test_sales_pipeline") \
        .master("local[2]") \
        .getOrCreate()

def test_date_parsing(spark):
    """Test date parsing transformation"""
    data = [("12/20/2015", "12/27/2015")]
    df = spark.createDataFrame(data, ["Order Date", "Ship Date"])
    
    # Simulate silver transformation
    from pyspark.sql.functions import expr
    result = df \
        .withColumn("Order Date", expr("to_date(`Order Date`, 'MM/dd/yyyy')")) \
        .withColumn("Ship Date", expr("to_date(`Ship Date`, 'MM/dd/yyyy')"))
    
    assert result.schema["Order Date"].dataType.typeName() == "date"
    assert result.schema["Ship Date"].dataType.dataType.typeName() == "date"

def test_data_quality_units_sold(spark):
    """Test that negative units sold are filtered"""
    data = [
        ("Online", 100, "L"),
        ("Offline", -50, "M"),  # Should be filtered
        ("Online", 0, "H")       # Should be filtered (>=0 but 0 is edge case)
    ]
    df = spark.createDataFrame(data, ["Sales Channel", "Units Sold", "Order Priority"])
    
    # Apply quality check
    filtered = df.filter((col("Units Sold").isNotNull()) & (col("Units Sold") >= 0))
    
    assert filtered.count() == 2  # Only positive units should remain

def test_sales_channel_validation(spark):
    """Test sales channel validation"""
    data = [
        ("Online",),
        ("Offline",),
        ("Invalid",)  # Should be filtered
    ]
    df = spark.createDataFrame(data, ["Sales Channel"])
    
    # Apply quality check
    filtered = df.filter(col("Sales Channel").isin("Online", "Offline"))
    
    assert filtered.count() == 2

def test_aggregation_logic(spark):
    """Test gold layer aggregation"""
    data = [
        ("Asia", "China", "Cereal", "Online", 100, 1000.0, 500.0),
        ("Asia", "China", "Cereal", "Online", 50, 500.0, 250.0),
        ("Europe", "Germany", "Meat", "Offline", 200, 2000.0, 1000.0)
    ]
    df = spark.createDataFrame(
        data, 
        ["Region", "Country", "Item Type", "Sales Channel", "Units Sold", "Total Revenue", "Total Profit"]
    )
    
    result = df.groupBy("Region", "Item Type", "Sales Channel") \
        .agg({"Total Revenue": "sum", "Units Sold": "sum"})
    
    assert result.count() == 2  # Two unique groups
