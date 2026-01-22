from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, DateType
from pyspark.sql.functions import col, expr

# Expected types for silver layer after transformations
Expected_types = {
    "Region": StringType(),
    "Country": StringType(),
    "Item Type": StringType(),
    "Sales Channel": StringType(),
    "Order Priority": StringType(),
    "Order Date": DateType(),
    "Order ID": LongType(),
    "Ship Date": DateType(),
    "Units Sold": IntegerType(),
    "Unit Price": DoubleType(),
    "Unit Cost": DoubleType(),
    "Total Revenue": DoubleType(),
    "Total Cost": DoubleType(),
    "Total Profit": DoubleType()
}

def test_silver_schema():
    """Test that silver layer schema matches expected types after transformations"""
    # Read raw CSV
    df = spark.read.csv("tests/data/sample_sales_data.csv", header=True, inferSchema=True)
    
    # Apply silver layer transformations (matching clean_sales.py)
    silver_df = df \
        .withColumn("Region", col("Region").cast("string")) \
        .withColumn("Country", col("Country").cast("string")) \
        .withColumn("Item Type", col("Item Type").cast("string")) \
        .withColumn("Sales Channel", col("Sales Channel").cast("string")) \
        .withColumn("Order Priority", col("Order Priority").cast("string")) \
        .withColumn("Order Date", expr("to_date(`Order Date`, 'MM/dd/yyyy')")) \
        .withColumn("Ship Date", expr("to_date(`Ship Date`, 'MM/dd/yyyy')")) \
        .withColumn("Units Sold", col("Units Sold").cast("integer")) \
        .withColumn("Unit Price", col("Unit Price").cast("double")) \
        .withColumn("Unit Cost", col("Unit Cost").cast("double")) \
        .withColumn("Total Revenue", col("Total Revenue").cast("double")) \
        .withColumn("Total Cost", col("Total Cost").cast("double")) \
        .withColumn("Total Profit", col("Total Profit").cast("double")) \
        .withColumn("Order ID", col("Order ID").cast("long"))
    
    # Validate schema
    for field in silver_df.schema.fields:
        if field.name in Expected_types:
            assert field.dataType == Expected_types[field.name], \
                f"Type mismatch for {field.name}: expected {Expected_types[field.name]}, got {field.dataType}"
    
    # Ensure all expected fields are present
    actual_fields = {field.name for field in silver_df.schema.fields}
    expected_fields = set(Expected_types.keys())
    missing_fields = expected_fields - actual_fields
    assert not missing_fields, f"Missing fields in schema: {missing_fields}"
    
    print("Silver schema validation passed!")

# Run the test
test_silver_schema()
print("All silver schema tests passed!")       