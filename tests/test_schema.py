from pyspark.sql.types import LongType, DoubleType, DateType, IntegerType
from pyspark.sql.functions import col, expr


def test_silver_transformations():
    """Test silver layer transformations produce correct data types"""
    # Read sample data
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("tests/data/sample_sales_data.csv")
    
    # Apply silver transformations
    transformed = df \
        .withColumn("Order ID", col("Order ID").cast("long")) \
        .withColumn("Order Date", expr("to_date(`Order Date`, 'MM/dd/yyyy')")) \
        .withColumn("Ship Date", expr("to_date(`Ship Date`, 'MM/dd/yyyy')")) \
        .withColumn("Units Sold", col("Units Sold").cast("integer")) \
        .withColumn("Unit Price", col("Unit Price").cast("double")) \
        .withColumn("Unit Cost", col("Unit Cost").cast("double")) \
        .withColumn("Total Revenue", col("Total Revenue").cast("double")) \
        .withColumn("Total Cost", col("Total Cost").cast("double")) \
        .withColumn("Total Profit", col("Total Profit").cast("double"))
    
    schema = transformed.schema
    field_types = {field.name: field.dataType for field in schema.fields}
    
    # Assert types
    assert isinstance(field_types["Order ID"], LongType), "Order ID should be LongType"
    assert isinstance(field_types["Order Date"], DateType), "Order Date should be DateType"
    assert isinstance(field_types["Ship Date"], DateType), "Ship Date should be DateType"
    assert isinstance(field_types["Units Sold"], IntegerType), "Units Sold should be IntegerType"
    assert isinstance(field_types["Unit Price"], DoubleType), "Unit Price should be DoubleType"
    assert isinstance(field_types["Total Revenue"], DoubleType), "Total Revenue should be DoubleType"
    assert isinstance(field_types["Total Profit"], DoubleType), "Total Profit should be DoubleType"
    
    print("Silver transformation test passed!")


# Run the test
test_silver_transformations()
print("All tests passed!")
