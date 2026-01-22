# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation Tests
# MAGIC Tests date parsing transformations

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.types import DateType

# COMMAND ----------

def test_date_parsing():
    """Test date parsing transformation works correctly"""
    # Sample data with date strings
    data = [("12/20/2015", "12/27/2015"), ("7/20/2015", "7/27/2015"), ("5/13/2010", "6/15/2010")]
    df = spark.createDataFrame(data, ["Order Date", "Ship Date"])
    
    # Apply transformation
    result = df \
        .withColumn("Order Date", expr("to_date(`Order Date`, 'MM/dd/yyyy')")) \
        .withColumn("Ship Date", expr("to_date(`Ship Date`, 'MM/dd/yyyy')"))
    
    # Assert transformation worked
    assert result.filter("`Order Date` IS NULL").count() == 0, "Date parsing failed for Order Date"
    assert result.filter("`Ship Date` IS NULL").count() == 0, "Date parsing failed for Ship Date"
    
    # Assert correct type
    schema = result.schema
    assert isinstance([f.dataType for f in schema.fields if f.name == "Order Date"][0], DateType)
    assert isinstance([f.dataType for f in schema.fields if f.name == "Ship Date"][0], DateType)
    
    print("Date parsing test passed!")

# COMMAND ----------

# Run the test
test_date_parsing()
print("All transformation tests passed!")


