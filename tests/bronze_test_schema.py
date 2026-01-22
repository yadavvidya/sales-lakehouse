def test_data_quality_rules():
    """Test that sample data meets all bronze layer DLT expectations"""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("tests/data/sample_sales_data.csv")
    
    # valid_units_sold: Units Sold IS NOT NULL AND Units Sold >=0
    bad_units = df.filter("`Units Sold` IS NULL OR `Units Sold` < 0")
    assert bad_units.count() == 0, f"Found {bad_units.count()} records violating valid_units_sold expectation"
    
    # valid_sales_channel: Sales Channel IN ('Online', 'Offline')
    bad_channel = df.filter("`Sales Channel` NOT IN ('Online', 'Offline')")
    assert bad_channel.count() == 0, f"Found {bad_channel.count()} records violating valid_sales_channel expectation"
    
    # valid_priority: Order Priority IN ('L','M','H','C')
    bad_priority = df.filter("`Order Priority` NOT IN ('L', 'M', 'H', 'C')")
    assert bad_priority.count() == 0, f"Found {bad_priority.count()} records violating valid_priority expectation"
    
    # valid_order_date: Order Date IS NOT NULL
    bad_order_date = df.filter("`Order Date` IS NULL")
    assert bad_order_date.count() == 0, f"Found {bad_order_date.count()} records violating valid_order_date expectation"
    
    # valid_ship_date: Ship Date IS NOT NULL
    bad_ship_date = df.filter("`Ship Date` IS NULL")
    assert bad_ship_date.count() == 0, f"Found {bad_ship_date.count()} records violating valid_ship_date expectation"
    
    # valid_order_id: Order ID IS NOT NULL
    bad_order_id = df.filter("`Order ID` IS NULL")
    assert bad_order_id.count() == 0, f"Found {bad_order_id.count()} records violating valid_order_id expectation"
    
    print("All bronze layer DLT expectations passed!")


# Run the test
test_data_quality_rules()
print("All data quality tests passed!")
    