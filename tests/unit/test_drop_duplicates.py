from utils.common_functions_raw import drop_duplicates

# Test dropping duplicate rows based on MMSI and BaseDateTime
def test_drop_duplicates(spark):
    df = spark.createDataFrame(
        [
            (111, "2024-01-01 10:00:00"),
            (111, "2024-01-01 10:00:00"),
            (222, "2024-01-01 11:00:00")
        ],
        ["MMSI", "BaseDateTime"]
    )
    out = drop_duplicates(df)
    assert out.count() == 2
