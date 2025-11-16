from utils.common_functions_raw import clean_coordinates

# Test cleaning coordinates with valid and invalid values
def test_clean_coordinates_valid_only(spark):
    df = spark.createDataFrame(
        [(10, 20), (200, 30), (40, -300)],
        ["LAT", "LON"]
    )
    out = clean_coordinates(df, quarantine_path=None)
    assert out.count() == 1
    assert out.collect()[0].LAT == 10
    assert out.collect()[0].LON == 20
