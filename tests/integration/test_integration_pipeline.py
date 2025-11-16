from utils.common_functions import (
    parse_base_datetime,
    clean_coordinates,
    clean_sog_cog_heading,
    replace_empty_with_null,
    derive_movement_flag,
    drop_duplicates
)

# Integration test combining multiple functions
def test_integration_pipeline(spark):
    df = spark.createDataFrame(
        [
            ("2024-01-01 10:00:00", 10, 20, 5, 90, 200, "SHIPA", 111),
            ("2024-01-01 10:00:00", 10, 20, 5, 90, 200, "SHIPA", 111),   # duplicate
            ("2024-01-02 12:00:00", 500, 20, 5, 90, 200, "SHIPB", 222), # invalid LAT
            ("2024-01-02 10:00:00", 10, -500, 5, 90, 200, "SHIPC", 333) # invalid LON
        ],
        ["BaseDateTime","LAT","LON","SOG","COG","Heading","CallSign","MMSI"]
    )

    df = replace_empty_with_null(df)
    df = parse_base_datetime(df)
    df = clean_coordinates(df, quarantine_path=None)
    df = clean_sog_cog_heading(df)
    df = drop_duplicates(df)
    df = derive_movement_flag(df)

    assert df.count() == 1                      # Only one valid, non-duplicate row should remain 
    row = df.collect()[0]
    assert row.MMSI == 111
    assert row.MovementFlag == 1
    assert row.year == "2024"
    assert row.month == "01"
    assert row.day == "01"
