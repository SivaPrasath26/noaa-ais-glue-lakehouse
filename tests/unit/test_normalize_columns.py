from utils.common_functions_raw import normalize_columns, COLUMN_MAPPING

# Test normalizing column names based on COLUMN_MAPPING
def test_normalize_columns(spark):
    df = spark.createDataFrame(
        [(1, 2, 3)],
        ["mmsi_number", "lat_val", "lon_val"]
    )
    out = normalize_columns(df)
    for src, dst in COLUMN_MAPPING.items():
        if src in df.columns:
            assert dst in out.columns
