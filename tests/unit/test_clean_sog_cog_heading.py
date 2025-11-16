from utils.common_functions_raw import clean_sog_cog_heading

# Test cleaning SOG, COG, and Heading with out-of-bounds values
def test_clean_sog_cog_heading(spark):
    df = spark.createDataFrame(
        [(150, 500, 900)],
        ["SOG", "COG", "Heading"]
    )
    out = clean_sog_cog_heading(df)
    r = out.collect()[0]
    assert r.SOG == 100
    assert r.COG == 360
    assert r.Heading == 511
