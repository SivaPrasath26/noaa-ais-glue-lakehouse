from utils.common_functions_raw import parse_base_datetime
from pyspark.sql import functions as F

# Test parsing valid BaseDateTime
def test_parse_base_datetime_valid(spark):
    df = spark.createDataFrame(
        [("2024-01-01 10:00:00",)],
        ["BaseDateTime"]
    )
    out = parse_base_datetime(df)
    assert out.count() == 1
    assert "year" in out.columns
    assert out.select("year").first()[0] == "2024"

# Test parsing invalid BaseDateTime
def test_parse_base_datetime_invalid(spark):
    df = spark.createDataFrame(
        [("not-a-date",)],
        ["BaseDateTime"]
    )
    out = parse_base_datetime(df)
    assert out.count() == 0

# Test parsing mixed valid and invalid BaseDateTime
def test_parse_base_datetime_mixed(spark):
    df = spark.createDataFrame(
        [("2024-01-01 10:00:00",), ("invalid-date",)],
        ["BaseDateTime"]
    )
    out = parse_base_datetime(df)
    assert out.count() == 1
    assert out.select("year").first()[0] == "2024"

# Test parsing empty BaseDateTime
def test_parse_base_datetime_empty(spark):
    df = spark.createDataFrame(
        [("",)],
        ["BaseDateTime"]
    )
    out = parse_base_datetime(df)
    assert out.count() == 0

# Test parsing null BaseDateTime
def test_parse_base_datetime_null(spark):
    df = spark.createDataFrame(
        [(None,)],
        ["BaseDateTime"]
    )
    out = parse_base_datetime(df)
    assert out.count() == 0