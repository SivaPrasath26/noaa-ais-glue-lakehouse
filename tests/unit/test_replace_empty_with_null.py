from utils.common_functions_raw import replace_empty_with_null
from pyspark.sql import functions as F

# Test replacing empty strings with nulls
def test_replace_empty_with_null(spark):
    df = spark.createDataFrame(
        [("", "A"), ("B", "")],
        ["col1", "col2"]
    )
    out = replace_empty_with_null(df)
    assert out.filter(F.col("col1").isNull()).count() == 1
    assert out.filter(F.col("col2").isNull()).count() == 1
