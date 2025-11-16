import pytest
from pyspark.sql import SparkSession

# Fixture to provide a Spark session for tests
@pytest.fixture(scope="session")
def spark():
    try:
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("tests")
            .getOrCreate()
        )
        return spark
    except Exception as e:
        print("spark init error:", e)
        raise
