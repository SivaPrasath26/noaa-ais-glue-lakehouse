# common_functions.py
"""
Reusable transformation and validation utilities for NOAA AIS data processing.
All functions are designed for import and use in PySpark ETL scripts.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType


# ==============================================================
# Datetime Parsing and Partition Derivation
# ==============================================================

def parse_base_datetime(df: DataFrame, input_col: str = "BaseDateTime") -> DataFrame:
    """
    Convert BaseDateTime string column to timestamp and derive year, month, day partitions.
    """
    df = df.withColumn("BaseDateTime_UTC", F.to_timestamp(F.col(input_col)))
    df = df.withColumn("year", F.year("BaseDateTime_UTC"))
    df = df.withColumn("month", F.month("BaseDateTime_UTC"))
    df = df.withColumn("day", F.dayofmonth("BaseDateTime_UTC"))
    return df


# ==============================================================
# Data Cleaning Functions
# ==============================================================

def clean_coordinates(df: DataFrame, lat_col: str = "LAT", lon_col: str = "LON") -> DataFrame:
    """
    Filter out invalid or impossible latitude and longitude values.
    """
    return df.filter(
        (F.col(lat_col).between(-90, 90)) &
        (F.col(lon_col).between(-180, 180))
    )


def clean_sog_cog_heading(df: DataFrame) -> DataFrame:
    """
    Cap SOG, COG, and Heading values within realistic maritime bounds.
    """
    df = df.withColumn("SOG", F.when(F.col("SOG") > 100, 100).otherwise(F.col("SOG")))
    df = df.withColumn("COG", F.when(F.col("COG") > 360, 360).otherwise(F.col("COG")))
    df = df.withColumn("Heading", F.when(F.col("Heading") > 511, 511).otherwise(F.col("Heading")))
    return df


def replace_empty_with_null(df: DataFrame) -> DataFrame:
    """
    Replace empty strings with null values across all string columns.
    """
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)))
    return df


# ==============================================================
# Enrichment Functions
# ==============================================================

def derive_movement_flag(df: DataFrame, sog_col: str = "SOG") -> DataFrame:
    """
    Add binary movement flag column where 1 indicates vessel is moving (SOG > 0).
    """
    return df.withColumn("MovementFlag", F.when(F.col(sog_col) > 0, 1).otherwise(0))


def join_lookup(df: DataFrame, lookup_df: DataFrame, join_col: str, lookup_key: str, lookup_value: str, output_col: str) -> DataFrame:
    """
    Generic lookup join for mapping code columns to descriptive text.
    """
    return df.join(
        lookup_df.select(F.col(lookup_key).alias(join_col), F.col(lookup_value).alias(output_col)),
        on=join_col,
        how="left"
    )


# ==============================================================
# Validation and Quality Checks
# ==============================================================

def compute_summary_stats(df: DataFrame) -> DataFrame:
    """
    Compute simple count-based quality statistics for logging.
    """
    return df.select(
        F.count("*").alias("total_records"),
        F.countDistinct("MMSI").alias("unique_mmsi"),
        F.sum(F.when(F.col("LAT").isNull(), 1).otherwise(0)).alias("null_lat"),
        F.sum(F.when(F.col("LON").isNull(), 1).otherwise(0)).alias("null_lon")
    )


def drop_duplicates(df: DataFrame, subset_cols: list = ["MMSI", "BaseDateTime"]) -> DataFrame:
    """
    Remove duplicate AIS records based on MMSI and timestamp combination.
    """
    return df.dropDuplicates(subset=subset_cols)


# ==============================================================
# Utility
# ==============================================================

def cast_numeric_columns(df: DataFrame, columns: list, dtype=DoubleType()) -> DataFrame:
    """
    Cast specified columns to numeric type (default DoubleType).
    """
    for col_name in columns:
        df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    return df
