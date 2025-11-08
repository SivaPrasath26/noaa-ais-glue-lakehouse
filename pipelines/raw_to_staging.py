# raw_to_processed.py
"""
ETL job for transforming NOAA AIS raw CSV data to processed analytics-ready Parquet.
Step 1: Read raw data, apply schema, and perform all cleaning transformations.
Lookup enrichment will be implemented in the next step.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.schema_definitions import SCHEMA_MAP
from utils.common_functions import (
    parse_base_datetime,
    clean_coordinates,
    clean_sog_cog_heading,
    replace_empty_with_null,
    derive_movement_flag,
    compute_summary_stats,
    drop_duplicates,
)

# ==============================================================
# Spark Session Initialization
# ==============================================================

def get_spark_session(app_name: str = "NOAA_AIS_Raw_To_Stagin") -> SparkSession:
    """
    Create and configure Spark session for ETL job.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark


# ==============================================================
# ETL Transformation Pipeline
# ==============================================================

def transform_raw_to_processed(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Perform cleaning and transformation on raw AIS data and write processed output.
    """
    # Read raw NOAA AIS CSV files with predefined schema
    df_raw = (
        spark.read
        .option("header", True)
        .schema(SCHEMA_MAP["raw"])
        .csv(input_path)
    )

    # Data cleaning
    df = replace_empty_with_null(df_raw)
    df = clean_coordinates(df)
    df = clean_sog_cog_heading(df)
    df = drop_duplicates(df)

    # Datetime parsing and partition derivation
    df = parse_base_datetime(df)

    # Derive movement flag
    df = derive_movement_flag(df)

    # Summary statistics for validation
    summary_df = compute_summary_stats(df)
    summary = summary_df.collect()[0].asDict()
    print("Summary statistics:", summary)

    # Write processed Parquet partitioned by year/month/day
    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )


# ==============================================================
# Job Entry Point
# ==============================================================

if __name__ == "__main__":
    spark = get_spark_session()
    input_s3_path = "s3://noaa-ais-raw-data/2024/"
    output_s3_path = "s3://noaa-ais-staging/ais_positions/"
    transform_raw_to_processed(spark, input_s3_path, output_s3_path)
    spark.stop()
