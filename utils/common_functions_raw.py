"""
Reusable transformation and validation utilities for NOAA AIS data processing.
All functions are designed for import and use in AWS Glue or PySpark ETL scripts.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F, Window
from pyspark.sql.types import TimestampType, DoubleType, StringType, IntegerType
import math
from utils.config import setup_logger
from utils.column_mapping import COLUMN_MAPPING

# Initialize logger
logger = setup_logger(__name__)

# ============================================================== #
# Datetime Parsing and Partition Derivation                      #
# ============================================================== #

def parse_base_datetime(df: DataFrame, input_col: str = "BaseDateTime") -> DataFrame:
    """
    Parse BaseDateTime safely in place and derive year/month/day for partitioning.
    Handles both 'yyyy-MM-dd HH:mm:ss' and ISO 'yyyy-MM-ddTHH:mm:ss[.SSS][Z]'.
    Invalid timestamps are dropped.
    """
    try:
        #  DateTime parsing with multiple formats
        cleaned = F.regexp_replace(F.col(input_col), r"Z$", "")
        cleaned = F.regexp_extract(cleaned, r"^(.+?)([+-]\d{2}:\d{2})?$", 1)
        df = df.withColumn(
            input_col,
            F.coalesce(
                F.to_timestamp(cleaned, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                F.to_timestamp(cleaned, "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp(F.col(input_col), "yyyy-MM-dd HH:mm:ss")
            )
        )
        # Drop rows with invalid or null timestamps
        df = df.filter(F.col(input_col).isNotNull())

        # Derive partition columns
        df = df.withColumn("year", F.date_format(F.col(input_col), "yyyy"))
        df = df.withColumn("month", F.date_format(F.col(input_col), "MM"))
        df = df.withColumn("day", F.date_format(F.col(input_col), "dd"))

        return df

    except Exception as e:
        logger.error(f"Error parsing BaseDateTime column: {e}", exc_info=True)
        raise



# ============================================================== #
# Data Cleaning Functions                                        #
# ============================================================== #

def clean_coordinates(
    df: DataFrame,
    lat_col: str = "LAT",
    lon_col: str = "LON",
    quarantine_path: str | None = None
) -> DataFrame:
    """
    Filter out invalid or impossible latitude and longitude values.
    Writes invalid records to S3 if a quarantine path is provided.
    """
    try:
        condition = (F.col(lat_col).between(-90, 90)) & (F.col(lon_col).between(-180, 180))
        valid = df.filter(condition)
        invalid = df.filter(~condition)

        if quarantine_path:
            threshold = 100000
            invalid_count = invalid.limit(threshold + 1).count()

            if invalid_count > 0:
                # extract y/m/d for path
                if {"year", "month", "day"}.issubset(df.columns):
                    vals = df.select("year", "month", "day").first()
                    y, m, d = vals["year"], vals["month"], vals["day"]
                    quarantine_output = f"{quarantine_path.rstrip('/')}/year={y}/month={m}/day={d}/"
                else:
                    quarantine_output = f"{quarantine_path.rstrip('/')}/undated/"

                # threshold logic
                if invalid_count <= threshold:
                    invalid.coalesce(1).write.mode("overwrite").option("header", True).csv(quarantine_output)
                else:
                    invalid.write.mode("overwrite").option("header", True).csv(quarantine_output)

                logger.info(f"Invalid coordinate records written to {quarantine_output} ({invalid_count} rows)")
            else:
                logger.info("No invalid coordinate records to quarantine.")

        return valid

    except Exception as e:
        logger.error(f"Error cleaning coordinates: {e}", exc_info=True)
        raise


def clean_sog_cog_heading(df: DataFrame) -> DataFrame:
    """
    Cap SOG, COG, and Heading values within realistic maritime bounds.
    """
    try:
        df = df.withColumn("SOG", F.when(F.col("SOG") > 100, 100).otherwise(F.col("SOG")))
        df = df.withColumn("COG", F.when(F.col("COG") > 360, 360).otherwise(F.col("COG")))
        df = df.withColumn("Heading", F.when(F.col("Heading") > 511, 511).otherwise(F.col("Heading")))
        return df
    except Exception as e:
        logger.error(f"Error cleaning SOG/COG/Heading: {e}", exc_info=True)
        raise


def replace_empty_with_null(df: DataFrame) -> DataFrame:
    """
    Replace empty strings with null values across all string columns.
    """
    try:
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)))
        return df
    except Exception as e:
        logger.error(f"Error replacing empty strings with nulls: {e}", exc_info=True)
        raise

# ============================================================== #
# Column Normalization                                          #
# ============================================================== #

def normalize_columns(df: DataFrame) -> DataFrame:
    """
    Rename source columns based on COLUMN_MAPPING.
    Columns already matching schema names are skipped.
    """
    try:
        for src, target in COLUMN_MAPPING.items():
            if src in df.columns:
                df = df.withColumnRenamed(src, target)
        return df
    except Exception as e:
        logger.error(f"Error normalizing column names: {e}", exc_info=True)
        raise

# ============================================================== #
# Enrichment Functions                                           #
# ============================================================== #

def derive_movement_flag(df: DataFrame, sog_col: str = "SOG") -> DataFrame:
    """
    Add binary movement flag column where 1 indicates vessel is moving (SOG > 0).
    """
    try:
        return df.withColumn("MovementFlag", F.when(F.col(sog_col) > 0, 1).otherwise(0))
    except Exception as e:
        logger.error(f"Error deriving movement flag: {e}", exc_info=True)
        raise


def join_lookup(df: DataFrame, lookup_df: DataFrame, join_col: str, lookup_key: str, lookup_value: str, output_col: str) -> DataFrame:
    """
    Generic lookup join for mapping code columns to descriptive text.
    """
    try:
        return df.join(
            lookup_df.select(F.col(lookup_key).alias(join_col), F.col(lookup_value).alias(output_col)),
            on=join_col,
            how="left"
        )
    except Exception as e:
        logger.error(f"Error performing lookup join: {e}", exc_info=True)
        raise


# ============================================================== #
# Validation and Quality Checks                                  #
# ============================================================== #

def compute_summary_stats(df: DataFrame) -> DataFrame:
    """
    Compute simple count-based quality statistics for logging.
    """
    try:
        return df.select(
            F.count("*").alias("total_records"),
            F.countDistinct("MMSI").alias("unique_mmsi"),
            F.sum(F.when(F.col("LAT").isNull(), 1).otherwise(0)).alias("null_lat"),
            F.sum(F.when(F.col("LON").isNull(), 1).otherwise(0)).alias("null_lon")
        )
    except Exception as e:
        logger.error(f"Error computing summary statistics: {e}", exc_info=True)
        raise


def drop_duplicates(df: DataFrame) -> DataFrame:
    """
    Robust deduplication using a content-based hash key.
    Ensures duplicate rows are removed even across multiple files or reruns.
    """
    try:
        # Create a stable hash from all business columns
        from pyspark.sql.functions import sha2, concat_ws, struct, to_json
        
        deduped = df.withColumn(
            "dedupe_key",
            sha2(to_json(struct(*df.columns)), 256)
        ).dropDuplicates(["dedupe_key"])
        
        # Remove dedupe_key from final output
        deduped = deduped.drop("dedupe_key")
        
        return deduped

    except Exception as e:
        logger.error(f"Error during deduplication: {e}", exc_info=True)
        raise



# ============================================================== #
# Utility                                                        #
# ============================================================== #

def cast_numeric_columns(df: DataFrame, columns: list, dtype=DoubleType()) -> DataFrame:
    """
    Cast specified columns to numeric type (default DoubleType).
    """
    try:
        for col_name in columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
        return df
    except Exception as e:
        logger.error(f"Error casting numeric columns: {e}", exc_info=True)
        raise