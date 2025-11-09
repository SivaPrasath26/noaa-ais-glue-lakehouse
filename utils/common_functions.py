"""
Reusable transformation and validation utilities for NOAA AIS data processing.
All functions are designed for import and use in AWS Glue or PySpark ETL scripts.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType
from utils.config import setup_logger

# Initialize logger
logger = setup_logger(__name__)

# ============================================================== #
# Datetime Parsing and Partition Derivation                      #
# ============================================================== #

def parse_base_datetime(df: DataFrame, input_col: str = "BaseDateTime") -> DataFrame:
    """
    Parse BaseDateTime safely in place and derive year/month/day for partitioning.
    Invalid or unparsable timestamps are dropped to avoid Spark path errors.
    """
    try:
        # Convert BaseDateTime to proper timestamp (in place)
        df = df.withColumn(
            input_col,
            F.to_timestamp(
                F.regexp_replace(F.col(input_col), "Z$", ""),  # strip trailing Z if any
                "yyyy-MM-dd'T'HH:mm:ss[.SSS]"                 # support milliseconds too
            )
        )

        # Drop rows with invalid or null timestamps (prevents empty partition dirs)
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
        valid = df.filter(
            (F.col(lat_col).between(-90, 90)) &
            (F.col(lon_col).between(-180, 180))
        )

        invalid = df.exceptAll(valid)

        if quarantine_path:
            try:
                invalid_count = invalid.count()
                if quarantine_path and invalid_count > 0:
                    try:
                        if "year" in df.columns and "month" in df.columns and "day" in df.columns:
                            quarantine_output = f"{quarantine_path.rstrip('/')}/year={df.select('year').first()[0]}/month={df.select('month').first()[0]}/day={df.select('day').first()[0]}/"
                        else:
                            quarantine_output = f"{quarantine_path.rstrip('/')}/undated/"

                        invalid.coalesce(1).write.mode("overwrite").option("header", True).csv(quarantine_output)
                        logger.info(f"Invalid coordinate records written to {quarantine_output} ({invalid_count} rows)")

                    except Exception as e:
                        logger.warning(f"Could not write quarantine data to {quarantine_path}: {e}")
                else:
                    logger.info(f"No invalid coordinate records to quarantine.")

            except Exception as e:
                logger.warning(f"Could not write quarantine data to {quarantine_path}: {e}")

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


def drop_duplicates(df: DataFrame, subset_cols: list = ["MMSI", "BaseDateTime"]) -> DataFrame:
    """
    Remove duplicate AIS records based on MMSI and timestamp combination.
    """
    try:
        return df.dropDuplicates(subset=subset_cols)
    except Exception as e:
        logger.error(f"Error dropping duplicates: {e}", exc_info=True)
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
