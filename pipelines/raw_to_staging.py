"""
ETL job for transforming NOAA AIS raw CSV data to cleaned, partitioned Parquet format.
Supports both full (historical) and daily (incremental) data loads.
Designed for AWS Glue environment (Glue 4.0).
"""

import os
import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

# ============================================================== #
# Spark + Hadoop configuration
# ============================================================== #

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# Configure Hadoop for S3A access in Glue 4.0
spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.defaultFS", "s3a:///")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark._jsc.hadoopConfiguration().set("spark.sql.parquet.output.committer.class",
    "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")

# ============================================================== #
# Imports from utils modules                                     #
# ============================================================== #
from utils.config import CFG, setup_logger
from utils.schema_definitions import SCHEMA_MAP
from utils.common_functions_raw import (
    parse_base_datetime,
    normalize_columns,
    clean_coordinates,
    clean_sog_cog_heading,
    replace_empty_with_null,
    derive_movement_flag,
    compute_summary_stats,
    drop_duplicates,
)

# Initialize Glue job and logger
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "mode", "start_date", "end_date"]
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = setup_logger(__name__)
logger.info("Glue job initialized successfully")
logger.info(f"Spark version: {spark.version}")


# ============================================================== #
# ETL Transformation Pipeline                                    #
# ============================================================== #

def transform_raw_to_staging(spark, input_path: str, output_path: str, quarantine_path: str) -> None:
    """
    Perform cleaning and transformation on raw AIS data and write processed Parquet.
    Includes schema enforcement, coordinate validation, null handling, and partitioning.
    """
    try:
        # ------------------------------------------------------ #
        # Step 1: Read raw input CSV data from S3 using schema   #
        # ------------------------------------------------------ #
        logger.info(f"Reading raw data from {input_path}")
        df_raw = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(input_path)
        )


        # Step 1.1: Normalize column names to handle schema drift
        # Ensures source field names like “latitude” → “LAT”, etc.
        df_raw = normalize_columns(df_raw)

        incoming_cols = set(df_raw.columns)
        expected_cols = set([f.name for f in SCHEMA_MAP["raw"].fields])

        # Add missing columns
        for col in expected_cols - incoming_cols:
            df_raw = df_raw.withColumn(col, F.lit(None))

        # Drop unexpected columns (but log them)
        for col in incoming_cols - expected_cols:
            logger.info(f"Unexpected column seen: {col}")
            df_raw = df_raw.drop(col)
            logger.info(f"Dropped unexpected column: {col}")

        for field in SCHEMA_MAP["raw"].fields:
            if field.name in df_raw.columns:
                df_raw = df_raw.withColumn(field.name, F.col(field.name).cast(field.dataType))


        # raw_count = df_raw.count()
        # logger.info(f"Raw record count: {raw_count}")

        # ------------------------------------------------------ #
        # Step 2: Data Cleaning                                 #
        # Replace blanks, validate coordinates, and deduplicate #
        # ------------------------------------------------------ #
        logger.info("Starting data cleaning process")

        # Step 2.1: Replace empty strings with null values
        df = replace_empty_with_null(df_raw)

        # Step 2.2: Derive partition fields (year/month/day) early
        # Ensures undated records are correctly quarantined later
        df = parse_base_datetime(df)

        # Step 2.3: Filter invalid coordinates and quarantine bad data
        df = clean_coordinates(df, lat_col="LAT", lon_col="LON",quarantine_path=quarantine_path)

        # Step 2.4: Clamp SOG, COG, and Heading within valid maritime bounds
        df = clean_sog_cog_heading(df)

        # Step 2.5: Drop duplicate AIS records
        df = drop_duplicates(df)

        # clean_count = df.count()
        # logger.info(f"Cleaned record count: {clean_count}")
        # logger.info(f"Records removed during cleaning: {raw_count - clean_count}")

        # ------------------------------------------------------ #
        # Step 3: Add movement flag                             #
        # Flag indicates whether vessel is in motion (SOG > 0)  #
        # ------------------------------------------------------ #
        logger.info("Adding movement flag")
        df = derive_movement_flag(df)

        # ------------------------------------------------------ #
        # Step 4: Compute and print summary stats                #
        # Used for validation and record quality checks          #
        # ------------------------------------------------------ #
        # logger.info("Computing summary statistics")
        # summary_df = compute_summary_stats(df)
        # summary = summary_df.collect()[0].asDict()
        # logger.info(f"Summary statistics: {summary}")

        # ------------------------------------------------------ #
        # Step 5: Write cleaned Parquet output to staging S3     #
        # Single-partition explicit write                        #
        # ------------------------------------------------------ #
        output_path = (output_path.rstrip("/") + "/cleaned/")

        partitions = (
            df.select("year", "month", "day")
            .distinct()
            .orderBy("year", "month", "day")
            .collect()
        )

        for p in partitions:
            logger.info(
                f"Partition to be written: year={p['year']}/month={p['month']}/day={p['day']}"
            )

        (
            df.write
            .mode("overwrite")
            .format("parquet")
            .partitionBy("year", "month", "day")
            .save(output_path)
        )

        logger.info("Raw to staging transformation completed successfully.")


    except Exception as e:
        logger.error(f"ETL transformation failed: {e}", exc_info=True)
        raise


# ============================================================== #
# Job Entry Point                                                #
# ============================================================== #

try:
    logger.info("Starting NOAA AIS ETL Glue job execution")

    # ---------------------------------------------------------- #
    # Step 1: Extract runtime parameters                        #
    # ---------------------------------------------------------- #

    mode = args["mode"]
    start_date_str = args["start_date"]
    end_date_str = args["end_date"]
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    base_path = CFG.RAW_DATA_PATH
    output_s3_path = CFG.STAGING_DATA_PATH
    if not output_s3_path:
        raise ValueError("Output S3 path is empty. Check Glue job parameters or config defaults.")
    quarantine_s3_path = CFG.QUARANTINE_PATH

    # ---------------------------------------------------------- #
    # Step 2: Determine input path(s) based on run mode          #
    # ---------------------------------------------------------- #
    from datetime import timedelta

    if mode == "full":
        logger.info("Running full backfill for all available data.")
        input_paths = [f"{base_path}year=*/month=*/day=*/"]
    else:
        input_paths = []
        delta_days = (end_date - start_date).days + 1
        for i in range(delta_days):
            d = start_date + timedelta(days=i)
            y, m, dd = d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")
            input_paths.append(f"{base_path}year={y}/month={m}/day={dd}/")

        logger.info(f"Running batch load from {start_date.date()} to {end_date.date()} ({delta_days} days)")

    # ---------------------------------------------------------- #
    # Step 3: Execute ETL pipeline                               #
    # ---------------------------------------------------------- #
    for p in input_paths:
        date_str = p.split("year=")[1].replace("/", "-").replace("month=", "").replace("day=", "")
        logger.info(f"Processing date path: {p} (derived date: {date_str})")
        transform_raw_to_staging(spark, p, output_s3_path, quarantine_s3_path)

except Exception as e:
    logger.error(f"Pipeline terminated due to fatal error: {e}", exc_info=True)
    raise


finally:
    # ---------------------------------------------------------- #
    # Step 4: Commit and finalize Glue job                       #
    # ---------------------------------------------------------- #
    logger.info("Finalizing and committing Glue job")
    try:
        job.commit()
        logger.info("Glue job committed successfully.")
    except Exception as e:
        logger.warning(f"Glue job commit failed: {e}")
    