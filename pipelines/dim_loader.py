"""
Glue job for building AIS Dimension Tables (Country, NavStatus, VesselType).
Runs all three dimension builders in a single Glue job.

This orchestrator:
- Reads config dynamically from utils.config.CFG
- Calls individual dim builders (no logic duplicated)
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Hadoop S3 settings
spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.defaultFS", "s3a:///")
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider,"
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
)
spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark._jsc.hadoopConfiguration().set(
    "spark.sql.parquet.output.committer.class",
    "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
)

# Config + logger
from utils.config import CFG, setup_logger
logger = setup_logger(__name__)

# Import dimension functions
from transformations.dims.dim_country import build_dim_country
from transformations.dims.dim_nav_status import build_dim_nav_status
from transformations.dims.dim_vessel_type import build_dim_vessel_type


# ===========================================
# Glue Job Setup
# ===========================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)

try:
    job.init(args["JOB_NAME"], args)
    logger.info("Dimension loader Glue job initialized.")
    logger.info(f"Spark version: {spark.version}")

    # =================================================================
    # Compute dynamic S3 paths from config
    # =================================================================
    mid_path = CFG.S3_LOOKUP + CFG.LOOKUP_MID_FILE
    call_sign_path = CFG.S3_LOOKUP + CFG.LOOKUP_CALLSIGN_FILE
    nav_status_path = CFG.S3_LOOKUP + CFG.LOOKUP_NAV_STATUS_FILE
    vessel_type_path = CFG.S3_LOOKUP + CFG.LOOKUP_VESSEL_TYPE_FILE

    out_country = CFG.S3_LOOKUP + CFG.DIM_COUNTRY_DIR
    out_nav = CFG.S3_LOOKUP + CFG.DIM_NAV_STATUS_DIR
    out_vessel = CFG.S3_LOOKUP + CFG.DIM_VESSEL_TYPE_DIR

    # =================================================================
    # Run DIM Country
    # =================================================================
    try:
        logger.info(f"Building Country Dimension from {mid_path} and {call_sign_path}")
        build_dim_country(mid_path, call_sign_path, out_country)
        logger.info("Country Dimension completed.")
    except Exception as e:
        logger.error(f"Country Dimension failed: {e}", exc_info=True)
        raise

    # =================================================================
    # Run DIM Navigational Status
    # =================================================================
    try:
        logger.info(f"Building Navigational Status Dimension from {nav_status_path}")
        build_dim_nav_status(nav_status_path, out_nav)
        logger.info("Navigational Status Dimension completed.")
    except Exception as e:
        logger.error(f"NavStatus Dimension failed: {e}", exc_info=True)
        raise

    # =================================================================
    # Run DIM Vessel Type
    # =================================================================
    try:
        logger.info(f"Building Vessel Type Dimension from {vessel_type_path}")
        build_dim_vessel_type(vessel_type_path, out_vessel)
        logger.info("Vessel Type Dimension completed.")
    except Exception as e:
        logger.error(f"Vessel Type Dimension failed: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"DIM Loader failed: {e}", exc_info=True)
    raise

finally:
    try:
        job.commit()
        logger.info("DIM Loader job committed successfully.")
    except Exception as e:
        logger.warning(f"Commit failed: {e}")
