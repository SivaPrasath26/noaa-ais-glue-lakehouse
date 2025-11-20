# staging_to_curated.py
"""
Curated ETL Orchestrator (Staging -> Curated)
Runs Fact 1 (trajectory_points) then Fact 2 (voyage_summary) in one Glue job.
Respect incremental/recompute semantics via Fact 1 state handling.
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from utils.config import CFG, setup_logger
from transformations.facts.fact_voyage_trajectory import run_trajectory_job
from transformations.facts.fact_voyage_summary import summarize_voyages


# ----------------------------
# Spark / Glue initialization
# ----------------------------
# =========================================================
# init_glue
# Purpose: bootstrap Glue/Spark with safe S3 configs
# =========================================================
def init_glue():
    """
    Initialize Glue and Spark. Sets S3A configs for Glue 4.0.
    """
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.defaultFS", "s3a:///")
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark._jsc.hadoopConfiguration().set(
        "spark.sql.parquet.output.committer.class",
        "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
    )
    return glue_ctx, spark


# =========================================================
# parse_args
# Purpose: resolve Glue arguments for window + mode
# =========================================================
def parse_args():
    """
    Resolve Glue job args.
    Required: JOB_NAME, mode (incremental|recompute), start_date, end_date.
    """
    return getResolvedOptions(sys.argv, ["JOB_NAME", "mode", "start_date", "end_date"])


# =========================================================
# write_checkpoint
# Purpose: lightweight row/MMSI counts with non-fatal logging
# =========================================================
def write_checkpoint(df, label: str):
    """Lightweight checkpoint for row/mmsi counts."""
    try:
        cnt = df.count()
        distinct_mmsi = df.select("MMSI").distinct().count() if "MMSI" in df.columns else 0
        logger = setup_logger(__name__)
        logger.info(f"[CHECKPOINT {label}] rows={cnt}, distinct_mmsi={distinct_mmsi}")
    except Exception as e:
        logger = setup_logger(__name__)
        logger.warning(f"[CHECKPOINT {label}] failed: {e}")


# =========================================================
# run_orchestrator
# Purpose: orchestrate Fact 1 then Fact 2 for given window/mode
# =========================================================
def run_orchestrator():
    """End-to-end: run trajectory fact then voyage summary for the window."""
    glue_ctx, spark = init_glue()
    logger = setup_logger(__name__)

    args = parse_args()
    job = Job(glue_ctx)
    try:
        job.init(args["JOB_NAME"], args)
        logger.info(f"Curated Orchestrator started. Spark {spark.version}")
        logger.info(f"Mode={args['mode']} window={args['start_date']}..{args['end_date']}")

        # Paths/config
        staging_path = CFG.S3_STAGING + "cleaned/"
        traj_out = CFG.S3_CURATED + "trajectory_points/"
        voy_out = CFG.S3_CURATED + "voyage_summary/"
        state_latest = CFG.STATE_LATEST_PATH
        state_by_date = CFG.STATE_BY_DATE_PATH

        # Fact 1: trajectory_points (handles incremental/recompute)
        run_trajectory_job(
            input_path=staging_path,
            output_path=traj_out,
            state_latest_path=state_latest,
            state_by_date_prefix=state_by_date,
            start_date=args["start_date"],
            end_date=args["end_date"],
            mode=args["mode"],
        )
        logger.info("Fact 1 (trajectory_points) completed.")

        # Load Fact 1 output for Fact 2 aggregation
        df_traj = spark.read.parquet(traj_out)
        write_checkpoint(df_traj, "trajectory_points_loaded")

        # Optionally filter summary to window; current summarize_voyages aggregates all
        summarize_voyages(df_traj, voy_out)
        logger.info("Fact 2 (voyage_summary) completed.")

        logger.info("Curated Orchestrator finished successfully.")
    except Exception as e:
        logger = setup_logger(__name__)
        logger.error(f"Pipeline terminated: {e}", exc_info=True)
        raise
    finally:
        try:
            job.commit()
            logger = setup_logger(__name__)
            logger.info("Glue job committed.")
        except Exception as e:
            logger = setup_logger(__name__)
            logger.warning(f"Glue job commit failed: {e}")


if __name__ == "__main__":
    run_orchestrator()
