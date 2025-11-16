# staging_to_curated.py
"""
Curated ETL Orchestrator (Staging → Curated)
Runs Fact-1 (trajectory_points) then Fact-2 (voyage_summary) in one Glue job.
Algorithms covered: window sort, hashmap-style partitioning, time-gap segmentation,
Haversine distance, spatial hashing, and group-by reduction.
"""

import sys
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.config import CFG, setup_logger
from transformations.facts.fact_voyage_trajectory import reconstruct_trajectory
from transformations.facts.fact_voyage_summary import summarize_voyages

# ----------------------------
# Spark / Glue initialization
# ----------------------------
def init_glue():
    """
    Initialize Glue and Spark. Sets S3A configs for Glue 4.0.
    Concept: runtime bootstrap for distributed ETL.
    """
    try:
        sc = SparkContext()
        glue_ctx = GlueContext(sc)
        spark = glue_ctx.spark_session

        # Safe commit/write settings
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
    except Exception as e:
        # Last-resort print because logger may not be ready yet
        print(f"[FATAL] Glue/Spark init failed: {e}")
        raise


def parse_args():
    """
    Resolve Glue job args.
    Required: JOB_NAME, mode (full|daily), start_date, end_date
    Concept: param-driven batch windowing.
    """
    try:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "mode", "start_date", "end_date"])
        return args
    except Exception as e:
        print(f"[FATAL] Arg parsing failed: {e}")
        raise


def resolve_input_paths(mode: str, start_date: str, end_date: str) -> list[str]:
    """
    Build S3 staging input paths for the requested window.
    Concept: predicate pushdown via partition pruning by path.
    """
    try:
        base = CFG.STAGING_DATA_PATH.rstrip("/") + "/cleaned/"
        if mode == "full":
            return [f"{base}year=*/month=*/day=*/"]

        sd = datetime.strptime(start_date, "%Y-%m-%d").date()
        ed = datetime.strptime(end_date, "%Y-%m-%d").date()
        if ed < sd:
            raise ValueError("end_date < start_date")

        paths = []
        d = sd
        one = timedelta(days=1)
        while d <= ed:
            y = d.strftime("%Y")
            m = d.strftime("%m")
            dd = d.strftime("%d")
            paths.append(f"{base}year={y}/month={m}/day={dd}/")
            d += one
        return paths
    except Exception as e:
        raise RuntimeError(f"Failed to resolve input paths: {e}")


def read_staging(spark, paths: list[str]):
    """
    Read staging parquet for the computed paths.
    Concept: union over partitioned inputs with schema alignment.
    """
    try:
        df = None
        for p in paths:
            try:
                part = spark.read.parquet(p)
                df = part if df is None else df.unionByName(part, allowMissingColumns=True)
            except Exception:
                # Skip missing partitions silently
                continue
        if df is None:
            raise RuntimeError("No staging data found for given window.")
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to read staging data: {e}")


def write_checkpoint(spark, df: DataFrame, label: str):
    """
    Checkpoint to validate DF before next step.
    Concept: defensive engineering — spot schema/row-count issues early.
    """
    try:
        cnt = df.count()
        distinct_mmsi = df.select("MMSI").distinct().count() if "MMSI" in df.columns else 0
        logger = setup_logger(__name__)
        logger.info(f"[CHECKPOINT {label}] rows={cnt}, distinct_mmsi={distinct_mmsi}")
    except Exception as e:
        # Non-fatal
        logger = setup_logger(__name__)
        logger.warning(f"[CHECKPOINT {label}] failed: {e}")


def run_orchestrator():
    """
    End-to-end: read staging window → build trajectory_points → build voyage_summary.
    """
    glue_ctx, spark = init_glue()
    logger = setup_logger(__name__)

    args = parse_args()
    job = Job(glue_ctx)
    try:
        job.init(args["JOB_NAME"], args)
        logger.info(f"Curated Orchestrator started. Spark {spark.version}")

        # Resolve inputs
        input_paths = resolve_input_paths(args["mode"], args["start_date"], args["end_date"])
        logger.info(f"Input paths: {len(input_paths)}")
        for p in input_paths[:5]:
            logger.info(f"  {p}")
        if len(input_paths) > 5:
            logger.info("  ...")

        # Load staging slice
        df_staging = read_staging(spark, input_paths)
        write_checkpoint(spark, df_staging, "staging_loaded")

        # Fact 1: trajectory_points
        traj_out = CFG.S3_CURATED + "trajectory_points/"
        
        try:
            reconstruct_trajectory(df_staging, traj_out)
            logger.info("Fact 1 (trajectory_points) completed.")
        except Exception as e:
            logger.error(f"Fact 1 failed: {e}", exc_info=True)
            raise

        # Load Fact 1 output for Fact 2 aggregation
        try:
            df_traj = spark.read.parquet(traj_out)
            write_checkpoint(spark, df_traj, "trajectory_points_loaded")
        except Exception as e:
            logger.error(f"Failed to read trajectory_points output: {e}", exc_info=True)
            raise

        # Fact 2: voyage_summary
        
        voy_out = CFG.S3_CURATED + "voyage_summary/"

        try:
            summarize_voyages(df_traj, voy_out)
            logger.info("Fact 2 (voyage_summary) completed.")
        except Exception as e:
            logger.error(f"Fact 2 failed: {e}", exc_info=True)
            raise

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
