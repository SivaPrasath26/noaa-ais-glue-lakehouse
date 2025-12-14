# fact_voyage_daily.py
"""
Daily Voyage Processing (Replacement for OLD Fact 2)
----------------------------------------------------
Reads trajectory_points for the given date range and produces:

1) voyage_segments/
      Per-voyage per-day first/last timestamps.

2) voyage_summary_staging/
      Per-voyage per-day metrics:
          - distance for the day
          - sum(sog)
          - point count
          - sum(lat)
          - sum(lon)

This eliminates voyage_state, incremental merge logic,
and all state-by-date snapshots.
"""

from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from utils.config import CFG, setup_logger


logger = setup_logger(__name__)


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _parse_date(date_str: str):
    return datetime.fromisoformat(date_str).date()


def _paths_for_range(base_path: str, start_date: str, end_date: str):
    """
    Build partition paths for fact1 (trajectory_points) for a date range.
    Example path: s3a://.../trajectory_points/year=2024/month=01/day=03/
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)

    d = start
    paths = []
    while d <= end:
        p = f"{base_path}year={d:%Y}/month={d:%m}/day={d:%d}/"
        paths.append(p)
        d = d.fromordinal(d.toordinal() + 1)
    return paths


# ------------------------------------------------------------------------------
# Main daily processing logic
# ------------------------------------------------------------------------------

def run_daily_voyage_processing(
    spark: SparkSession,
    traj_path: str,
    seg_out: str,
    staging_out: str,
    start_date: str,
    end_date: str,
):
    logger.info(f"Daily voyage processing window={start_date}..{end_date}")

    # ------------------------------------------------------------------
    # Load trajectory_points (Fact 1 output) for the window
    # ------------------------------------------------------------------
    paths = _paths_for_range(traj_path, start_date, end_date)
    logger.info(f"Loading trajectory partitions: {paths}")

    df = spark.read.parquet(*paths)

    if df.rdd.isEmpty():
        logger.warning("No trajectory points found for window")
        return

    # normalize schema
    df = df.select(
        F.col("mmsi"),
        F.col("voyageid"),
        F.col("BaseDateTime").alias("basedatetime"),
        F.col("SegmentDistanceKM").alias("segmentdistancekm"),
        "LAT",
        "LON",
        "SOG"
    )

    df.cache()
    df.count()  # materialize

    # ------------------------------------------------------------------
    # Add day column
    # ------------------------------------------------------------------
    df = df.withColumn("day", F.to_date("basedatetime"))

    # Helpful partitioning for parallel grouping
    df = df.repartition(200, "mmsi", "voyageid")

    # ------------------------------------------------------------------
    # 1. Generate voyage_segments (per-voyage per-day start/end)
    # ------------------------------------------------------------------
    logger.info("Computing voyage_segments")

    df_segments = (
        df.groupBy("mmsi", "voyageid", "day")
          .agg(
              F.min("basedatetime").alias("day_first_time"),
              F.max("basedatetime").alias("day_last_time"),
          )
    )

    df_segments.write.mode("overwrite") \
        .partitionBy("day") \
        .parquet(seg_out)

    logger.info("voyage_segments written")

    # ------------------------------------------------------------------
    # 2. Generate voyage_summary_staging (per-voyage per-day metrics)
    # ------------------------------------------------------------------
    logger.info("Computing voyage_summary_staging")

    df_stage = (
        df.groupBy("mmsi", "voyageid", "day")
          .agg(
              F.sum("segmentdistancekm").alias("day_distance_km"),
              F.sum("SOG").alias("day_sum_sog"),
              F.count(F.lit(1)).alias("day_pointcount"),
              F.sum("LAT").alias("day_sum_lat"),
              F.sum("LON").alias("day_sum_lon"),
          )
    )

    df_stage.write.mode("overwrite") \
        .partitionBy("day") \
        .parquet(staging_out)

    logger.info("voyage_summary_staging written")

    logger.info("Daily voyage processing completed successfully.")


# ------------------------------------------------------------------------------
# Glue-compatible public API (used by orchestrator)
# ------------------------------------------------------------------------------

def run_daily_voyage_jobs(
    spark: SparkSession,
    traj_path: str,
    segment_path: str,
    staging_path: str,
    start_date: str,
    end_date: str,
):
    """
    Orchestrator-compatible wrapper.
    """
    run_daily_voyage_processing(
        spark=spark,
        traj_path=traj_path,
        seg_out=segment_path,
        staging_out=staging_path,
        start_date=start_date,
        end_date=end_date,
    )


# ------------------------------------------------------------------------------
# Standalone execution for local testing
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("VoyageDailyProcessor").getOrCreate()

    run_daily_voyage_jobs(
        spark=spark,
        traj_path=CFG.S3_CURATED + "trajectory_points/",
        segment_path=CFG.S3_CURATED + "voyage_segments/",
        staging_path=CFG.S3_CURATED + "voyage_summary_staging/",
        start_date=datetime.utcnow().strftime("%Y-%m-%d"),
        end_date=datetime.utcnow().strftime("%Y-%m-%d"),
    )
