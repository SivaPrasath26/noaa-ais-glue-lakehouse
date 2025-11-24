"""
fact_voyage_summary.py

Curated Fact 2 - Voyage-level summary statistics.
Consumes trajectory_points data and computes voyage aggregates:
  - Total distance traveled (sum of segment distances)
  - Voyage duration (min and max timestamp)
  - Average speed (distance / duration)
  - Average latitude/longitude (for spatial centroids)
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
from utils.common_functions_curated import safe_cast_columns, log_df_stats
from utils.config import setup_logger, CFG

logger = setup_logger(__name__)


# -----------------------------------------------------------------------------
# Voyage summary aggregation
# -----------------------------------------------------------------------------
# =========================================================
# summarize_voyages
# Purpose: aggregate trajectory points into voyage-level metrics
# =========================================================

def summarize_voyages(df_traj, output_path: str) -> None:
    """
    Aggregate trajectory points into voyage-level metrics.
    Group by MMSI/VoyageID and compute temporal and spatial rollups.
    """
    try:
        logger.info("Step: voyage_summary - aggregation started")

        df = (
            df_traj.groupBy("MMSI", "VoyageID")
            .agg(
                F.min("BaseDateTime").alias("VoyageStart"),
                F.max("BaseDateTime").alias("VoyageEnd"),
                F.sum("SegmentDistanceKM").alias("TotalDistanceKM"),
                F.avg("SOG").alias("AvgSpeed"),
                F.avg("LAT").alias("AvgLAT"),
                F.avg("LON").alias("AvgLON"),
            )
            .withColumn(
                "DurationHours",
                (F.unix_timestamp("VoyageEnd") - F.unix_timestamp("VoyageStart")) / 3600,
            )
        )

        # Enforce curated types
        df = safe_cast_columns(
            df,
            {
                "TotalDistanceKM": DoubleType(),
                "AvgSpeed": DoubleType(),
                "DurationHours": DoubleType(),
            },
        )

        log_df_stats(df, "voyage_summary")

        # Limit small-file explosion: cap writer partitions while still partitioning by MMSI
        df = df.repartition(200, "MMSI")
        logger.info("Step: voyage_summary - write partitioned by MMSI")
        df.write.mode("overwrite").partitionBy("MMSI").parquet(output_path)
        logger.info(f"Step: voyage_summary - written to {output_path}")

    except Exception as e:
        logger.error(f"Voyage summary computation failed: {e}", exc_info=True)
        raise


# -----------------------------------------------------------------------------
# Job entrypoint
# -----------------------------------------------------------------------------
# =========================================================
# run_voyage_summary_job
# Purpose: Glue-friendly entrypoint to build voyage summary
# =========================================================
def run_voyage_summary_job(input_path: str, output_path: str):
    """
    Entry point for voyage summary Glue job.
    """
    try:
        spark = SparkSession.builder.appName("VoyageSummaryBuilder").getOrCreate()
        df_traj = spark.read.parquet(input_path)
        summarize_voyages(df_traj, output_path)
    except Exception as e:
        logger.error(f"Failed to run voyage summary job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_voyage_summary_job(
        CFG.S3_CURATED + "trajectory_points/",
        CFG.S3_CURATED + "voyage_summary/",
    )
