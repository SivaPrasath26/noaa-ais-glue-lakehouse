"""
fact_voyage_summary.py

Curated Fact 2 — Voyage-level summary statistics.
Consumes trajectory_points data and computes voyage aggregates:
  - Total distance traveled (sum of segment distances)
  - Voyage duration (min → max timestamp)
  - Average speed (distance / duration)
  - Average latitude/longitude (for spatial centroids)
Implements aggregation and reduction principles.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
from utils.common_functions_curated import safe_cast_columns, log_df_stats
from utils.config import setup_logger, CFG

logger = setup_logger(__name__)


def summarize_voyages(df_traj, output_path: str) -> None:
    """
    Aggregate trajectory data into voyage-level metrics.
    Concept: Group-By reduction (MapReduce).
    """
    try:
        logger.info("Starting voyage-level summary computation")

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

        # Handle type enforcement
        df = safe_cast_columns(
            df,
            {
                "TotalDistanceKM": DoubleType(),
                "AvgSpeed": DoubleType(),
                "DurationHours": DoubleType(),
            },
        )

        log_df_stats(df, "voyage_summary")

        df.write.mode("overwrite").partitionBy("MMSI").parquet(output_path)
        logger.info(f"Voyage summary written to {output_path}")

    except Exception as e:
        logger.error(f"Voyage summary computation failed: {e}", exc_info=True)
        raise


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

