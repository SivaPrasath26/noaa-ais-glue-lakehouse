"""
fact_voyage_trajectory.py

Curated Fact 1 — Vessel trajectory reconstruction.
Transforms staging AIS data into voyage-segmented, spatially indexed trajectories.
Implements:
  - Window-based sorting by timestamp (per MMSI)
  - Voyage segmentation using time gap > 3 hours
  - Haversine distance computation between consecutive points
  - Spatial hashing (GeoHash grid key for map joins)
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from utils.common_functions_curated import (
    calculate_haversine,
    add_geohash,
    safe_cast_columns,
    log_df_stats,
)
from utils.config import setup_logger, CFG

logger = setup_logger(__name__)


def reconstruct_trajectory(df_staging, output_path: str) -> None:
    """
    Reconstruct ordered trajectories with voyage segmentation and geospatial enrichment.
    """
    try:
        logger.info("Starting trajectory reconstruction")

        # ------------------------------------------------------------
        # Step 1: Sort by timestamp within each vessel (HashMap concept)
        # ------------------------------------------------------------
        w = Window.partitionBy("MMSI").orderBy("BaseDateTime")

        df = df_staging.withColumn("PrevTime", F.lag("BaseDateTime").over(w))
        df = df.withColumn(
            "GapHours",
            F.when(
                F.col("PrevTime").isNotNull(),
                (F.unix_timestamp("BaseDateTime") - F.unix_timestamp("PrevTime")) / 3600,
            ),
        )

        # ------------------------------------------------------------
        # Step 2: Voyage segmentation (new voyage if gap > 3 hours)
        # ------------------------------------------------------------
        df = df.withColumn(
            "VoyageID",
            F.sum(F.when(F.col("GapHours") > 3, 1).otherwise(0)).over(
                w.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )

        # ------------------------------------------------------------
        # Step 3: Compute distance between consecutive points
        # ------------------------------------------------------------
        df = df.withColumn("PrevLAT", F.lag("LAT").over(w))
        df = df.withColumn("PrevLON", F.lag("LON").over(w))
        df = df.withColumn(
            "SegmentDistanceKM",
            calculate_haversine("PrevLAT", "PrevLON", "LAT", "LON"),
        )

        # ------------------------------------------------------------
        # Step 4: Add GeoHash (Spatial Hash Grid)
        # ------------------------------------------------------------
        df = add_geohash(df, lat_col="LAT", lon_col="LON", precision=6)

        # ------------------------------------------------------------
        # Step 5: Schema enforcement for curated layer
        # ------------------------------------------------------------
        df = safe_cast_columns(
            df,
            {
                "LAT": DoubleType(),
                "LON": DoubleType(),
                "SOG": DoubleType(),
                "COG": DoubleType(),
                "SegmentDistanceKM": DoubleType(),
            },
        )

        # ------------------------------------------------------------
        # Step 6: Log stats and write
        # ------------------------------------------------------------
        log_df_stats(df, "trajectory_points")

        df.write.mode("overwrite").partitionBy("MMSI", "VoyageID").parquet(output_path)
        logger.info(f"Trajectory fact written to {output_path}")

    except Exception as e:
        logger.error(f"Trajectory reconstruction failed: {e}", exc_info=True)
        raise


def run_trajectory_job(input_path: str, output_path: str):
    """
    Entry point for trajectory reconstruction Glue job.
    """
    try:
        spark = SparkSession.builder.appName("TrajectoryFactBuilder").getOrCreate()
        df_staging = spark.read.parquet(input_path)
        reconstruct_trajectory(df_staging, output_path)
    except Exception as e:
        logger.error(f"Failed to run trajectory job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_trajectory_job(
        CFG.S3_STAGING + "cleaned/",
        CFG.S3_CURATED + "trajectory_points/",
    )
