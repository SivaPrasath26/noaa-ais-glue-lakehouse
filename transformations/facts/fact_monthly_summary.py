# fact_monthly_summary.py
"""
Monthly Voyage Summary Rebuild
------------------------------
Reads:
    - voyage_segments/  (schema: VOYAGE_SEGMENTS_SCHEMA)
    - voyage_summary_staging/  (schema: VOYAGE_SUMMARY_STAGING_SCHEMA)

Produces:
    voyage_summary/
One row per (mmsi, voyageid) with full-voyage metrics.
"""

from datetime import datetime
from calendar import monthrange
from pyspark.sql import SparkSession, functions as F

from utils.config import CFG, setup_logger
from utils.schema_definitions import (
    VOYAGE_SEGMENTS_SCHEMA,
    VOYAGE_SUMMARY_STAGING_SCHEMA,
)

logger = setup_logger(__name__)


# ----------------------------------------------------------------------
# Helper: enumerate all day partitions for a month
# ----------------------------------------------------------------------
def _month_day_paths(base: str, year: int, month: int):
    """Return partition paths for all days in the given month."""
    days = monthrange(year, month)[1]
    return [
        f"{base}day={year:04d}-{month:02d}-{d:02d}/"
        for d in range(1, days + 1)
    ]


# ----------------------------------------------------------------------
# Monthly summary logic
# ----------------------------------------------------------------------
def run_monthly_summary(
    spark: SparkSession,
    seg_path: str,
    stage_path: str,
    out_path: str,
    year: int,
    month: int,
):
    logger.info(f"Starting monthly voyage summary rebuild for {year}-{month:02d}")

    # ----------------------------------------------------------
    # Load voyage_segments for the month (schema enforced)
    # ----------------------------------------------------------
    seg_paths = _month_day_paths(seg_path, year, month)
    logger.info(f"Loading voyage_segments: {seg_paths}")

    df_seg_month = (
        spark.read
             .schema(VOYAGE_SEGMENTS_SCHEMA)
             .parquet(*seg_paths)
    )

    # Identify voyages present in this month
    df_voy = df_seg_month.select("mmsi", "voyageid").distinct()

    # Load complete voyage_segments table with schema enforcement
    df_seg_all = (
        spark.read
             .schema(VOYAGE_SEGMENTS_SCHEMA)
             .parquet(seg_path)
    )

    # Filter to voyages active in this month
    df_seg = df_seg_all.join(df_voy, ["mmsi", "voyageid"])

    # Compute voyage start/end across all days
    df_span = (
        df_seg.groupBy("mmsi", "voyageid")
              .agg(
                  F.min("day_first_time").alias("voyagestart"),
                  F.max("day_last_time").alias("voyageend")
              )
    )

    # ----------------------------------------------------------
    # Load voyage_summary_staging for the month (schema enforced)
    # ----------------------------------------------------------
    stage_paths = _month_day_paths(stage_path, year, month)
    logger.info(f"Loading voyage_summary_staging: {stage_paths}")

    df_stage_month = (
        spark.read
             .schema(VOYAGE_SUMMARY_STAGING_SCHEMA)
             .parquet(*stage_paths)
    )

    df_voy_metrics = df_stage_month.select("mmsi", "voyageid").distinct()

    # Load all staging rows with schema enforcement
    df_stage_all = (
        spark.read
             .schema(VOYAGE_SUMMARY_STAGING_SCHEMA)
             .parquet(stage_path)
    )

    # Filter relevant voyages
    df_stage = df_stage_all.join(df_voy_metrics, ["mmsi", "voyageid"])

    # Aggregate metrics across all dates
    df_metrics = (
        df_stage.groupBy("mmsi", "voyageid")
                .agg(
                    F.sum("day_distance_km").alias("totaldistancekm"),
                    F.sum("day_sum_sog").alias("sumsog"),
                    F.sum("day_pointcount").alias("pointcount"),
                    F.sum("day_sum_lat").alias("sumlat"),
                    F.sum("day_sum_lon").alias("sumlon"),
                )
    )

    # ----------------------------------------------------------
    # Join spans + metrics to produce final summary rows
    # ----------------------------------------------------------
    df = (
        df_span.join(df_metrics, ["mmsi", "voyageid"])
               .withColumn(
                   "duration_hours",
                   (F.unix_timestamp("voyageend") -
                    F.unix_timestamp("voyagestart")) / 3600
               )
               .withColumn("avgspeed", F.col("totaldistancekm") / F.col("duration_hours"))
               .withColumn("avglat", F.col("sumlat") / F.col("pointcount"))
               .withColumn("avglon", F.col("sumlon") / F.col("pointcount"))
               .withColumn("voyagestartdate", F.to_date("voyagestart"))
    )

    # ----------------------------------------------------------
    # Write final voyage_summary (partition overwrite)
    # ----------------------------------------------------------
    logger.info("Writing final voyage_summary")

    (
        df.write
          .mode("overwrite")
          .partitionBy("voyagestartdate")
          .parquet(out_path)
    )

    logger.info("Monthly summary rebuild completed.")


# ----------------------------------------------------------------------
# Standalone execution
# ----------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("MonthlyVoyageSummary").getOrCreate()

    run_monthly_summary(
        spark=spark,
        seg_path=CFG.S3_CURATED + "voyage_segments/",
        stage_path=CFG.S3_CURATED + "voyage_summary_staging/",
        out_path=CFG.S3_CURATED + "voyage_summary/",
        year=datetime.utcnow().year,
        month=datetime.utcnow().month,
    )
