"""
State IO utilities for AIS incremental processing.
Handles reading/writing MMSI-level state snapshots for voyage continuity.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from utils.schema_definitions import STATE_SNAPSHOT_SCHEMA


# =========================================================
# read_state_by_date
# Purpose: load dated snapshot (e.g., prior-day seed for recompute)
# =========================================================
def read_state_by_date(
    spark: SparkSession,
    by_date_prefix: str,
    date_str: str,
    fallback_empty: bool = True,
) -> DataFrame:
    """
    Read a dated state snapshot. Expects prefix like '.../state/by_date='.
    """
    path = by_date_prefix.rstrip("/") + f"{date_str}/"
    try:
        return spark.read.schema(STATE_SNAPSHOT_SCHEMA).parquet(path)
    except Exception:
        if not fallback_empty:
            raise
        return spark.createDataFrame([], schema=STATE_SNAPSHOT_SCHEMA)


# =========================================================
# write_state_by_date
# Purpose: persist dated snapshot for recompute seeds
# =========================================================
def write_state_by_date(df: DataFrame, by_date_prefix: str, date_str: str) -> None:
    """Write a dated state snapshot for a given day."""
    path = by_date_prefix.rstrip("/") + f"{date_str}/"
    (
        df.select("MMSI", "BaseDateTime", "LAT", "LON", "VoyageID")
        .dropna(subset=["MMSI"])
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(path)
    )


# =========================================================
# latest_per_mmsi
# Purpose: select last-known row per MMSI (continuity seed)
# =========================================================
def latest_per_mmsi(df: DataFrame) -> DataFrame:
    """Pick the last row per MMSI by BaseDateTime."""
    w = Window.partitionBy("MMSI").orderBy(F.col("BaseDateTime").desc())
    return (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
