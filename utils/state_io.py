"""
State IO utilities for AIS incremental processing.
Handles reading/writing MMSI-level state snapshots for voyage continuity.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from utils.schema_definitions import STATE_SNAPSHOT_SCHEMA


# =========================================================
# read_state_snapshot
# Purpose: load latest state snapshot safely
# =========================================================
def read_state_snapshot(spark: SparkSession, path: str, fallback_empty: bool = True) -> DataFrame:
    """Read the latest state snapshot. Optionally return empty DataFrame if missing."""
    try:
        return spark.read.schema(STATE_SNAPSHOT_SCHEMA).parquet(path)
    except Exception:
        if not fallback_empty:
            raise
        return spark.createDataFrame([], schema=STATE_SNAPSHOT_SCHEMA)


# =========================================================
# write_state_snapshot
# Purpose: persist compact state (one row per MMSI) to a path
# =========================================================
def write_state_snapshot(df: DataFrame, path: str) -> None:
    """Write state snapshot (one row per MMSI)."""
    (
        df.select("MMSI", "BaseDateTime", "LAT", "LON", "VoyageID")
        .dropna(subset=["MMSI"])
        .write.mode("overwrite")
        .parquet(path)
    )


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
    return read_state_snapshot(spark, path, fallback_empty=fallback_empty)


# =========================================================
# write_state_by_date
# Purpose: persist dated snapshot for recompute seeds
# =========================================================
def write_state_by_date(df: DataFrame, by_date_prefix: str, date_str: str) -> None:
    """Write a dated state snapshot for a given day."""
    path = by_date_prefix.rstrip("/") + f"{date_str}/"
    write_state_snapshot(df, path)


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
