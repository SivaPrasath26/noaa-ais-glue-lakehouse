"""
Voyage state IO utilities for incremental voyage summary.
Maintains running aggregates per (MMSI, VoyageID) as dated snapshots.
"""

from pyspark.sql import DataFrame, SparkSession
from utils.schema_definitions import VOYAGE_STATE_SCHEMA


def read_voyage_state_by_date(
    spark: SparkSession,
    by_date_prefix: str,
    date_str: str,
    fallback_empty: bool = True,
) -> DataFrame:
    """Read a dated voyage state snapshot (voyage_state_by_date=YYYY-MM-DD/)."""
    path = by_date_prefix.rstrip("/") + f"{date_str}/"
    try:
        return spark.read.schema(VOYAGE_STATE_SCHEMA).parquet(path)
    except Exception:
        if not fallback_empty:
            raise
        return spark.createDataFrame([], schema=VOYAGE_STATE_SCHEMA)


def write_voyage_state_by_date(df: DataFrame, by_date_prefix: str, date_str: str) -> None:
    """Write a dated snapshot of voyage state for the window end date."""
    path = by_date_prefix.rstrip("/") + f"{date_str}/"
    (
        df.select(VOYAGE_STATE_SCHEMA.fieldNames())
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(path)
    )
