"""
common_functions_curated.py
Reusable PySpark utilities for curated AIS transformations.
Includes algorithms: hashing via JSON-struct, sliding-window voyage
segmentation, spatial grid quantization, and distributed sorting.
"""

import os
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import IntegerType, StringType
from utils.config import setup_logger, LOG_COUNTS_DEFAULT
from utils.column_mapping import COLUMN_MAPPING

# Initialize logger
logger = setup_logger(__name__)


# -----------------------------------------------------------------------------
# Voyage segmentation and ordering
# -----------------------------------------------------------------------------
# =========================================================
# [1] segment_voyages
# Purpose: segment voyages using time-gap > 3h rule
# =========================================================
def segment_voyages(df: DataFrame, id_col: str, time_col: str) -> DataFrame:
    """
    Algorithm: Sliding Window + Conditional Prefix Sum
    Steps:
        1. Partition by vessel id and order by timestamp.
        2. Lag the previous timestamp within the window.
        3. Compute hour difference using unix_timestamp.
        4. Mark gaps >3h as segment boundaries (0/1).
        5. Prefix-sum the boundary flags to generate voyage_id.
    """
    try:
        w = Window.partitionBy(id_col).orderBy(time_col)

        df = df.withColumn("prev_ts", F.lag(F.col(time_col)).over(w))

        df = df.withColumn(
            "gap_hours",
            (F.unix_timestamp(F.col(time_col)) -
             F.unix_timestamp(F.col("prev_ts"))) / 3600
        )

        df = df.withColumn(
            "voyage_id",
            F.sum(F.when(F.col("gap_hours") > 3, 1).otherwise(0))
             .over(w.rowsBetween(Window.unboundedPreceding, 0))
        )

        return df.drop("prev_ts", "gap_hours")

    except Exception as e:
        raise RuntimeError(f"voyage segmentation failed: {e}")


# =========================================================
# [2] assign_spatial_grid
# Purpose: bucket lat/lon into coarse grids
# =========================================================
def assign_spatial_grid(df: DataFrame,
                        lat_col="LAT",
                        lon_col="LON",
                        precision=1.0) -> DataFrame:
    """
    Algorithm: Spatial Quantization via Floor Division
    Steps:
        1. Compute floor(lat / precision) to get grid index.
        2. Multiply index * precision to obtain bucket boundary.
        3. Repeat for longitude.
        4. Floor ensures correct indexing for negative coordinates.
    """
    try:
        df = df.withColumn(
            "grid_lat",
            (F.floor(F.col(lat_col) / precision) * precision).cast(IntegerType())
        )

        df = df.withColumn(
            "grid_lon",
            (F.floor(F.col(lon_col) / precision) * precision).cast(IntegerType())
        )

        return df

    except Exception as e:
        raise RuntimeError(f"spatial grid assignment failed: {e}")


# =========================================================
# [3] sort_by_timestamp
# Purpose: repartition + sort within partitions by time
# =========================================================
def sort_by_timestamp(df: DataFrame, id_col: str, time_col: str) -> DataFrame:
    """
    Algorithm: Distributed Sort with Repartition
    Steps:
        1. Repartition by vessel id for grouping locality.
        2. Apply sortWithinPartitions to order records by timestamp.
        3. Guarantees per-vessel sorted timelines without global shuffle.
    """
    try:
        df = df.repartition(id_col)
        return df.sortWithinPartitions(id_col, time_col)
    except Exception as e:
        raise RuntimeError(f"sorting failed: {e}")

# =========================================================
# [4] calculate_haversine
# Purpose: great-circle distance between consecutive points
# =========================================================
def calculate_haversine(lat1_col: str, lon1_col: str,
                        lat2_col: str, lon2_col: str):
    """
    Algorithm: Haversine Formula (Spherical Trigonometry)
    Steps:
        1. Convert lat/lon deltas to radians.
        2. Apply haversine(theta) = sin²(dlat/2) + cos(lat1)*cos(lat2)*sin²(dlon/2).
        3. Compute central angle using asin(sqrt(h)).
        4. Multiply by Earth radius (R=6371 km) to get great-circle distance.
    """
    R = 6371.0

    try:
        return (
            2 * R *
            F.asin(
                F.sqrt(
                    F.pow(F.sin((F.radians(F.col(lat2_col) - F.col(lat1_col)) / 2)), 2)
                    + F.cos(F.radians(F.col(lat1_col)))
                    * F.cos(F.radians(F.col(lat2_col)))
                    * F.pow(F.sin((F.radians(F.col(lon2_col) - F.col(lon1_col)) / 2)), 2)
                )
            )
        )
    except Exception as e:
        raise RuntimeError(f"haversine failed: {e}")

# =========================================================
# [5] safe_cast_columns
# Purpose: enforce target dtypes only on existing columns
# =========================================================
def safe_cast_columns(df: DataFrame, schema_map: dict) -> DataFrame:
    """
    Algorithm: Column-wise Type Casting
    Steps:
        1. Iterate key→type pairs in the schema map.
        2. For each existing column: cast to target dtype.
        3. Avoid touching missing columns to prevent analysis exceptions.
    """
    try:
        for col_name, dtype in schema_map.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(dtype))
        return df
    except Exception as e:
        raise RuntimeError(f"safe_cast_columns failed: {e}")

# =========================================================
# [6] add_geohash
# Purpose: spatial hash for joins/grouping
# =========================================================
def add_geohash(df: DataFrame,
                lat_col: str = "LAT",
                lon_col: str = "LON",
                precision: int = 6) -> DataFrame:
    """
    Algorithm: Spatial Hash Encoding (Geohash)
    Steps:
        1. Map each (lat, lon) into a base-32 geohash string of given precision.
        2. Encodes recursive grid subdivision of Earth surface.
        3. Produces deterministic spatial index useful for grouping and joins.
    """
    try:
        fn = F.udf(
            lambda lat, lon: _encode_geohash(lat, lon, precision)
            if lat is not None and lon is not None else None,
            StringType(),
        )

        return df.withColumn("GeoHash", fn(F.col(lat_col), F.col(lon_col)))

    except Exception as e:
        raise RuntimeError(f"geohash encoding failed: {e}")

# =========================================================
# [7] add_hash_key
# Purpose: stable hash key via JSON struct
# =========================================================
def add_hash_key(df: DataFrame, cols: list) -> DataFrame:
    """
    Algorithm: Deterministic Hash via JSON-Struct
    Steps:
        1. Build a struct of selected columns.
        2. Serialize struct to JSON to preserve field order.
        3. Feed JSON string into xxhash64 to get stable 64-bit key.
    """
    try:
        expr = F.to_json(F.struct(*cols))
        return df.withColumn("hash_key", F.xxhash64(expr))
    except Exception as e:
        raise RuntimeError(f"hash key creation failed: {e}")


# =========================================================
# [8] log_df_stats
# Purpose: lightweight logging of row/mmsi counts
# =========================================================
def log_df_stats(df: DataFrame, label: str) -> None:
    """
    Algorithm: Lightweight Analytical Profiling
    Steps:
        1. Count total rows (O(1) incremental stats in Spark).
        2. Count distinct MMSI when present.
        3. Emit metrics into logger for checkpoint diagnostics.
    """
    try:
        if os.getenv("LOG_COUNTS", LOG_COUNTS_DEFAULT) != "1":
            logger.info(f"[{label}] row counts skipped (set LOG_COUNTS=1 to enable).")
            return

        total = df.count()
        distinct = df.select("MMSI").distinct().count() if "MMSI" in df.columns else 0
        logger.info(f"[{label}] total={total}, distinct_mmsi={distinct}")
    except Exception as e:
        logger.warning(f"df stats failed for {label}: {e}")


# -----------------------------------------------------------------------------
# Movement and seeding helpers
# -----------------------------------------------------------------------------
# =========================================================
# [9] add_movement_state
# Purpose: classify anchored vs moving from SOG threshold
# =========================================================
def add_movement_state(df: DataFrame, sog_col: str = "SOG", threshold: float = 0.5) -> DataFrame:
    """
    Label vessel movement state using speed over ground.
    < threshold => anchored, else moving.
    """
    try:
        return df.withColumn(
            "movement_state",
            F.when(F.col(sog_col) < threshold, F.lit("anchored")).otherwise(F.lit("moving")),
        )
    except Exception as e:
        raise RuntimeError(f"movement state derivation failed: {e}")


# =========================================================
# [10] prepare_seeded_union
# Purpose: attach seed markers and union state + staging
# =========================================================
def prepare_seeded_union(df_state: DataFrame,
                         df_staging: DataFrame,
                         voyage_col: str = "VoyageID") -> DataFrame:
    """
    Attach seed markers and union state + staging for seeded voyage continuity.
    """
    try:
        df_state_prepped = (
            df_state
            .withColumn("SeedVoyageID", F.col(voyage_col))
            .drop(voyage_col)
            .withColumn("is_seed", F.lit(True))
        )

        df_staging_prepped = df_staging
        if voyage_col in df_staging_prepped.columns:
            df_staging_prepped = df_staging_prepped.drop(voyage_col)

        df_staging_prepped = (
            df_staging_prepped
            .withColumn("SeedVoyageID", F.lit(None))
            .withColumn("is_seed", F.lit(False))
        )

        return df_state_prepped.unionByName(df_staging_prepped, allowMissingColumns=True)
    except Exception as e:
        raise RuntimeError(f"seeded union failed: {e}")
# Simple geohash encoder (pure Python) to avoid external dependency
def _encode_geohash(lat: float, lon: float, precision: int = 6) -> str:
    base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_interval = [-90.0, 90.0]
    lon_interval = [-180.0, 180.0]
    bits = [16, 8, 4, 2, 1]
    is_even = True
    bit = 0
    ch = 0
    geohash = []

    while len(geohash) < precision:
        if is_even:
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if lon > mid:
                ch |= bits[bit]
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if lat > mid:
                ch |= bits[bit]
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid

        is_even = not is_even
        if bit < 4:
            bit += 1
        else:
            geohash.append(base32[ch])
            bit = 0
            ch = 0

    return "".join(geohash)
