"""
fact_voyage_trajectory.py

Curated Fact 1 - Vessel trajectory reconstruction with incremental/recompute support.
Transforms staging AIS data into voyage-segmented, spatially indexed trajectories.
Adds state seeding to keep voyage continuity across days.
"""

import argparse
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from utils.common_functions_curated import (
    calculate_haversine,
    add_geohash,
    safe_cast_columns,
    add_movement_state,
    prepare_seeded_union,
    log_df_stats,
)
from utils.config import setup_logger, CFG
from utils.schema_definitions import AIS_STAGING_SCHEMA
from utils.state_io import (
    read_state_snapshot,
    read_state_by_date,
    write_state_snapshot,
    write_state_by_date,
    latest_per_mmsi,
)

logger = setup_logger(__name__)


# -----------------------------------------------------------------------------
# Date parsing and window load helpers
# -----------------------------------------------------------------------------

# =========================================================
# _parse_date
# Purpose: convert ISO date string to date
# =========================================================
def _parse_date(date_str: str) -> date:
    return datetime.fromisoformat(date_str).date()


def _build_partition_paths(base_path: str, start: date, end: date) -> list[str]:
    paths = []
    d = start
    one = timedelta(days=1)
    while d <= end:
        y, m, dd = d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")
        paths.append(f"{base_path}year={y}/month={m}/day={dd}/")
        d += one
    return paths


# =========================================================
# load_staging_window
# Purpose: read staging slice limited to start/end dates (partition pruning)
# =========================================================
def load_staging_window(spark: SparkSession, base_path: str, start_date: str, end_date: str):
    """Load staging data limited to [start_date, end_date] by partition paths. Skips unreadable partitions."""
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    start_ts = datetime.combine(start, datetime.min.time())
    end_ts = datetime.combine(end + timedelta(days=1), datetime.min.time())

    paths = _build_partition_paths(base_path, start, end)
    dfs = []
    type_map = {f.name: f.dataType for f in AIS_STAGING_SCHEMA.fields}
    for p in paths:
        try:
            part = (
                spark.read
                .schema(AIS_STAGING_SCHEMA)
                .option("mergeSchema", "false")
                .parquet(p)
            )
            dfs.append(part)
        except Exception as e:
            logger.warning(f"Schema-enforced read failed for {p}: {e}; attempting cast fallback")
            try:
                raw_part = spark.read.parquet(p)
                part = safe_cast_columns(raw_part, type_map)
                dfs.append(part)
                logger.warning(f"Loaded {p} with fallback casting; rows={part.count()}")
            except Exception as e2:
                logger.warning(f"Skipping partition {p}: {e2}")
                continue

    if not dfs:
        raise RuntimeError("No staging data found for requested window")

    df = dfs[0]
    for part in dfs[1:]:
        df = df.unionByName(part, allowMissingColumns=True)

    return df.filter(
        (F.col("BaseDateTime") >= F.lit(start_ts)) &
        (F.col("BaseDateTime") < F.lit(end_ts))
    )

# =========================================================
# compute_trajectory
# Purpose: seeded voyage segmentation + distance + geohash + movement_state
# =========================================================
def compute_trajectory(df_input, start_ts: datetime, end_ts: datetime):
    """Seeded voyage segmentation, distance, geohash, and movement state."""
    w = Window.partitionBy("MMSI").orderBy("BaseDateTime")

    # Ordered timeline per MMSI, including seed row to connect prior day
    df = df_input

    # Lag-based features (includes seed rows so day1 compares to day0)
    df = df.withColumn("PrevTime", F.lag("BaseDateTime").over(w))
    df = df.withColumn("GapHours", (
        F.unix_timestamp("BaseDateTime") - F.unix_timestamp("PrevTime")
    ) / 3600)

    df = df.withColumn("PrevLAT", F.lag("LAT").over(w))
    df = df.withColumn("PrevLON", F.lag("LON").over(w))

    # Voyage offset from state seed
    base_voyage = F.first("SeedVoyageID", ignorenulls=True).over(Window.partitionBy("MMSI"))
    voyage_increments = F.sum(F.when(F.col("GapHours") > 3, 1).otherwise(0)).over(
        w.rowsBetween(Window.unboundedPreceding, 0)
    )
    df = df.withColumn("VoyageID", F.coalesce(base_voyage, F.lit(0)) + voyage_increments)

    # Distance and spatial index
    df = df.withColumn(
        "SegmentDistanceKM",
        calculate_haversine("PrevLAT", "PrevLON", "LAT", "LON"),
    )
    df = add_geohash(df, lat_col="LAT", lon_col="LON", precision=6)

    # Cast curated schema expectations
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

    df = add_movement_state(df, sog_col="SOG", threshold=0.5)

    # Drop seeds from curated outputs; keep only target window rows
    df_output = df.filter(~F.col("is_seed"))
    df_output = df_output.filter(
        (F.col("BaseDateTime") >= F.lit(start_ts)) &
        (F.col("BaseDateTime") < F.lit(end_ts))
    )

    # Clean helpers
    return df_output.drop("PrevTime", "PrevLAT", "PrevLON", "GapHours", "SeedVoyageID", "is_seed")


# -----------------------------------------------------------------------------
# Main job
# -----------------------------------------------------------------------------

# =========================================================
# run_trajectory_job
# Purpose: orchestrate trajectory build (incremental/recompute)
# =========================================================
def run_trajectory_job(input_path: str,
                       output_path: str,
                       state_latest_path: str,
                       state_by_date_prefix: str,
                       start_date: str,
                       end_date: str,
                       mode: str = "incremental"):
    try:
        spark = SparkSession.builder.appName("TrajectoryFactBuilder").getOrCreate()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        logger.info(f"Run mode={mode}, window={start_date}..{end_date}")

        # Load data
        df_staging = load_staging_window(spark, input_path, start_date, end_date)
        start_date_obj = _parse_date(start_date)
        end_date_obj = _parse_date(end_date)

        if mode == "recompute":
            seed_date = (start_date_obj - timedelta(days=1)).isoformat()
            logger.info(f"Recompute mode: seeding with state from {seed_date}")
            df_state = read_state_by_date(
                spark, state_by_date_prefix, seed_date, fallback_empty=True
            )
        else:
            df_state = read_state_snapshot(spark, state_latest_path, fallback_empty=True)

        # Prepare seed and union (prior day state + window staging)
        df_union = prepare_seeded_union(df_state, df_staging, voyage_col="VoyageID")

        start_ts = datetime.combine(start_date_obj, datetime.min.time())
        end_ts = datetime.combine(end_date_obj + timedelta(days=1), datetime.min.time())

        df_curated = compute_trajectory(df_union, start_ts, end_ts)

        log_df_stats(df_curated, "trajectory_points")

        (
            df_curated
            .write
            .mode("overwrite")
            .partitionBy("MMSI", "VoyageID")
            .parquet(output_path)
        )
        logger.info(f"Trajectory fact written to {output_path}")

        # Update state with last row per MMSI after this window
        df_state_out = latest_per_mmsi(df_curated)
        write_state_snapshot(df_state_out, state_latest_path)
        write_state_by_date(df_state_out, state_by_date_prefix, end_date_obj.isoformat())
        logger.info(
            f"State snapshot updated at {state_latest_path} and dated version for {end_date_obj.isoformat()}"
        )

    except Exception as e:
        logger.error(f"Trajectory reconstruction failed: {e}", exc_info=True)
        raise


# =========================================================
# parse_args
# Purpose: CLI argument parsing for trajectory job
# =========================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Build trajectory fact with incremental support")
    parser.add_argument("--mode", default="incremental", choices=["incremental", "recompute"])
    parser.add_argument("--start_date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--staging_path",
        default=CFG.S3_STAGING + "cleaned/",
        help="Input staging base path",
    )
    parser.add_argument(
        "--curated_path",
        default=CFG.S3_CURATED + "trajectory_points/",
        help="Output curated path",
    )
    parser.add_argument(
        "--state_path",
        default=CFG.STATE_LATEST_PATH,
        help="State snapshot path (latest)",
    )
    parser.add_argument(
        "--state_by_date_prefix",
        default=CFG.STATE_BY_DATE_PATH,
        help="Prefix for dated state snapshots (append YYYY-MM-DD/)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_trajectory_job(
        input_path=args.staging_path,
        output_path=args.curated_path,
        state_latest_path=args.state_path,
        state_by_date_prefix=args.state_by_date_prefix,
        start_date=args.start_date,
        end_date=args.end_date,
        mode=args.mode,
    )
