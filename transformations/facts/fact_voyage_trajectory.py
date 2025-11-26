"""
fact_voyage_trajectory.py

Curated Fact 1 - Vessel trajectory reconstruction with incremental/recompute support.
Transforms staging AIS data into voyage-segmented, spatially indexed trajectories.
Adds state seeding to keep voyage continuity across days.
"""

import argparse
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
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
    read_state_by_date,
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
    logger.info("Step: compute_trajectory - start window filtering and feature derivation")
    w = Window.partitionBy("MMSI").orderBy("BaseDateTime")

    # Ordered timeline per MMSI, including seed row to connect prior day
    df = df_input

    # Lag-based features (includes seed rows so day1 compares to day0)
    logger.info("Step: compute_trajectory - deriving lag features (PrevTime, PrevLAT, PrevLON)")
    df = df.withColumn("PrevTime", F.lag("BaseDateTime").over(w))
    df = df.withColumn("GapHours", (
        F.unix_timestamp("BaseDateTime") - F.unix_timestamp("PrevTime")
    ) / 3600)

    df = df.withColumn("PrevLAT", F.lag("LAT").over(w))
    df = df.withColumn("PrevLON", F.lag("LON").over(w))

    # Voyage offset from state seed
    logger.info("Step: compute_trajectory - computing voyage_id with seed continuity and 3h gap rule")
    base_voyage = F.first("SeedVoyageID", ignorenulls=True).over(Window.partitionBy("MMSI"))
    voyage_increments = F.sum(F.when(F.col("GapHours") > 3, 1).otherwise(0)).over(
        w.rowsBetween(Window.unboundedPreceding, 0)
    )
    df = df.withColumn("VoyageID", F.coalesce(base_voyage, F.lit(0)) + voyage_increments)

    # Distance and spatial index
    logger.info("Step: compute_trajectory - calculating haversine distance and geohash")
    df = df.withColumn(
        "SegmentDistanceKM",
        calculate_haversine("PrevLAT", "PrevLON", "LAT", "LON"),
    )
    df = add_geohash(df, lat_col="LAT", lon_col="LON", precision=6)

    # Cast curated schema expectations
    logger.info("Step: compute_trajectory - casting numeric columns to curated types")
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

    logger.info("Step: compute_trajectory - deriving movement_state")
    df = add_movement_state(df, sog_col="SOG", threshold=0.5)

    # Drop seeds from curated outputs; keep only target window rows
    logger.info("Step: compute_trajectory - filtering window and dropping seed rows")
    df_output = df.filter(~F.col("is_seed"))
    df_output = df_output.filter(
        (F.col("BaseDateTime") >= F.lit(start_ts)) &
        (F.col("BaseDateTime") < F.lit(end_ts))
    )

    # Clean helpers
    logger.info("Step: compute_trajectory - dropping helper columns and returning")
    return df_output.drop("PrevTime", "PrevLAT", "PrevLON", "GapHours", "SeedVoyageID", "is_seed")


# =========================================================
# sample_trajectory
# Purpose: reduce trajectory density while keeping path fidelity
# =========================================================
def sample_trajectory(df_input: DataFrame,
                      fast_sog_threshold_knots: float = 12.0,
                      fast_interval_min: int = 10,
                      slow_interval_min: int = 30,
                      anchor_interval_min: int = 120) -> DataFrame:
    """
    Sample points by:
      - keeping all day/voyage endpoints
      - keeping moving points sampled per bucket (fast vs slow cadence)
      - keeping anchored points sparsely (default first/last per 120m bucket)
    Then recompute segment distances on the sampled set.
    """
    df = df_input

    df = (
        df
        .withColumn(
            "bucket_move",
            F.floor(
                F.unix_timestamp("BaseDateTime") /
                F.when(F.coalesce(F.col("SOG"), F.lit(0.0)) >= fast_sog_threshold_knots,
                       fast_interval_min * 60)
                 .otherwise(slow_interval_min * 60)
            )
        )
        .withColumn("bucket_anchor", F.floor(F.unix_timestamp("BaseDateTime") / (anchor_interval_min * 60)))
        .withColumn("year", F.year("BaseDateTime"))
        .withColumn("month", F.month("BaseDateTime"))
        .withColumn("day", F.dayofmonth("BaseDateTime"))
    )

    w_move = Window.partitionBy("MMSI", "VoyageID", "bucket_move").orderBy("BaseDateTime")
    w_anchor_asc = Window.partitionBy("MMSI", "VoyageID", "bucket_anchor").orderBy("BaseDateTime")
    w_anchor_desc = Window.partitionBy("MMSI", "VoyageID", "bucket_anchor").orderBy(F.col("BaseDateTime").desc())
    w_day_first = Window.partitionBy("MMSI", "year", "month", "day").orderBy("BaseDateTime")
    w_day_last = Window.partitionBy("MMSI", "year", "month", "day").orderBy(F.col("BaseDateTime").desc())

    df = (
        df
        .withColumn("keep_move", F.when(F.col("movement_state") != "anchored", F.row_number().over(w_move)).otherwise(F.lit(None)))
        .withColumn("keep_anchor_first", F.when(F.col("movement_state") == "anchored", F.row_number().over(w_anchor_asc)).otherwise(F.lit(None)))
        .withColumn("keep_anchor_last", F.when(F.col("movement_state") == "anchored", F.row_number().over(w_anchor_desc)).otherwise(F.lit(None)))
        .withColumn("keep_day_first", F.row_number().over(w_day_first))
        .withColumn("keep_day_last", F.row_number().over(w_day_last))
    )

    df = df.filter(
        (F.col("keep_move") == 1) |
        (F.col("keep_anchor_first") == 1) |
        (F.col("keep_anchor_last") == 1) |
        (F.col("keep_day_first") == 1) |
        (F.col("keep_day_last") == 1)
    )

    df = df.drop("bucket_move", "bucket_anchor", "keep_move", "keep_anchor_first", "keep_anchor_last", "keep_day_first", "keep_day_last")

    # Recompute distances on sampled set
    w_prev = Window.partitionBy("MMSI", "VoyageID").orderBy("BaseDateTime")
    df = df.withColumn("PrevLAT_thin", F.lag("LAT").over(w_prev))
    df = df.withColumn("PrevLON_thin", F.lag("LON").over(w_prev))
    df = df.withColumn(
        "SegmentDistanceKM",
        calculate_haversine("PrevLAT_thin", "PrevLON_thin", "LAT", "LON"),
    )
    df = df.drop("PrevLAT_thin", "PrevLON_thin")

    return df

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
        logger.info("Step: load staging window")

        # Load data
        df_staging = load_staging_window(spark, input_path, start_date, end_date)
        start_date_obj = _parse_date(start_date)
        end_date_obj = _parse_date(end_date)

        seed_date = (start_date_obj - timedelta(days=1)).isoformat()
        logger.info(f"Step: load state seed from prior day {seed_date}")
        df_state = read_state_by_date(
            spark, state_by_date_prefix, seed_date, fallback_empty=True
        )

        # Prepare seed and union (prior day state + window staging)
        logger.info("Step: prepare seeded union of state + staging")
        df_union = prepare_seeded_union(df_state, df_staging, voyage_col="VoyageID")

        start_ts = datetime.combine(start_date_obj, datetime.min.time())
        end_ts = datetime.combine(end_date_obj + timedelta(days=1), datetime.min.time())

        logger.info("Step: compute trajectory features (voyage segmentation, haversine, geohash, movement state)")
        df_curated = compute_trajectory(df_union, start_ts, end_ts)

        logger.info("Step: write trajectory_points (partitioned by date)")
        log_df_stats(df_curated, "trajectory_points_full")

        df_thin = sample_trajectory(df_curated)
        log_df_stats(df_thin, "trajectory_points_sampled")

        dates = []
        d_iter = start_date_obj
        one_day = timedelta(days=1)
        while d_iter <= end_date_obj:
            dates.append(d_iter)
            d_iter += one_day
        date_filter_expr = " OR ".join(
            [f"year={d.year} AND month={d.month} AND day={d.day}" for d in dates]
        )

        writer_partitions = max(200, len(df_thin.columns))
        writer = (
            df_thin
            .repartition(writer_partitions, "mmsi")
            .write
            .mode("overwrite")
            .partitionBy("year", "month", "day")
        )
        if date_filter_expr:
            writer = writer.option("replaceWhere", date_filter_expr)
        writer.parquet(output_path)
        logger.info(f"Trajectory fact written to {output_path} (partitioned by year/month/day)")

        # Update state with last row per MMSI after this window (by_date only)
        df_state_out = latest_per_mmsi(df_curated)
        write_state_by_date(df_state_out, state_by_date_prefix, end_date_obj.isoformat())
        logger.info(
            f"State snapshot written for {end_date_obj.isoformat()} under by_date"
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
