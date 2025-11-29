"""
fact_voyage_summary.py

Curated Fact 2 - Voyage-level summary statistics.
One row per voyage (MMSI + VoyageID) with full-span metrics. Incremental:
- Reads only the window partitions of trajectory_points
- Updates per-voyage running state
- Upserts summary rows for touched voyages (partitioned by VoyageStartDate)
"""

from datetime import datetime, timedelta, date
from typing import Optional, Tuple

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
from utils.common_functions_curated import safe_cast_columns, log_df_stats
from utils.config import setup_logger, CFG
from utils.voyage_state_io import read_voyage_state_by_date, write_voyage_state_by_date

logger = setup_logger(__name__)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _parse_date(date_str: str):
    return datetime.fromisoformat(date_str).date()


def _date_range(start: date, end: date):
    d = start
    one = timedelta(days=1)
    while d <= end:
        yield d
        d += one


def _build_partition_paths(base_path: str, start: datetime.date, end: datetime.date) -> list:
    paths = []
    d = start
    one = timedelta(days=1)
    while d <= end:
        paths.append(f"{base_path}year={d.strftime('%Y')}/month={d.strftime('%m')}/day={d.strftime('%d')}/")
        d += one
    return paths


def _window_bounds(start_date: str, end_date: str) -> Tuple[datetime, datetime]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    start_ts = datetime.combine(start, datetime.min.time())
    end_ts = datetime.combine(end + timedelta(days=1), datetime.min.time())
    return start_ts, end_ts


def _load_trajectory_window(spark: SparkSession, base_path: str, start_date: str, end_date: str):
    """Read only the trajectory partitions for [start_date, end_date]."""
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    paths = _build_partition_paths(base_path, start, end)
    dfs = []
    for p in paths:
        try:
            dfs.append(spark.read.parquet(p))
        except Exception as e:
            logger.warning(f"Skipping trajectory partition {p}: {e}")
            continue
    if not dfs:
        raise RuntimeError("No trajectory data found for requested window")
    df = dfs[0]
    for part in dfs[1:]:
        df = df.unionByName(part, allowMissingColumns=True)
    start_ts, end_ts = _window_bounds(start_date, end_date)
    df = df.filter((F.col("BaseDateTime") >= F.lit(start_ts)) & (F.col("BaseDateTime") < F.lit(end_ts)))

    # Normalize column names to lowercase for downstream processing
    return df.selectExpr(
        "MMSI as mmsi",
        "VoyageID as voyageid",
        "BaseDateTime as basedatetime",
        "SegmentDistanceKM as segmentdistancekm",
        "SOG as sog",
        "LAT as lat",
        "LON as lon",
    )


def _compute_window_deltas(df):
    """Aggregate window-only rows into per-voyage deltas."""
    return (
        df.groupBy("mmsi", "voyageid")
        .agg(
            F.min("basedatetime").alias("windowstart"),
            F.max("basedatetime").alias("windowend"),
            F.sum("segmentdistancekm").alias("distancedelta"),
            F.sum("sog").alias("sumsogdelta"),
            F.count(F.col("sog")).alias("countsogdelta"),
            F.sum("lat").alias("sumlatdelta"),
            F.sum("lon").alias("sumlondelta"),
            F.count(F.lit(1)).alias("pointcountdelta"),
        )
    )


def _merge_state(state_df, deltas_df, window_end_ts: datetime, gap_cushion_hours: int = 24):
    """
    Merge window deltas into voyage state.
    - If voyage seen in this window: add deltas, mark complete=False.
    - If not seen: keep prior totals; mark complete=True if stale past cushion.
    """
    cutoff_ts = window_end_ts - timedelta(hours=gap_cushion_hours)

    joined = state_df.alias("s").join(deltas_df.alias("d"), ["mmsi", "voyageid"], "full_outer")
    joined = joined.withColumn("d_present", F.col("d.mmsi").isNotNull())
    joined = joined.withColumn("s_present", F.col("s.mmsi").isNotNull())

    updated = (
        joined
        .select(
            F.coalesce(F.col("s.mmsi"), F.col("d.mmsi")).alias("mmsi"),
            F.coalesce(F.col("s.voyageid"), F.col("d.voyageid")).alias("voyageid"),
            F.coalesce(F.col("s.voyagestart"), F.col("d.windowstart")).alias("voyagestart"),
            F.greatest(F.col("s.lasttime"), F.col("d.windowend")).alias("lasttime"),
            (F.coalesce(F.col("s.totaldistancekm"), F.lit(0.0)) + F.coalesce(F.col("d.distancedelta"), F.lit(0.0))).alias("totaldistancekm"),
            (F.coalesce(F.col("s.sumsog"), F.lit(0.0)) + F.coalesce(F.col("d.sumsogdelta"), F.lit(0.0))).alias("sumsog"),
            (F.coalesce(F.col("s.countsog"), F.lit(0)) + F.coalesce(F.col("d.countsogdelta"), F.lit(0))).alias("countsog"),
            (F.coalesce(F.col("s.sumlat"), F.lit(0.0)) + F.coalesce(F.col("d.sumlatdelta"), F.lit(0.0))).alias("sumlat"),
            (F.coalesce(F.col("s.sumlon"), F.lit(0.0)) + F.coalesce(F.col("d.sumlondelta"), F.lit(0.0))).alias("sumlon"),
            (F.coalesce(F.col("s.pointcount"), F.lit(0)) + F.coalesce(F.col("d.pointcountdelta"), F.lit(0))).alias("pointcount"),
            F.col("d_present"),
            F.col("s_present"),
            F.col("s.is_complete").alias("prev_complete"),
            F.col("s.lasttime").alias("prev_last_time"),
            F.coalesce(F.col("s.needs_publish"), F.lit(False)).alias("prev_needs_publish"),
        )
        .withColumn(
            "lasttime",
            F.coalesce(F.col("lasttime"), F.col("voyagestart"))
        )
    )

    # Completion heuristic: if voyage is seen in this window => not complete.
    # If not seen, mark complete if prior last time is older than cutoff or was already marked complete.
    updated = updated.withColumn(
        "is_complete",
        F.when(F.col("d_present"), F.lit(False))
        .when(F.coalesce(F.col("prev_complete"), F.lit(False)), F.col("prev_complete"))
        .when(F.col("prev_last_time").isNull(), F.lit(False))
        .when(F.col("prev_last_time") < F.lit(cutoff_ts), F.lit(True))
        .otherwise(F.lit(False))
    )

    # Flag voyages that need summary rewrite (touched or completion status flipped)
    updated = updated.withColumn(
        "needs_publish",
        (F.col("prev_needs_publish")) |
        (F.col("d_present")) |
        (F.coalesce(F.col("prev_complete"), F.lit(False)) != F.col("is_complete"))
    )

    return updated.drop("d_present", "s_present", "prev_complete", "prev_last_time", "prev_needs_publish")


def _emit_summary(df_state, output_path: str):
    """Write summary rows for voyages flagged needs_publish."""
    df_publish = df_state.filter(F.col("needs_publish"))
    if df_publish.rdd.isEmpty():
        logger.info("Step: voyage_summary - nothing to publish for this window")
        return

    df = (
        df_publish
        .withColumn("durationhours", (F.unix_timestamp("lasttime") - F.unix_timestamp("voyagestart")) / 3600)
        .withColumn("avgspeed", F.when(F.col("durationhours") > 0, F.col("totaldistancekm") / F.col("durationhours")))
        .withColumn("avglat", F.when(F.col("pointcount") > 0, F.col("sumlat") / F.col("pointcount")))
        .withColumn("avglon", F.when(F.col("pointcount") > 0, F.col("sumlon") / F.col("pointcount")))
        .withColumn("voyagestartdate", F.to_date("voyagestart"))
    )

    df = safe_cast_columns(
        df,
        {
            "totaldistancekm": DoubleType(),
            "avgspeed": DoubleType(),
            "durationhours": DoubleType(),
            "avglat": DoubleType(),
            "avglon": DoubleType(),
        },
    )

    log_df_stats(df, "voyage_summary")

    # Limit overwrite to touched partitions
    dates = [row["voyagestartdate"] for row in df.select("voyagestartdate").distinct().collect()]
    date_filter_expr = " OR ".join([f"voyagestartdate = DATE '{d}'" for d in dates]) if dates else ""

    logger.info("Step: voyage_summary - write (partitioned by voyagestartdate)")
    writer = (
        df.repartition("voyagestartdate")
        .write
        .mode("overwrite")
        .partitionBy("voyagestartdate")
    )
    if date_filter_expr:
        writer = writer.option("replaceWhere", date_filter_expr)
    writer.parquet(output_path)
    logger.info(f"Step: voyage_summary - written to {output_path}")


# -----------------------------------------------------------------------------
# Voyage summary aggregation
# -----------------------------------------------------------------------------
def summarize_voyages(
    spark: SparkSession,
    traj_path: str,
    output_path: str,
    state_by_date_prefix: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Incremental voyage summary:
    - read trajectory window partitions
    - compute per-voyage deltas
    - merge into voyage state
    - publish summaries for touched voyages only
    """
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        logger.info(f"Step: voyage_summary - window={start_date}..{end_date}")

        # Read only window partitions from trajectory_points
        df_window = _load_trajectory_window(spark, traj_path, start_date, end_date)

        # Seed voyage state from prior day snapshot; initialize needs_publish flag
        start_date_obj = _parse_date(start_date)
        end_date_obj = _parse_date(end_date)
        prev_day = (start_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        df_state = read_voyage_state_by_date(spark, state_by_date_prefix, prev_day, fallback_empty=True)
        if "needs_publish" not in df_state.columns:
            df_state = df_state.withColumn("needs_publish", F.lit(False))

        # Process each day in the window sequentially, writing a snapshot per day
        for d in _date_range(start_date_obj, end_date_obj):
            day_start = datetime.combine(d, datetime.min.time())
            day_end = datetime.combine(d + timedelta(days=1), datetime.min.time())
            df_day = df_window.filter(
                (F.col("BaseDateTime") >= F.lit(day_start)) &
                (F.col("BaseDateTime") < F.lit(day_end))
            )
            df_deltas = _compute_window_deltas(df_day)
            df_state = _merge_state(df_state, df_deltas, day_end)
            df_state_clean = df_state.drop("needs_publish")
            write_voyage_state_by_date(df_state_clean, state_by_date_prefix, d.strftime("%Y-%m-%d"))

        # Publish summaries for voyages touched in this window (final state holds cumulative needs_publish)
        _emit_summary(df_state, output_path)

    except Exception as e:
        logger.error(f"Voyage summary computation failed: {e}", exc_info=True)
        raise


# -----------------------------------------------------------------------------
# Job entrypoint (standalone)
# -----------------------------------------------------------------------------
def run_voyage_summary_job(input_path: str, output_path: str, state_by_date_prefix: str, start_date: str, end_date: str):
    try:
        spark = SparkSession.builder.appName("VoyageSummaryBuilder").getOrCreate()
        summarize_voyages(
            spark,
            input_path,
            output_path,
            state_by_date_prefix,
            start_date,
            end_date,
        )
    except Exception as e:
        logger.error(f"Failed to run voyage summary job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_voyage_summary_job(
        CFG.S3_CURATED + "trajectory_points/",
        CFG.S3_CURATED + "voyage_summary/",
        CFG.VOYAGE_STATE_BY_DATE_PATH,
        start_date=datetime.utcnow().strftime("%Y-%m-%d"),
        end_date=datetime.utcnow().strftime("%Y-%m-%d"),
    )
