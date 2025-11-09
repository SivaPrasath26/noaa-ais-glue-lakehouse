## NOAA AIS ETL Pipeline Debugging and Stabilization Notes

### Overview

This document captures all critical issues encountered and resolved during the NOAA AIS ETL Glue job development. The objective was to make the pipeline production-grade, schema-drift-tolerant, and data-quality resilient.

---

## 2025 Historical Backfill Debug Summary

This entire debugging sequence originated during the 2025 historical backfill run, when the NOAA AIS Glue ETL pipeline processed the `year=2025/month=01/day=01/` partition.  
The issue surfaced only during this full backfill (not incremental loads), because the 2025 raw feed schema had subtle structural drift from 2024 files:

- The raw CSV column order was changed to **longitude first, then latitude**.  
- The ETL job, using a positional schema definition, assumed **LAT first, then LON**.

This mismatch silently inverted coordinate assignments across 7 million records, leading to:

- False detection of invalid coordinates.  
- A large quarantine file (~4 million rows).  
- Misleading summary statistics (LAT mean ≈ -107, LON mean ≈ +35).

Once detected, the fix involved switching to **header-based reads**, introducing **column normalization**, and adding an **auto-swap safeguard** to prevent future schema drifts from corrupting coordinates again.

This backfill run ultimately became the turning point that hardened the pipeline into a fully schema-drift-tolerant, production-grade ETL system.

---

### 1. Timestamp Parsing Failure

**Symptom:**

* Rows dropped after timestamp parsing.
* Log: `parsed_count = 0`.

**Cause:**
Raw files had timestamps in Excel-style (`1/1/2025 0:00`), while the parser expected ISO format (`yyyy-MM-dd'T'HH:mm:ss[.SSS]`).

**Fix:**
Added multi-format parsing logic inside `parse_base_datetime()`:

```python
df = df.withColumn(
    input_col,
    F.coalesce(
        F.to_timestamp(F.col(input_col), "yyyy-MM-dd'T'HH:mm:ss[.SSS]"),
        F.to_timestamp(F.col(input_col), "M/d/yyyy H:mm")
    )
)
```

This allows both ISO and Excel timestamp variants.

---

### 2. Schema Drift from Source

**Symptom:**
Column name mismatches across years (e.g., `latitude` vs `LAT`, `vessel_name` vs `VesselName`).

**Fix:**
Added `normalize_columns(df)` that standardizes column names using a YAML mapping. Example:

```yaml
latitude: LAT
longitude: LON
vessel_name: VesselName
base_date_time: BaseDateTime
```

This ensures column consistency across all input years.

---

### 3. Critical: Wrong Column Assignment (Latitude/Longitude Swapped)

**Symptom:**
~4 million quarantined rows. LAT mean = -107, LON mean = +35.

**Cause:**
Spark assigned CSV columns **by position**, not **by name**, since `.schema()` enforces positional mapping.
CSV header order was: `longitude, latitude`. Schema expected: `LAT, LON`.

**Fix:**
Read CSV columns by name first, then cast manually:

```python
df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(input_path)
)

df_raw = normalize_columns(df_raw)

for field in SCHEMA_MAP["raw"].fields:
    if field.name in df_raw.columns:
        df_raw = df_raw.withColumn(field.name, F.col(field.name).cast(field.dataType))
```

---

### 4. Overwrite vs Append Issue

**Symptom:**
No output in `/cleaned/`, only old data visible.

**Cause:**
Dynamic overwrite mode was active, but data was written at the wrong level.

**Fix:**
Explicitly build partition paths:

```python
y, m, d = df.select("year", "month", "day").first()
partition_path = f"{output_path.rstrip('/')}/year={y}/month={m}/day={d}/"

df.write.mode("overwrite").parquet(partition_path)
```

Ensures deterministic single-day partition writes.

---

### 5. False Quarantine Inflation

**Symptom:**
Millions of rows flagged invalid due to coordinate filter.

**Cause:**
Latitude/Longitude mismatch (see issue #3).

**Fix:**
Once column order was corrected, the same logic kept >99% of records valid.

---

### 6. Limited Debug Visibility

**Fix:**
Added detailed debugging checkpoints:

```python
logger.info(f"DEBUG A: raw_count = {df_raw.count()}")
logger.info(f"DEBUG B: parsed_count = {df.count()}")
logger.info(f"DEBUG C: columns = {df.columns}")
```

This exposed intermediate record counts and transformation results.

---

### 7. Auto-Detection and Swap for Misaligned Coordinates

**Problem:**
Future sources might again flip latitude/longitude.

**Fix:**
Added `_fix_swapped_latlon()` inside `clean_coordinates()`:

```python
def _fix_swapped_latlon(df: DataFrame, lat_col: str, lon_col: str) -> DataFrame:
    stats = df.select(
        F.mean(lat_col).alias('mean_lat'),
        F.mean(lon_col).alias('mean_lon')
    ).collect()[0]

    mean_lat, mean_lon = stats['mean_lat'], stats['mean_lon']
    if abs(mean_lat) > 90 and abs(mean_lon) <= 90:
        logger.warning("Detected swapped LAT/LON — auto-correcting.")
        df = df.withColumnRenamed(lat_col, "__tmp_lat").withColumnRenamed(lon_col, lat_col).withColumnRenamed("__tmp_lat", lon_col)
    return df
```

This runs **before** coordinate filtering. It checks column means and auto-swaps when out of geographic bounds.

---

### 8. Production-Grade Stability Enhancements

* Early partition derivation ensures quarantines remain date-aware.
* Dynamic path construction guarantees clean partitioning.
* Centralized logging for schema, stats, and quarantine counts.
* No positional column dependence — fully header-driven.

---

### Final Result

| Year | Month | Records   | Invalid | Notes                                    |
| ---- | ----- | --------- | ------- | ---------------------------------------- |
| 2025 | 01    | 7,336,243 | 0       | Correctly parsed, no invalid coordinates |

---

### Key Takeaways

1. Never use `.schema()` with CSVs where header order may differ.
2. Always log schema and sample data early in the pipeline.
3. Column normalization + dynamic casting ensures schema consistency.
4. Auto-swap logic prevents silent coordinate reversals.
5. Explicit partition write paths guarantee deterministic outputs.

This final build is a **production-ready Glue ETL** with full schema drift protection, coordinate validation, and historical compatibility.

