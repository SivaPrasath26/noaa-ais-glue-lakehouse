# Detailed Architecture and Flows

This file captures deep, step-by-step flows for the NOAA AIS pipeline, covering raw→staging, staging→curated (Facts 1 & 2 with state), and the end-to-end architecture including dimensions and consumption.

---

## Raw → Staging (detailed)
```mermaid
flowchart TD
    R00[NOAA AIS raw CSVs] --> R01[Land to S3 raw layer]
    R01 --> R02[AWS Glue crawler updates Catalog]
    R01 --> R03[Read raw CSV (AIS_RAW_SCHEMA enforced)]
    R03 --> R04[Normalize columns via mapping (MMSI/BaseDateTime/etc.)]
    R04 --> R05[Type cast to staging schema (ints/doubles/timestamps)]
    R05 --> R06[Empty-to-null cleansing]
    R06 --> R07[Parse BaseDateTime to TimestampType]
    R07 --> R08[Coordinate validation (lat/lon bounds)]
    R08 --> R09[Quarantine invalid coords]
    R09 --> R10[Clamp SOG/COG/Heading]
    R10 --> R11[Hash-based deduplication]
    R11 --> R12[Movement flag derivation]
    R12 --> R13[Partition columns year/month/day]
    R13 --> R14[Write staging parquet (year/month/day)]
    R14 --> R15[Staging ready for downstream]
```

---

## Staging → Curated (Fact 1 & Fact 2, detailed)
```mermaid
flowchart TD
    %% Inputs & mode
    A00[Staging partitions (year/month/day)] --> A01[Load window (start_date..end_date) with schema enforcement]
    A02[Glue args: mode, start_date, end_date, LOG_COUNTS] -->|incremental/recompute| A03[Seed = prior-day state (by_date = start_date - 1)]

    %% Union and prep
    A03 --> A04[Union seed + window (prepare_seeded_union)]
    A01 --> A04

    %% Trajectory reconstruction
    A04 --> A10[Lag features (PrevTime/PrevLAT/PrevLON)]
    A10 --> A11[GapHours > 3h → voyage_id increment]
    A11 --> A12[Compute voyage_id with seed continuity]
    A12 --> A13[Haversine segment distance]
    A13 --> A14[Geohash (precision 6)]
    A14 --> A15[Movement_state (SOG threshold)]
    A15 --> A16[Cast curated numeric types]
    A16 --> A17[Filter to window; drop seeds]

    %% Sampling
    A17 --> A20[Sampling: bucket_move (10m fast / 30m slow)]
    A20 --> A21[Sampling: bucket_anchor (sparse endpoints)]
    A21 --> A22[Keep day start/end]
    A22 --> A23[Filtered sampled set]

    %% Fact 1 write
    A23 --> A30[Repartition by mmsi]
    A30 --> A31[Write FACT 1: trajectory_points\npartition year/month/day\nreplaceWhere = window]

    %% State snapshot
    A23 --> A32[Latest per MMSI]
    A32 --> A33[Write state snapshot by_date = end_date]

    %% Fact 2 aggregation
    A31 --> A40[Voyage-level aggregation]
    A40 --> A41[Write FACT 2: voyage_summary\ncoalesced (no partitions)]

    %% Next run seed
    A33 --> A50[Next run seeds from by_date]
    A41 --> A50
```

---

## End-to-End Architecture (raw→staging→curated + dims + consumption)
```mermaid
flowchart TD
    %% Raw layer
    X00[NOAA AIS raw CSVs] --> X01[S3 raw layer]
    X01 --> X02[AWS Glue crawler]
    X02 --> X03[Glue Data Catalog]
    X01 --> X04[Raw→Staging job\nnormalize/cast/clean\nvalidate coords/quarantine\nclamp SOG/COG/Heading\nhash dedup + movement flag]
    X04 --> X05[S3 staging parquet (year/month/day)]

    %% Staging→Curated job
    X05 --> X10[Staging→Curated Glue job\nmode = incremental/recompute]
    X10 --> X11[Seed from prior-day state (by_date)]
    X10 --> X12[Load window (start..end) with schema enforcement]
    X11 --> X13[Union seed + window]
    X12 --> X13
    X13 --> X14[Trajectory reconstruction\n(3h gap voyage_id, lag, haversine, geohash, movement_state)]
    X14 --> X15[Sampling (10/30m moving; sparse anchor endpoints; keep day ends)]
    X15 --> X16[FACT 1: trajectory_points\npartition year/month/day\nreplaceWhere window\nrepartition by mmsi]
    X15 --> X17[State snapshot by_date = end_date]
    X16 --> X18[FACT 2: voyage_summary (coalesced)]
    X17 --> X19[Seed for next run]

    %% Dimensions
    D00[S3 lookups (MID, callsign, nav status, vessel type)] --> D01[Glue dims job (spark-excel package)]
    D01 --> D02[Dim country/nav_status/vessel_type parquet]

    %% Consumption
    X16 --> C01[Athena / BI / APIs]
    X18 --> C01
    D02 --> C01
```
