# Detailed Architecture and Flows

This file capture, step-by-step flows for the NOAA AIS pipeline, covering raw -> staging, staging -> curated (Facts 1 & 2 with state), and the end-to-end architecture including dimensions and consumption.

---

## Raw -> Staging (detailed)

![alt text](../assets/images/pipelines/raw_to_staging.png)

Steps:
- Ingest NOAA AIS daily drops via CloudShell scripts directly into `s3://.../raw/year=YYYY/month=MM/day=DD/`
- Trigger Glue `raw_to_staging` job for the date window; read raw CSV with schema normalization and column mapping
- Validate lat/lon/SOG/COG/Heading; quarantine invalid rows; enforce types and null handling
- Deduplicate with a content hash to drop replays; partition by `year/month/day`
- Write cleaned Parquet to `s3://.../staging/year=YYYY/month=MM/day=DD/`; log row/quality metrics to CloudWatch

---

## Staging -> Curated (Fact 1 & Fact 2, detailed)

![alt text](../assets/images/pipelines/staging_to_curated.png)

Steps:
- Read staging partitions for the target dates with enforced schema
- Derive movement_state (anchored vs moving), apply 3h-gap voyage segmentation, and bucket timestamps (fast/slow/anchor)
- Sample timeline: keep day first/last, voyage boundaries, and first/last per movement bucket; recompute segmentdistancekm on the thinned path
- Build Fact 1 (trajectory) and Fact 2 (voyage summary) with geohash and aggregates; attach partition columns
- Write curated Parquet to `s3://.../curated/fact_voyage_trajectory` and `.../fact_voyage_summary` partitioned by `year/month/day`; update incremental state snapshot

---

## End-to-End Architecture (raw -> staging -> curated + dims + consumption)

![alt text](../assets/images/pipelines/architecture.png)

Steps:
- Ingest raw AIS to S3 (raw layer), then cleanse/dedup into staging via Glue job 1
- Transform staging to curated facts with state-aware incremental logic via Glue job 2
- Load dimensions (country/MID, nav status, vessel type) from lookup files into curated dims via Glue job 3
- Register Athena external tables/views over curated facts and dims; serve Power BI/GeoJSON/API queries
- Monitor via CloudWatch logs/metrics; rerun recompute mode for backfills; archive cold data to Glacier as needed
