## NOAA AIS Athena Integration - Row Count Analysis

### Overview

This document explains how Athena was configured to query **NOAA AIS raw data (2024–2025)** stored in **Amazon S3** without any data movement or additional cost.
The goal was to analyze total record counts across all daily CSV files efficiently.

---

### S3 Data Layout

Raw AIS data is stored in partitioned folders by year, month, and day:

```
s3://noaa-ais-raw-data/year=2024/month=01/day=01/ais-2024-01-01.csv
s3://noaa-ais-raw-data/year=2025/month=01/day=01/ais-2025-01-01.csv
...
```

Each folder represents a **logical partition** (`year`, `month`, `day`), allowing Athena to prune data scans.

---

### Step 1. Create Result Output Location

Athena requires a query result bucket for storing output files.

```bash
aws s3 mb s3://noaa-athena-query-results --region us-east-1
```

In the Athena console:

* Navigate to **Settings → Manage**
* Set **Query result location** to `s3://noaa-athena-query-results/`

---

### Step 2. Create External Table

The table is defined as an *external* table. It stores **metadata only** (no data duplication).

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS noaa_ais_all (
  mmsi STRING,
  base_datetime STRING,
  lat DOUBLE,
  lon DOUBLE,
  sog DOUBLE,
  cog DOUBLE,
  heading DOUBLE,
  vessel_name STRING,
  imo STRING,
  call_sign STRING,
  vessel_type STRING,
  status STRING,
  length DOUBLE,
  width DOUBLE,
  draft DOUBLE,
  cargo STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
LOCATION 's3://noaa-ais-raw-data/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

---

### Step 3. Register S3 Partitions

Run `MSCK REPAIR TABLE` to auto-discover partition folders.

```sql
MSCK REPAIR TABLE noaa_ais_all;
```

This scans the S3 directory tree and registers all paths like
`year=2024/month=01/day=01/` as partitions.
It **does not rewrite or move any S3 files.**

---

### Step 4. Count Total Records

Query row counts by year or overall totals.

```sql
-- Count total for each year
SELECT year, COUNT(*) AS row_count
FROM noaa_ais_all
GROUP BY year
ORDER BY year;

-- Result:
-- 2024 | 3119805347
-- 2025 | 1438950302
```

---

### Step 5. Cost Analysis

* **Athena pricing:** $5 per TB scanned.
* 2024 dataset: ~320 GB
* 2025 dataset: ~136 GB
* One Athena scan ≈ 455 GB ≈ **$2.2 (₹185)**
* Metadata storage in Glue and S3 bucket sitting are **free**.

---

### Key Points

* **External table** = metadata pointer, not data copy.
* **Repair table** = registers partitions only.
* **No write operations** to S3.
* **Cost** occurs only when scanning data (queries).
* **Reusable setup** for any future AIS years (just add partitions).

---

### Optional: Drop Table (metadata only)

If needed, we can remove the table and keep S3 data intact:

```sql
DROP TABLE noaa_ais_all;
```

---

### Summary

THis is a **cost-efficient Athena interface** to query multi-year AIS datasets directly from S3.
This setup supports large-scale record analysis (billions of rows) without Glue ETL jobs or intermediate storage.
