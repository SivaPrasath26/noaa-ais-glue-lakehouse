-- ======================================================================
-- Script: create_noaa_ais_tables.sql
-- Purpose: Define Athena external tables for NOAA AIS data lake
-- Author: Siva Prasath
-- Description:
--   1. Defines the raw AIS table (CSV source, OpenCSVSerde)
--   2. Defines the cleaned staging table (Parquet format)
-- ======================================================================


-- =========================================================
-- 1. RAW AIS TABLE (CSV files in s3://noaa-ais-raw-data/)
-- =========================================================
CREATE EXTERNAL TABLE noaa_ais_all_raw (
  mmsi STRING,
  basedatetime STRING,
  lat STRING,
  lon STRING,
  sog STRING,
  cog STRING,
  heading STRING,
  vesselname STRING,
  imo STRING,
  callsign STRING,
  vesseltype STRING,
  status STRING,
  length STRING,
  width STRING,
  draft STRING,
  cargo STRING
)
PARTITIONED BY (
  year STRING,
  month STRING,
  day STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://noaa-ais-raw-data/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Discover partitions for the raw table
MSCK REPAIR TABLE noaa_ais_all_raw;


-- =========================================================
-- 2. CLEANED / STAGING AIS TABLE (Parquet format)
-- =========================================================
CREATE EXTERNAL TABLE noaa_ais_cleaned (
  MMSI INT,
  BaseDateTime TIMESTAMP,
  LAT DOUBLE,
  LON DOUBLE,
  SOG DOUBLE,
  COG DOUBLE,
  Heading DOUBLE,
  VesselName STRING,
  IMO STRING,
  CallSign STRING,
  VesselType INT,
  Status INT,
  Length DOUBLE,
  Width DOUBLE,
  Draft DOUBLE,
  Cargo INT,
  TransceiverClass STRING,
  MovementFlag INT
)
PARTITIONED BY (
  year INT,
  month INT,
  day INT
)
STORED AS PARQUET
LOCATION 's3://noaa-ais-staging-data/cleaned/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Discover partitions for the cleaned table
MSCK REPAIR TABLE noaa_ais_cleaned;

-- ======================================================================
-- End of Script
-- ======================================================================
