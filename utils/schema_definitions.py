# schema_definitions.py
"""
PySpark schema definitions for NOAA AIS raw input.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    LongType,
    BooleanType,
)

# ----------------------------------------------------------
# RAW SCHEMA — for CSV ingestion (bronze layer)
# ----------------------------------------------------------
AIS_RAW_SCHEMA = StructType([
    StructField("MMSI", IntegerType(), True),           # Maritime Mobile Service Identity
    StructField("BaseDateTime", StringType(), True),    # UTC timestamp string, parsed in ETL
    StructField("LAT", DoubleType(), True),             # Latitude (decimal degrees)
    StructField("LON", DoubleType(), True),             # Longitude (decimal degrees)
    StructField("SOG", DoubleType(), True),             # Speed over ground (knots)
    StructField("COG", DoubleType(), True),             # Course over ground (degrees)
    StructField("Heading", DoubleType(), True),         # Heading (degrees)
    StructField("VesselName", StringType(), True),      # Vessel name
    StructField("IMO", StringType(), True),             # IMO number (string to keep leading zeros)
    StructField("CallSign", StringType(), True),        # Radio call sign
    StructField("VesselType", IntegerType(), True),     # Vessel type code
    StructField("Status", IntegerType(), True),         # Navigational status code
    StructField("Length", DoubleType(), True),          # Vessel length (meters)
    StructField("Width", DoubleType(), True),           # Vessel width (meters)
    StructField("Draft", DoubleType(), True),           # Draft (meters)
    StructField("Cargo", IntegerType(), True),          # Cargo type code
    StructField("TransceiverClass", StringType(), True) # AIS class ('A' or 'B')
])


# ----------------------------------------------------------
# STAGING SCHEMA — for cleaned parquet output (silver layer)
# ----------------------------------------------------------
AIS_STAGING_SCHEMA = StructType([
    StructField("MMSI", IntegerType(), True),           # Maritime Mobile Service Identity
    StructField("BaseDateTime", TimestampType(), True), # UTC timestamp parsed to Spark TimestampType
    StructField("LAT", DoubleType(), True),             # Latitude (decimal degrees)
    StructField("LON", DoubleType(), True),             # Longitude (decimal degrees)
    StructField("SOG", DoubleType(), True),             # Speed over ground (knots)
    StructField("COG", DoubleType(), True),             # Course over ground (degrees)
    StructField("Heading", DoubleType(), True),         # Heading (degrees)
    StructField("VesselName", StringType(), True),      # Vessel name
    StructField("IMO", StringType(), True),             # IMO number (string to keep leading zeros)
    StructField("CallSign", StringType(), True),        # Radio call sign
    StructField("VesselType", IntegerType(), True),     # Vessel type code
    StructField("Status", IntegerType(), True),         # Navigational status code
    StructField("Length", DoubleType(), True),          # Vessel length (meters)
    StructField("Width", DoubleType(), True),           # Vessel width (meters)
    StructField("Draft", DoubleType(), True),           # Draft (meters)
    StructField("Cargo", IntegerType(), True),          # Cargo type code
    StructField("TransceiverClass", StringType(), True),# AIS class ('A' or 'B')
    StructField("MovementFlag", IntegerType(), True),   # Derived movement status flag
    StructField("year", IntegerType(), True),           # Partition: year
    StructField("month", IntegerType(), True),          # Partition: month
    StructField("day", IntegerType(), True)             # Partition: day
])


# ----------------------------------------------------------
# SCHEMA MAP - stage-based schema enforcement
# ----------------------------------------------------------
SCHEMA_MAP = {
    "raw": AIS_RAW_SCHEMA,
    "staging": AIS_STAGING_SCHEMA,
}

# ----------------------------------------------------------
# STATE SNAPSHOT SCHEMA - compact per-MMSI continuity (curated state)
# ----------------------------------------------------------
STATE_SNAPSHOT_SCHEMA = StructType([
    StructField("MMSI", IntegerType(), False),
    StructField("BaseDateTime", TimestampType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("VoyageID", LongType(), True),
])

SCHEMA_MAP["state_snapshot"] = STATE_SNAPSHOT_SCHEMA


# ----------------------------------------------------------
# VOYAGE STATE SCHEMA - running aggregates per voyage
# ----------------------------------------------------------
VOYAGE_STATE_SCHEMA = StructType([
    StructField("mmsi", IntegerType(), False),
    StructField("voyageid", LongType(), False),
    StructField("voyagestart", TimestampType(), True),
    StructField("lasttime", TimestampType(), True),
    StructField("totaldistancekm", DoubleType(), True),
    StructField("sumsog", DoubleType(), True),
    StructField("countsog", LongType(), True),
    StructField("sumlat", DoubleType(), True),
    StructField("sumlon", DoubleType(), True),
    StructField("pointcount", LongType(), True),
    StructField("is_complete", BooleanType(), True),
])

SCHEMA_MAP["voyage_state"] = VOYAGE_STATE_SCHEMA
