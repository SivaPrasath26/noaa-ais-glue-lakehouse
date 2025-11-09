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
)

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

SCHEMA_MAP = {
    "raw": AIS_RAW_SCHEMA
}
