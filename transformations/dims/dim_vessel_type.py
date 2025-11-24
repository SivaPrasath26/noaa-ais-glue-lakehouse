# dim_vessel_type.py
"""
Dimension builder for NOAA AIS Vessel Type Codes.
Reads vessel type classification XLSX from S3, standardizes schema, and writes Parquet to lookup zone.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from utils.config import CFG

def build_dim_vessel_type(input_path: str, output_path: str) -> None:
    """
    Reads vessel type Excel file from S3, cleans data, and writes Parquet dimension table.
    """
    try:
        spark = SparkSession.builder.appName("DimVesselType").getOrCreate()

        schema = StructType([
            StructField("AIS_Code", StringType(), True),
            StructField("Vessel_Group", StringType(), True),
            StructField("Vessel_Type", StringType(), True),
            StructField("Description", StringType(), True)
        ])

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .schema(schema)
            .csv(input_path)
        )

        # Clean and standardize
        df = df.withColumn("AIS_Code", F.col("AIS_Code").cast(IntegerType()))
        df = df.dropDuplicates(["AIS_Code"])
        df = df.filter(F.col("AIS_Code").isNotNull())

        # Rename columns to standardized schema
        df = (
            df.withColumnRenamed("Vessel_Group", "VesselGroup")
              .withColumnRenamed("Vessel_Type", "VesselType")
              .withColumnRenamed("Description", "VesselDescription")
        )

        df.write.mode("overwrite").parquet(output_path)
        print(f"Vessel Type Dimension written to: {output_path}")

    except Exception as e:
        print(f"Error building Vessel Type dimension: {e}")
        raise

if __name__ == "__main__":
    build_dim_vessel_type(
        input_path=CFG.S3_LOOKUP + CFG.LOOKUP_VESSEL_TYPE_FILE,
        output_path=CFG.S3_LOOKUP + CFG.DIM_VESSEL_TYPE_DIR
    )
