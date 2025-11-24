# dim_nav_status.py
"""
Dimension builder for AIS Navigational Status Codes.
Reads Excel lookup file from S3, cleans it, and writes Parquet to lookup zone.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from utils.config import CFG

def build_dim_nav_status(input_path: str, output_path: str) -> None:
    """
    Reads navigational status Excel file from S3, cleans data, and writes Parquet dimension table.
    """
    try:
        spark = SparkSession.builder.appName("DimNavStatus").getOrCreate()

        schema = StructType([
            StructField("Code", IntegerType(), True),
            StructField("Navigational_Status", StringType(), True),
            StructField("Meaning", StringType(), True)
        ])

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .schema(schema)
            .csv(input_path)
        )

        # Remove duplicates and nulls
        df = df.dropDuplicates(["Code"])
        df = df.filter(F.col("Code").isNotNull())

        # Standardize naming
        df = (
            df.withColumnRenamed("Navigational_Status", "StatusName")
              .withColumnRenamed("Meaning", "StatusDescription")
        )

        df.write.mode("overwrite").parquet(output_path)
        print(f"Navigational Status Dimension written to: {output_path}")

    except Exception as e:
        print(f"Error building Navigational Status dimension: {e}")
        raise

if __name__ == "__main__":
    build_dim_nav_status(
        input_path=CFG.S3_LOOKUP + CFG.LOOKUP_NAV_STATUS_FILE,
        output_path=CFG.S3_LOOKUP + CFG.DIM_NAV_STATUS_DIR
    )
