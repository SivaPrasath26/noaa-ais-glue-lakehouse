# dim_country.py
"""
Dimension builder for AIS country identification.
Merges Maritime Identification Digits (MID) and Call Sign prefix allocations
into a unified country lookup table.

Columns:
- Country
- MID (optional, 3-digit)
- CallSignPrefix (optional, prefix or range)
- Source ('MID' or 'CallSign')
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utils.config import CFG

def build_dim_country(mid_path: str, call_sign_path: str, output_path: str) -> None:
    """
    Builds combined Country Dimension using MID and Call Sign lookup tables from S3.
    """
    try:
        spark = SparkSession.builder.appName("DimCountry").getOrCreate()

        # -----------------------------
        # Read MID Table
        # -----------------------------
        mid_schema = StructType([
            StructField("Digit", IntegerType(), True),
            StructField("Allocated_to", StringType(), True)
        ])

        mid_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .schema(mid_schema)
            .csv(mid_path)
        )

        mid_df = (
            mid_df.withColumnRenamed("Digit", "MID")
            .withColumnRenamed("Allocated_to", "Country")
            .withColumn("CallSignPrefix", F.lit(None).cast(StringType()))
            .withColumn("Source", F.lit("MID"))
        )

        # -----------------------------
        # Read Call Sign Table
        # -----------------------------
        cs_schema = StructType([
            StructField("Series", StringType(), True),
            StructField("Allocated_to", StringType(), True)
        ])

        cs_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .schema(cs_schema)
            .csv(call_sign_path)
        )

        cs_df = (
            cs_df.withColumnRenamed("Series", "CallSignPrefix")
            .withColumnRenamed("Allocated_to", "Country")
            .withColumn("MID", F.lit(None).cast(IntegerType()))
            .withColumn("Source", F.lit("CallSign"))
        )

        # -----------------------------
        # Combine and Clean
        # -----------------------------
        df_combined = mid_df.unionByName(cs_df)
        df_combined = df_combined.dropDuplicates(["Country", "CallSignPrefix", "MID"])

        # Clean whitespace and nulls
        df_combined = df_combined.withColumn("Country", F.trim(F.col("Country")))

        # -----------------------------
        # Write to S3 as Parquet
        # -----------------------------
        df_combined.write.mode("overwrite").parquet(output_path)
        print(f"Country Dimension written to: {output_path}")

    except Exception as e:
        print(f"Error building Country Dimension: {e}")
        raise


if __name__ == "__main__":
    build_dim_country(
        mid_path=CFG.S3_LOOKUP + CFG.LOOKUP_MID_FILE,
        call_sign_path=CFG.S3_LOOKUP + CFG.LOOKUP_CALLSIGN_FILE,
        output_path=CFG.S3_LOOKUP + CFG.DIM_COUNTRY_DIR
    )
