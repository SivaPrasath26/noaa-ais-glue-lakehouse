# job_monthly_summary.py
"""
Glue Job: Monthly Voyage Summary
--------------------------------
Runs monthly_summary.run_monthly_summary for given year & month.
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from utils.config import CFG, setup_logger
from transformations.facts.fact_monthly_summary import run_monthly_summary


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "year", "month"])
    year = int(args["year"])
    month = int(args["month"])

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    logger = setup_logger(__name__)
    logger.info(f"Running monthly summary job for {year}-{month:02d}")

    run_monthly_summary(
        spark=spark,
        seg_path=CFG.S3_CURATED + "voyage_segments/",
        stage_path=CFG.S3_CURATED + "voyage_summary_staging/",
        out_path=CFG.S3_CURATED + "voyage_summary/",
        year=year,
        month=month,
    )

    job.commit()


if __name__ == "__main__":
    main()
