"""
utils/config.py

Central configuration for NOAA AIS ETL pipeline.
Glue-compatible, environment-overridable, and safe to import from Glue jobs.
Preserves your comments and keeps all values configurable via environment variables.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Dict

# ------------------------
# Environment helpers
# ------------------------
def _env(key: str, default: str) -> str:
    val = os.environ.get(key, default)
    val = val.strip() if val else default
    # enforce Glue compatibility (s3a://)
    if val.startswith("s3://"):
        val = val.replace("s3://", "s3a://", 1)
    return val

# ------------------------
# Defaults (can be overridden by env vars in Glue job parameters or job properties)
# ------------------------
DEFAULT_AWS_REGION = _env("AWS_REGION", "us-east-1")
DEFAULT_GLUE_DATABASE = _env("GLUE_DATABASE", "noaa_ais_db")
DEFAULT_GLUE_TEMP_DIR = _env("GLUE_TEMP_DIR", "s3://noaa-ais-temp/glue-temp/")
DEFAULT_GLUE_ROLE_NAME = _env("GLUE_ROLE_NAME", "AWSGlueServiceRole-Project")

DEFAULT_S3_RAW = _env("S3_RAW_PATH", "s3://noaa-ais-raw-data/")
DEFAULT_S3_STAGING = _env("S3_STAGING_PATH", "s3://noaa-ais-staging-data/")
DEFAULT_S3_LOOKUP = _env("S3_LOOKUP_PATH", "s3://noaa-ais-lookup-data/")
DEFAULT_S3_CURATED = _env("S3_CURATED_PATH", "s3://noaa-ais-curated-data/")

# Lookup filenames (no paths, only filenames)
LOOKUP_MID_FILE = "maritime_identification_digits.xlsx"
LOOKUP_CALLSIGN_FILE = "international_call_signs.xlsx"
LOOKUP_NAV_STATUS_FILE = "navigational_status_codes.xlsx"
LOOKUP_VESSEL_TYPE_FILE = "vessel_type_codes.xlsx"

# Output folders for dims
DIM_COUNTRY_DIR = "dim_country/"
DIM_NAV_STATUS_DIR = "dim_nav_status/"
DIM_VESSEL_TYPE_DIR = "dim_vessel_type/"

# --------------------------------------------------------
# Normalize all S3 defaults to s3a:// (Glue 4.0 compatible)
# --------------------------------------------------------
for var_name in list(globals()):
    if var_name.startswith("DEFAULT_S3_") and isinstance(globals()[var_name], str):
        val = globals()[var_name]
        if val.startswith("s3://"):
            globals()[var_name] = val.replace("s3://", "s3a://", 1)

# ------------------------
# AISConfig dataclass (single-source-of-truth)
# ------------------------
@dataclass(frozen=True)
class AISConfig:
    REGION: str = DEFAULT_AWS_REGION
    GLUE_DATABASE: str = DEFAULT_GLUE_DATABASE
    GLUE_TEMP_DIR: str = DEFAULT_GLUE_TEMP_DIR
    GLUE_ROLE_NAME: str = DEFAULT_GLUE_ROLE_NAME

    DEFAULT_PARTITIONS: tuple = ("year", "month", "day")
    DEFAULT_PARAMS: Dict[str, str] = None

    # ------------------------------------------------------------------
    # Dynamic S3 path getters (evaluate at runtime, not import time)
    # ------------------------------------------------------------------
    @property
    def S3_RAW(self) -> str:
        return _env("S3_RAW_PATH", DEFAULT_S3_RAW).rstrip("/") + "/"

    @property
    def S3_STAGING(self) -> str:
        return _env("S3_STAGING_PATH", DEFAULT_S3_STAGING).rstrip("/") + "/"

    @property
    def S3_LOOKUP(self) -> str:
        return _env("S3_LOOKUP_PATH", DEFAULT_S3_LOOKUP).rstrip("/") + "/"

    @property
    def S3_CURATED(self) -> str:
        return _env("S3_CURATED_PATH", DEFAULT_S3_CURATED).rstrip("/") + "/"

    @property
    def QUARANTINE_PATH(self) -> str:
        return self.S3_STAGING + "quarantine/"

    # State snapshot locations
    @property
    def STATE_BASE_PATH(self) -> str:
        return self.S3_CURATED + "state/"

    @property
    def STATE_LATEST_PATH(self) -> str:
        return self.STATE_BASE_PATH + "latest/"

    @property
    def STATE_BY_DATE_PATH(self) -> str:
        """Prefix for dated state snapshots. Append formatted date (YYYY-MM-DD)."""
        return self.STATE_BASE_PATH + "by_date="

    # ------------------------------------------------------------------
    # Runtime parameters and backward-compatible aliases
    # ------------------------------------------------------------------
    def __post_init__(self):
        object.__setattr__(self, "DEFAULT_PARAMS", {
            "mode": os.environ.get("DEFAULT_MODE", "daily"),
            "date": os.environ.get("DEFAULT_DATE", datetime.utcnow().strftime("%Y-%m-%d")),
            "input_path": self.S3_RAW,
            "output_path": self.S3_STAGING,
            "database": self.GLUE_DATABASE,
            "region": self.REGION,
        })

    @property
    def RAW_DATA_PATH(self) -> str:
        return self.S3_RAW

    @property
    def STAGING_DATA_PATH(self) -> str:
        return self.S3_STAGING


# single instance to import
CFG = AISConfig()

# ------------------------
# Logging helper
# ------------------------
def setup_logger(name: str):
    """
    Initialize a Glue-safe logger only once per job.
    """
    try:
        logger = logging.getLogger(name)
        if logger.handlers:          
            return logger

        log_dir = Path("/tmp/logs")
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / f"etl_log_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        log_format = "%(asctime)s [%(levelname)s] %(message)s"

        handler_file = logging.FileHandler(log_file, encoding="utf-8")
        handler_stream = logging.StreamHandler()
        formatter = logging.Formatter(log_format)

        handler_file.setFormatter(formatter)
        handler_stream.setFormatter(formatter)

        logger.setLevel(logging.INFO)
        logger.addHandler(handler_file)
        logger.addHandler(handler_stream)

        logger.info(f"Glue-safe logging initialized. Log file: {log_file}")
        return logger

    except Exception as e:
        print(f"[WARN] Failed to initialize logger: {e}")
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)
