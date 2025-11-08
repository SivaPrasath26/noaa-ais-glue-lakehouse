"""
NOAA AIS EDA Script
-------------------
Performs exploratory data analysis on two days of NOAA AIS raw data.
Reads file paths from environment variables, analyzes structure, missingness,
and key maritime metrics, then exports summary visuals as PNG images.

Author: Siva Prasath
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
from IPython.display import display
import dataframe_image as dfi

sns.set(style="whitegrid")
pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 200)

# -------------------------------------------------------------------
# Load environment variables and file paths
# -------------------------------------------------------------------
load_dotenv()

file_paths = {
    "day1": os.getenv("file_path_day_1"),
    "day2": os.getenv("file_path_day_2")
}

SAMPLE_ROWS = None  # Load full dataset

# -------------------------------------------------------------------
# Utility Function: Save summary and plots as PNG images
# -------------------------------------------------------------------
def save_eda_outputs(df: pd.DataFrame, label: str) -> None:
    """
    Saves summary tables and visualizations for a given day's dataset.
    Output stored under: data/assets/eda/<label>/
    """
    out_dir = os.path.join("data", "assets", "eda", label)
    os.makedirs(out_dir, exist_ok=True)

    # 1. Summary statistics table
    summary_df = df.describe(include="all").T.round(2)
    summary_path = os.path.join(out_dir, "summary_overview.png")
    try:
        dfi.export(summary_df.head(20), summary_path)
        print(f"Saved → {summary_path}")
    except Exception as e:
        print("Summary export failed:", e)

    # 2. Vessel type distribution
    if "VesselType" in df.columns:
        plt.figure(figsize=(8, 4))
        df["VesselType"].value_counts().head(15).plot(kind="bar")
        plt.title(f"Top Vessel Types ({label})")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "vessel_type_distribution.png"))
        plt.close()

    # 3. Speed over ground (SOG) histogram
    if "SOG" in df.columns:
        df["SOG"] = pd.to_numeric(df["SOG"], errors="coerce")
        plt.figure(figsize=(8, 4))
        sns.histplot(df["SOG"], bins=40, kde=True)
        plt.title(f"SOG Distribution ({label})")
        plt.xlabel("Speed (knots)")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "sog_distribution.png"))
        plt.close()

    # 4. Spatial scatter (LAT/LON)
    if {"LAT", "LON"}.issubset(df.columns):
        sample = df[["LAT", "LON"]].dropna().sample(n=min(2000, len(df)))
        plt.figure(figsize=(6, 4))
        plt.scatter(sample["LON"], sample["LAT"], s=3, alpha=0.5)
        plt.title(f"Vessel Position Scatter ({label})")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "vessel_positions.png"))
        plt.close()

    print(f"All visuals saved → {out_dir}\n")


# -------------------------------------------------------------------
# Main EDA loop
# -------------------------------------------------------------------
for label, path in file_paths.items():
    print("\n" + "=" * 80)
    print(f"EDA for {label}")
    print("=" * 80)

    if not path:
        print(f"No path found for {label}; skipping")
        continue

    # ------------------------------
    # Load dataset
    # ------------------------------
    try:
        df = pd.read_csv(path, nrows=SAMPLE_ROWS, low_memory=False)
        print("Loaded rows:", len(df))
    except Exception as e:
        print("Error reading file:", e)
        continue

    # ------------------------------
    # Basic overview
    # ------------------------------
    display(df.head())
    print("\nShape:", df.shape)
    print("\nDtypes:")
    display(df.dtypes)

    # ------------------------------
    # Missingness summary
    # ------------------------------
    miss = df.isna().sum().sort_values(ascending=False)
    print("\nTop missing columns:")
    display(miss[miss > 0].head(20))

    # ------------------------------
    # Column detection
    # ------------------------------
    mmsi_col = next((c for c in df.columns if "mmsi" in c.lower()), None)
    lat_col = next((c for c in df.columns if "lat" in c.lower()), None)
    lon_col = next((c for c in df.columns if "lon" in c.lower()), None)
    time_col = next((c for c in df.columns if "time" in c.lower() or "date" in c.lower()), None)

    print(f"\nDetected → MMSI: {mmsi_col}, LAT: {lat_col}, LON: {lon_col}, TIME: {time_col}")

    # ------------------------------
    # Coordinate checks
    # ------------------------------
    if lat_col and lon_col:
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
        print("Lat/Lon nulls:", df[lat_col].isna().sum(), df[lon_col].isna().sum())

    # ------------------------------
    # Datetime parsing
    # ------------------------------
    if time_col:
        try:
            df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
            print(f"{time_col} range:", df[time_col].min(), "→", df[time_col].max())
        except Exception as e:
            print("Datetime parse error:", e)

    # ------------------------------
    # Top MMSI counts
    # ------------------------------
    if mmsi_col:
        print("\nTop 10 MMSI by message count:")
        display(df[mmsi_col].value_counts().head(10))

    # ------------------------------
    # Temporal summary (daily)
    # ------------------------------
    if time_col and pd.api.types.is_datetime64_any_dtype(df[time_col]):
        try:
            daily = df.set_index(time_col).resample("D").size()
            print("\nSample daily counts:")
            display(daily.head(10))
        except Exception as e:
            print("Resample error:", e)

    # ------------------------------
    # Geographic bounds
    # ------------------------------
    if lat_col and lon_col:
        bbox = (
            df[lat_col].min(),
            df[lat_col].max(),
            df[lon_col].min(),
            df[lon_col].max(),
        )
        print(f"\nLat range: {bbox[0]} → {bbox[1]}")
        print(f"Lon range: {bbox[2]} → {bbox[3]}")

    # ------------------------------
    # Save outputs as visuals
    # ------------------------------
    save_eda_outputs(df, label)


print("\nEDA complete for all available files.")
