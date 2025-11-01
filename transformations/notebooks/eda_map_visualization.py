import os
import folium
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read file paths
file_paths = {
    'day1': os.getenv('file_path_day_1'),
    'day2': os.getenv('file_path_day_2')
}

# Output directory for EDA maps
OUTPUT_DIR = os.path.join('data', 'assets', 'eda_raw')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_map(df, label, lat_col='LAT', lon_col='LON', mmsi_col='MMSI'):
    """Generate an interactive Folium map showing vessel positions."""
    df = df.dropna(subset=[lat_col, lon_col])
    if df.empty:
        print(f"No valid lat/lon data for {label}. Skipping map.")
        return
    
    # Center map on mean coordinates
    center_lat = df[lat_col].mean()
    center_lon = df[lon_col].mean()
    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=4, tiles='CartoDB positron')

    # Sample if too large for rendering
    sample_df = df.sample(n=min(5000, len(df)), random_state=42)

    for _, row in sample_df.iterrows():
        folium.CircleMarker(
            location=[row[lat_col], row[lon_col]],
            radius=2,
            color='blue',
            fill=True,
            fill_opacity=0.6,
            popup=f"MMSI: {row.get(mmsi_col, 'N/A')}"
        ).add_to(fmap)

    # Save HTML map
    out_path = os.path.join(OUTPUT_DIR, f"vessel_positions_{label}.html")
    fmap.save(out_path)
    print(f"Saved interactive map → {out_path}")


if __name__ == "__main__":
    for label, path in file_paths.items():
        if not path:
            print(f"Missing path for {label}. Skipping.")
            continue
        
        try:
            df = pd.read_csv(path, low_memory=False)
            if {'LAT', 'LON'}.issubset(df.columns):
                generate_map(df, label)
            else:
                print(f"Missing LAT/LON in {label}. Columns found: {list(df.columns)}")
        except Exception as e:
            print(f"Error reading {label}: {e}")
