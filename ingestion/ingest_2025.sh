#!/bin/bash
set -e  # Exit immediately if any command fails
 
BUCKET="noaa-ais-raw-data"  # Target S3 bucket name
BASE_URL="https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025"  # NOAA AIS 2025 data source URL
 
START_MONTH=1  # Starting month for download loop
START_DAY=1    # Starting day within the first month
 
# Loop through all months (01–12)
for month in $(seq -w $START_MONTH 12); do
  # Loop through all days (01–31)
  for day in $(seq -w 1 31); do
 
    # Skip days before the defined start date for the first month
    if [ "$month" -eq "$START_MONTH" ] && [ "$day" -lt "$START_DAY" ]; then
        continue
    fi
 
    FILE_DATE="2025-${month}-${day}"  # Construct date identifier (e.g., 2025-01-01)
    ZST_FILE="ais-${FILE_DATE}.csv.zst"  # Source compressed file name
    CSV_FILE="ais-${FILE_DATE}.csv"      # Decompressed target CSV name
    S3_PATH="s3://${BUCKET}/year=2025/month=${month}/day=${day}/${CSV_FILE}"  # Destination S3 path
 
    # Skip upload if this file already exists in S3
    if aws s3 ls "${S3_PATH}" >/dev/null 2>&1; then
        echo "Skipping ${FILE_DATE} (already in S3)"
        continue
    fi
 
    echo "Processing ${FILE_DATE}..."
 
    # Stream download, decompress, and upload directly to S3 without writing to disk
    if ! wget -q -O - "${BASE_URL}/${ZST_FILE}" | python3 -c "import sys, zstandard; d=zstandard.ZstdDecompressor(); d.copy_stream(sys.stdin.buffer, sys.stdout.buffer)" | aws s3 cp - "${S3_PATH}" --no-progress; then
        echo "Failed or missing data for ${FILE_DATE}, skipping."
        continue
    fi
 
    echo "Uploaded ${FILE_DATE} → ${S3_PATH}"
    echo "------------------------------------"
  done
done
 
echo "All available 2025 AIS data uploaded successfully."
