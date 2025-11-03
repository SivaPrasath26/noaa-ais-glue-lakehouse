#!/bin/bash
set -e  # Exit immediately if any command fails
 
BUCKET="noaa-ais-raw-data"  # Target S3 bucket name
BASE_URL="https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024"  # NOAA AIS 2024 data source URL
 
START_MONTH=1  # Starting month for download loop
START_DAY=1    # Starting day within the first month
 
# Loop through all months (01–12)
for month in $(seq -w $START_MONTH 12); do
  # Loop through all days (01–31)
  for day in $(seq -w 1 31); do
 
    # Skip dates before the defined start day for the starting month
    if [ "$month" -eq "$START_MONTH" ] && [ "$day" -lt "$START_DAY" ]; then
        continue
    fi
 
    FILE_DATE="2024_${month}_${day}"  # Construct date identifier
    ZIP_FILE="AIS_${FILE_DATE}.zip"   # Source zip filename
    CSV_FILE="AIS_${FILE_DATE}.csv"   # Target CSV filename
    S3_PATH="s3://${BUCKET}/year=2024/month=${month}/day=${day}/${CSV_FILE}"  # Destination path in S3
 
    # Skip upload if file already exists in S3
    if aws s3 ls "${S3_PATH}" >/dev/null 2>&1; then
        echo "Skipping ${FILE_DATE} (already in S3)"
        continue
    fi
 
    echo "Processing ${FILE_DATE}..."
 
    # Check available disk space (MB) and clean up if below 300MB
    FREE_SPACE=$(df --output=avail -m . | tail -1)
    if [ "$FREE_SPACE" -lt 300 ]; then
        echo "Low disk space (${FREE_SPACE}MB). Cleaning up..."
        rm -f *.zip *.csv || true
        sync
    fi
 
    # Download zip file from NOAA, unzip in-memory, and stream directly to S3
    if ! wget -q -O - "${BASE_URL}/${ZIP_FILE}" | funzip | aws s3 cp - "${S3_PATH}" --no-progress; then
        echo "Failed or missing data for ${FILE_DATE}, skipping."
        continue
    fi
 
    echo "Uploaded ${FILE_DATE} → ${S3_PATH}"
    echo "------------------------------------"
  done
done
 
echo "All available 2024 AIS data uploaded successfully."
