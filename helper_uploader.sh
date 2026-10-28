
set -uo pipefail
#!/usr/bin/env bash
set -uo pipefail
export PATH="$PATH:/c/Program Files/Amazon/AWSCLIV2"
BUCKET="aws-ai-gds-dev"
PREFIX="spark/assignment/ny_yellow_taxi"
REGION="us-east-1"
BASE="https://d37ci6vzurychx.cloudfront.net"

for y in 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025; do
  for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
    f="yellow_tripdata_${y}-${m}.parquet"
    echo "-> $f"
    curl -fsSL "$BASE/trip-data/$f" \
      | aws s3 cp - "s3://${BUCKET}/${PREFIX}/taxi/$f" --region "$REGION" \
      || echo "FAILED: $f" >&2
  done
done

# Zone lookup (small CSV)
curl -fsSL "$BASE/misc/taxi_zone_lookup.csv" \
  | aws s3 cp - "s3://${BUCKET}/${PREFIX}/lookup/taxi_zone_lookup.csv" --region "$REGION"

  