#!/bin/bash

# Run ETL Pipeline Script

# Set environment variables
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PYTHONPATH=$PYTHONPATH:$(pwd)
export PYSPARK_PYTHON=python3

# AWS Profile configuration
export AWS_PROFILE=${AWS_PROFILE:-toy-root}

# Setup AWS environment if not already set
if [ -z "$AWS_ACCESS_KEY_ID" ] && [ -f "${HOME}/.aws/credentials" ]; then
    source "$(dirname "$0")/setup_aws_env.sh"
fi

# Default values - use S3A paths (Spark compatible)
DATE=${1:-$(date +%Y-%m-%d)}
INPUT_PATH=${2:-s3a://theshop-lake-dev/spark/input/}
OUTPUT_PATH=${3:-s3a://theshop-lake-dev/spark/output/}
MASTER=${SPARK_MASTER:-local[*]}

echo "Starting ETL Pipeline..."
echo "Date: $DATE"
echo "Input Path: $INPUT_PATH"
echo "Output Path: $OUTPUT_PATH"
echo "Spark Master: $MASTER"

# Run Spark submit
spark-submit \
  --master $MASTER \
  --name "ETL-Pipeline-$DATE" \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.eventLog.enabled=false \
  src/app.py \
  --mode etl \
  --date $DATE \
  --input-path $INPUT_PATH \
  --output-path $OUTPUT_PATH

# Check exit status
if [ $? -eq 0 ]; then
    echo "ETL Pipeline completed successfully!"
else
    echo "ETL Pipeline failed!"
    exit 1
fi