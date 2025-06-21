#!/bin/bash

# Run ETL Pipeline Script

# Set environment variables
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PYTHONPATH=$PYTHONPATH:$(pwd)
export PYSPARK_PYTHON=python3

# Default values
DATE=${1:-$(date +%Y-%m-%d)}
INPUT_PATH=${2:-data/input}
OUTPUT_PATH=${3:-data/output}
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