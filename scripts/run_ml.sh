#!/bin/bash

# Run ML Pipeline Script

# Set environment variables
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PYTHONPATH=$PYTHONPATH:$(pwd)
export PYSPARK_PYTHON=python3

# AWS Profile configuration
export AWS_PROFILE=${AWS_PROFILE:-toy-root}

# Default values - use S3A paths (Spark compatible)
TRAINING_DATA=${1:-s3a://theshop-lake-dev/spark/input/training}
MODEL_PATH=${2:-s3a://theshop-lake-dev/spark/output/models}
MASTER=${SPARK_MASTER:-local[*]}

echo "Starting ML Pipeline..."
echo "Training Data: $TRAINING_DATA"
echo "Model Path: $MODEL_PATH"
echo "Spark Master: $MASTER"

# Note: Model directory creation is handled by Spark when writing to S3

# Run Spark submit
spark-submit \
  --master $MASTER \
  --name "ML-Pipeline" \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.cores=8 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.ml.feature.maxBins=100 \
  --packages io.delta:delta-spark_2.12:3.2.1 \
  src/app.py \
  --mode ml \
  --input-path $TRAINING_DATA \
  --model-path $MODEL_PATH

# Check exit status
if [ $? -eq 0 ]; then
    echo "ML Pipeline completed successfully!"
    echo "Model saved to: $MODEL_PATH"
else
    echo "ML Pipeline failed!"
    exit 1
fi