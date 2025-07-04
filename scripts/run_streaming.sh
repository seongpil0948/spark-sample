#!/bin/bash

# Run Streaming Pipeline Script

# Set environment variables
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PYTHONPATH=$PYTHONPATH:$(pwd)
export PYSPARK_PYTHON=python3

# AWS Profile configuration
export AWS_PROFILE=${AWS_PROFILE:-toy-root}

# Default values - use S3A paths for output and checkpoints (Spark compatible)
KAFKA_SERVERS=${1:-localhost:9092}
KAFKA_TOPIC=${2:-events}
CHECKPOINT_PATH=${3:-s3a://theshop-lake-dev/spark/checkpoint/streaming}
OUTPUT_PATH=${4:-s3a://theshop-lake-dev/spark/output/streaming}
MASTER=${SPARK_MASTER:-local[*]}

echo "Starting Streaming Pipeline..."
echo "Kafka Servers: $KAFKA_SERVERS"
echo "Kafka Topic: $KAFKA_TOPIC"
echo "Checkpoint Path: $CHECKPOINT_PATH"
echo "Output Path: $OUTPUT_PATH"
echo "Spark Master: $MASTER"

# Note: Checkpoint directory creation is handled by Spark when writing to S3

# Run Spark submit
spark-submit \
  --master $MASTER \
  --name "Streaming-Pipeline" \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  --conf spark.sql.streaming.checkpointLocation=$CHECKPOINT_PATH \
  --conf spark.sql.streaming.metricsEnabled=true \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.2.1 \
  src/app.py \
  --mode streaming \
  --kafka-servers $KAFKA_SERVERS \
  --kafka-topic $KAFKA_TOPIC \
  --checkpoint-path $CHECKPOINT_PATH \
  --output-path $OUTPUT_PATH

# Check exit status
if [ $? -eq 0 ]; then
    echo "Streaming Pipeline stopped gracefully!"
else
    echo "Streaming Pipeline failed!"
    exit 1
fi