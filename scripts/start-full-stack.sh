#!/bin/bash

# Start Spark Cluster with Essential Services

echo "Starting Spark Cluster..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

# Set AWS profile
export AWS_PROFILE=${AWS_PROFILE:-toy-root}

# Create necessary directories
echo "Creating directories..."
mkdir -p logs/spark-events
mkdir -p data/input data/output
mkdir -p models
mkdir -p checkpoint
mkdir -p notebooks

# Generate sample data if not exists
if [ ! -f "data/input/date=2024-01-01/part_0000.parquet" ]; then
    echo "Generating sample data..."
    python scripts/generate_sample_data.py
fi

# Start the services
echo "Starting services..."
docker-compose up -d --build --remove-orphans --force-recreate

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 30

# Check service health
echo "Checking service health..."
docker-compose ps

# Create Kafka topics
echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true
docker exec kafka kafka-topics --create --topic processed_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true
docker exec kafka kafka-topics --create --topic predictions --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true

# Display access URLs
echo ""
echo "=== Services are ready! ==="
echo ""
echo "Spark Master UI: http://localhost:8080"
echo "Spark Worker 1 UI: http://localhost:8081"
echo "Spark Worker 2 UI: http://localhost:8082"
echo "Spark History Server: http://localhost:18080"
echo "Jupyter Lab: http://localhost:8888"
echo ""
echo "Kafka is available at: localhost:9092"
echo ""
echo "To submit a Spark job:"
echo "  ./scripts/run_etl.sh 2024-01-01"
echo "  ./scripts/run_ml.sh"
echo "  ./scripts/run_streaming.sh"
echo ""
echo "To generate sample data to S3:"
echo "  python scripts/generate_sample_data.py --s3"
echo ""
echo "To stop all services:"
echo "  docker-compose down"
echo ""