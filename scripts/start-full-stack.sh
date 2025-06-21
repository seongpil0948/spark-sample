#!/bin/bash

# Start Full Observability Stack with Spark

echo "Starting Full Spark + Observability Stack..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

# Create necessary directories
echo "Creating directories..."
mkdir -p logs/spark-events
mkdir -p data/input data/output
mkdir -p models
mkdir -p checkpoint
mkdir -p config/grafana/dashboards/{spark,system}
mkdir -p notebooks

# Generate sample data if not exists
if [ ! -f "data/input/date=2024-01-01/part_0000.parquet" ]; then
    echo "Generating sample data..."
    python scripts/generate_sample_data.py
fi

# Start the full stack
echo "Starting services..."
docker-compose -f docker-compose.full.yml up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 30

# Check service health
echo "Checking service health..."
docker-compose -f docker-compose.full.yml ps

# Create Kafka topics
echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true
docker exec kafka kafka-topics --create --topic processed_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true
docker exec kafka kafka-topics --create --topic predictions --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true
docker exec kafka kafka-topics --create --topic spark-logs --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true

# Create Elasticsearch indices
echo "Creating Elasticsearch indices..."
curl -X PUT "localhost:9200/spark-logs" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "level": { "type": "keyword" },
      "message": { "type": "text" },
      "logger": { "type": "keyword" },
      "spark_app_id": { "type": "keyword" },
      "spark_app_name": { "type": "keyword" }
    }
  }
}' 2>/dev/null || true

# Display access URLs
echo ""
echo "=== Services are ready! ==="
echo ""
echo "Spark Master UI: http://localhost:8080"
echo "Spark History Server: http://localhost:18080"
echo "Jupyter Lab: http://localhost:8888"
echo ""
echo "Grafana: http://localhost:3000 (admin/admin)"
echo "Prometheus: http://localhost:9090"
echo "Jaeger UI: http://localhost:16686"
echo "Kibana: http://localhost:5601"
echo "OpenTelemetry Collector: http://localhost:55679/debug/tracez"
echo ""
echo "Kafka is available at: localhost:9092"
echo "Elasticsearch is available at: http://localhost:9200"
echo ""
echo "To submit a Spark job:"
echo "  docker exec spark-master /app/scripts/run_etl.sh"
echo ""
echo "To stop all services:"
echo "  docker-compose -f docker-compose.full.yml down"
echo ""