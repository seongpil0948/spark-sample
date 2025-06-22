#!/bin/bash

set -e

# Setup AWS environment if credentials file exists
if [ -f "/home/spark/.aws/credentials" ]; then
    source /app/scripts/setup_aws_env.sh
fi

# Function to start Spark master
start_master() {
    echo "Starting Spark Master..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
        --host ${SPARK_MASTER_HOST:-0.0.0.0} \
        --port ${SPARK_MASTER_PORT:-7077} \
        --webui-port ${SPARK_MASTER_WEBUI_PORT:-8080}
}

# Function to start Spark worker
start_worker() {
    echo "Starting Spark Worker..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
        ${SPARK_MASTER_URL:-spark://spark-master:7077} \
        --cores ${SPARK_WORKER_CORES:-1} \
        --memory ${SPARK_WORKER_MEMORY:-1g} \
        --webui-port ${SPARK_WORKER_WEBUI_PORT:-8081}
}

# Function to start Spark history server
start_history() {
    echo "Starting Spark History Server..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.history.HistoryServer
}

# Main logic
case "${SPARK_MODE}" in
    master)
        start_master
        ;;
    worker)
        start_worker
        ;;
    history)
        start_history
        ;;
    *)
        echo "Starting default command: $@"
        exec "$@"
        ;;
esac