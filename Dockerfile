# Production Spark Application Docker Image
FROM apache/spark:4.0.0

# Switch to root for package installation
USER root

# Install Python dependencies
RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Python as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/

# Create necessary directories
RUN mkdir -p data/input data/output checkpoint models logs artifacts notebooks && \
    mkdir -p /home/spark/.jupyter /home/spark/.local && \
    chown -R spark:spark /app /home/spark

# Copy Spark jars for Delta Lake and Kafka (Spark 4.0 compatible)
RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar

# Set environment variables
ENV PYTHONPATH=/app:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/spark

# Make entrypoint script executable
RUN chmod +x /app/scripts/entrypoint.sh

# Switch back to spark user
USER spark

# Set entrypoint
ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["--help"]