# Production Spark Application Docker Image
FROM apache/spark:4.0.0-python3

# Switch to root for package installation
USER root

# Set working directory
WORKDIR /app

# Copy pyproject.toml and install Python dependencies using pip
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/

# Create necessary directories
RUN mkdir -p data/input data/output checkpoint models logs artifacts notebooks && \
    mkdir -p /home/spark/.jupyter /home/spark/.local && \
    chown -R spark:spark /app /home/spark

# Copy Spark jars for Delta Lake, Kafka, and S3 (Spark 4.0 compatible)
RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.0/kafka-clients-4.0.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.787/aws-java-sdk-bundle-1.12.787.jar && \
    curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.48/bundle-2.29.48.jar

# Set environment variables
ENV PYTHONPATH=/app:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk

# Make entrypoint script executable
RUN chmod +x /app/scripts/entrypoint.sh

# Switch back to spark user
USER spark

# Set entrypoint
ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["--help"]