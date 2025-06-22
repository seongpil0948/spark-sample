"""
Spark Configuration Module
Manages Spark session creation and configuration
"""

import os
from typing import Dict, Optional
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import structlog

logger = structlog.get_logger()


def get_spark_config(mode: str = "batch") -> Dict[str, str]:
    """Get Spark 4.0 configuration based on execution mode"""

    # Base configuration for Spark 4.0
    config = {
        # Application settings
        "spark.app.name": os.getenv("SPARK_APP_NAME", "SparkProductionApp"),
        "spark.master": os.getenv("SPARK_MASTER", "local[*]"),
        # Memory settings
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "8g"),
        "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "4"),
        "spark.driver.maxResultSize": "2g",
        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrationRequired": "false",
        "spark.kryoserializer.buffer.max": "1024m",
        # Spark 4.0 Enhanced Adaptive Query Execution (AQE)
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "100MB",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.optimizeSkewedJoin.enabled": "true",
        # Spark 4.0 Query Optimization
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "1MB",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin": "0.2",
        # Performance tuning
        "spark.sql.shuffle.partitions": "200",
        "spark.default.parallelism": "400",
        "spark.sql.autoBroadcastJoinThreshold": "100MB",
        "spark.sql.broadcastTimeout": "600",
        # Dynamic allocation
        "spark.dynamicAllocation.enabled": os.getenv(
            "SPARK_DYNAMIC_ALLOCATION", "false"
        ),
        "spark.dynamicAllocation.minExecutors": "2",
        "spark.dynamicAllocation.maxExecutors": "20",
        "spark.dynamicAllocation.executorIdleTimeout": "60s",
        "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
        # Spark 4.0 Columnar Processing
        "spark.sql.columnVector.offheap.enabled": "true",
        "spark.sql.inMemoryColumnarStorage.compressed": "true",
        "spark.sql.inMemoryColumnarStorage.batchSize": "10000",
        "spark.rdd.compress": "true",
        # Arrow optimization for Python (Enhanced in Spark 4.0)
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        # Delta Lake 4.0 settings
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        # Monitoring
        "spark.eventLog.enabled": os.getenv("SPARK_EVENTLOG_ENABLED", "false"),
        "spark.eventLog.dir": os.getenv("SPARK_EVENTLOG_DIR", "/app/logs/spark-events"),
        "spark.history.fs.logDirectory": os.getenv(
            "SPARK_HISTORY_DIR", "/app/logs/spark-events"
        ),
        # Network settings
        "spark.network.timeout": "600s",
        "spark.rpc.askTimeout": "600s",
        # GC settings
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35",
        "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35",
    }

    # Mode-specific configurations
    if mode == "streaming":
        streaming_config = {
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.streaming.backpressure.enabled": "true",
            "spark.streaming.backpressure.initialRate": "1000",
            "spark.streaming.receiver.writeAheadLog.enable": "true",
            "spark.sql.streaming.checkpointLocation": os.getenv(
                "CHECKPOINT_PATH", "checkpoint/"
            ),
            "spark.sql.streaming.metricsEnabled": "true",
            "spark.sql.streaming.numRecentProgressUpdates": "100",
        }
        config.update(streaming_config)

    # Environment-specific configurations
    env = os.getenv("SPARK_ENV", "local")

    if env == "production":
        prod_config = {
            "spark.master": os.getenv("SPARK_MASTER", "yarn"),
            "spark.submit.deployMode": "cluster",
            "spark.yarn.maxAppAttempts": "3",
            "spark.task.maxFailures": "4",
            "spark.speculation": "true",
            "spark.speculation.interval": "100ms",
            "spark.speculation.multiplier": "1.5",
            "spark.speculation.quantile": "0.9",
        }
        config.update(prod_config)

    elif env == "kubernetes":
        k8s_config = {
            "spark.master": "k8s://https://kubernetes.default.svc",
            "spark.kubernetes.namespace": os.getenv("K8S_NAMESPACE", "spark"),
            "spark.kubernetes.container.image": os.getenv(
                "SPARK_IMAGE", "apache/spark:4.0.0"
            ),
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.kubernetes.executor.request.cores": "1",
            "spark.kubernetes.executor.limit.cores": "4",
            "spark.kubernetes.executor.request.memory": "4g",
            "spark.kubernetes.executor.limit.memory": "8g",
        }
        config.update(k8s_config)

    # S3 configuration - support both environment variables and AWS profiles
    aws_profile = os.getenv("AWS_PROFILE", "toy-root")
    
    # Base S3 configuration with both s3 and s3a schemes
    base_s3_config = {
        # Enable both s3 and s3a filesystems
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # Performance and reliability settings
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.multipart.size": "104857600",
        "spark.hadoop.fs.s3a.connection.maximum": "100",
        "spark.hadoop.fs.s3a.path.style.access": "false",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
        "spark.hadoop.fs.s3a.attempts.maximum": "20",
        "spark.hadoop.fs.s3a.connection.timeout": "200000",
        "spark.hadoop.fs.s3a.retry.limit": "7",
        "spark.hadoop.fs.s3a.retry.interval": "500ms",
        # Performance optimizations for S3
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.speculation": "false",  # Disable speculation for S3 writes
        # Block size optimization
        "spark.hadoop.fs.s3a.block.size": "128M",
        "spark.hadoop.fs.s3a.multipart.threshold": "64M",
    }
    
    if os.getenv("AWS_ACCESS_KEY_ID"):
        # Use explicit credentials from environment variables
        s3_config = {
            **base_s3_config,
            "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            # "spark.hadoop.fs.s3a.session.token": os.getenv("AWS_SESSION_TOKEN", ""),
            "spark.hadoop.fs.s3a.endpoint": os.getenv(
                "AWS_ENDPOINT", "s3.amazonaws.com"
            ),
        }
        config.update(s3_config)  # type: ignore[arg-type]
    elif aws_profile:
        # Use AWS profile credentials with DefaultAWSCredentialsProviderChain
        # This will check environment variables, system properties, and profile in order
        s3_config = {
            **base_s3_config,
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.hadoop.fs.s3a.endpoint": os.getenv(
                "AWS_ENDPOINT", "s3.ap-northeast-2.amazonaws.com"  # Seoul region endpoint
            ),
        }
        # Pass AWS profile through Java system properties and environment
        config["spark.driver.extraJavaOptions"] = config.get(
            "spark.driver.extraJavaOptions", ""
        ) + f" -Daws.profile={aws_profile} -Daws.region=ap-northeast-2"
        config["spark.executor.extraJavaOptions"] = config.get(
            "spark.executor.extraJavaOptions", ""
        ) + f" -Daws.profile={aws_profile} -Daws.region=ap-northeast-2"
        # Also set as Hadoop configuration
        config[f"spark.hadoop.aws.profile"] = aws_profile
        config[f"spark.hadoop.aws.region"] = "ap-northeast-2"
        config.update(s3_config)  # type: ignore[arg-type]

    return config


def create_spark_session(
    app_name: str, config: Optional[Dict[str, str]] = None
) -> SparkSession:
    """Create and configure Spark session"""

    if config is None:
        config = get_spark_config()

    # Create SparkConf
    spark_conf = SparkConf()
    for key, value in config.items():
        spark_conf.set(key, value)

    # Create SparkSession
    spark = (
        SparkSession.builder.appName(app_name)
        .config(conf=spark_conf)
        .enableHiveSupport()
        .getOrCreate()
    )

    # Set log level
    log_level = os.getenv("SPARK_LOG_LEVEL", "WARN")
    spark.sparkContext.setLogLevel(log_level)

    # Log configuration
    logger.info(
        "spark_session_created",
        app_name=app_name,
        master=spark.conf.get("spark.master"),
        version=spark.version,
        config_items=len(config),
    )

    return spark


def optimize_for_operation(spark: SparkSession, operation: str) -> None:
    """Optimize Spark configuration for specific operations"""

    if operation == "large_join":
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    elif operation == "aggregation":
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        spark.conf.set(
            "spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "10000"
        )

    elif operation == "window_function":
        spark.conf.set("spark.sql.windowExec.buffer.spill.threshold", "10000")
        spark.conf.set("spark.sql.windowExec.buffer.in.memory.threshold", "10000")

    logger.info("spark_optimized_for_operation", operation=operation)
