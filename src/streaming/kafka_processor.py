"""
Streaming Processing Module
Implements Structured Streaming with Kafka integration
"""

import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    struct,
    current_timestamp,
    window,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    collect_list,
    expr,
    concat_ws,
    when,
    coalesce,
    date_format,
    hour,
    minute,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
    MapType,
)
from pyspark.sql.streaming import StreamingQuery

logger = structlog.get_logger()


class StreamingProcessor:
    """Kafka Streaming Processor with exactly-once semantics"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._configure_streaming()

    def _configure_streaming(self) -> None:
        """Configure Spark 4.0 streaming specific settings"""
        # Enable checkpointing
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", "checkpoint/")

        # Configure processing time
        self.spark.conf.set("spark.sql.streaming.processingTime", "10 seconds")

        # Enable Spark 4.0 streaming metrics and optimizations
        self.spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
        self.spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "100")
        self.spark.conf.set("spark.sql.streaming.stateStore.compression.codec", "lz4")

        # Spark 4.0 Adaptive streaming
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # State store optimizations
        self.spark.conf.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        )

    def get_event_schema(self) -> StructType:
        """Define schema for Kafka events"""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("user_id", LongType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("event_type", StringType(), False),
                StructField("category", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("properties", MapType(StringType(), StringType()), True),
                StructField(
                    "location",
                    StructType(
                        [
                            StructField("country", StringType(), True),
                            StructField("city", StringType(), True),
                            StructField("lat", DoubleType(), True),
                            StructField("lon", DoubleType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    def process_kafka_stream(
        self, bootstrap_servers: str, topic: str, checkpoint_path: str, output_path: str
    ) -> StreamingQuery:
        """Process Kafka stream with watermarking and windowing"""
        logger.info(
            "starting_kafka_stream_processing", servers=bootstrap_servers, topic=topic
        )

        # Read from Kafka with Spark 4.0 optimizations
        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 10000)
            .option("kafka.security.protocol", "PLAINTEXT")
            .option("failOnDataLoss", "false")
            .option("kafka.consumer.cache.capacity", "64")
            .option("kafka.consumer.poll.ms", "512")
            .option("includeHeaders", "true")
            .load()
        )

        # Parse JSON data
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), self.get_event_schema()).alias(
                "data"
            ),
            col("timestamp").alias("kafka_timestamp"),
        ).select(col("kafka_key"), col("data.*"), col("kafka_timestamp"))

        # Add watermark for late data handling with Spark 4.0 improvements
        watermarked_df = parsed_df.withWatermark("timestamp", "10 minutes").hint(
            "watermark_optimization"
        )

        # Real-time aggregations with windowing
        windowed_aggregations = (
            watermarked_df.groupBy(
                window(col("timestamp"), "5 minutes", "1 minute"),
                col("event_type"),
                col("category"),
            )
            .agg(
                count("*").alias("event_count"),
                spark_sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                spark_max("amount").alias("max_amount"),
                collect_list("user_id").alias("user_list"),
            )
            .withColumn("unique_users", expr("size(array_distinct(user_list))"))
            .drop("user_list")
            .withColumn("window_start", col("window.start"))
            .withColumn("window_end", col("window.end"))
            .drop("window")
        )

        # Apply business logic transformations
        enriched_stream = self._apply_streaming_transformations(watermarked_df)

        # Write aggregations to Delta Lake (batch mode)
        windowed_aggregations.writeStream.outputMode("append").format("delta").option(
            "checkpointLocation", f"{checkpoint_path}/aggregations"
        ).trigger(processingTime="30 seconds").option(
            "path", f"{output_path}/aggregations"
        ).start()

        # Write detailed events (append mode)
        detail_query = (
            enriched_stream.writeStream.outputMode("append")
            .format("delta")
            .partitionBy("event_date", "event_hour")
            .option("checkpointLocation", f"{checkpoint_path}/details")
            .trigger(processingTime="10 seconds")
            .option("path", f"{output_path}/details")
            .start()
        )

        # Real-time alerts for high-value transactions
        self._setup_alerting(watermarked_df, checkpoint_path)

        # Console output for monitoring (in production, send to monitoring system)
        windowed_aggregations.writeStream.outputMode("complete").format(
            "console"
        ).option("truncate", False).trigger(processingTime="30 seconds").start()

        return detail_query

    def _apply_streaming_transformations(self, df: DataFrame) -> DataFrame:
        """Apply streaming-specific transformations"""
        return (
            df.withColumn("processing_timestamp", current_timestamp())
            .withColumn("event_date", date_format("timestamp", "yyyy-MM-dd"))
            .withColumn("event_hour", hour("timestamp"))
            .withColumn("event_minute", minute("timestamp"))
            .withColumn(
                "amount_range",
                when(col("amount") < 100, "0-100")
                .when(col("amount") < 500, "100-500")
                .when(col("amount") < 1000, "500-1000")
                .when(col("amount") < 5000, "1000-5000")
                .otherwise("5000+"),
            )
            .withColumn(
                "location_info",
                when(
                    col("location").isNotNull(),
                    concat_ws(
                        ", ",
                        coalesce(col("location.city"), lit("Unknown")),
                        coalesce(col("location.country"), lit("Unknown")),
                    ),
                ).otherwise("Unknown Location"),
            )
        )

    def _setup_alerting(self, df: DataFrame, checkpoint_path: str) -> None:
        """Setup real-time alerting for anomalies"""
        # High-value transaction alerts
        high_value_alerts = df.filter(col("amount") > 10000).select(
            col("event_id"),
            col("user_id"),
            col("amount"),
            col("timestamp"),
            col("event_type"),
        )

        # Write alerts to separate stream
        high_value_alerts.writeStream.outputMode("append").format("json").option(
            "checkpointLocation", f"{checkpoint_path}/alerts"
        ).option("path", "alerts/high_value").trigger(
            processingTime="5 seconds"
        ).start()

        logger.info("alerting_system_initialized")

    def process_with_stateful_operations(
        self, bootstrap_servers: str, topic: str, checkpoint_path: str
    ) -> StreamingQuery:
        """Advanced streaming with stateful operations"""
        # Read stream
        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .load()
        )

        # Parse and prepare for stateful processing
        events = kafka_df.select(
            from_json(col("value").cast("string"), self.get_event_schema()).alias(
                "data"
            )
        ).select("data.*")

        # Sessionization - group events by user sessions
        sessionized = (
            events.withWatermark("timestamp", "30 minutes")
            .groupBy(
                col("user_id"), window(col("timestamp"), "20 minutes", "5 minutes")
            )
            .agg(
                count("*").alias("events_in_session"),
                collect_list("event_type").alias("event_sequence"),
                spark_sum("amount").alias("session_value"),
                spark_max("timestamp").alias("last_activity"),
            )
        )

        # Pattern detection - find specific event sequences
        pattern_detection = sessionized.withColumn(
            "has_purchase_pattern",
            expr(
                "array_contains(event_sequence, 'VIEW') AND "
                + "array_contains(event_sequence, 'ADD_TO_CART') AND "
                + "array_contains(event_sequence, 'PURCHASE')"
            ),
        ).filter(col("has_purchase_pattern"))

        # Write results
        query = (
            pattern_detection.writeStream.outputMode("update")
            .format("delta")
            .option("checkpointLocation", f"{checkpoint_path}/patterns")
            .trigger(processingTime="1 minute")
            .start()
        )

        return query

    def create_streaming_ml_pipeline(
        self,
        model_path: str,
        bootstrap_servers: str,
        input_topic: str,
        output_topic: str,
    ) -> StreamingQuery:
        """Real-time ML predictions on streaming data"""
        from pyspark.ml import PipelineModel

        # Load pre-trained model
        model = PipelineModel.load(model_path)

        # Read prediction requests
        requests = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", input_topic)
            .load()
        )

        # Parse requests
        parsed_requests = requests.select(
            col("key").cast("string").alias("request_id"),
            from_json(col("value").cast("string"), self.get_event_schema()).alias(
                "data"
            ),
        ).select("request_id", "data.*")

        # Make predictions
        predictions = model.transform(parsed_requests)

        # Format output
        output_df = predictions.select(
            col("request_id").alias("key"),
            to_json(
                struct(
                    col("request_id"),
                    col("prediction"),
                    col("probability"),
                    current_timestamp().alias("prediction_timestamp"),
                )
            ).alias("value"),
        )

        # Write predictions back to Kafka
        query = (
            output_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", output_topic)
            .option("checkpointLocation", "checkpoint/ml_predictions")
            .outputMode("append")
            .trigger(processingTime="1 second")
            .start()
        )

        return query
