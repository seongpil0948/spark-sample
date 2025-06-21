#!/usr/bin/env python3
"""
Production-ready Apache Spark Application
Demonstrates ETL, Streaming, and Machine Learning pipelines
"""

import sys
import os
from datetime import datetime
import click
import structlog
from pyspark.sql import SparkSession
from typing import Optional

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.spark_config import create_spark_session, get_spark_config
from src.utils.logging import setup_logging
from src.etl.pipeline import ETLPipeline
from src.streaming.kafka_processor import StreamingProcessor
from src.ml.training_pipeline import MLPipeline
from src.utils.monitoring import MetricsCollector

# Setup structured logging
logger = structlog.get_logger()


class SparkProductionApp:
    """Main Spark application orchestrator"""

    def __init__(self, app_name: str = "SparkProductionApp"):
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self.metrics = MetricsCollector()
        setup_logging()

    def initialize_spark(self, mode: str = "batch") -> SparkSession:
        """Initialize Spark session with production configurations"""
        config = get_spark_config(mode)
        self.spark = create_spark_session(self.app_name, config)
        logger.info(
            "spark_session_created",
            app_name=self.app_name,
            mode=mode,
            spark_version=self.spark.version,
        )
        return self.spark

    def run_etl_pipeline(self, input_path: str, output_path: str, date: str) -> None:
        """Execute ETL pipeline"""
        logger.info("starting_etl_pipeline", input_path=input_path, date=date)
        if not self.spark:
            raise RuntimeError(
                "Spark session not initialized. Call initialize_spark() first."
            )
        try:
            with self.metrics.timer("etl_pipeline_duration"):
                etl = ETLPipeline(self.spark)

                # Run data quality checks
                quality_report = etl.run_quality_checks(input_path, date)
                logger.info("data_quality_report", report=quality_report)

                # Execute transformation
                result_df = etl.transform_data(input_path, date)

                # Generate aggregations
                summary_df = etl.generate_summary(result_df)

                # Save results
                etl.save_results(result_df, summary_df, output_path, date)

                self.metrics.increment("etl_pipeline_success")
                logger.info(
                    "etl_pipeline_completed", records_processed=result_df.count()
                )

        except Exception as e:
            self.metrics.increment("etl_pipeline_failure")
            logger.error("etl_pipeline_failed", error=str(e))
            raise

    def run_streaming_pipeline(self, kafka_config: dict, checkpoint_path: str) -> None:
        """Execute streaming pipeline"""
        logger.info("starting_streaming_pipeline", kafka_config=kafka_config)
        if not self.spark:
            raise RuntimeError(
                "Spark session not initialized. Call initialize_spark() first."
            )
        try:
            streaming = StreamingProcessor(self.spark)
            query = streaming.process_kafka_stream(
                kafka_config["bootstrap_servers"],
                kafka_config["topic"],
                checkpoint_path,
                output_path=kafka_config.get("output_path", "data/output/streaming"),
            )

            logger.info("streaming_pipeline_started", query_id=query.id)

            # Run for specified duration or until stopped
            if kafka_config.get("duration"):
                query.awaitTermination(kafka_config["duration"])
            else:
                query.awaitTermination()

        except Exception as e:
            logger.error("streaming_pipeline_failed", error=str(e))
            raise

    def run_ml_pipeline(self, training_data_path: str, model_path: str) -> None:
        """Execute machine learning pipeline"""
        logger.info(
            "starting_ml_pipeline",
            training_data_path=training_data_path,
            model_path=model_path,
        )
        if not self.spark:
            raise RuntimeError(
                "Spark session not initialized. Call initialize_spark() first."
            )

        try:
            with self.metrics.timer("ml_pipeline_duration"):
                ml = MLPipeline(self.spark)

                # Load and prepare data
                data = ml.load_data(training_data_path)

                # Train model
                model, metrics = ml.train_model(data)
                logger.info("model_training_completed", metrics=metrics)

                # Save model
                ml.save_model(model, model_path)

                # Start real-time prediction service if configured
                if os.getenv("ENABLE_PREDICTION_SERVICE", "false").lower() == "true":
                    ml.start_prediction_service(model)

                self.metrics.increment("ml_pipeline_success")

        except Exception as e:
            self.metrics.increment("ml_pipeline_failure")
            logger.error("ml_pipeline_failed", error=str(e))
            raise

    def cleanup(self) -> None:
        """Clean up resources"""
        if self.spark:
            self.spark.stop()
            logger.info("spark_session_stopped")
        self.metrics.close()


@click.command()
@click.option(
    "--mode",
    type=click.Choice(["etl", "streaming", "ml", "all"]),
    default="etl",
    help="Pipeline mode to run",
)
@click.option("--input-path", default="data/input", help="Input data path")
@click.option("--output-path", default="data/output", help="Output data path")
@click.option(
    "--date",
    default=datetime.now().strftime("%Y-%m-%d"),
    help="Processing date (YYYY-MM-DD)",
)
@click.option(
    "--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers"
)
@click.option("--kafka-topic", default="events", help="Kafka topic to consume")
@click.option(
    "--checkpoint-path", default="checkpoint/", help="Streaming checkpoint path"
)
@click.option("--model-path", default="models/", help="ML model path")
@click.option(
    "--config-file", type=click.Path(exists=True), help="Configuration file path"
)
def main(
    mode: str,
    input_path: str,
    output_path: str,
    date: str,
    kafka_servers: str,
    kafka_topic: str,
    checkpoint_path: str,
    model_path: str,
    config_file: Optional[str],
) -> None:
    """
    Production-ready Spark Application

    Examples:
        # Run ETL pipeline
        python app.py --mode etl --date 2024-01-01

        # Run streaming pipeline
        python app.py --mode streaming --kafka-topic events

        # Run ML pipeline
        python app.py --mode ml --input-path data/training

        # Run all pipelines
        python app.py --mode all
    """

    app = SparkProductionApp()

    try:
        # Initialize Spark based on mode
        spark_mode = "streaming" if mode == "streaming" else "batch"
        app.initialize_spark(spark_mode)

        # Execute requested pipeline(s)
        if mode in ["etl", "all"]:
            app.run_etl_pipeline(input_path, output_path, date)

        if mode in ["streaming", "all"]:
            kafka_config = {
                "bootstrap_servers": kafka_servers,
                "topic": kafka_topic,
                "output_path": f"{output_path}/streaming",
                "duration": 300,  # 5 minutes for demo
            }
            app.run_streaming_pipeline(kafka_config, checkpoint_path)

        if mode in ["ml", "all"]:
            training_path = f"{input_path}/training"
            app.run_ml_pipeline(training_path, model_path)

        logger.info("application_completed_successfully", mode=mode)

    except Exception as e:
        logger.error("application_failed", mode=mode, error=str(e))
        sys.exit(1)

    finally:
        app.cleanup()


if __name__ == "__main__":
    main()
