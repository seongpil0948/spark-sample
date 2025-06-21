"""
Logging Configuration Module
Sets up structured logging for production
"""

import sys
import os
import logging
import structlog
from typing import Any, Dict
import json
from datetime import datetime


def setup_logging(log_level: str = "INFO"):
    """Configure structured logging for the application"""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            add_application_context,
            structlog.processors.CallsiteParameterAdder(
                parameters=[
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            ),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper())
    )
    
    # Suppress verbose logs from libraries
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)


def add_application_context(logger: Any, log_method: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add application-specific context to all log entries"""
    
    event_dict["application"] = "spark-production-app"
    event_dict["environment"] = os.getenv("ENVIRONMENT", "development")
    event_dict["hostname"] = os.getenv("HOSTNAME", "localhost")
    
    # Add Spark context if available
    try:
        from pyspark import SparkContext
        sc = SparkContext._active_spark_context
        if sc:
            event_dict["spark_app_id"] = sc.applicationId
            event_dict["spark_app_name"] = sc.appName
    except:
        pass
    
    return event_dict


class SparkLogger:
    """Custom logger for Spark operations with metrics"""
    
    def __init__(self, name: str):
        self.logger = structlog.get_logger(name)
        self.metrics = {}
        
    def log_dataframe_info(self, df_name: str, df):
        """Log DataFrame information"""
        try:
            schema_json = df.schema.json()
            self.logger.info("dataframe_info",
                           name=df_name,
                           row_count=df.count(),
                           column_count=len(df.columns),
                           columns=df.columns,
                           schema=json.loads(schema_json))
        except Exception as e:
            self.logger.error("failed_to_log_dataframe_info", 
                            name=df_name, 
                            error=str(e))
    
    def log_performance_metrics(self, operation: str, start_time: datetime, 
                               end_time: datetime, records_processed: int):
        """Log performance metrics for operations"""
        duration = (end_time - start_time).total_seconds()
        throughput = records_processed / duration if duration > 0 else 0
        
        self.logger.info("performance_metrics",
                        operation=operation,
                        duration_seconds=duration,
                        records_processed=records_processed,
                        throughput_per_second=throughput,
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat())
    
    def log_spark_metrics(self, spark_context):
        """Log Spark runtime metrics"""
        try:
            status = spark_context.statusTracker()
            
            self.logger.info("spark_metrics",
                           active_jobs=len(status.getActiveJobIds()),
                           active_stages=len(status.getActiveStageIds()),
                           cache_memory_used=status.getCacheFraction() if hasattr(status, 'getCacheFraction') else None)
        except Exception as e:
            self.logger.error("failed_to_log_spark_metrics", error=str(e))


# Convenience function for getting logger
def get_logger(name: str = __name__) -> SparkLogger:
    """Get a configured SparkLogger instance"""
    return SparkLogger(name)