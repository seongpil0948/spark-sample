"""
Monitoring Module
Implements metrics collection and monitoring
"""

import time
import os
from typing import Dict, Any, Optional, Iterator, List
from contextlib import contextmanager
from datetime import datetime
import structlog
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    CollectorRegistry,
    push_to_gateway,
)

logger = structlog.get_logger()


class MetricsCollector:
    """Collect and export application metrics"""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self._initialize_metrics()
        self.push_gateway = os.getenv("PROMETHEUS_GATEWAY", None)

    def _initialize_metrics(self) -> None:
        """Initialize Prometheus metrics"""

        # Counters
        self.pipeline_runs = Counter(
            "spark_pipeline_runs_total",
            "Total number of pipeline runs",
            ["pipeline_type", "status"],
            registry=self.registry,
        )

        self.records_processed = Counter(
            "spark_records_processed_total",
            "Total number of records processed",
            ["operation"],
            registry=self.registry,
        )

        self.errors = Counter(
            "spark_errors_total",
            "Total number of errors",
            ["error_type", "component"],
            registry=self.registry,
        )

        # Histograms
        self.operation_duration = Histogram(
            "spark_operation_duration_seconds",
            "Duration of operations in seconds",
            ["operation"],
            buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
            registry=self.registry,
        )

        self.data_quality_score = Histogram(
            "spark_data_quality_score",
            "Data quality scores",
            ["dataset"],
            buckets=(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
            registry=self.registry,
        )

        # Gauges
        self.active_jobs = Gauge(
            "spark_active_jobs", "Number of active Spark jobs", registry=self.registry
        )

        self.memory_usage = Gauge(
            "spark_memory_usage_bytes",
            "Memory usage in bytes",
            ["component"],
            registry=self.registry,
        )

        self.cache_hit_ratio = Gauge(
            "spark_cache_hit_ratio", "Cache hit ratio", registry=self.registry
        )

    @contextmanager
    def timer(self, operation: str) -> Iterator[None]:
        """Context manager for timing operations"""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.operation_duration.labels(operation=operation).observe(duration)
            logger.info(
                "operation_completed", operation=operation, duration_seconds=duration
            )

    def increment(
        self,
        metric_name: str,
        labels: Optional[Dict[str, str]] = None,
        value: float = 1,
    ) -> None:
        """Increment a counter metric"""
        try:
            if metric_name == "pipeline_runs":
                self.pipeline_runs.labels(**labels if labels else {}).inc(value)
            elif metric_name == "records_processed":
                self.records_processed.labels(**labels if labels else {}).inc(value)
            elif metric_name == "errors":
                self.errors.labels(**labels if labels else {}).inc(value)
            else:
                logger.warning("unknown_metric", metric=metric_name)
        except Exception as e:
            logger.error("metric_increment_failed", metric=metric_name, error=str(e))

    def set_gauge(
        self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Set a gauge metric"""
        try:
            if metric_name == "active_jobs":
                self.active_jobs.set(value)
            elif metric_name == "memory_usage":
                self.memory_usage.labels(**labels if labels else {}).set(value)
            elif metric_name == "cache_hit_ratio":
                self.cache_hit_ratio.set(value)
            else:
                logger.warning("unknown_gauge", metric=metric_name)
        except Exception as e:
            logger.error("gauge_set_failed", metric=metric_name, error=str(e))

    def record_data_quality(self, dataset: str, score: float) -> None:
        """Record data quality score"""
        self.data_quality_score.labels(dataset=dataset).observe(score)
        logger.info("data_quality_recorded", dataset=dataset, score=score)

    def collect_spark_metrics(self, spark_context: Any) -> None:
        """Collect metrics from Spark context"""
        try:
            status = spark_context.statusTracker()

            # Active jobs
            active_jobs = len(status.getActiveJobIds())
            self.set_gauge("active_jobs", active_jobs)

            # Memory usage (approximate)
            if hasattr(status, "getExecutorInfos"):
                executor_infos = status.getExecutorInfos()
                total_memory = sum(e.maxMemory for e in executor_infos)
                used_memory = sum(e.memoryUsed for e in executor_infos)

                self.set_gauge(
                    "memory_usage", used_memory, labels={"component": "executors"}
                )

                # Cache metrics
                if total_memory > 0:
                    cache_ratio = used_memory / total_memory
                    self.set_gauge("cache_hit_ratio", cache_ratio)

            logger.debug("spark_metrics_collected", active_jobs=active_jobs)

        except Exception as e:
            logger.error("failed_to_collect_spark_metrics", error=str(e))

    def push_metrics(self) -> None:
        """Push metrics to Prometheus gateway"""
        if self.push_gateway:
            try:
                push_to_gateway(
                    self.push_gateway,
                    job="spark_production_app",
                    registry=self.registry,
                )
                logger.info("metrics_pushed_to_gateway", gateway=self.push_gateway)
            except Exception as e:
                logger.error("failed_to_push_metrics", error=str(e))

    def close(self) -> None:
        """Clean up and push final metrics"""
        self.push_metrics()


class PerformanceMonitor:
    """Monitor and analyze performance metrics"""

    def __init__(self) -> None:
        self.metrics_history: List[Dict[str, Any]] = []

    def record_operation(
        self,
        operation: str,
        start_time: datetime,
        end_time: datetime,
        records: int,
        additional_metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record operation performance"""
        duration = (end_time - start_time).total_seconds()

        metric = {
            "operation": operation,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "records_processed": records,
            "throughput_per_second": records / duration if duration > 0 else 0,
            "timestamp": datetime.now().isoformat(),
        }

        if additional_metrics:
            metric.update(additional_metrics)

        self.metrics_history.append(metric)

        # Analyze performance trends
        self._analyze_performance(operation)

    def _analyze_performance(self, operation: str) -> None:
        """Analyze performance trends and detect anomalies"""
        operation_metrics = [
            m for m in self.metrics_history if m["operation"] == operation
        ]

        if len(operation_metrics) >= 5:
            # Calculate statistics
            durations = [m["duration_seconds"] for m in operation_metrics[-5:]]
            avg_duration = sum(durations) / len(durations)

            # Check for performance degradation
            latest_duration = durations[-1]
            if latest_duration > avg_duration * 1.5:
                logger.warning(
                    "performance_degradation_detected",
                    operation=operation,
                    latest_duration=latest_duration,
                    average_duration=avg_duration,
                    degradation_factor=latest_duration / avg_duration,
                )

    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        if not self.metrics_history:
            return {}

        # Group by operation
        operations: Dict[str, Any] = {}
        for metric in self.metrics_history:
            op = metric["operation"]
            if op not in operations:
                operations[op] = []
            operations[op].append(metric)

        # Calculate summaries
        summary = {}
        for op, metrics in operations.items():
            durations = [m["duration_seconds"] for m in metrics]
            throughputs = [m["throughput_per_second"] for m in metrics]

            summary[op] = {
                "count": len(metrics),
                "avg_duration": sum(durations) / len(durations),
                "min_duration": min(durations),
                "max_duration": max(durations),
                "avg_throughput": sum(throughputs) / len(throughputs),
                "total_records": sum(m["records_processed"] for m in metrics),
            }

        return summary
