"""
OpenTelemetry Integration Module
Provides telemetry instrumentation for Spark applications
"""

import os
from typing import Optional, Dict, Any, Iterator
from functools import wraps
import time

from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.urllib3 import URLLib3Instrumentor
from opentelemetry.trace import Status, StatusCode
from opentelemetry.metrics import set_meter_provider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

import structlog

logger = structlog.get_logger()


class TelemetryManager:
    """Manages OpenTelemetry instrumentation for Spark applications"""

    def __init__(self, service_name: str = "spark-production-app"):
        self.service_name = service_name
        self.tracer: Optional[Any] = None
        self.meter: Optional[Any] = None
        self._initialize_telemetry()

    def _initialize_telemetry(self) -> None:
        """Initialize OpenTelemetry providers"""
        # Create resource
        resource = Resource.create(
            {
                "service.name": self.service_name,
                "service.version": "1.0.0",
                "environment": os.getenv("ENVIRONMENT", "development"),
                "host.name": os.getenv("HOSTNAME", "localhost"),
            }
        )

        # Initialize tracing
        otlp_endpoint = os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
        )

        # Trace provider
        trace_provider = TracerProvider(resource=resource)
        trace_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
        trace_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
        trace.set_tracer_provider(trace_provider)
        self.tracer = trace.get_tracer(__name__)

        # Metric provider
        metric_exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
        metric_reader = PeriodicExportingMetricReader(
            exporter=metric_exporter, export_interval_millis=10000
        )
        meter_provider = MeterProvider(
            resource=resource, metric_readers=[metric_reader]
        )
        set_meter_provider(meter_provider)
        self.meter = metrics.get_meter(__name__)

        # Create metrics
        self._create_metrics()

        # Auto-instrumentation
        RequestsInstrumentor().instrument()
        URLLib3Instrumentor().instrument()

        logger.info(
            "telemetry_initialized",
            service_name=self.service_name,
            endpoint=otlp_endpoint,
        )

    def _create_metrics(self) -> None:
        """Create application metrics"""
        if not self.meter:
            raise RuntimeError("Meter not initialized")
        # Counters
        self.pipeline_counter = self.meter.create_counter(
            name="spark_pipeline_executions",
            description="Number of pipeline executions",
            unit="1",
        )

        self.record_counter = self.meter.create_counter(
            name="spark_records_processed",
            description="Number of records processed",
            unit="1",
        )

        self.error_counter = self.meter.create_counter(
            name="spark_errors", description="Number of errors", unit="1"
        )

        # Histograms
        self.duration_histogram = self.meter.create_histogram(
            name="spark_operation_duration",
            description="Duration of operations",
            unit="s",
        )

        self.batch_size_histogram = self.meter.create_histogram(
            name="spark_batch_size", description="Size of processed batches", unit="1"
        )

        # Gauges (using UpDownCounter as gauge alternative)
        self.active_jobs_gauge = self.meter.create_up_down_counter(
            name="spark_active_jobs",
            description="Number of active Spark jobs",
            unit="1",
        )

        self.memory_usage_gauge = self.meter.create_observable_gauge(
            name="spark_memory_usage",
            callbacks=[self._get_memory_usage],
            description="Memory usage in bytes",
            unit="By",
        )

    def _get_memory_usage(self, options: Any) -> Iterator[metrics.Observation]:
        """Callback for memory usage gauge"""
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            yield metrics.Observation(memory_info.rss, {"type": "rss"})
            yield metrics.Observation(memory_info.vms, {"type": "vms"})
        except Exception as e:
            logger.error("failed_to_get_memory_usage", error=str(e))
            yield metrics.Observation(0, {"type": "unknown"})

    def trace_operation(self, operation_name: str) -> Any:
        """Decorator for tracing operations"""

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                if not self.tracer:
                    raise RuntimeError("Tracer not initialized")
                with self.tracer.start_as_current_span(operation_name) as span:
                    # Add span attributes
                    span.set_attribute("operation.type", operation_name)
                    span.set_attribute("spark.app.name", self.service_name)

                    start_time = time.time()
                    try:
                        result = func(*args, **kwargs)
                        span.set_status(Status(StatusCode.OK))

                        # Record success metrics
                        self.pipeline_counter.add(
                            1, {"status": "success", "operation": operation_name}
                        )

                        return result
                    except Exception as e:
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        span.record_exception(e)

                        # Record error metrics
                        self.error_counter.add(
                            1,
                            {
                                "operation": operation_name,
                                "error_type": type(e).__name__,
                            },
                        )

                        raise
                    finally:
                        duration = time.time() - start_time
                        span.set_attribute("duration_seconds", duration)

                        # Record duration metric
                        self.duration_histogram.record(
                            duration, {"operation": operation_name}
                        )

            return wrapper

        return decorator

    def record_batch_processing(
        self, batch_size: int, operation: str, duration: float
    ) -> None:
        """Record batch processing metrics"""
        self.record_counter.add(batch_size, {"operation": operation})
        self.batch_size_histogram.record(batch_size, {"operation": operation})
        self.duration_histogram.record(duration, {"operation": f"{operation}_batch"})

    def create_span(
        self, name: str, attributes: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Create a new span with optional attributes"""
        if not self.tracer:
            raise RuntimeError("Tracer not initialized")
        span = self.tracer.start_span(name)
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        return span

    def add_spark_context_to_span(self, span: Any, spark_context: Any) -> None:
        """Add Spark context information to span"""
        try:
            span.set_attribute("spark.app.id", spark_context.applicationId)
            span.set_attribute("spark.app.name", spark_context.appName)
            span.set_attribute("spark.master", spark_context.master)
            span.set_attribute("spark.version", spark_context.version)

            # Add executor information
            status = spark_context.statusTracker()
            span.set_attribute(
                "spark.executors.count",
                len(status.getExecutorInfos())
                if hasattr(status, "getExecutorInfos")
                else 0,
            )
            span.set_attribute("spark.jobs.active", len(status.getActiveJobIds()))

        except Exception as e:
            logger.error("failed_to_add_spark_context", error=str(e))

    def propagate_context(self) -> Dict[str, str]:
        """Get trace context for propagation"""
        propagator = TraceContextTextMapPropagator()
        carrier: Dict[str, str] = {}
        propagator.inject(carrier)
        return carrier

    def extract_context(self, carrier: Dict[str, str]) -> Any:
        """Extract trace context from carrier"""
        propagator = TraceContextTextMapPropagator()
        return propagator.extract(carrier)


# Global telemetry instance
_telemetry_manager = None


def get_telemetry_manager() -> TelemetryManager:
    """Get or create telemetry manager instance"""
    global _telemetry_manager
    if _telemetry_manager is None:
        _telemetry_manager = TelemetryManager()
    return _telemetry_manager


def instrument_spark_submit() -> Dict[str, str]:
    """Instrument Spark submit for telemetry"""
    telemetry = get_telemetry_manager()

    # Add Spark configuration for telemetry
    spark_conf = {
        "spark.opentelemetry.enabled": "true",
        "spark.opentelemetry.endpoint": os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
        ),
        "spark.opentelemetry.service.name": telemetry.service_name,
        "spark.driver.extraJavaOptions": "-javaagent:/opt/opentelemetry-javaagent.jar",
        "spark.executor.extraJavaOptions": "-javaagent:/opt/opentelemetry-javaagent.jar",
    }

    return spark_conf
