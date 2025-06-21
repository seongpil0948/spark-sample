# Production-Ready Apache Spark Application

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

A comprehensive Apache Spark application demonstrating ETL pipelines, streaming processing, and machine learning at scale with full observability stack. Built with modern Python tooling using uv for package management and Ruff for code quality.

## Features

- **ETL Pipeline**: Production-ready batch processing with data quality checks
- **Streaming Processing**: Real-time data processing with Kafka integration
- **Machine Learning**: Complete ML pipeline with feature engineering and model training
- **Performance Optimization**: AQE, broadcast joins, and custom partitioning strategies
- **Full Observability**: OpenTelemetry integration with metrics, traces, and logs
- **Monitoring**: Prometheus metrics and structured logging
- **Docker Support**: Full containerization with Docker Compose

## Architecture

```
spark-production-app/
├── src/
│   ├── app.py              # Main application entry point
│   ├── etl/                # ETL pipeline modules
│   ├── streaming/          # Streaming processing modules
│   ├── ml/                 # Machine learning modules
│   └── utils/              # Utilities and configurations
├── config/                 # Configuration files
├── data/                   # Data directories
├── models/                 # Saved ML models
├── scripts/                # Execution scripts
├── Docker files            # Container configuration
└── requirements.txt        # Python dependencies
```

## Observability Stack

The application includes a complete observability stack powered by OpenTelemetry Collector:

- **OpenTelemetry Collector**: Central telemetry pipeline for traces, metrics, and logs
- **Prometheus**: Metrics storage and querying
- **Jaeger**: Distributed tracing visualization
- **Elasticsearch + Kibana**: Log aggregation and search
- **Grafana**: Unified dashboards for metrics, logs, and traces

## Quick Start

### Prerequisites

- Python 3.9+
- Apache Spark 4.0.0
- Docker and Docker Compose
- 16GB+ RAM recommended
- uv (for Python package management)
- Optional: Ruff (installed automatically with dev dependencies)

### Installation

1. Clone the repository

2. Install uv (if not already installed):
```bash
# On macOS and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

3. Create virtual environment with uv:
```bash
# uv automatically creates a virtual environment in .venv directory when you run sync
# You don't need to create it manually - uv handles everything!
uv sync --dev

# After running the above command:
# - A .venv directory is created by uv (not by python -m venv)
# - All dependencies are installed in .venv
# - The virtual environment uses the Python version from .python-version (or system default)

# To use a specific Python version:
uv python pin 3.9  # This creates/updates .python-version file
uv sync --dev     # This will recreate .venv with Python 3.9

# OPTIONAL: Manual activation (not required when using 'uv run')
# The .venv created by uv can be activated like any standard Python virtual environment:
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate     # On Windows
```

4. Install project dependencies:
```bash
# Install production dependencies only
uv sync

# Or install with development dependencies (includes Ruff)
uv sync --dev

# Alternative: use Make
make install       # Production dependencies
make install-dev   # Include dev dependencies
```

5. Generate sample data:
```bash
# Using uv run (recommended - no need to activate venv)
uv run python scripts/generate_sample_data.py

# Or use Make
make generate-data
```

### Running the Full Stack (Recommended)

Start Spark with the complete observability stack:

```bash
./scripts/start-full-stack.sh
```

This will start:
- Spark cluster (master + 2 workers)
- Kafka message broker
- OpenTelemetry Collector
- Prometheus, Jaeger, Elasticsearch, Kibana, Grafana
- Jupyter notebook

Access the services:
- **Spark Master UI**: http://localhost:8080
- **Grafana**: http://localhost:3000 (admin/admin)
- **Jaeger UI**: http://localhost:16686
- **Kibana**: http://localhost:5601
- **Prometheus**: http://localhost:9090
- **Jupyter Lab**: http://localhost:8888

### Running Individual Pipelines

#### ETL Pipeline
```bash
# Using uv
uv run spark-app --mode etl --date 2024-01-01

# Using Make
make run-etl DATE=2024-01-01

# Using shell script
./scripts/run_etl.sh 2024-01-01

# Or in Docker:
docker exec spark-master /app/scripts/run_etl.sh
```

#### Streaming Pipeline
```bash
# Using uv
uv run spark-app --mode streaming

# Using Make
make run-streaming

# Using shell script
./scripts/run_streaming.sh

# Or in Docker:
docker exec spark-master /app/scripts/run_streaming.sh
```

#### ML Pipeline
```bash
# Using uv
uv run spark-app --mode ml

# Using Make
make run-ml

# Using shell script
./scripts/run_ml.sh

# Or in Docker:
docker exec spark-master /app/scripts/run_ml.sh
```

#### All Pipelines
```bash
# Using uv
uv run spark-app --mode all

# Using Make
make run-all

# Direct Python
uv run python src/app.py --mode all
```

### Using Docker Compose

For Spark only:
```bash
docker-compose up -d
```

For Spark + Observability:
```bash
docker-compose -f docker-compose.full.yml up -d
```

For Observability stack only:
```bash
docker-compose -f docker-compose.observability.yml up -d
```

## Configuration

The application uses YAML configuration files and environment variables:

- `config/application.yaml`: Main application configuration
- `config/otel-collector-config.yaml`: OpenTelemetry Collector configuration
- `config/prometheus.yml`: Prometheus scrape configuration
- Environment variables override config values
- Supports development, staging, and production environments

## Telemetry and Monitoring

### Metrics
- Application metrics via Prometheus client
- Spark metrics via JMX and Pushgateway
- Host metrics via node-exporter
- Custom business metrics

### Traces
- Distributed tracing with OpenTelemetry
- Automatic instrumentation for HTTP calls
- Manual instrumentation for critical paths
- Trace correlation with logs

### Logs
- Structured JSON logging with structlog
- Log aggregation in Elasticsearch
- Real-time log streaming via Kafka
- Log correlation with traces

### Dashboards
Pre-configured Grafana dashboards for:
- Spark cluster overview
- ETL pipeline performance
- Streaming metrics
- ML model training progress
- System resource utilization

## Key Technologies

- **Apache Spark 4.0.0**: Next-generation distributed processing engine with enhanced AQE, improved columnar processing, and dynamic optimization
- **Delta Lake 4.0**: Advanced ACID transactions, time travel, auto-compaction, and optimized writes
- **Structured Streaming**: Real-time processing with enhanced state management, improved watermarking, and state store compression
- **MLlib**: Machine learning at scale with Spark 4.0 optimizations, enhanced feature selection, and improved model training performance
- **Kafka**: Message streaming with Spark 4.0 optimized connectors
- **OpenTelemetry**: Vendor-neutral observability
- **Prometheus**: Metrics collection
- **Jaeger**: Distributed tracing
- **Elasticsearch**: Log storage
- **Grafana**: Visualization
- **Docker**: Containerization

## Performance Optimizations (Spark 4.0)

1. **Enhanced Adaptive Query Execution (AQE)**: Advanced runtime query optimization with improved skew handling and local shuffle readers
2. **Dynamic Partition Pruning**: Reduced data scanning with better predicate pushdown and enhanced filter optimization
3. **Adaptive Broadcast Joins**: Intelligent join strategy selection with optimized skew join handling
4. **Kryo Serialization**: Faster data serialization with improved buffer management and reduced memory overhead
5. **Columnar Processing**: Enhanced off-heap columnar operations with improved vectorization
6. **Delta Lake 4.0 Optimization**: Advanced Z-ordering, auto-compaction, optimized writes, and enhanced time travel
7. **State Store Compression**: Improved streaming state management with LZ4 compression and optimized checkpointing
8. **Query Compilation**: Enhanced code generation, vectorization, and whole-stage code generation
9. **ML Pipeline Optimization**: Enhanced feature selection, improved model training, and optimized hyperparameter tuning
10. **Arrow Integration**: Improved PySpark performance with enhanced Arrow-based data transfer
11. **Adaptive Caching**: Intelligent caching strategies with lazy materialization and memory optimization

## Production Deployment

### Kubernetes
```yaml
kubectl apply -f k8s/spark-operator.yaml
kubectl apply -f k8s/spark-application.yaml
```

### YARN
```bash
spark-submit --master yarn --deploy-mode cluster src/app.py
```

### AWS EMR
Configure with S3 paths and appropriate IAM roles.

## Development Tools

### Virtual Environment Management with uv

```bash
# IMPORTANT: uv creates and manages the .venv directory automatically!
# You do NOT need to run 'python -m venv .venv' - uv does this for you

# Create virtual environment and install dependencies:
uv sync              # Creates .venv with production dependencies
uv sync --dev        # Creates .venv with all dependencies (including dev)

# The .venv directory is created by uv, not by standard Python venv module
# However, it's compatible with standard Python virtual environments

# To use a specific Python version:
uv python pin 3.9        # Creates .python-version file
uv python pin 3.9.18     # Pin to specific patch version
uv sync --dev            # Recreates .venv with the specified Python version

# List available Python versions:
uv python list

# Install a specific Python version if needed:
uv python install 3.9    # uv can download and install Python for you!

# Run commands in the uv-created virtual environment:
uv run python script.py   # Automatically uses .venv
uv run pytest            # No need to activate venv
uv run spark-app         # Runs the project's CLI

# Or activate the uv-created virtual environment manually:
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows
python script.py           # Now runs in the activated environment
deactivate                 # To deactivate

# Check virtual environment details:
uv run which python       # Shows path to Python in .venv
uv run python --version   # Shows Python version
make venv-info           # Shows comprehensive venv information
```

### Code Quality with Ruff

```bash
# Format code
uv run ruff format src/ tests/
# Or: make format

# Lint code
uv run ruff check src/ tests/
# Or: make lint

# Auto-fix lint issues
uv run ruff check --fix src/ tests/

# Run all checks
make check  # Runs lint + type checking
```

### Pre-commit Hooks

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run manually on all files
uv run pre-commit run --all-files
```

## Testing

```bash
# Run unit tests
uv run pytest tests/
# Or: make test

# Run tests with coverage
uv run pytest --cov=src tests/
# Or: make test-cov

# Run type checking
uv run mypy src/
# Or: make type-check

# Run all development checks
make dev-check  # format + lint + type-check + test
```

## Troubleshooting

### Check Service Health
```bash
# All services status
docker-compose -f docker-compose.full.yml ps

# OpenTelemetry Collector health
curl http://localhost:13133/health

# Prometheus targets
curl http://localhost:9090/api/v1/targets
```

### Common Issues

1. **No metrics showing**: Check Prometheus targets and OTEL Collector logs
2. **No traces**: Verify OTLP endpoint configuration
3. **No logs**: Check Kafka connectivity and Elasticsearch indices
4. **High memory usage**: Adjust batch sizes and executor memory
5. **Slow performance**: Enable AQE and check partition skew

## Contributing

1. Install development dependencies: `uv sync --dev`
2. Install pre-commit hooks: `uv run pre-commit install`
3. Follow PEP 8 style guide (enforced by Ruff)
4. Write comprehensive tests
5. Update documentation
6. Use type hints (checked by mypy)
7. Add meaningful log messages
8. Instrument new features with OpenTelemetry
9. Run `make dev-check` before committing

### Development Workflow

```bash
# 1. Make changes to code

# 2. Format and lint
make format

# 3. Run tests
make test

# 4. Run all checks
make dev-check

# 5. Commit (pre-commit hooks will run automatically)
git commit -m "Your message"
```

## License

Apache License 2.0

## Tool Documentation

- [uv Documentation](https://docs.astral.sh/uv/) - Fast Python package manager
- [Ruff Documentation](https://docs.astral.sh/ruff/) - Fast Python linter and formatter

## Additional Resources

- [README-OBSERVABILITY.md](README-OBSERVABILITY.md) - Detailed observability documentation
- [VIRTUAL_ENV_GUIDE.md](docs/VIRTUAL_ENV_GUIDE.md) - Virtual environment management with uv
- [Spark Documentation](https://spark.apache.org/docs/latest/)
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Delta Lake Documentation](https://docs.delta.io/)