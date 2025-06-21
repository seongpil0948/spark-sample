#!/bin/bash

# Development Environment Setup Script

set -e

echo "Setting up Spark Production App development environment..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    echo "Please restart your terminal or run 'source ~/.bashrc' (or ~/.zshrc) and run this script again."
    exit 1
fi

echo "✓ uv is installed"

# Create virtual environment and install dependencies
echo "Creating virtual environment and installing dependencies..."
# uv will automatically create a .venv directory
uv sync --dev

echo "✓ Virtual environment created at .venv"
echo "✓ Dependencies installed"

# Show Python version being used
echo "Python version:"
uv run python --version

# Install pre-commit hooks
echo "Installing pre-commit hooks..."
uv run pre-commit install

echo "✓ Pre-commit hooks installed"

# Generate sample data
echo "Generating sample data..."
uv run python scripts/generate_sample_data.py

echo "✓ Sample data generated"

# Run initial checks
echo "Running initial code quality checks..."
uv run ruff check --fix src/ tests/ scripts/ || true
uv run ruff format src/ tests/ scripts/

echo "✓ Code formatting applied"

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p logs/spark-events
mkdir -p data/input data/output
mkdir -p models
mkdir -p checkpoint
mkdir -p config/grafana/dashboards/{spark,system}
mkdir -p notebooks

echo "✓ Directories created"

# Display next steps
echo ""
echo "=== Development environment setup complete! ==="
echo ""
echo "Next steps:"
echo "1. Run tests: make test"
echo "2. Start local Spark: make docker-up"
echo "3. Start full stack: make docker-full-up"
echo "4. Run ETL pipeline: make run-etl"
echo ""
echo "Available Make targets:"
make help