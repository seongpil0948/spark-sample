# Virtual Environment Guide with uv

This guide explains how to manage virtual environments using uv for the Spark Production App.

## Overview

uv automatically manages virtual environments for Python projects. When you run `uv sync`, uv creates a virtual environment in the `.venv` directory at the project root.

**Important**: The `.venv` directory is created and managed by uv, not by the standard `python -m venv` command. However, the virtual environment created by uv is fully compatible with standard Python virtual environments.

## Key Concepts

1. **Automatic Virtual Environment**: uv creates the `.venv` directory automatically (you don't use `python -m venv`)
2. **No Activation Needed**: When using `uv run`, you don't need to activate the virtual environment
3. **Python Version Management**: uv can download and manage Python versions for you
4. **Standard Compatibility**: The `.venv` created by uv is a standard Python virtual environment

## Basic Usage

### Creating a Virtual Environment

```bash
# This creates .venv directory (managed by uv) and installs dependencies
# You do NOT need to run 'python -m venv .venv' first!
uv sync

# With development dependencies
uv sync --dev

# What happens when you run 'uv sync':
# 1. uv checks for .python-version file (or uses system Python)
# 2. uv creates .venv directory if it doesn't exist
# 3. uv installs all dependencies from pyproject.toml into .venv
# 4. The .venv is a standard Python virtual environment (but created by uv)
```

### Using the Virtual Environment

#### Method 1: Using `uv run` (Recommended)

```bash
# Run any command in the virtual environment
uv run python script.py
uv run pytest
uv run spark-app --mode etl

# No need to activate the virtual environment!
```

#### Method 2: Traditional Activation

```bash
# Activate the virtual environment
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows

# Now you can run commands directly
python script.py
pytest

# Deactivate when done
deactivate
```

## Python Version Management

### Setting Python Version

```bash
# Pin to a specific Python version
uv python pin 3.9      # Uses latest 3.9.x
uv python pin 3.9.18   # Uses exact version

# This creates/updates .python-version file
```

### Installing Python Versions

```bash
# List available Python versions
uv python list

# Install a specific version
uv python install 3.9
uv python install 3.9.18

# uv will automatically download and install Python if needed
```

### Checking Python Version

```bash
# Check which Python is being used
uv run python --version
uv run which python

# With virtual environment info
make venv-info
```

## Common Commands

### Installation

```bash
# Install production dependencies
uv sync
# or
make install

# Install with dev dependencies
uv sync --dev
# or
make install-dev
```

### Running Commands

```bash
# Run Python scripts
uv run python src/app.py

# Run Spark application
uv run spark-app --mode etl

# Run tests
uv run pytest
# or
make test

# Format code
uv run ruff format src/
# or
make format
```

### Cleanup

```bash
# Remove virtual environment
rm -rf .venv
# or
make clean-venv

# Remove everything including venv
make clean-all
```

## Integration with IDEs

### VS Code

1. VS Code should automatically detect `.venv`
2. Select the interpreter: `Cmd/Ctrl + Shift + P` → "Python: Select Interpreter" → `.venv/bin/python`

### PyCharm

1. Go to Settings → Project → Python Interpreter
2. Click the gear icon → Add
3. Select "Existing environment"
4. Browse to `.venv/bin/python`

### Jupyter

```bash
# Install kernel in virtual environment
uv run python -m ipykernel install --user --name spark-prod

# Use the kernel in Jupyter
jupyter notebook  # Select "spark-prod" kernel
```

## Troubleshooting

### Virtual Environment Not Found

```bash
# Recreate virtual environment
uv sync --dev
```

### Wrong Python Version

```bash
# Check current Python version
uv run python --version

# Change Python version
uv python pin 3.9
uv sync --reinstall
```

### Permission Issues

```bash
# On macOS/Linux, ensure uv is executable
chmod +x ~/.local/bin/uv

# Or reinstall uv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Package Installation Issues

```bash
# Clear uv cache
uv cache clean

# Force reinstall
uv sync --reinstall
```

## Best Practices

1. **Always use `uv run`**: This ensures you're using the correct virtual environment
2. **Pin Python version**: Use `.python-version` file for consistency across team
3. **Don't commit .venv**: The `.venv` directory is already in `.gitignore`
4. **Use Make targets**: They wrap uv commands for convenience
5. **Regular updates**: Keep uv updated with `uv self update`

## Environment Variables

uv respects these environment variables:

```bash
# Use a different virtual environment location
UV_PROJECT_ENVIRONMENT=/path/to/venv uv sync

# Use system Python instead of downloading
UV_PYTHON_PREFERENCE=only-system uv sync

# Disable virtual environment creation
UV_NO_SYNC=1 uv run python script.py
```

## Quick Reference

| Task | Command |
|------|---------|
| Create venv | `uv sync` |
| Install dev deps | `uv sync --dev` |
| Run Python | `uv run python` |
| Run script | `uv run python script.py` |
| Run pytest | `uv run pytest` |
| Format code | `uv run ruff format src/` |
| Check Python | `uv run python --version` |
| Clean venv | `rm -rf .venv` |
| Pin Python | `uv python pin 3.9` |

## Additional Resources

- [uv Documentation](https://docs.astral.sh/uv/)
- [uv Virtual Environments](https://docs.astral.sh/uv/pip/environments/)
- [Python Version Management](https://docs.astral.sh/uv/guides/install-python/)