# Makefile for building the Envilink Spark Jobs package

# CI/CD Best Practices
SHELL := /bin/bash
.DELETE_ON_ERROR:

# Define variables
PROJECT_ROOT := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
SPARK_JOBS_DIR := $(PROJECT_ROOT)
BUILD_DIR := $(PROJECT_ROOT)/dist
VENV_DIR := $(PROJECT_ROOT)/.venv
ENV_FILE ?=
LIBS_DIR := $(BUILD_DIR)/libs
PYTHON_VERSION := 3.11

# Extract version from pyproject.toml
VERSION := $(shell grep -m 1 '^version = ' pyproject.toml | cut -d '"' -f 2)

# Configuration variables (overridable via environment variables or command line)
# Usage: CONFIG_BUCKET=my-bucket make deploy
# Or: export CONFIG_BUCKET=my-bucket && make deploy
# CONFIG_BUCKET is automatically prefixed with gs://
PIPELINES_DIR ?= conf

# Phony targets
.PHONY: all build clean install render-env render-ddl upload-config config-sync deploy help

# Default target
all: build

# Target: help
# Description: Display available targets and their descriptions
help:
	@echo "Available targets:"
	@echo "  build              - Builds the wheel file and dependencies zip"
	@echo "  deploy             - Uploads wheel, dependencies, and main.py to GCS"
	@echo "  clean              - Removes build directory and virtual environment"
	@echo "  install            - Installs package in virtual environment"
	@echo "  render-env          - Generate env_config.yaml from environment variables"
	@echo "  render-ddl          - Generate all DDL files (discovery and standardized zones)"
	@echo "  upload-config      - Upload pipeline configurations to GCS"
	@echo "  config-sync       - Sync pipeline configs from GCS config bucket to $(PIPELINES_DIR)"
	@echo ""
	@echo "Configuration variables (can be overridden via environment variables):"
	@echo "  CONFIG_BUCKET (required) - GCS bucket for deployment"
	@echo "  PIPELINES_DIR=$(PIPELINES_DIR) (default: conf)"
	@echo ""
	@echo "Example: ENV_FILE=.env make render-env"
	@echo "Example: CONFIG_BUCKET=my-bucket make deploy"
	@echo "Example: CONFIG_BUCKET=my-bucket make config-sync"

# Target: build
# Description: Builds the wheel file for the Spark jobs package.
build:
	@echo "Building Envilink Spark Jobs package..."
	@rm -rf $(BUILD_DIR)
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(LIBS_DIR)
	@echo "Installing dependencies to $(LIBS_DIR)..."
	@uv pip install -r requirements.txt --no-deps --target=$(LIBS_DIR)
	@echo "Creating dependences-$(VERSION).zip..."
	@cd $(LIBS_DIR) && zip -r $(BUILD_DIR)/dependences-$(VERSION).zip . && cd $(PROJECT_ROOT)
	@echo "Building wheel file..."
	@uv build --wheel --out-dir $(BUILD_DIR)
	@echo "Build complete! Artifacts are in $(BUILD_DIR)"
	@echo "  - Wheel file: $(BUILD_DIR)/pipeline-*.whl"
	@echo "  - Dependencies: $(BUILD_DIR)/dependences-$(VERSION).zip"

# Target: deploy
# Description: Uploads wheel, dependencies, and main.py to GCS
deploy:
	@if [ -z "$(CONFIG_BUCKET)" ]; then \
		echo "Error: CONFIG_BUCKET is required. Usage: CONFIG_BUCKET=my-bucket make deploy"; \
		exit 1; \
	fi
	@echo "Deploying artifacts to gs://$(CONFIG_BUCKET)..."
	@gcloud storage cp $(BUILD_DIR)/pipeline-*.whl gs://$(CONFIG_BUCKET)/whl/
	@gcloud storage cp $(BUILD_DIR)/dependences-$(VERSION).zip gs://$(CONFIG_BUCKET)/zip/
	@gcloud storage cp $(PROJECT_ROOT)/src/pipeline/main.py gs://$(CONFIG_BUCKET)/scripts/main.py
	@echo "Deployment complete!"

# Target: upload-config
# Description: Uploads pipeline configuration files to the config bucket
upload-config:
	@if [ -z "$(CONFIG_BUCKET)" ]; then \
		echo "Error: CONFIG_BUCKET is required. Usage: CONFIG_BUCKET=my-bucket make upload-config"; \
		exit 1; \
	fi
	@echo "Uploading pipeline configurations to gs://$(CONFIG_BUCKET)/pipelines/refine/..."
	@gcloud storage rsync -r $(PIPELINES_DIR)/ gs://$(CONFIG_BUCKET)/pipelines/refine/
	@echo "Pipeline configurations uploaded successfully!"

# Target: config-sync
# Description: Sync pipeline configurations from GCS config bucket to local conf
config-sync:
	@if [ -z "$(CONFIG_BUCKET)" ]; then \
		echo "Error: CONFIG_BUCKET is required. Usage: CONFIG_BUCKET=my-bucket make config-sync"; \
		exit 1; \
	fi
	@mkdir -p $(PIPELINES_DIR)
	@echo "Syncing from gs://$(CONFIG_BUCKET)/pipelines/refine/ to $(PIPELINES_DIR)/..."
	@gcloud storage rsync -r "gs://$(CONFIG_BUCKET)/pipelines/refine/" "$(PIPELINES_DIR)/"
	@echo "Config sync complete."

# Target: clean
# Description: Removes the build directory and the virtual environment.
clean:
	@echo "Cleaning up..."
	@rm -rf $(BUILD_DIR)
	@rm -rf $(VENV_DIR)
	@echo "Clean complete."

# Target: install
# Description: Installs the package in the virtual environment.
install:
	@echo "Installing package in the virtual environment..."
	@uv venv $(VENV_DIR) --python $(PYTHON_VERSION)
	@(\
		source $(VENV_DIR)/bin/activate; \
		python -V; \
		uv pip install -r requirements.txt; \
		uv pip install -e .; \
		uv pip list; \
	)
	@echo "Installation complete."

# Target: render-env
# Description: Generate env_config.yaml from environment variables
# Usage: make render-env [ENV_FILE=/path/to/.env]
#        If ENV_FILE is specified, loads environment variables from the .env file
render-env:
	@echo "Generating env_config.yaml from environment variables..."
	@uv run --no-project --with pyyaml --with python-dotenv src/pipeline/tools/generate_env_config.py $(if $(ENV_FILE),--env-file $(ENV_FILE),)
	@echo "env_config.yaml generated successfully in $(SPARK_JOBS_DIR)/"

# Target: render-ddl
# Description: Generate all DDL files for discovery and standardized zones
render-ddl:
	@echo "Generating DDL files for all pipeline configurations..."
	@uv run src/pipeline/tools/generate_ddl.py \
		--config-dir $(PIPELINES_DIR) \
		--output-dir ddl
	@echo "All DDL files generated successfully!"
