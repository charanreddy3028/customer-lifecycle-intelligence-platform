# Makefile for Customer Lifecycle Intelligence Platform
# This file saves you from having to memorize long terminal commands!

# Set the Python path automatically for all commands below
export PYTHONPATH := $(PYTHONPATH):.

.PHONY: generate-data check-db export-s3 help

help:
	@echo "Available commands:"
	@echo "  make generate-data  - Generate fake data and load it into MySQL"
	@echo "  make check-db       - Check the row counts in your MySQL database"
	@echo "  make export-s3      - Export tables from MySQL and push them to S3 Bronze layer"
	@echo "  make export-s3-api     - Export tables from API and push them to S3 Bronze layer"


generate-data:
	./.venv/bin/python -m data_generation.data_generation

check-db:
	./.venv/bin/python -m data_generation.check_db

export-s3:
	./.venv/bin/python -m source_to_bronze.rdbms_to_s3

export-s3-api:
	./.venv/bin/python -m source_to_bronze.api_to_s3
