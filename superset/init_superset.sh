#!/bin/bash
# Superset Initialization Script
# This script initializes Superset database and creates admin user

set -e

echo "Initializing Superset..."

# PostgreSQL should already be healthy due to docker-compose depends_on healthcheck
# Adding a small sleep as extra precaution
echo "Waiting for PostgreSQL to be ready..."
sleep 5
echo "PostgreSQL should be ready!"

# Initialize Superset database
echo "Initializing Superset database..."
export SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
superset db upgrade

# Create admin user (skip if already exists)
echo "Creating admin user..."
export FLASK_APP=superset
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin || echo "Admin user may already exist"

# Initialize Superset
echo "Initializing Superset..."
superset init || echo "Superset may already be initialized"

echo "Superset initialization completed!"
