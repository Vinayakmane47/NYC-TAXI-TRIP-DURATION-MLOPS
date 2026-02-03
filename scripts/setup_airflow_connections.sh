#!/bin/bash
# Setup Airflow connections for Spark

set -e  # Exit on error

echo "Setting up Airflow connections..."

# Configuration
CONN_ID='spark_default'
CONN_TYPE='spark'
CONN_HOST='spark-master'
CONN_PORT='7077'
CONN_EXTRA='{"master": "spark://spark-master:7077", "queue": "root.default", "deploy-mode": "client"}'
MAX_RETRIES=5
RETRY_DELAY=10

# Function to wait for Airflow webserver to be ready
wait_for_airflow() {
    local retries=0
    echo "Waiting for Airflow webserver to be ready..."
    
    while [ $retries -lt $MAX_RETRIES ]; do
        if docker exec airflow-webserver airflow version > /dev/null 2>&1; then
            echo "✅ Airflow webserver is ready!"
            return 0
        fi
        retries=$((retries + 1))
        echo "Attempt $retries/$MAX_RETRIES: Airflow not ready yet, waiting ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
    done
    
    echo "❌ Airflow webserver not ready after $MAX_RETRIES attempts"
    return 1
}

# Function to check if connection exists
connection_exists() {
    docker exec airflow-webserver airflow connections get "$CONN_ID" > /dev/null 2>&1
}

# Function to delete connection if it exists
delete_connection() {
    if connection_exists; then
        echo "Deleting existing connection '$CONN_ID'..."
        docker exec airflow-webserver airflow connections delete "$CONN_ID" || true
        sleep 2
    fi
}

# Function to create or update connection
create_connection() {
    local retries=0
    
    while [ $retries -lt $MAX_RETRIES ]; do
        echo "Attempt $((retries + 1))/$MAX_RETRIES: Creating connection '$CONN_ID'..."
        
        if docker exec airflow-webserver airflow connections add "$CONN_ID" \
            --conn-type "$CONN_TYPE" \
            --conn-host "$CONN_HOST" \
            --conn-port "$CONN_PORT" \
            --conn-extra "$CONN_EXTRA" 2>&1; then
            
            echo "✅ Connection '$CONN_ID' created successfully!"
            return 0
        fi
        
        retries=$((retries + 1))
        if [ $retries -lt $MAX_RETRIES ]; then
            echo "Failed to create connection, retrying in ${RETRY_DELAY}s..."
            sleep $RETRY_DELAY
        fi
    done
    
    echo "❌ Failed to create connection after $MAX_RETRIES attempts"
    return 1
}

# Function to verify connection
verify_connection() {
    echo "Verifying connection '$CONN_ID'..."
    
    if ! connection_exists; then
        echo "❌ Connection '$CONN_ID' does not exist!"
        return 1
    fi
    
    # Get connection details
    local conn_info=$(docker exec airflow-webserver airflow connections get "$CONN_ID" 2>&1)
    
    if echo "$conn_info" | grep -q "$CONN_HOST"; then
        echo "✅ Connection '$CONN_ID' verified successfully!"
        echo "Connection details:"
        echo "$conn_info"
        return 0
    else
        echo "❌ Connection verification failed - host mismatch"
        return 1
    fi
}

# Main execution
main() {
    # Wait for Airflow to be ready
    if ! wait_for_airflow; then
        exit 1
    fi
    
    # Delete existing connection if present (to ensure clean state)
    delete_connection
    
    # Create connection with retry logic
    if ! create_connection; then
        echo "❌ Failed to setup connection after retries"
        exit 1
    fi
    
    # Verify connection was created correctly
    if ! verify_connection; then
        echo "❌ Connection verification failed"
        exit 1
    fi
    
    echo "✅ Spark connection configured and verified!"
}

# Run main function
main
