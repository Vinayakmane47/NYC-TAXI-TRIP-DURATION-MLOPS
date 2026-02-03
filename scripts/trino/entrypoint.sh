#!/bin/bash
# Trino entrypoint with Hadoop S3A configuration for MinIO

# Set Hadoop S3A system properties for MinIO
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dfs.s3a.endpoint=http://minio:9000"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dfs.s3a.access.key=minioadmin"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dfs.s3a.secret.key=minioadmin"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dfs.s3a.path.style.access=true"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dfs.s3a.connection.ssl.enabled=false"

# Run Trino
exec /usr/lib/trino/bin/run-trino
