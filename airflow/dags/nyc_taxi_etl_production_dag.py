"""
Production ETL DAG for NYC Taxi Trip Duration Pipeline

This DAG orchestrates Spark jobs running on a separate Spark cluster.
Airflow handles orchestration only, not computation.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path
if Path('/opt/airflow').exists():
    project_root = Path('/opt/airflow')
else:
    project_root = Path(__file__).parent.parent.parent

sys.path.insert(0, str(project_root))

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Connection
from airflow import settings
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'mlops-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    "nyc_taxi_etl_production2",
    default_args=default_args,
    description="Production ETL Pipeline with separate Spark cluster",
    schedule=None,                    # manual-only
    start_date=datetime(2026, 2, 1),  # stable, still required
    catchup=False,
    max_active_runs=1,
    tags=["etl", "nyc-taxi", "production", "spark"],
)


def verify_spark_connection(**context):
    """
    Verify spark_default connection exists, create it if missing, fix it if wrong format.
    
    Returns:
        bool: True if connection exists or was created, False otherwise
    """
    session = None
    try:
        session = settings.Session()
        conn = session.query(Connection).filter(Connection.conn_id == 'spark_default').first()
        
        if not conn:
            logger.warning("spark_default connection not found! Creating it now...")
            try:
                # Create the connection programmatically
                # For Spark connections, Airflow constructs master as spark://{host}:{port}
                # So we set host to just the hostname and port to the port number
                new_conn = Connection(
                    conn_id='spark_default',
                    conn_type='spark',
                    host='spark-master',  # Just the hostname
                    port=7077,  # Port number
                    extra='{"master": "spark://spark-master:7077", "queue": "root.default", "deploy-mode": "client"}'
                )
                session.add(new_conn)
                session.commit()
                logger.info("spark_default connection created successfully!")
                logger.info(f"Connection: spark-master:7077 (will be formatted as spark://spark-master:7077)")
                return True
            except Exception as create_error:
                logger.error(f"Failed to create spark_default connection: {create_error}", exc_info=True)
                session.rollback()
                return False
        else:
            # Check if connection needs to be updated
            needs_update = False
            if conn.host != 'spark-master' or conn.port != 7077:
                logger.warning(f"Connection host/port is '{conn.host}:{conn.port}', updating to 'spark-master:7077'")
                conn.host = 'spark-master'
                conn.port = 7077
                # Update extra to ensure master URL is correct
                import json
                extra_dict = {}
                if conn.extra:
                    try:
                        extra_dict = json.loads(conn.extra) if isinstance(conn.extra, str) else conn.extra
                    except:
                        extra_dict = {}
                extra_dict['master'] = 'spark://spark-master:7077'
                extra_dict['queue'] = 'root.default'
                extra_dict['deploy-mode'] = 'client'
                conn.extra = json.dumps(extra_dict)
                needs_update = True
            
            if needs_update:
                try:
                    session.commit()
                    logger.info("Connection updated successfully!")
                except Exception as update_error:
                    logger.error(f"Failed to update connection: {update_error}", exc_info=True)
                    session.rollback()
                    return False
            
            logger.info(f"spark_default connection found: {conn.host}:{conn.port}")
            logger.info(f"Connection type: {conn.conn_type}")
            logger.info(f"Connection extra: {conn.extra}")
            return True
    except Exception as e:
        logger.error(f"Error verifying spark_default connection: {e}", exc_info=True)
        logger.warning("Connection verification failed.")
        return False
    finally:
        if session:
            session.close()




# Task 0: Connection Verification (runs first to verify Spark connection)
connection_verification_task = PythonOperator(
    task_id='verify_spark_connection',
    python_callable=verify_spark_connection,
    dag=dag,
)


def run_data_ingestion(**context):
    """
    Data ingestion doesn't use Spark - it's a simple Python download task.
    This runs directly in Airflow worker.
    """
    from src.data_ingestion.data_ingestion import DataIngestion
    from src.config.loader import load_ingestion_config

    logger.info("Starting data ingestion task...")

    try:
        config = load_ingestion_config()
        ingestion = DataIngestion(config)

        year = context.get('year', config.year)
        downloaded_files = ingestion.ingest_data(year)

        logger.info(f"Data ingestion completed. Files: {len(downloaded_files)}")
        return {'files_count': len(downloaded_files), 'status': 'success'}
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}", exc_info=True)
        raise


# ============================================================================
# DAG TASKS (3 tasks total)
# ============================================================================

# Task 1: Data Ingestion (Python task - no Spark needed)
ingestion_task = PythonOperator(
    task_id='data_ingestion',
    python_callable=run_data_ingestion,
    dag=dag,
)

# Task 2: Data Preprocessing (Spark job - Bronze -> Silver)
# Spark job internally loops through months 1-12 and processes sequentially
# MEMORY-OPTIMIZED CONFIGURATION to prevent OOM errors
preprocessing_task = SparkSubmitOperator(
    task_id='data_preprocessing',
    application='/opt/airflow/spark_jobs/data_preprocessing_job.py',
    conf={
        'spark.master': 'spark://spark-master:7077',
        # Memory settings - conservative to prevent OOM
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
        'spark.driver.maxResultSize': '512m',
        'spark.driver.cores': '1',
        'spark.executor.memoryOverhead': '512m',  # Extra memory for overhead
        # Memory management
        'spark.memory.fraction': '0.6',  # Reduced from 0.7 for stability
        'spark.memory.storageFraction': '0.3',  # Increased for caching
        'spark.cleaner.referenceTracking.cleanCheckpoints': 'true',
        'spark.cleaner.periodicGC.interval': '10min',
        # Shuffle optimization - reduced partitions to save memory
        'spark.sql.shuffle.partitions': '50',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.adaptive.skewJoin.enabled': 'true',
        'spark.sql.adaptive.advisoryPartitionSizeInBytes': '67108864',  # 64MB
        # File size optimization
        'spark.sql.files.maxPartitionBytes': '67108864',  # 64MB per partition
        'spark.sql.files.openCostInBytes': '4194304',  # 4MB
        # Broadcast optimization
        'spark.sql.autoBroadcastJoinThreshold': '10485760',  # 10MB
        # Serialization
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.kryoserializer.buffer.max': '512m',
        # Disable Arrow to prevent OOM
        'spark.sql.execution.arrow.pyspark.enabled': 'false',
        # Compression for in-memory data
        'spark.sql.inMemoryColumnarStorage.compressed': 'true',
        'spark.sql.inMemoryColumnarStorage.batchSize': '5000',
        # S3/MinIO configuration
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    application_args=['--year', '2025'],  # Processes all months internally
    dag=dag,
)

# Task 3: Data Transformation / Feature Engineering (Spark job - Silver -> Gold)
# Spark job internally loops through months 1-12 and processes sequentially
# MEMORY-OPTIMIZED CONFIGURATION - feature engineering is memory-intensive
transformation_task = SparkSubmitOperator(
    task_id='data_transformation',
    application='/opt/airflow/spark_jobs/feature_engineering_job.py',
    conf={
        'spark.master': 'spark://spark-master:7077',
        # Memory settings - conservative to prevent OOM
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
        'spark.driver.maxResultSize': '512m',
        'spark.driver.cores': '1',
        'spark.executor.memoryOverhead': '512m',  # Extra memory for overhead
        # Memory management
        'spark.memory.fraction': '0.6',  # Reduced from 0.7 for stability
        'spark.memory.storageFraction': '0.3',  # Increased for caching
        'spark.cleaner.referenceTracking.cleanCheckpoints': 'true',
        'spark.cleaner.periodicGC.interval': '10min',
        # Shuffle optimization - reduced partitions to save memory
        'spark.sql.shuffle.partitions': '50',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.adaptive.skewJoin.enabled': 'true',
        'spark.sql.adaptive.advisoryPartitionSizeInBytes': '67108864',  # 64MB
        # File size optimization
        'spark.sql.files.maxPartitionBytes': '67108864',  # 64MB per partition
        'spark.sql.files.openCostInBytes': '4194304',  # 4MB
        # Broadcast optimization - increased for frequency features
        'spark.sql.autoBroadcastJoinThreshold': '10485760',  # 10MB
        # Serialization
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.kryoserializer.buffer.max': '512m',
        # Disable Arrow to prevent OOM
        'spark.sql.execution.arrow.pyspark.enabled': 'false',
        # Compression for in-memory data
        'spark.sql.inMemoryColumnarStorage.compressed': 'true',
        'spark.sql.inMemoryColumnarStorage.batchSize': '5000',
        # S3/MinIO configuration
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    application_args=['--year', '2025'],  # Processes all months internally
    dag=dag,
)

# Task dependencies: Connection Verification -> Ingestion -> Preprocessing -> Transformation
connection_verification_task >> ingestion_task >> preprocessing_task >> transformation_task
