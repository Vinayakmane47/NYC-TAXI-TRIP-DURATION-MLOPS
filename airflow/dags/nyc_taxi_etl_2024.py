"""
NYC Taxi ETL Pipeline for 2024 Data
Separate DAG for processing 2024 historical data
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'nyc_taxi_etl_2024',
    default_args=default_args,
    description='NYC Taxi ETL Pipeline for 2024 historical data',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['nyc-taxi', 'etl', '2024', 'historical'],
)

# Task 1: Connection Verification
connection_verification_task = BashOperator(
    task_id='verify_connections',
    bash_command='''
    echo "Verifying MinIO connection..."
    docker exec minio mc alias set myminio http://minio:9000 minioadmin minioadmin 2>/dev/null || true
    docker exec minio mc ls myminio/ > /dev/null && echo "✓ MinIO connection successful" || exit 1
    ''',
    dag=dag,
)

# Task 2: Data Ingestion (Downloads 2024 data from NYC TLC)
ingestion_task = SparkSubmitOperator(
    task_id='data_ingestion',
    application='/opt/airflow/spark_jobs/data_ingestion_job.py',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '1g',
        'spark.driver.memory': '1g',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    application_args=['--year', '2024', '--months', '1,2,3,4,5,6,7,8,9,10,11,12'],
    dag=dag,
)

# Task 3: Data Preprocessing (Bronze -> Silver)
preprocessing_task = SparkSubmitOperator(
    task_id='data_preprocessing',
    application='/opt/airflow/spark_jobs/preprocessing_job.py',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '1536m',
        'spark.driver.memory': '1536m',
        'spark.driver.maxResultSize': '384m',
        'spark.sql.shuffle.partitions': '8',
        'spark.sql.adaptive.enabled': 'true',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    application_args=['--year', '2024'],
    dag=dag,
)

# Task 4: Feature Engineering (Silver -> Gold)
transformation_task = SparkSubmitOperator(
    task_id='data_transformation',
    application='/opt/airflow/spark_jobs/feature_engineering_job.py',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '1536m',
        'spark.driver.memory': '1536m',
        'spark.driver.maxResultSize': '384m',
        'spark.sql.shuffle.partitions': '8',
        'spark.sql.adaptive.enabled': 'true',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    application_args=['--year', '2024', '--months', '1,2,3,4,5,6,7,8,9,10,11,12'],
    dag=dag,
)

# Task dependencies
connection_verification_task >> ingestion_task >> preprocessing_task >> transformation_task
