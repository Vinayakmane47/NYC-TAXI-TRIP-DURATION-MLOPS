"""
DAG to export Gold layer data to PostgreSQL for Superset visualization
Run this after your data pipeline completes
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'export_gold_to_postgres_for_superset',
    default_args=default_args,
    description='Export Gold layer to PostgreSQL for Superset',
    schedule_interval='@daily',  # Run daily after your main pipeline
    catchup=False,
    tags=['export', 'superset', 'gold-layer'],
)

export_to_postgres = SparkSubmitOperator(
    task_id='export_gold_to_postgres',
    application='/opt/airflow/spark_jobs/export_to_postgres.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',  # Set master via conf
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.jars': '/opt/spark/jars/postgresql-42.2.18.jar',
    },
    dag=dag,
)

def notify_completion():
    print("SUCCESS: Gold layer data exported to PostgreSQL")
    print("  Tables: gold_daily_trip_summary, gold_hourly_patterns,")
    print("          gold_pickup_location_stats, gold_recent_trips")
    print("  Access via Superset at http://localhost:8088")

notify = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    dag=dag,
)

export_to_postgres >> notify
