"""
Spark Job: Data Validation (Bronze -> Validation Report)

This standalone Spark job can be submitted to a Spark cluster.
Validates bronze layer data quality and schema.
Usage: spark-submit data_validation_job.py --year 2025
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, '/opt/airflow')

from src.data_validation.data_validation import DataValidation, load_validation_config
from src.utils.spark_utils import SparkSessionFactory, SparkConfig
from src.utils.s3_utils import S3Client
from src.config import S3Config


def main():
    parser = argparse.ArgumentParser(description='Run data validation on Spark')
    parser.add_argument('--year', type=int, help='Year to validate', default=2025)
    args = parser.parse_args()

    print(f"Starting data validation for year={args.year}")

    # Load validation config
    validation_config = load_validation_config()

    # Create Spark session
    spark_config = SparkConfig(
        app_name="data_validation",
        master="spark://spark-master:7077",
        config={
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g',
        }
    )
    spark = SparkSessionFactory.create_session(spark_config)

    try:
        # Initialize validation
        validator = DataValidation(validation_config, spark=spark)

        # Create S3 client for reading bronze data
        s3_config = S3Config(
            endpoint_url="http://minio:9000",
            access_key_id="minioadmin",
            secret_access_key="minioadmin",
            region="us-east-1",
            use_ssl=False,
            path_style_access=True
        )
        s3_client = S3Client(s3_config)
        
        # Get bronze files
        bronze_bucket = "nyc-taxi-bronze"
        bronze_files = []
        for month in range(1, 13):
            prefix = f"bronze/{args.year}/{month:02d}/"
            s3_files = s3_client.list_objects(bronze_bucket, prefix)
            parquet_files = [
                f"s3a://{bronze_bucket}/{f}"
                for f in s3_files if f.endswith('.parquet')
            ]
            bronze_files.extend(parquet_files)

        if not bronze_files:
            print(f"WARNING: No bronze files found for year {args.year}")
            return

        print(f"Reading {len(bronze_files)} bronze files for validation...")
        df = spark.read.option("mergeSchema", "true").parquet(*bronze_files)

        # Run validation
        results = validator.validate(df)

        # Print results
        print(f"\n{'='*60}")
        print(f"Validation Results for year {args.year}")
        print(f"{'='*60}")
        print(f"Overall Valid: {results.get('overall_valid', False)}")
        print(f"Schema Validation: {results.get('schema_validation', {}).get('valid', False)}")
        print(f"Data Validation: {results.get('data_validation', {}).get('valid', False)}")
        
        if not results.get('overall_valid', False):
            print("\nWARNING: Validation issues found. Check logs for details.")
        else:
            print("\nSUCCESS: All validations passed!")

    except Exception as e:
        print(f"ERROR: Validation failed: {e}")
        raise
    finally:
        SparkSessionFactory.stop_session()


if __name__ == "__main__":
    main()
