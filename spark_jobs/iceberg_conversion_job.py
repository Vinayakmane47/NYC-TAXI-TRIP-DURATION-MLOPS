"""
Spark Job: Convert Parquet to Iceberg Tables

This standalone Spark job converts parquet files to Iceberg tables.
Supports bronze, silver, and gold layer conversions.
Usage: spark-submit iceberg_conversion_job.py --layer gold --year 2025 --months 1,2,3
       or spark-submit iceberg_conversion_job.py --layer gold --year 2025 (processes all months)
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, '/opt/airflow')

from src.lakehouse.iceberg_manager import IcebergManager
from src.lakehouse.table_registry import TableRegistry
from src.config.loader import load_iceberg_config
from src.utils.s3_utils import S3Client


def main():
    parser = argparse.ArgumentParser(description='Convert parquet files to Iceberg tables')
    parser.add_argument('--layer', type=str, required=True, choices=['bronze', 'silver', 'gold'],
                       help='Data layer to convert (bronze, silver, or gold)')
    parser.add_argument('--year', type=int, help='Year to process')
    parser.add_argument('--months', type=str, help='Comma-separated months (e.g., 1,2,3) or "all" for all months')
    parser.add_argument('--mode', type=str, choices=['overwrite', 'append'], default='append',
                       help='Conversion mode: overwrite or append (default: append)')
    args = parser.parse_args()

    # Load config
    config = load_iceberg_config()

    # Determine year
    year = args.year if args.year else 2025

    # Parse months
    if args.months:
        if args.months.lower() == 'all':
            months = list(range(1, 13))  # All 12 months
        else:
            months = [int(m.strip()) for m in args.months.split(',')]
    else:
        # Default: process all months
        months = list(range(1, 13))

    print(f"Starting Iceberg conversion for layer={args.layer}, year={year}, months={months}")
    print(f"Mode: {args.mode}")

    # Get table config for the layer
    layer_map = {
        'bronze': config.tables.bronze,
        'silver': config.tables.silver,
        'gold': config.tables.gold
    }
    table_config = layer_map[args.layer]

    # Setup S3 client if using S3
    s3_client = None
    if config.use_s3 and config.s3:
        s3_client = S3Client(config.s3)
        # Ensure bucket exists
        bucket_map = {
            'bronze': 'nyc-taxi-bronze',
            'silver': 'nyc-taxi-silver',
            'gold': 'nyc-taxi-gold'
        }
        bucket = bucket_map.get(args.layer)
        if bucket:
            s3_client.ensure_bucket_exists(bucket)

    # Setup table registry
    registry_file = config.registry.file_path if config.registry.persist else None
    registry = TableRegistry(registry_file=registry_file) if registry_file else TableRegistry()

    # Create Iceberg manager
    iceberg_manager = IcebergManager(
        spark_config=config.spark,
        s3_config=config.s3 if config.s3 else None,
        catalog_name=config.catalog.name,
        registry=registry
    )

    try:
        # Build source parquet path
        source_base = config.conversion.source_paths[args.layer]
        
        if config.use_s3:
            # S3 path
            bucket_map = {
                'bronze': 'nyc-taxi-bronze',
                'silver': 'nyc-taxi-silver',
                'gold': 'nyc-taxi-gold'
            }
            bucket = bucket_map.get(args.layer, 'nyc-taxi-gold')
            parquet_paths = []
            
            for month in months:
                # S3 path: s3a://bucket/year/MM/
                month_path = f"s3a://{bucket}/{source_base}/{year}/{month:02d}/"
                parquet_paths.append((month, month_path))
        else:
            # Local path
            root_dir = Path(config.root_dir)
            parquet_paths = []
            
            for month in months:
                month_path = root_dir / source_base / str(year) / f"{month:02d}"
                if month_path.exists():
                    parquet_paths.append((month, str(month_path)))
                else:
                    print(f"WARNING: Path does not exist: {month_path}. Skipping...")

        if not parquet_paths:
            print("ERROR: No parquet paths found to convert")
            return 1

        # Check if table exists
        table_exists = iceberg_manager.table_exists(
            table_config.schema_name,
            table_config.table_name
        )

        successful_months = []
        failed_months = []

        if args.mode == 'overwrite' or not table_exists:
            # Create or overwrite table
            if parquet_paths:
                # Use first month's data to create table
                first_month, first_path = parquet_paths[0]
                print(f"\n{'='*60}")
                print(f"Creating Iceberg table from {first_path}")
                print(f"{'='*60}")
                
                try:
                    iceberg_manager.create_table_from_parquet(
                        schema_name=table_config.schema_name,
                        table_name=table_config.table_name,
                        parquet_path=first_path,
                        partition_columns=table_config.partition_columns,
                        table_location=table_config.location
                    )
                    successful_months.append(first_month)
                    print(f"SUCCESS: Created table from month {first_month:02d}")
                    
                    # Append remaining months
                    for month, path in parquet_paths[1:]:
                        print(f"\nAppending month {month:02d} data...")
                        try:
                            iceberg_manager.append_to_table(
                                schema_name=table_config.schema_name,
                                table_name=table_config.table_name,
                                parquet_path=path
                            )
                            successful_months.append(month)
                            print(f"SUCCESS: Appended month {month:02d}")
                        except Exception as e:
                            print(f"ERROR: Failed to append month {month:02d}: {e}")
                            failed_months.append(month)
                except Exception as e:
                    print(f"ERROR: Failed to create table: {e}")
                    failed_months.append(first_month)
        else:
            # Append mode - append all months
            for month, path in parquet_paths:
                print(f"\n{'='*60}")
                print(f"Appending month {month:02d} data from {path}")
                print(f"{'='*60}")
                
                try:
                    iceberg_manager.append_to_table(
                        schema_name=table_config.schema_name,
                        table_name=table_config.table_name,
                        parquet_path=path
                    )
                    successful_months.append(month)
                    print(f"SUCCESS: Appended month {month:02d}")
                except Exception as e:
                    print(f"ERROR: Failed to append month {month:02d}: {e}")
                    failed_months.append(month)

        # Print summary
        print(f"\n{'='*60}")
        print(f"Conversion Summary:")
        print(f"  Layer: {args.layer}")
        print(f"  Table: {table_config.schema_name}.{table_config.table_name}")
        print(f"  Successful: {len(successful_months)} months {successful_months}")
        if failed_months:
            print(f"  Failed: {len(failed_months)} months {failed_months}")
        print(f"{'='*60}")

        # Get table info
        table_info = iceberg_manager.get_table_info(
            table_config.schema_name,
            table_config.table_name
        )
        if table_info:
            print(f"\nTable Information:")
            print(f"  Location: {table_info['location']}")
            print(f"  Row Count: {table_info['row_count']:,}")
            print(f"  Version: {table_info['version']}")

        # Fail if no months succeeded
        if not successful_months:
            raise RuntimeError(f"All months failed to convert. Failed months: {failed_months}")

    finally:
        # Cleanup
        iceberg_manager.stop()

    return 0


if __name__ == "__main__":
    exit(main())
