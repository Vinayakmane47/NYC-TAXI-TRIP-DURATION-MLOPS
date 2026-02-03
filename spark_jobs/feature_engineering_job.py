"""
Spark Job: Feature Engineering / Data Transformation (Silver -> Gold)

This standalone Spark job can be submitted to a Spark cluster.
Processes data month-by-month internally to prevent OOM.
Usage: spark-submit feature_engineering_job.py --year 2025 --months 1,2,3
       or spark-submit feature_engineering_job.py --year 2025 (processes all months)
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, '/opt/airflow')

from src.data_transformation.feature_engineering import FeatureEngineering
from src.config.loader import load_feature_engineering_config


def main():
    parser = argparse.ArgumentParser(description='Run feature engineering on Spark')
    parser.add_argument('--year', type=int, help='Year to process')
    parser.add_argument('--months', type=str, help='Comma-separated months (e.g., 1,2,3) or "all" for all months')
    args = parser.parse_args()

    # Load config
    config = load_feature_engineering_config()

    # Override year if provided
    year = args.year if args.year else config.processing.year

    # Parse months
    if args.months:
        if args.months.lower() == 'all':
            months = list(range(1, 13))  # All 12 months
        else:
            months = [int(m.strip()) for m in args.months.split(',')]
    else:
        # Default: process all months
        months = list(range(1, 13))

    print(f"Starting feature engineering for year={year}, processing months: {months}")
    print(f"Processing {len(months)} months sequentially to prevent OOM...")

    # Create feature engineering pipeline (reuse SparkSession across months)
    feature_eng = FeatureEngineering(config)

    try:
        # Process each month sequentially
        successful_months = []
        skipped_months = []
        failed_months = []
        
        for month in months:
            print(f"\n{'='*60}")
            print(f"Processing month {month:02d}/{year}")
            print(f"{'='*60}")
            
            try:
                # Check if input files exist before processing
                file_paths = feature_eng._get_data_files(year, [month], layer="silver")
                if not file_paths:
                    print(f"WARNING: No silver files found for month {month:02d}. Skipping...")
                    skipped_months.append(month)
                    continue
                
                # Process single month
                gold_path = feature_eng.process_and_save(year, [month])
                print(f"SUCCESS: Month {month:02d} feature engineering completed! Data saved to: {gold_path}")
                successful_months.append(month)
                
                # Clear Spark cache to free memory between months
                feature_eng.spark.catalog.clearCache()
                print(f"Cleared Spark cache after processing month {month:02d}")
            except Exception as e:
                print(f"ERROR: Error processing month {month:02d}: {e}")
                failed_months.append(month)
                # Continue processing other months instead of failing immediately
                continue
        
        print(f"\n{'='*60}")
        print(f"Processing Summary:")
        print(f"  Successful: {len(successful_months)} months {successful_months}")
        if skipped_months:
            print(f"  Skipped (no data): {len(skipped_months)} months {skipped_months}")
        if failed_months:
            print(f"  Failed: {len(failed_months)} months {failed_months}")
        print(f"{'='*60}")
        
        # Fail the job only if all months failed or no months succeeded
        if not successful_months:
            if failed_months:
                raise RuntimeError(f"All months failed to process. Failed months: {failed_months}")
            else:
                raise RuntimeError(f"No months had data to process. Check preprocessing stage.")

    finally:
        # Cleanup
        feature_eng.stop()


if __name__ == "__main__":
    main()
