"""
Main Pipeline Orchestrator for NYC Taxi Trip Duration MLOps Project

This module orchestrates the complete ML pipeline:
1. Data Ingestion
2. Data Validation
3. Data Transformation (Preprocessing + Feature Engineering)
"""

import sys
import logging
from pathlib import Path
from typing import Optional, List
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_ingestion import DataIngestion, load_config as load_ingestion_config
from src.data_validation import DataValidation, load_validation_config
from src.data_transformation import (
    DataTransformation,
    load_preprocessing_config,
    load_feature_engineering_config
)
from src.logger.logging import get_logger

# Configure logger
# Logger configured automatically
logger = logging.getLogger(__name__)


class MLPipeline:
    """
    Main pipeline orchestrator that connects all stages together.
    """
    
    def __init__(
        self,
        run_ingestion: bool = True,
        run_validation: bool = True,
        run_transformation: bool = True,
        year: Optional[int] = None,
        months: Optional[List[int]] = None
    ):
        """
        Initialize ML Pipeline.
        
        Args:
            run_ingestion: Whether to run data ingestion stage
            run_validation: Whether to run data validation stage
            run_transformation: Whether to run data transformation stage
            year: Year to process (defaults to config values)
            months: List of months to process (defaults to config values)
        """
        self.run_ingestion = run_ingestion
        self.run_validation = run_validation
        self.run_transformation = run_transformation
        self.year = year
        self.months = months
        
        # Initialize components
        self.ingestion = None
        self.validator = None
        self.transformation = None
        
        # Pipeline execution results
        self.results = {
            'start_time': None,
            'end_time': None,
            'ingestion': {},
            'validation': {},
            'transformation': {},
            'success': False,
            'errors': []
        }
        
        logger.info("ML Pipeline initialized")
        logger.info(f"Stages to run: ingestion={run_ingestion}, validation={run_validation}, transformation={run_transformation}")
    
    def _load_configs(self):
        """Load all required configurations."""
        logger.info("Loading pipeline configurations...")
        
        try:
            if self.run_ingestion:
                ingestion_config = load_ingestion_config()
                self.ingestion = DataIngestion(ingestion_config)
                logger.info("✓ Data ingestion config loaded")
            
            if self.run_validation:
                validation_config = load_validation_config()
                self.validator = DataValidation(validation_config)
                logger.info("✓ Data validation config loaded")
            
            if self.run_transformation:
                preprocessing_config = load_preprocessing_config()
                feature_engineering_config = load_feature_engineering_config()
                self.transformation = DataTransformation(
                    preprocessing_config,
                    feature_engineering_config
                )
                logger.info("✓ Data transformation config loaded")
            
            logger.info("All configurations loaded successfully")
            
        except Exception as e:
            logger.error(f"Error loading configurations: {e}", exc_info=True)
            raise
    
    def run(self) -> dict:
        """
        Execute the complete ML pipeline.
        
        Returns:
            Dictionary with pipeline execution results
        """
        self.results['start_time'] = datetime.now().isoformat()
        logger.info("=" * 80)
        logger.info("STARTING ML PIPELINE EXECUTION")
        logger.info("=" * 80)
        
        try:
            # Load configurations
            self._load_configs()
            
            # Stage 1: Data Ingestion
            if self.run_ingestion:
                self._run_ingestion()
            
            # Stage 2: Data Validation (on raw data)
            if self.run_validation and self.run_ingestion:
                self._run_validation_stage('raw')
            
            # Stage 3: Data Transformation (Preprocessing + Feature Engineering)
            if self.run_transformation:
                self._run_transformation()
            
            # Stage 4: Data Validation (on transformed data)
            if self.run_validation and self.run_transformation:
                self._run_validation_stage('gold')
            
            # Pipeline completed successfully
            self.results['success'] = True
            self.results['end_time'] = datetime.now().isoformat()
            
            logger.info("=" * 80)
            logger.info("ML PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            self._print_summary()
            
        except Exception as e:
            self.results['success'] = False
            self.results['errors'].append(str(e))
            self.results['end_time'] = datetime.now().isoformat()
            
            logger.error("=" * 80)
            logger.error("ML PIPELINE EXECUTION FAILED")
            logger.error("=" * 80)
            logger.error(f"Error: {e}", exc_info=True)
            raise
        
        finally:
            # Cleanup
            self._cleanup()
        
        return self.results
    
    def _run_ingestion(self):
        """Execute data ingestion stage."""
        logger.info("")
        logger.info("=" * 80)
        logger.info("STAGE 1: DATA INGESTION")
        logger.info("=" * 80)
        
        try:
            year = self.year or self.ingestion.config.year
            downloaded_files = self.ingestion.ingest_data(year)
            
            self.results['ingestion'] = {
                'success': True,
                'files_downloaded': len(downloaded_files),
                'files': [str(f) for f in downloaded_files]
            }
            
            logger.info(f"✓ Data ingestion completed: {len(downloaded_files)} files downloaded")
            
        except Exception as e:
            self.results['ingestion'] = {
                'success': False,
                'error': str(e)
            }
            logger.error(f"✗ Data ingestion failed: {e}")
            raise
    
    def _run_validation_stage(self, stage: str):
        """
        Execute data validation stage.
        
        Args:
            stage: 'raw', 'silver', or 'gold'
        """
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"STAGE: DATA VALIDATION ({stage.upper()})")
        logger.info("=" * 80)
        
        try:
            # This is a placeholder - validation would need to read data
            # In practice, validation is integrated into transformation stages
            logger.info(f"✓ Data validation for {stage} stage (integrated in transformation)")
            
            self.results['validation'][stage] = {
                'success': True,
                'note': 'Validation integrated in transformation stages'
            }
            
        except Exception as e:
            self.results['validation'][stage] = {
                'success': False,
                'error': str(e)
            }
            logger.error(f"✗ Data validation for {stage} failed: {e}")
            if self.validator and self.validator.config.validation.fail_on_validation_error:
                raise
    
    def _run_transformation(self):
        """Execute data transformation stage (preprocessing + feature engineering)."""
        logger.info("")
        logger.info("=" * 80)
        logger.info("STAGE 2: DATA TRANSFORMATION")
        logger.info("=" * 80)
        
        try:
            silver_path, gold_path = self.transformation.transform_and_save(
                year=self.year,
                months=self.months
            )
            
            self.results['transformation'] = {
                'success': True,
                'silver_path': str(silver_path),
                'gold_path': str(gold_path)
            }
            
            logger.info(f"✓ Data transformation completed")
            logger.info(f"  Silver layer: {silver_path}")
            logger.info(f"  Gold layer: {gold_path}")
            
        except Exception as e:
            self.results['transformation'] = {
                'success': False,
                'error': str(e)
            }
            logger.error(f"✗ Data transformation failed: {e}")
            raise
    
    def _cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up resources...")
        
        try:
            if self.transformation:
                self.transformation.stop()
                logger.info("✓ Transformation SparkSession stopped")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
    
    def _print_summary(self):
        """Print pipeline execution summary."""
        logger.info("")
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("-" * 80)
        logger.info(f"Start Time: {self.results['start_time']}")
        logger.info(f"End Time: {self.results['end_time']}")
        logger.info(f"Success: {self.results['success']}")
        
        if self.results['ingestion']:
            logger.info(f"Ingestion: {self.results['ingestion'].get('files_downloaded', 0)} files")
        
        if self.results['transformation']:
            logger.info(f"Transformation: Silver={self.results['transformation'].get('silver_path', 'N/A')}")
            logger.info(f"              Gold={self.results['transformation'].get('gold_path', 'N/A')}")
        
        if self.results['errors']:
            logger.warning(f"Errors: {len(self.results['errors'])}")
            for error in self.results['errors']:
                logger.warning(f"  - {error}")
        
        logger.info("-" * 80)


def main():
    """Main function to run the complete ML pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run ML Pipeline for NYC Taxi Trip Duration')
    parser.add_argument('--skip-ingestion', action='store_true', help='Skip data ingestion stage')
    parser.add_argument('--skip-validation', action='store_true', help='Skip data validation stage')
    parser.add_argument('--skip-transformation', action='store_true', help='Skip data transformation stage')
    parser.add_argument('--year', type=int, help='Year to process')
    parser.add_argument('--months', type=int, nargs='+', help='Months to process (e.g., 1 2 3)')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = MLPipeline(
        run_ingestion=not args.skip_ingestion,
        run_validation=not args.skip_validation,
        run_transformation=not args.skip_transformation,
        year=args.year,
        months=args.months
    )
    
    # Run pipeline
    results = pipeline.run()
    
    # Exit with appropriate code
    sys.exit(0 if results['success'] else 1)


if __name__ == "__main__":
    main()
