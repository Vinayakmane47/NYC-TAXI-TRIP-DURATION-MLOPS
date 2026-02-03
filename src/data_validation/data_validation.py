"""
Data Validation Module for NYC Yellow Taxi Trip Data using PySpark

This module performs schema validation and data quality validation
on processed data to ensure data quality and consistency.
"""

import os
import sys
import logging
import json
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import (
    DataValidationConfig,
    ValidationSettings,
    SchemaValidationRules,
    DataQualityRules,
    ReportingConfig
)
from src.logger.logging import get_logger

# Configure logger
# Logger configured automatically
logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates DataFrame schema against expected schema definition."""
    
    def __init__(self, features_config: Dict[str, Any], validation_rules: SchemaValidationRules):
        """
        Initialize SchemaValidator.
        
        Args:
            features_config: Dictionary of feature definitions from features.yaml
            validation_rules: SchemaValidationRules configuration
        """
        self.features_config = features_config
        self.rules = validation_rules
        self.expected_schema = self._build_expected_schema()
        
    def _build_expected_schema(self) -> Dict[str, Dict[str, Any]]:
        """Build expected schema dictionary from features config."""
        return self.features_config.get('features', {})
    
    def _spark_type_mapping(self, type_str: str) -> T.DataType:
        """Map string type to Spark DataType."""
        type_mapping = {
            'string': T.StringType(),
            'integer': T.IntegerType(),
            'long': T.LongType(),
            'double': T.DoubleType(),
            'boolean': T.BooleanType(),
            'timestamp_ntz': T.TimestampNTZType(),
            'timestamp': T.TimestampType(),
            'date': T.DateType(),
        }
        return type_mapping.get(type_str.lower(), T.StringType())
    
    def validate_schema(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame schema against expected schema.
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        logger.info("Starting schema validation...")
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'column_checks': {}
        }
        
        actual_columns = set(df.columns)
        expected_columns = set(self.expected_schema.keys())
        
        # Check for missing columns
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            msg = f"Missing columns: {missing_columns}"
            if self.rules.allow_missing_columns:
                results['warnings'].append(msg)
                logger.warning(msg)
            else:
                results['errors'].append(msg)
                results['valid'] = False
                logger.error(msg)
        
        # Check for extra columns
        extra_columns = actual_columns - expected_columns
        if extra_columns:
            msg = f"Extra columns found: {extra_columns}"
            if self.rules.allow_extra_columns:
                results['warnings'].append(msg)
                logger.warning(msg)
            else:
                results['errors'].append(msg)
                results['valid'] = False
                logger.error(msg)
        
        # Validate each column
        for col_name in actual_columns:
            if col_name not in self.expected_schema:
                continue
                
            col_info = self.expected_schema[col_name]
            col_checks = {
                'valid': True,
                'errors': [],
                'warnings': []
            }
            
            # Check data type
            actual_type = str(df.schema[col_name].dataType)
            expected_type = col_info.get('type', 'string')
            
            # Type validation (simplified - Spark types can vary)
            type_valid = self._validate_type(actual_type, expected_type)
            if not type_valid:
                msg = f"Column {col_name}: Expected type {expected_type}, got {actual_type}"
                col_checks['errors'].append(msg)
                col_checks['valid'] = False
                logger.warning(msg)
            
            # Check nullability
            actual_nullable = df.schema[col_name].nullable
            expected_nullable = col_info.get('nullable', True)
            
            if actual_nullable != expected_nullable:
                msg = f"Column {col_name}: Expected nullable={expected_nullable}, got {actual_nullable}"
                if self.rules.strict_mode:
                    col_checks['errors'].append(msg)
                    col_checks['valid'] = False
                    logger.error(msg)
                else:
                    col_checks['warnings'].append(msg)
                    logger.warning(msg)
            
            results['column_checks'][col_name] = col_checks
            if not col_checks['valid']:
                results['valid'] = False
        
        if results['valid']:
            logger.info("Schema validation passed")
        else:
            logger.error(f"Schema validation failed with {len(results['errors'])} errors")
        
        return results
    
    def _validate_type(self, actual_type: str, expected_type: str) -> bool:
        """Validate if actual type matches expected type (flexible matching)."""
        type_mapping = {
            'string': ['stringtype', 'string'],
            'integer': ['integertype', 'inttype', 'integer'],
            'long': ['longtype', 'biginttype', 'long'],
            'double': ['doubletype', 'floattype', 'double', 'float'],
            'boolean': ['booleantype', 'booltype', 'boolean', 'bool'],
            'timestamp_ntz': ['timestampntztype', 'timestampntzt', 'timestamp'],
            'timestamp': ['timestamptype', 'timestamp'],
        }
        
        expected_lower = expected_type.lower()
        actual_lower = actual_type.lower()
        
        if expected_lower in type_mapping:
            return any(t in actual_lower for t in type_mapping[expected_lower])
        
        return True  # Unknown types pass validation


class DataValidator:
    """Validates data quality, consistency, and business rules."""
    
    def __init__(self, features_config: Dict[str, Any], validation_config: DataValidationConfig):
        """
        Initialize DataValidator.
        
        Args:
            features_config: Dictionary of feature definitions from features.yaml
            validation_config: DataValidationConfig object
        """
        self.features_config = features_config
        self.config = validation_config
        self.features = features_config.get('features', {})
        
    def validate_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive data validation.
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        logger.info("Starting data validation...")
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'metrics': {},
            'checks': {}
        }
        
        # Completeness checks
        if self.config.data_quality.completeness:
            completeness_results = self._check_completeness(df)
            results['checks']['completeness'] = completeness_results
            if not completeness_results['valid']:
                results['valid'] = False
        
        # Consistency checks
        if self.config.data_quality.consistency:
            consistency_results = self._check_consistency(df)
            results['checks']['consistency'] = consistency_results
            if not consistency_results['valid']:
                results['valid'] = False
        
        # Business rules validation
        if self.config.data_quality.business_rules:
            business_results = self._check_business_rules(df)
            results['checks']['business_rules'] = business_results
            if not business_results['valid']:
                results['valid'] = False
        
        # Range and value validation
        range_results = self._check_ranges_and_values(df)
        results['checks']['ranges'] = range_results
        if not range_results['valid']:
            results['valid'] = False
        
        # Missing value checks
        missing_results = self._check_missing_values(df)
        results['checks']['missing_values'] = missing_results
        results['metrics']['missing_value_percentages'] = missing_results['percentages']
        
        # Duplicate detection
        if self.config.validation.duplicate_detection.enabled:
            duplicate_results = self._check_duplicates(df)
            results['checks']['duplicates'] = duplicate_results
            if not duplicate_results['valid']:
                results['valid'] = False
        
        # Overall metrics
        results['metrics']['total_rows'] = df.count()
        results['metrics']['total_columns'] = len(df.columns)
        
        if results['valid']:
            logger.info("Data validation passed")
        else:
            logger.error(f"Data validation failed with {len(results['errors'])} errors")
        
        return results
    
    def _check_completeness(self, df: DataFrame) -> Dict[str, Any]:
        """Check data completeness."""
        results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        rules = self.config.data_quality.completeness
        row_count = df.count()
        
        if row_count < rules.min_record_count:
            msg = f"Record count {row_count} is below minimum {rules.min_record_count}"
            results['errors'].append(msg)
            results['valid'] = False
            logger.error(msg)
        
        return results
    
    def _check_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Check data consistency."""
        results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        rules = self.config.data_quality.consistency
        
        # Temporal consistency
        if rules.check_temporal_consistency and 'tpep_pickup_datetime' in df.columns and 'trip_duration_min' in df.columns:
            invalid_temporal = df.filter(
                (F.col("trip_duration_min") < rules.min_trip_duration_seconds / 60.0) |
                (F.col("trip_duration_min") > rules.max_trip_duration_hours * 60.0)
            ).count()
            
            if invalid_temporal > 0:
                msg = f"Found {invalid_temporal} rows with invalid trip duration"
                results['warnings'].append(msg)
                logger.warning(msg)
        
        # Derived feature consistency - cyclical encoding
        if rules.check_derived_features:
            if 'sin_hour' in df.columns and 'cos_hour' in df.columns:
                cyclical_check = df.withColumn(
                    'cyclical_check',
                    F.abs(F.pow(F.col('sin_hour'), 2) + F.pow(F.col('cos_hour'), 2) - 1.0)
                ).filter(F.col('cyclical_check') > rules.cyclical_tolerance).count()
                
                if cyclical_check > 0:
                    msg = f"Found {cyclical_check} rows with invalid cyclical encoding (hour)"
                    results['warnings'].append(msg)
                    logger.warning(msg)
            
            if 'sin_day_of_week' in df.columns and 'cos_day_of_week' in df.columns:
                cyclical_check = df.withColumn(
                    'cyclical_check',
                    F.abs(F.pow(F.col('sin_day_of_week'), 2) + F.pow(F.col('cos_day_of_week'), 2) - 1.0)
                ).filter(F.col('cyclical_check') > rules.cyclical_tolerance).count()
                
                if cyclical_check > 0:
                    msg = f"Found {cyclical_check} rows with invalid cyclical encoding (day of week)"
                    results['warnings'].append(msg)
                    logger.warning(msg)
        
        return results
    
    def _check_business_rules(self, df: DataFrame) -> Dict[str, Any]:
        """Check business logic rules."""
        results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        rules = self.config.data_quality.business_rules
        
        # Route ID consistency
        if rules.check_route_id_consistency and 'route_id' in df.columns:
            if 'PULocationID' in df.columns and 'DOLocationID' in df.columns:
                inconsistent = df.filter(
                    F.col('route_id') != F.concat_ws('_', F.col('PULocationID'), F.col('DOLocationID'))
                ).count()
                
                if inconsistent > 0:
                    msg = f"Found {inconsistent} rows with inconsistent route_id"
                    results['warnings'].append(msg)
                    logger.warning(msg)
        
        # Tip logic
        if rules.check_tip_logic and 'tip_amount' in df.columns and 'fare_amount' in df.columns:
            invalid_tips = df.filter(
                F.col('tip_amount') > F.col('fare_amount') * rules.max_tip_multiplier
            ).count()
            
            if invalid_tips > 0:
                msg = f"Found {invalid_tips} rows with invalid tip amounts"
                results['warnings'].append(msg)
                logger.warning(msg)
        
        # Speed reasonableness
        if rules.check_speed_reasonableness:
            if 'trip_distance' in df.columns and 'trip_duration_min' in df.columns:
                invalid_speed = df.filter(
                    (F.col('trip_duration_min') > 0) &
                    (F.col('trip_distance') / (F.col('trip_duration_min') / 60.0) < rules.min_speed_mph) |
                    (F.col('trip_distance') / (F.col('trip_duration_min') / 60.0) > rules.max_speed_mph)
                ).count()
                
                if invalid_speed > 0:
                    msg = f"Found {invalid_speed} rows with unreasonable speeds"
                    results['warnings'].append(msg)
                    logger.warning(msg)
        
        return results
    
    def _check_ranges_and_values(self, df: DataFrame) -> Dict[str, Any]:
        """Check value ranges and valid values."""
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'violations': {}
        }
        
        for col_name, col_info in self.features.items():
            if col_name not in df.columns:
                continue
            
            violations = []
            
            # Check valid range
            if 'valid_range' in col_info:
                min_val, max_val = col_info['valid_range']
                out_of_range = df.filter(
                    (F.col(col_name) < min_val) | (F.col(col_name) > max_val)
                ).count()
                
                if out_of_range > 0:
                    violations.append(f"{out_of_range} values out of range [{min_val}, {max_val}]")
            
            # Check valid values
            if 'valid_values' in col_info:
                valid_vals = col_info['valid_values']
                invalid_values = df.filter(~F.col(col_name).isin(valid_vals)).count()
                
                if invalid_values > 0:
                    violations.append(f"{invalid_values} values not in {valid_vals}")
            
            if violations:
                results['violations'][col_name] = violations
                results['warnings'].append(f"Column {col_name}: {', '.join(violations)}")
                logger.warning(f"Column {col_name} has validation violations")
        
        return results
    
    def _check_missing_values(self, df: DataFrame) -> Dict[str, Any]:
        """Check missing values per column."""
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'percentages': {}
        }
        
        total_rows = df.count()
        thresholds = self.config.validation.missing_value_thresholds
        
        for col_name, col_info in self.features.items():
            if col_name not in df.columns:
                continue
            
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            results['percentages'][col_name] = null_percentage
            
            expected_nullable = col_info.get('nullable', True)
            
            # Check against thresholds
            if not expected_nullable and null_count > 0:
                msg = f"Column {col_name}: {null_count} null values found (expected non-nullable)"
                results['errors'].append(msg)
                results['valid'] = False
                logger.error(msg)
            elif null_percentage > thresholds.optional_columns:
                msg = f"Column {col_name}: {null_percentage:.2f}% null values (exceeds threshold)"
                results['warnings'].append(msg)
                logger.warning(msg)
        
        return results
    
    def _check_duplicates(self, df: DataFrame) -> Dict[str, Any]:
        """Check for duplicate rows."""
        results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        total_rows = df.count()
        duplicate_count = total_rows - df.distinct().count()
        duplicate_percentage = (duplicate_count / total_rows * 100) if total_rows > 0 else 0
        
        max_allowed = self.config.validation.duplicate_detection.max_duplicate_percentage
        
        if duplicate_percentage > max_allowed:
            msg = f"Duplicate percentage {duplicate_percentage:.2f}% exceeds threshold {max_allowed}%"
            results['errors'].append(msg)
            results['valid'] = False
            logger.error(msg)
        elif duplicate_count > 0:
            msg = f"Found {duplicate_count} duplicate rows ({duplicate_percentage:.2f}%)"
            results['warnings'].append(msg)
            logger.warning(msg)
        
        results['duplicate_count'] = duplicate_count
        results['duplicate_percentage'] = duplicate_percentage
        
        return results


class DataValidation:
    """
    Main class for data validation pipeline.
    """
    
    def __init__(
        self,
        validation_config: DataValidationConfig,
        features_config_path: Optional[Path] = None,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize DataValidation.
        
        Args:
            validation_config: DataValidationConfig object
            features_config_path: Path to features.yaml. If None, uses default.
            spark: Optional SparkSession
        """
        self.config = validation_config
        self.spark = spark
        
        # Load features config
        if features_config_path is None:
            features_config_path = project_root / "src" / "config" / "schema.yaml"
        
        with open(features_config_path, 'r') as f:
            self.features_config = yaml.safe_load(f)
        
        # Initialize validators
        self.schema_validator = SchemaValidator(
            self.features_config,
            self.config.schema_validation
        )
        self.data_validator = DataValidator(
            self.features_config,
            self.config
        )
        
        self._setup_directories()
    
    def _setup_directories(self):
        """Create necessary directories."""
        self.config.reporting.report_dir.mkdir(parents=True, exist_ok=True)
    
    def validate(self, df: DataFrame) -> Dict[str, Any]:
        """
        Perform complete validation (schema + data).
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            Dictionary with complete validation results
        """
        logger.info("Starting complete data validation...")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'schema_validation': {},
            'data_validation': {},
            'overall_valid': True
        }
        
        # Schema validation
        if self.config.validation.enable_schema_validation:
            schema_results = self.schema_validator.validate_schema(df)
            results['schema_validation'] = schema_results
            
            if not schema_results['valid'] and self.config.validation.fail_on_validation_error:
                raise ValueError(f"Schema validation failed: {schema_results['errors']}")
        
        # Data validation
        if self.config.validation.enable_data_validation:
            data_results = self.data_validator.validate_data(df)
            results['data_validation'] = data_results
            
            if not data_results['valid'] and self.config.validation.fail_on_validation_error:
                raise ValueError(f"Data validation failed: {data_results['errors']}")
        
        # Overall validation status
        results['overall_valid'] = (
            results.get('schema_validation', {}).get('valid', True) and
            results.get('data_validation', {}).get('valid', True)
        )
        
        # Generate report if enabled
        if self.config.reporting.generate_reports:
            self._generate_report(results)
        
        logger.info(f"Validation completed. Overall valid: {results['overall_valid']}")
        
        return results
    
    def _generate_report(self, results: Dict[str, Any]):
        """Generate validation report."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if 'json' in self.config.reporting.report_format or 'both' in self.config.reporting.report_format:
            json_path = self.config.reporting.report_dir / f"validation_report_{timestamp}.json"
            with open(json_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logger.info(f"Validation report saved to: {json_path}")


def load_validation_config(config_path: Optional[Path] = None) -> DataValidationConfig:
    """
    Load validation configuration from YAML file.
    
    Args:
        config_path: Path to config YAML file. If None, uses default.
        
    Returns:
        DataValidationConfig object
    """
    if config_path is None:
        config_path = project_root / "src" / "config" / "validation.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    # Import nested config classes
    from src.config import (
        ValidationSettings, MissingValueThresholds, OutlierDetection,
        DuplicateDetection, SchemaValidationRules, CompletenessRules,
        ConsistencyRules, BusinessRules, DataQualityRules, ReportingConfig
    )
    
    # Build nested config objects
    # Extract nested dicts first to avoid duplicate keyword arguments
    validation_dict = config_dict['validation'].copy()
    missing_val_data = validation_dict.pop('missing_value_thresholds')
    outlier_data = validation_dict.pop('outlier_detection')
    duplicate_data = validation_dict.pop('duplicate_detection')
    
    validation = ValidationSettings(
        **validation_dict,  # Now only contains top-level fields
        missing_value_thresholds=MissingValueThresholds(**missing_val_data),
        outlier_detection=OutlierDetection(**outlier_data),
        duplicate_detection=DuplicateDetection(**duplicate_data)
    )
    
    schema_validation = SchemaValidationRules(**config_dict['schema_validation'])
    
    data_quality = DataQualityRules(
        completeness=CompletenessRules(**config_dict['data_quality']['completeness']),
        consistency=ConsistencyRules(**config_dict['data_quality']['consistency']),
        business_rules=BusinessRules(**config_dict['data_quality']['business_rules'])
    )
    
    # Extract report_dir first to avoid duplicate keyword argument
    reporting_dict = config_dict['reporting'].copy()
    report_dir_str = reporting_dict.pop('report_dir')
    
    reporting = ReportingConfig(
        **reporting_dict,  # Now only contains top-level fields
        report_dir=project_root / report_dir_str
    )
    
    return DataValidationConfig(
        validation=validation,
        schema_validation=schema_validation,
        data_quality=data_quality,
        reporting=reporting
    )


def main():
    """Main function for standalone validation."""
    # This would typically be called from within a pipeline
    logger.info("Data validation module loaded. Use DataValidation class to validate DataFrames.")


if __name__ == "__main__":
    main()
