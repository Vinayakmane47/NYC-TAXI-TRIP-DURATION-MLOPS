"""
Custom exception classes for the NYC Taxi Trip Duration MLOps project.

This module provides a hierarchy of exceptions for better error handling
and debugging throughout the pipeline.
"""


class NYCTaxiMLOpsException(Exception):
    """Base exception class for all NYC Taxi MLOps errors."""
    pass


class ConfigurationError(NYCTaxiMLOpsException):
    """Raised when there's an error in configuration loading or validation."""
    pass


class DataIngestionError(NYCTaxiMLOpsException):
    """Raised when data ingestion fails."""
    pass


class DataValidationError(NYCTaxiMLOpsException):
    """Raised when data validation fails."""
    pass


class DataTransformationError(NYCTaxiMLOpsException):
    """Raised when data transformation (preprocessing or feature engineering) fails."""
    pass


class SparkSessionError(NYCTaxiMLOpsException):
    """Raised when SparkSession creation or management fails."""
    pass


class S3StorageError(NYCTaxiMLOpsException):
    """Raised when S3/MinIO storage operations fail."""
    pass


class FeatureStoreError(NYCTaxiMLOpsException):
    """Raised when feature store operations fail."""
    pass


class ModelError(NYCTaxiMLOpsException):
    """Raised when model building, evaluation, or registration fails."""
    pass
