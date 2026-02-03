"""
Data Transformation Module

This module combines data preprocessing and feature engineering.
Refactored to use base class architecture and eliminate code duplication.

Lazy imports are used to avoid PySpark dependencies at module load time.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any

# Only import non-PySpark dependent modules at module level
# data_transformation.py uses lazy imports internally, so it's safe
from src.data_transformation.data_transformation import (
    DataTransformation,
    create_preprocessing_pipeline,
    create_feature_engineering_pipeline,
    create_full_pipeline
)

if TYPE_CHECKING:
    # These are only for type checking, not runtime
    from src.data_transformation.preprocessing import (
        DataPreprocessing,
        DataCleaningPipeline
    )
    from src.data_transformation.feature_engineering import (
        FeatureEngineering,
        FeatureEngineeringPipeline
    )
    from src.data_transformation.base_stage import BaseDataTransformationStage

__all__ = [
    # Main orchestration
    'DataTransformation',
    # Convenience factory functions
    'create_preprocessing_pipeline',
    'create_feature_engineering_pipeline',
    'create_full_pipeline',
    # Individual stages (lazy loaded)
    'DataPreprocessing',
    'FeatureEngineering',
    # Pipeline classes (lazy loaded)
    'DataCleaningPipeline',
    'FeatureEngineeringPipeline',
    # Base class (lazy loaded)
    'BaseDataTransformationStage',
]


def __getattr__(name: str) -> Any:
    """
    Lazy load PySpark-dependent modules.

    This allows the module to be imported without PySpark installed,
    as long as the PySpark-dependent classes aren't actually used.
    """
    if name == 'DataPreprocessing' or name == 'DataCleaningPipeline':
        from src.data_transformation.preprocessing import DataPreprocessing, DataCleaningPipeline
        return DataPreprocessing if name == 'DataPreprocessing' else DataCleaningPipeline

    elif name == 'FeatureEngineering' or name == 'FeatureEngineeringPipeline':
        from src.data_transformation.feature_engineering import FeatureEngineering, FeatureEngineeringPipeline
        return FeatureEngineering if name == 'FeatureEngineering' else FeatureEngineeringPipeline

    elif name == 'BaseDataTransformationStage':
        from src.data_transformation.base_stage import BaseDataTransformationStage
        return BaseDataTransformationStage

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
