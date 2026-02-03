"""
Feast Feature Store Integration for NYC Taxi Trip Duration Project
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime

from feast import FeatureStore
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.logger.logging import get_logger

# Logger configured automatically
logger = logging.getLogger(__name__)


class FeastFeatureStore:
    """Wrapper for Feast feature store operations."""
    
    def __init__(self, feature_store_path: Optional[Path] = None):
        """
        Initialize Feast feature store.
        
        Args:
            feature_store_path: Path to feature_store.yaml. If None, uses default.
        """
        if feature_store_path is None:
            project_root = Path(__file__).parent.parent.parent
            feature_store_path = project_root / "feature_store" / "feature_store.yaml"
        
        self.feature_store_path = feature_store_path
        self.store = FeatureStore(repo_path=str(feature_store_path.parent))
        logger.info(f"Feast feature store initialized from: {feature_store_path}")
    
    def materialize_features(
        self,
        df: DataFrame,
        feature_view_name: str = "trip_features",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ):
        """
        Materialize features from Spark DataFrame to Feast online store.
        
        Args:
            df: Spark DataFrame with features
            feature_view_name: Name of the feature view
            start_date: Start date for materialization
            end_date: End date for materialization
        """
        logger.info(f"Materializing features for {feature_view_name}...")
        
        # Convert Spark DataFrame to Pandas for Feast
        # Note: For large datasets, consider batch materialization
        pandas_df = df.toPandas()
        
        # Ensure required columns exist
        required_cols = ['trip_id', 'location_pair', 'tpep_pickup_datetime']
        missing_cols = [col for col in required_cols if col not in pandas_df.columns]
        if missing_cols:
            logger.warning(f"Missing required columns: {missing_cols}. Creating them...")
            if 'trip_id' not in pandas_df.columns:
                pandas_df['trip_id'] = pandas_df.index
            if 'location_pair' not in pandas_df.columns:
                pandas_df['location_pair'] = (
                    pandas_df['PULocationID'].astype(str) + '_' + 
                    pandas_df['DOLocationID'].astype(str)
                )
            if 'tpep_pickup_datetime' not in pandas_df.columns:
                pandas_df['tpep_pickup_datetime'] = datetime.now()
        
        # Add created_at timestamp
        if 'created_at' not in pandas_df.columns:
            pandas_df['created_at'] = datetime.now()
        
        # Materialize features
        try:
            if start_date and end_date:
                self.store.materialize_incremental(
                    feature_views=[feature_view_name],
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                # Full materialization (for initial load)
                self.store.materialize(
                    feature_views=[feature_view_name],
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime.now()
                )
            logger.info(f"Successfully materialized features for {feature_view_name}")
        except Exception as e:
            logger.error(f"Error materializing features: {e}")
            raise
    
    def get_online_features(
        self,
        entity_rows: List[Dict[str, Any]],
        feature_refs: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Retrieve features from online store.
        
        Args:
            entity_rows: List of entity dictionaries (e.g., [{"trip_id": 123}])
            feature_refs: List of feature references. If None, returns all features.
            
        Returns:
            Dictionary with feature values
        """
        try:
            features = self.store.get_online_features(
                features=feature_refs or [],
                entity_rows=entity_rows
            )
            return features.to_dict()
        except Exception as e:
            logger.error(f"Error retrieving online features: {e}")
            raise
    
    def update_offline_store_path(self, s3_path: str):
        """
        Update offline store path in feature store configuration.
        
        Args:
            s3_path: S3 path to gold layer data
        """
        logger.info(f"Updating offline store path to: {s3_path}")
        # This would require updating the feature_store.yaml file
        # For now, we'll log it - actual implementation would modify the YAML
        logger.warning("Offline store path update requires manual configuration in feature_store.yaml")


def prepare_dataframe_for_feast(df: DataFrame) -> DataFrame:
    """
    Prepare Spark DataFrame for Feast materialization.
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        Prepared DataFrame with required columns
    """
    logger.info("Preparing DataFrame for Feast materialization...")
    
    # Add trip_id if not present
    if 'trip_id' not in df.columns:
        df = df.withColumn("trip_id", F.monotonically_increasing_id())
    
    # Add location_pair if not present
    if 'location_pair' not in df.columns and 'PULocationID' in df.columns and 'DOLocationID' in df.columns:
        df = df.withColumn(
            "location_pair",
            F.concat_ws("_", F.col("PULocationID"), F.col("DOLocationID"))
        )
    
    # Ensure timestamp column exists
    if 'tpep_pickup_datetime' not in df.columns:
        df = df.withColumn("tpep_pickup_datetime", F.current_timestamp())
    
    # Add created_at timestamp
    if 'created_at' not in df.columns:
        df = df.withColumn("created_at", F.current_timestamp())
    
    logger.info("DataFrame prepared for Feast")
    return df
