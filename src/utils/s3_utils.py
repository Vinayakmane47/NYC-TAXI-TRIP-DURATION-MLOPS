"""
S3/MinIO utility functions for data lake operations.

This module provides a client wrapper for S3/MinIO operations
and utilities for managing data lake buckets and objects.
"""

import logging
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pathlib import Path
from typing import Optional, List, Tuple

from src.config import S3Config
from src.exceptions import S3StorageError

logger = logging.getLogger(__name__)


class S3Client:
    """S3/MinIO client wrapper for data lake operations."""
    
    def __init__(self, config: S3Config):
        """
        Initialize S3 client.
        
        Args:
            config: S3Config object with connection details
        """
        self.config = config
        self.client = self._create_client()
        self.resource = self._create_resource()
    
    def _create_client(self):
        """Create boto3 S3 client."""
        return boto3.client(
            's3',
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            region_name=self.config.region,
            config=Config(
                signature_version='s3v4',
                s3={
                    'addressing_style': 'path' if self.config.path_style_access else 'auto'
                }
            ),
            use_ssl=self.config.use_ssl
        )
    
    def _create_resource(self):
        """Create boto3 S3 resource."""
        return boto3.resource(
            's3',
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            region_name=self.config.region,
            use_ssl=self.config.use_ssl
        )
    
    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """
        Ensure bucket exists, create if it doesn't.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            True if bucket exists or was created successfully
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' already exists")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    self.client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Created bucket '{bucket_name}'")
                    return True
                except ClientError as create_error:
                    logger.error(f"Failed to create bucket '{bucket_name}': {create_error}")
                    return False
            else:
                logger.error(f"Error checking bucket '{bucket_name}': {e}")
                return False
    
    def upload_file(self, local_path: Path, bucket: str, s3_key: str) -> bool:
        """
        Upload file to S3/MinIO.
        
        Args:
            local_path: Local file path
            bucket: S3 bucket name
            s3_key: S3 object key (path)
            
        Returns:
            True if upload successful
        """
        try:
            self.client.upload_file(
                str(local_path),
                bucket,
                s3_key
            )
            logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to s3://{bucket}/{s3_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading file: {e}")
            return False
    
    def download_file(self, bucket: str, s3_key: str, local_path: Path) -> bool:
        """
        Download file from S3/MinIO.
        
        Args:
            bucket: S3 bucket name
            s3_key: S3 object key (path)
            local_path: Local file path to save to
            
        Returns:
            True if download successful
        """
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.client.download_file(bucket, s3_key, str(local_path))
            logger.info(f"Downloaded s3://{bucket}/{s3_key} to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download s3://{bucket}/{s3_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading file: {e}")
            return False
    
    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """
        List objects in bucket with given prefix.
        
        Args:
            bucket: S3 bucket name
            prefix: Object key prefix
            
        Returns:
            List of object keys
        """
        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except ClientError as e:
            logger.error(f"Failed to list objects in bucket '{bucket}': {e}")
            return []
    
    def object_exists(self, bucket: str, s3_key: str) -> bool:
        """
        Check if object exists in bucket.
        
        Args:
            bucket: S3 bucket name
            s3_key: S3 object key
            
        Returns:
            True if object exists
        """
        try:
            self.client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False
            logger.error(f"Error checking object existence: {e}")
            return False
    
    def delete_object(self, bucket: str, s3_key: str) -> bool:
        """
        Delete object from bucket.

        Args:
            bucket: S3 bucket name
            s3_key: S3 object key

        Returns:
            True if deletion successful
        """
        try:
            self.client.delete_object(Bucket=bucket, Key=s3_key)
            logger.info(f"Deleted s3://{bucket}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete s3://{bucket}/{s3_key}: {e}")
            return False

    def get_s3_uri(self, bucket: str, key: str) -> str:
        """
        Construct S3 URI from bucket and key.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            S3 URI string (s3a://bucket/key)
        """
        return f"s3a://{bucket}/{key}"

    def ensure_buckets_exist(self, *bucket_names: str) -> bool:
        """
        Ensure multiple buckets exist, create if they don't.

        Args:
            *bucket_names: Variable number of bucket names

        Returns:
            True if all buckets exist or were created successfully
        """
        all_success = True
        for bucket_name in bucket_names:
            if not self.ensure_bucket_exists(bucket_name):
                all_success = False
        return all_success


class DataLakeManager:
    """
    Manager for multi-bucket data lake operations.

    Provides high-level operations for managing bronze, silver, and gold layers.
    """

    def __init__(self, s3_client: S3Client):
        """
        Initialize DataLakeManager.

        Args:
            s3_client: Configured S3Client instance
        """
        self.s3_client = s3_client

    def setup_data_lake(
        self,
        bronze_bucket: Optional[str] = None,
        silver_bucket: Optional[str] = None,
        gold_bucket: Optional[str] = None
    ) -> Tuple[bool, List[str]]:
        """
        Setup data lake buckets (bronze, silver, gold).

        Args:
            bronze_bucket: Bronze layer bucket name
            silver_bucket: Silver layer bucket name
            gold_bucket: Gold layer bucket name

        Returns:
            Tuple of (success, list of created/verified buckets)
        """
        buckets_to_create = []
        if bronze_bucket:
            buckets_to_create.append(bronze_bucket)
        if silver_bucket:
            buckets_to_create.append(silver_bucket)
        if gold_bucket:
            buckets_to_create.append(gold_bucket)

        if not buckets_to_create:
            logger.warning("No buckets specified for data lake setup")
            return False, []

        success = self.s3_client.ensure_buckets_exist(*buckets_to_create)
        return success, buckets_to_create

    def get_layer_path(self, bucket: str, year: int, month: int, filename: str) -> str:
        """
        Construct organized path for data lake layer.

        Args:
            bucket: Bucket name
            year: Year (e.g., 2023)
            month: Month (1-12)
            filename: File name

        Returns:
            Full S3 URI path
        """
        key = f"{year}/{month:02d}/{filename}"
        return self.s3_client.get_s3_uri(bucket, key)
