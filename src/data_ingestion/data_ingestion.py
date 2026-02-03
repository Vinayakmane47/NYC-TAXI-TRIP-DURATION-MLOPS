"""
Data Ingestion Module for NYC Yellow Taxi Trip Data

This module downloads Yellow taxi trip records for a specified year
and organizes them in raw/bronze folder structure by date.
"""

import os
import sys
import logging
import requests
from pathlib import Path
from datetime import datetime
from typing import Optional, List
import yaml
from urllib.parse import urljoin
import time

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import DataIngestionConfig, S3Config
from src.logger.logging import get_logger
from src.utils.s3_utils import S3Client

# Configure logger
# Logger configured automatically
logger = logging.getLogger(__name__)


class DataIngestion:
    """
    Handles downloading and organizing NYC Yellow Taxi trip data.
    """
    
    def __init__(self, config: DataIngestionConfig):
        """
        Initialize DataIngestion with configuration.
        
        Args:
            config: DataIngestionConfig object containing all configuration parameters
        """
        self.config = config
        self._setup_directories()
        
        # Initialize S3 client if configured
        self.s3_client = None
        if self.config.use_s3 and self.config.s3 and self.config.bronze_bucket:
            self.s3_client = S3Client(self.config.s3)
            # Ensure bucket exists
            self.s3_client.ensure_bucket_exists(self.config.bronze_bucket)
            logger.info(f"S3/MinIO client initialized for bucket: {self.config.bronze_bucket}")
        
    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        self.config.root_dir.mkdir(parents=True, exist_ok=True)
        self.config.raw_bronze_dir.mkdir(parents=True, exist_ok=True)
        self.config.download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Directories created/verified: {self.config.root_dir}")
        
    def _get_file_url(self, year: int, month: int) -> str:
        """
        Construct the URL for downloading a specific month's data.
        
        Args:
            year: Year (e.g., 2025)
            month: Month (1-12)
            
        Returns:
            Complete URL for the data file
        """
        file_name = self.config.file_name_pattern.format(year=year, month=month)
        url = urljoin(self.config.api_base_url + "/", file_name)
        return url
    
    def _download_file(self, url: str, destination: Path, retry_count: int = 0) -> bool:
        """
        Download a file from URL with retry logic.
        
        Args:
            url: URL to download from
            destination: Path where file should be saved
            retry_count: Current retry attempt number
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            logger.info(f"Downloading from: {url}")
            logger.info(f"Destination: {destination}")
            
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Check if file exists (some months might not have data yet)
            if response.status_code == 404:
                logger.warning(f"File not found at {url} (may not be available yet)")
                return False
            
            # Get file size for validation
            total_size = int(response.headers.get('content-length', 0))
            min_size_bytes = self.config.min_file_size_mb * 1024 * 1024
            
            if self.config.validate_download and total_size > 0 and total_size < min_size_bytes:
                logger.warning(f"File size ({total_size / 1024 / 1024:.2f} MB) is smaller than expected minimum ({self.config.min_file_size_mb} MB)")
            
            # Download file in chunks
            downloaded_size = 0
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.config.chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
            
            # Validate downloaded file
            if self.config.validate_download:
                actual_size = destination.stat().st_size
                if actual_size < min_size_bytes:
                    logger.warning(f"Downloaded file size ({actual_size / 1024 / 1024:.2f} MB) is smaller than expected")
                if total_size > 0 and actual_size != total_size:
                    logger.warning(f"Size mismatch: expected {total_size}, got {actual_size}")
            
            logger.info(f"Successfully downloaded: {destination.name} ({downloaded_size / 1024 / 1024:.2f} MB)")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            if retry_count < self.config.max_retries:
                logger.info(f"Retrying in {self.config.retry_delay} seconds... (Attempt {retry_count + 1}/{self.config.max_retries})")
                time.sleep(self.config.retry_delay)
                return self._download_file(url, destination, retry_count + 1)
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {str(e)}")
            return False
    
    def _organize_by_date(self, source_file: Path, year: int, month: int) -> Path:
        """
        Organize downloaded file into raw/bronze folder structure by date.
        Structure: raw/bronze/YYYY/MM/filename.parquet
        
        Args:
            source_file: Path to the downloaded file
            year: Year of the data
            month: Month of the data
            
        Returns:
            Path to the organized file location
        """
        # Create date-based directory structure: raw/bronze/YYYY/MM/
        date_dir = self.config.raw_bronze_dir / str(year) / f"{month:02d}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Move file to organized location
        destination = date_dir / source_file.name
        
        if destination.exists():
            logger.info(f"File already exists at {destination}, skipping move")
            # Remove source file if it's a duplicate
            if source_file.exists() and source_file != destination:
                source_file.unlink()
            return destination
        
        # Move file
        source_file.rename(destination)
        logger.info(f"Organized file: {source_file.name} -> {destination}")
        
        return destination
    
    def ingest_data(self, year: Optional[int] = None) -> List[Path]:
        """
        Download and organize data for the specified year.
        
        Args:
            year: Year to download data for (defaults to config year)
            
        Returns:
            List of paths to successfully downloaded and organized files
        """
        year = year or self.config.year
        logger.info(f"Starting data ingestion for year {year}")
        
        downloaded_files = []
        
        # Download data for each month (1-12)
        for month in range(1, 13):
            logger.info(f"Processing month {month:02d}/{year}")
            
            # Construct URL
            url = self._get_file_url(year, month)
            
            # Download to temporary location first
            file_name = self.config.file_name_pattern.format(year=year, month=month)
            temp_file = self.config.download_dir / file_name
            
            # Download file
            success = self._download_file(url, temp_file)
            
            if success and temp_file.exists():
                # Organize by date (local storage)
                organized_path = self._organize_by_date(temp_file, year, month)
                
                # Upload to S3/MinIO if configured
                if self.s3_client and self.config.bronze_bucket:
                    s3_key = f"bronze/{year}/{month:02d}/{file_name}"
                    upload_success = self.s3_client.upload_file(
                        organized_path,
                        self.config.bronze_bucket,
                        s3_key
                    )
                    if upload_success:
                        logger.info(f"Uploaded to S3: s3://{self.config.bronze_bucket}/{s3_key}")
                    else:
                        logger.warning(f"Failed to upload to S3: {s3_key}")
                
                downloaded_files.append(organized_path)
                logger.info(f"Successfully processed {file_name}")
            else:
                logger.warning(f"Failed to download or file not available: {file_name}")
                # Clean up failed download
                if temp_file.exists():
                    temp_file.unlink()
            
            # Small delay between downloads to be respectful to the server
            time.sleep(1)
        
        logger.info(f"Data ingestion completed. Successfully downloaded {len(downloaded_files)} files.")
        return downloaded_files


def load_config(config_path: Optional[Path] = None) -> DataIngestionConfig:
    """
    Load configuration from YAML file and create DataIngestionConfig object.
    
    Args:
        config_path: Path to config YAML file. If None, uses default location.
        
    Returns:
        DataIngestionConfig object
    """
    if config_path is None:
        config_path = project_root / "src" / "config" / "ingestion.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    # Convert string paths to Path objects
    root_dir = project_root / config_dict['root_dir']
    
    # Load S3 config if present
    s3_config = None
    if config_dict.get('use_s3', False) and 's3' in config_dict:
        s3_dict = config_dict['s3']
        s3_config = S3Config(
            endpoint_url=s3_dict['endpoint_url'],
            access_key_id=s3_dict['access_key_id'],
            secret_access_key=s3_dict['secret_access_key'],
            region=s3_dict.get('region', 'us-east-1'),
            use_ssl=s3_dict.get('use_ssl', False),
            path_style_access=s3_dict.get('path_style_access', True)
        )
    
    return DataIngestionConfig(
        root_dir=root_dir,
        api_base_url=config_dict['api_base_url'],
        file_name_pattern=config_dict['file_name_pattern'],
        year=config_dict['year'],
        raw_bronze_dir=root_dir / config_dict['raw_bronze_dir'],
        download_dir=root_dir / config_dict['download_dir'],
        chunk_size=config_dict['chunk_size'],
        max_retries=config_dict['max_retries'],
        retry_delay=config_dict['retry_delay'],
        validate_download=config_dict['validate_download'],
        min_file_size_mb=config_dict['min_file_size_mb'],
        s3=s3_config,
        bronze_bucket=config_dict.get('bronze_bucket'),
        use_s3=config_dict.get('use_s3', False)
    )


def main():
    """Main function to run data ingestion."""
    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded successfully")
        
        # Initialize data ingestion
        data_ingestion = DataIngestion(config)
        
        # Run ingestion
        downloaded_files = data_ingestion.ingest_data()
        
        logger.info(f"Data ingestion completed. Files downloaded: {len(downloaded_files)}")
        for file_path in downloaded_files:
            logger.info(f"  - {file_path}")
            
    except Exception as e:
        logger.error(f"Error in data ingestion: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
