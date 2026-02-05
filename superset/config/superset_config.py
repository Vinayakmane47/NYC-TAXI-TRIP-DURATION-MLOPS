"""
Superset Configuration for NYC Taxi Trip Duration MLOps Project
"""
import os
from pathlib import Path

# Superset metadata database
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DATABASE_URI",
    "postgresql://superset:superset@superset-postgres:5432/superset"
)

# Redis configuration for caching
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "ENABLE_EXPLORE_JSON_CSRF_PROTECTION": False,
}

# Security
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY", "your-secret-key-change-in-production"
)

# Language
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
}

# Timezone
DEFAULT_TIMEZONE = "America/New_York"

# Row limit
ROW_LIMIT = 50000
SQL_MAX_ROW = 50000

# Enable CORS if needed
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["*"],
}

# Enable proxy fix for reverse proxy setups
ENABLE_PROXY_FIX = True

# Logging
LOG_LEVEL = "INFO"

# Data source connections will be configured via Superset UI
# Spark SQL connection details:
# - Database Name: Spark SQL
# - SQLAlchemy URI: hive://spark-thrift-server:10000/nyc_taxi
# - Or use JDBC: jdbc:hive2://spark-thrift-server:10000/nyc_taxi
