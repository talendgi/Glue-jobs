# ============================================================================
# AWS Glue Job Configuration Template
# Copy this file and update with your actual values
# ============================================================================

# MySQL Configuration
MYSQL_HOST = "your-mysql-host.rds.amazonaws.com"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "production_db"
MYSQL_USER = "glue_user"
MYSQL_PASSWORD = "your_secure_password"
MYSQL_TABLE = "trip_data"

# Snowflake Configuration
SNOWFLAKE_ACCOUNT = "abc12345.us-east-1"  # Format: account.region
SNOWFLAKE_USER = "etl_user"
SNOWFLAKE_PASSWORD = "your_snowflake_password"
SNOWFLAKE_DATABASE = "ANALYTICS_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "ETL_WH"
SNOWFLAKE_TABLE = "TRIP_DATA"

# Incremental Load Settings
INCREMENTAL_COLUMN = "updated_at"  # Timestamp column for tracking changes
PRIMARY_KEY = "trip_id"            # Primary key for MERGE operations
LOAD_TYPE = "incremental"          # Options: "full" or "incremental"

# S3 Configuration
S3_WATERMARK_PATH = "s3://my-etl-bucket/glue-watermarks/"

# ============================================================================
# Advanced Configuration (Optional)
# ============================================================================

# Partition settings for large tables
PARTITION_COLUMN = None  # e.g., "date" for partitioned reads
NUM_PARTITIONS = 10      # Number of parallel partitions

# Performance tuning
BATCH_SIZE = 10000       # Records per batch for MERGE
QUERY_TIMEOUT = 3600     # Seconds

# Snowflake options
SNOWFLAKE_STAGING_S3 = "s3://my-staging-bucket/snowflake-stage/"
USE_S3_STAGING = True    # Use S3 for faster bulk loads