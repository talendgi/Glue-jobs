# MySQL to Snowflake Incremental Sync - AWS Glue Job

Complete AWS Glue job for synchronizing data from MySQL to Snowflake with automatic schema management and incremental loading.

## Features

✅ **Automatic Schema Management**
- Creates Snowflake tables if they don't exist
- Detects and adds new columns automatically
- Alerts on data type mismatches

✅ **Data Integrity**
- Supports both full and incremental loads
- MERGE/UPSERT support with primary keys
- Handles duplicate records intelligently
- Type-safe conversions from MySQL to Snowflake

✅ **Production-Ready**
- Comprehensive error handling
- Detailed logging with emojis for easy monitoring
- Transaction management with job commit

## Prerequisites

### 1. AWS Glue Setup
- AWS Glue job with Python 3.9+ runtime
- IAM role with permissions:
  - S3 read/write access
  - RDS/MySQL connectivity
  - CloudWatch Logs access

### 2. Network Configuration
- VPC configuration with RDS/MySQL access
- Snowflake IP whitelisting (if required)
- Security groups allowing outbound HTTPS (443) for Snowflake

### 3. Required Libraries
Add these JAR files to your Glue job's "Dependent JARs path":
```
s3://your-bucket/jars/mysql-connector-java-8.0.33.jar
s3://your-bucket/jars/snowflake-jdbc-3.13.30.jar
s3://your-bucket/jars/spark-snowflake_2.12-2.11.0.jar
```

Download links:
- MySQL Connector: https://dev.mysql.com/downloads/connector/j/
- Snowflake JDBC: https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/
- Spark Snowflake: https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/

## Configuration

### Step 1: Update Configuration Variables

Edit the configuration section in `mysql_to_snowflake_incremental.py`:

```python
# MySQL Configuration
MYSQL_HOST = "mydb.abc123.us-east-1.rds.amazonaws.com"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "production"
MYSQL_USER = "etl_user"
MYSQL_PASSWORD = "SecurePassword123!"
MYSQL_TABLE = "orders"

# Snowflake Configuration
SNOWFLAKE_ACCOUNT = "xy12345.us-east-1"
SNOWFLAKE_USER = "ETL_USER"
SNOWFLAKE_PASSWORD = "SnowPass123!"
SNOWFLAKE_DATABASE = "ANALYTICS"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "ETL_WH"
SNOWFLAKE_TABLE = "ORDERS"

# Incremental Configuration
INCREMENTAL_COLUMN = "updated_at"  # Your timestamp column
PRIMARY_KEY = "order_id"           # Your primary key
LOAD_TYPE = "incremental"          # or "full"
```

### Step 2: Store Secrets (Recommended)

For production, use AWS Secrets Manager instead of hardcoded credentials:

```python
import boto3
import json

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Load credentials
mysql_creds = get_secret('prod/mysql/credentials')
snowflake_creds = get_secret('prod/snowflake/credentials')

MYSQL_USER = mysql_creds['username']
MYSQL_PASSWORD = mysql_creds['password']
SNOWFLAKE_USER = snowflake_creds['username']
SNOWFLAKE_PASSWORD = snowflake_creds['password']
```

## How It Works

### Workflow Overview

```
┌─────────────────┐
│  1. Read MySQL  │
│  (Incremental)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. Get Schemas  │
│  MySQL + SF     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. Compare      │
│  Schemas        │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐ ┌──────────┐
│ CREATE │ │  ALTER   │
│ TABLE  │ │  TABLE   │
└───┬────┘ └────┬─────┘
    │           │
    └─────┬─────┘
          │
          ▼
    ┌──────────┐
    │  4. Load │
    │   Data   │
    └─────┬────┘
          │
          ▼
    ┌──────────┐
    │ 5. Update│
    │ Watermark│
    └──────────┘
```

### Schema Comparison Logic

1. **New Table**: Creates table with full schema from MySQL
2. **Existing Table**:
   - Detects new columns → Runs ALTER TABLE ADD COLUMN
   - Detects type changes → Logs warning (manual fix required)
   - No changes → Proceeds to data load

### Incremental Loading

The job tracks the last loaded record using a **watermark**:


## Load Modes

### 1. Incremental Load (Recommended)

```python
LOAD_TYPE = "incremental"
INCREMENTAL_COLUMN = "updated_at"
PRIMARY_KEY = "order_id"
```

**Best for:**
- Large tables with frequent updates
- Tracking changes over time
- Minimizing data transfer

**Requirements:**
- Table must have a timestamp or auto-increment column
- Column must be indexed in MySQL for performance

### 2. Full Load

```python
LOAD_TYPE = "full"
INCREMENTAL_COLUMN = None
PRIMARY_KEY = None
```

**Best for:**
- Small dimension tables
- Initial data loads
- Tables without tracking columns




