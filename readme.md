# MySQL to Snowflake Incremental Sync - AWS Glue Job

Complete AWS Glue job for synchronizing data from MySQL to Snowflake with automatic schema management and incremental loading.

## Features

✅ **Automatic Schema Management**
- Creates Snowflake tables if they don't exist
- Detects and adds new columns automatically
- Alerts on data type mismatches

✅ **Incremental Loading**
- Watermark-based incremental sync
- Stores watermarks in S3 for resume capability
- Supports both full and incremental loads

✅ **Data Integrity**
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
S3_WATERMARK_PATH = "s3://my-bucket/watermarks/"
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

```sql
-- First run: Load all data
SELECT * FROM orders

-- Subsequent runs: Load only new/updated records
SELECT * FROM orders 
WHERE updated_at > '2024-01-15 10:30:00'
```

Watermark is stored in S3: `s3://bucket/watermarks/jobname_tablename_watermark.txt`

## Usage

### Running the Glue Job

**Via AWS Console:**
1. Go to AWS Glue → Jobs
2. Select your job
3. Click "Run job"
4. Monitor in CloudWatch Logs

**Via AWS CLI:**
```bash
aws glue start-job-run \
  --job-name mysql-to-snowflake-sync \
  --region us-east-1
```

**Via Python (boto3):**
```python
import boto3

glue = boto3.client('glue')
response = glue.start_job_run(JobName='mysql-to-snowflake-sync')
print(f"Job started: {response['JobRunId']}")
```

### Monitoring

Check CloudWatch Logs for detailed execution steps:

```
🚀 Starting MySQL to Snowflake Sync Job
📥 Reading data from MySQL: orders
  📌 Incremental filter: updated_at > '2024-01-15 10:30:00'
  ✅ Records fetched: 1,234
📋 MySQL Schema:
  root
   |-- order_id: string (nullable = false)
   |-- customer_id: integer (nullable = true)
   |-- amount: double (nullable = true)
❄️  Fetching Snowflake schema for: ORDERS
  ✅ Table exists with 3 columns
🔍 Comparing schemas...
  ➕ New column: SHIPPING_ADDRESS (StringType)
🔧 Altering Snowflake table: ORDERS
  📝 ALTER TABLE ORDERS ADD COLUMN SHIPPING_ADDRESS VARCHAR(16777216)
  ✅ Column SHIPPING_ADDRESS added
📤 Loading data to Snowflake: ORDERS
  📊 Records to load: 1,234
  🔄 Mode: append
  ✅ Data loaded successfully
  💾 Watermark saved: 2024-01-15 15:45:30
🎉 Job completed successfully!
```

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

**Note:** Uses `mode="overwrite"` - replaces entire table each run

## Troubleshooting

### Issue: "Table does not exist" on first run
**Solution:** This is normal. The job will create the table automatically.

### Issue: Connection timeout to MySQL
**Solution:** 
- Check VPC/Security Group settings
- Verify Glue job is in same VPC as RDS
- Test connection with `mysql` CLI from Glue environment

### Issue: Snowflake authentication failed
**Solution:**
- Verify account identifier format: `account.region`
- Check user has required privileges: `CREATE TABLE`, `INSERT`, `SELECT`
- Test with SQL client first

### Issue: Type mismatch warnings
**Solution:**
- Review logged mismatches
- Manually alter Snowflake column types if needed:
  ```sql
  ALTER TABLE ORDERS ALTER COLUMN amount SET DATA TYPE DECIMAL(10,2);
  ```

### Issue: No new data but watermark not updating
**Solution:**
- Check if `INCREMENTAL_COLUMN` has NULL values
- Verify MySQL table has data with timestamps > last watermark

## Performance Optimization

### For Large Tables (Millions of rows)

**1. Use Pushdown Predicates:**
```python
# Instead of loading all data then filtering
query = f"""
(SELECT * FROM {table_name} 
 WHERE {incremental_col} > '{watermark}'
 AND date_partition = '2024-01-15'
) as filtered_data
"""
```

**2. Enable Partitioning:**
```python
df = spark.read.jdbc(
    url=MYSQL_JDBC_URL,
    table=query,
    properties=jdbc_properties,
    numPartitions=10,  # Parallel reads
    partitionColumn="id",
    lowerBound=1,
    upperBound=1000000
)
```

**3. Use S3 Staging for Snowflake:**
```python
SNOWFLAKE_OPTIONS["sfURL"] = "..."
SNOWFLAKE_OPTIONS["tempDir"] = "s3://bucket/snowflake-temp/"
SNOWFLAKE_OPTIONS["use_s3_staging_mode"] = "ON"
```

## Schema Change Scenarios

| Scenario | Action | Manual Required? |
|----------|--------|------------------|
| New column in MySQL | ALTER TABLE (auto) | No |
| Column renamed | Treated as new column | Yes - drop old column |
| Data type changed | Warning logged | Yes - alter manually |
| Column dropped | No action | Yes - drop in Snowflake |
| Primary key changed | Update config | Yes - update PRIMARY_KEY |

## Best Practices

1. **Schedule During Off-Peak Hours**
   - Set up triggers based on EventBridge/CloudWatch Events
   - Run incrementals every 15-60 minutes
   - Run full refresh weekly/monthly

2. **Monitor Watermark Progress**
   - Check S3 watermark files regularly
   - Alert on stale watermarks (no updates in 24h)

3. **Test Schema Changes**
   - Test in dev environment first
   - Review ALTER statements before production
   - Keep schema changes backward compatible

4. **Security**
   - Use AWS Secrets Manager for credentials
   - Enable VPC endpoints for S3/Secrets Manager
   - Rotate passwords regularly

5. **Data Quality**
   - Add data validation before load
   - Log row counts (source vs target)
   - Set up reconciliation jobs

## Example: Multi-Table Sync

To sync multiple tables, create a wrapper script:

```python
tables = [
    {"mysql": "orders", "snowflake": "ORDERS", "pk": "order_id"},
    {"mysql": "customers", "snowflake": "CUSTOMERS", "pk": "customer_id"},
    {"mysql": "products", "snowflake": "PRODUCTS", "pk": "product_id"}
]

for table in tables:
    MYSQL_TABLE = table["mysql"]
    SNOWFLAKE_TABLE = table["snowflake"]
    PRIMARY_KEY = table["pk"]
    
    # Run sync logic...
```
