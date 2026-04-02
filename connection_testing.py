import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ============================================================================
# CONFIGURATION
# ============================================================================

# MySQL Configuration
MYSQL_HOST = "host.docker.internal"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "its_mtd"
MYSQL_USER = "root"
MYSQL_PASSWORD = "Andorokta!321"


# Snowflake Configuration
SNOWFLAKE_ACCOUNT = "LOTAVEC-ZE89390"
SNOWFLAKE_USER = "logeshits"
SNOWFLAKE_PASSWORD = "Andoroktaits321"
SNOWFLAKE_DATABASE = "ITS"
SNOWFLAKE_SCHEMA = "WORKSPACE"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

# ============================================================================
# INITIALIZE
# ============================================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("=" * 60)
print("🔌 CONNECTION TEST")
print("=" * 60)

# ============================================================================
# TEST MYSQL CONNECTION
# ============================================================================
print("\n📊 Testing MySQL Connection...")

try:
    mysql_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    
    mysql_props = {
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    # Simple test query
    df = spark.read.jdbc(
        url=mysql_url,
        table="(SELECT * from its.trip_data LIMIT 10) as tmp",
        properties=mysql_props
    )
    
    df.show()
    print("✅ MySQL Connection: SUCCESS")
    
except Exception as e:
    print(f"❌ MySQL Connection: FAILED")
    print(f"Error: {str(e)}")

# ============================================================================
# TEST SNOWFLAKE CONNECTION
# ============================================================================
print("\n❄️  Testing Snowflake Connection...")

try:
    snowflake_options = {
        "sfURL": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SNOWFLAKE_USER,
        "sfPassword": SNOWFLAKE_PASSWORD,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE
    }
    
    # Simple test query
    df = spark.read \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("query", "SELECT top 3 * from ITS.workspace.city_data") \
        .load()
    
    df.show()
    print("✅ Snowflake Connection: SUCCESS")
    
except Exception as e:
    print(f"❌ Snowflake Connection: FAILED")
    print(f"Error: {str(e)}")

print("\n" + "=" * 60)
print("✨ Connection Test Complete")
print("=" * 60)