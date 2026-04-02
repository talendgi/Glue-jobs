import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.functions import col, max as spark_max, lit, current_timestamp, md5, concat_ws
from datetime import datetime
# import boto3
# import json
# import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# MySQL Configuration
MYSQL_HOST = "host.docker.internal"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "its_mtd"
MYSQL_DATABASE_MTD="its_mtd"
MYSQL_USER = "root"
MYSQL_PASSWORD = "Andorokta!321"
MYSQL_TABLE = "blood_donation_registry_ml_ready"

# Snowflake Configuration  
SNOWFLAKE_ACCOUNT = "kg88800"
SNOWFLAKE_USER = "logeshits"
SNOWFLAKE_PASSWORD = "Andoroktaits321"
SNOWFLAKE_DATABASE = "ITS"
SNOWFLAKE_SCHEMA = "WORKSPACE"
SNOWFLAKE_SCHEMA_TEMP='STG'
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_ROLE = "ITS_WORKSPACE"
SNOWFLAKE_TABLE = "blood_donation_registry_ml_ready"



# Snowflake Connection Options
SNOWFLAKE_OPTIONS = {
    "sfURL": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
    "sfUser": SNOWFLAKE_USER,
    "sfPassword": SNOWFLAKE_PASSWORD,
    "sfDatabase": SNOWFLAKE_DATABASE,
    "sfSchema": SNOWFLAKE_SCHEMA,
    "sfWarehouse": SNOWFLAKE_WAREHOUSE,
    "sfRole": SNOWFLAKE_ROLE
    # "tempDir": S3_TEMP_PATH  # S3 staging for better performance
}

MYSQL_JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/"

# ============================================================================
# INITIALIZE GLUE CONTEXT
# ============================================================================
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# Set Spark configurations for better performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print("=" * 80)
print(f"📈 MySQL to Snowflake Sync Job :")
print(f"⏰ Job Start time: {datetime.now()}")

# ===== STEP 1: Read process control entries from MySQL =====

control_options = {
    "url": MYSQL_JDBC_URL+MYSQL_DATABASE,
    "dbtable": "PROCESS_CONTROL_TABLE",
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}

control_df = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options=control_options
).toDF()

active_process = control_df.filter(
    (col("ACTIVE_FLAG") == "Y") &
    (col("SOURCE_SYSTEM") == "MYSQL") &
    (col("TARGET_SYSTEM") == "SNOWFLAKE") &
    (col("PROCESS_NAME") == "TRIP_DATA_SAMPLE") &
    (col("LOAD_TYPE") == "INCREMENTAL")
).collect()

if not active_process:
    raise Exception("❌ No active process")

last_enddate = active_process[0]["ENDDATE"]
target_table = active_process[0]["TARGET_TABLE_NAME"]
MYSQL_TABLE = active_process[0]["SOURCE_TABLE_NAME"]
MYSQL_DATABASE = active_process[0]["SOURCE_DATABASE"]
target_database = active_process[0]["TARGET_DATABASE"]
target_schema = active_process[0]["TARGET_SCHEMA"]
INCREMENTAL_COLUMN = active_process[0]["_CONDITION"]
PRIMARY_KEY = active_process[0]["PRIMARY_KEY"]
load_type = active_process[0]["LOAD_TYPE"]
process_name = active_process[0]["PROCESS_NAME"]
print("===========================================================")
print("📊 Reading PROCESS_CONTROL_TABLE...")

print(f" 📈 Last end date: {last_enddate}, Target table: {target_table} , Target Database: {MYSQL_DATABASE}, process name: {active_process[0]['PROCESS_NAME']}")
cutoff_ts = last_enddate.timestamp()
print(f"⏳  Cutoff timestamp: " + str(cutoff_ts) + " - " + str(datetime.fromtimestamp(cutoff_ts)))
print(f"📊 Source: {MYSQL_DATABASE}.{MYSQL_TABLE} , Process name: {process_name}")
print(f"❄️  Target: {target_database}.{target_schema}.{target_table}")
print(f"🔄 Load Type: {load_type}")
print(f"🔑 Primary Key: {PRIMARY_KEY}")
print(f"📌 Incremental Column: {INCREMENTAL_COLUMN}")
print("===========================================================")



# # Advanced Load Configuration
# ENABLE_SOFT_DELETE = False  # Track deleted records
# ENABLE_CDC = False  # Change Data Capture with before/after values

# # S3 Configuration
# S3_WATERMARK_PATH = "s3://your-bucket/glue-watermarks/"
# S3_TEMP_PATH = "s3://your-bucket/glue-temp/"

# # Performance Settings
# USE_BATCH_INSERT = True
# BATCH_SIZE = 10000
# MAX_RETRIES = 3

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_mysql_data_with_glue_context(table_name, incremental_col=None, watermark_value=None):
    """Read data from MySQL using Glue DynamicFrame"""
    print(f"\n📥 Reading data from MySQL: {table_name}")
    
    connection_options = {
        "url": MYSQL_JDBC_URL+MYSQL_DATABASE,
        "dbtable": table_name,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD
    }
    
    # Use Glue's DynamicFrame for better integration
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options=connection_options
    )
    
    # Convert to DataFrame
    df = dynamic_frame.toDF()
    
    # Apply incremental filter if needed
    if incremental_col and watermark_value:
        df = df.filter(col(incremental_col) > lit(watermark_value))
        print(f"  📌 Incremental filter: {incremental_col} > '{watermark_value}'")
    
    record_count = df.count()
    print(f"  ✅ Records fetched: {record_count:,}")
    
    return df


def get_snowflake_table_exists(table_name):
    """Check if Snowflake table exists"""
    try:
        query = f"SELECT COUNT(*) as cnt FROM {table_name} LIMIT 1"
        df = spark.read \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("query", query) \
            .load()
        # print(f"  ✅ Table {table_name} exists in Snowflake")
        return True
    except Exception as e:
        if "does not exist" in str(e).lower():
            return False
            print(f"  ⚠️  Table {table_name} does not exist in Snowflake")
        # Re-raise if different error
        raise e



def get_snowflake_schema(table_name):
    """Fetch schema from Snowflake table"""
    print(f"\n❄️  Fetching Snowflake schema for: {table_name}")
    
    try:
        df = spark.read \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("dbtable", table_name) \
            .load() \
            .limit(0)
        
        print(f"  ✅ Table exists with {len(df.schema.fields)} columns")
        return df.schema
    except Exception as e:
        print(f"  ⚠️  Table does not exist")
        return None

def spark_to_snowflake_type(spark_type):
    """Convert Spark data type to Snowflake data type with precision"""
    
    if isinstance(spark_type, DecimalType):
        return f"NUMBER({spark_type.precision},{spark_type.scale})"
    
    type_mapping = {
        StringType: "VARCHAR(16777216)",
        IntegerType: "INTEGER",
        LongType: "BIGINT",
        ShortType: "SMALLINT",
        ByteType: "BYTEINT",
        FloatType: "FLOAT",
        DoubleType: "DOUBLE",
        BooleanType: "BOOLEAN",
        DateType: "DATE",
        TimestampType: "TIMESTAMP_NTZ",
        BinaryType: "BINARY",
        ArrayType: "ARRAY",
        MapType: "OBJECT"
    }
    
    for spark_t, snowflake_t in type_mapping.items():
        if isinstance(spark_type, spark_t):
            return snowflake_t
    
    return "VARIANT"  # Snowflake's flexible type


def compare_schemas(source_schema, target_schema):
    """Compare schemas and return differences"""
    print("\n🔍 Comparing schemas...")
    
    source_fields = {field.name.upper(): field for field in source_schema.fields}
    target_fields = {field.name.upper(): field for field in target_schema.fields} if target_schema else {}
    
    new_columns = []
    type_mismatches = []
    missing_in_source = []
    
    # Check for new and modified columns
    for col_name, source_field in source_fields.items():
        if col_name not in target_fields:
            new_columns.append(source_field)
            print(f"  ➕ New column: {col_name} ({source_field.dataType})")
        else:
            target_field = target_fields[col_name]
            if type(source_field.dataType) != type(target_field.dataType):
                type_mismatches.append((col_name, source_field.dataType, target_field.dataType))
                print(f"  ⚠️  Type mismatch: {col_name}")
                print(f"      Source: {source_field.dataType}")
                print(f"      Target: {target_field.dataType}")
    
    # Check for columns in target but not in source (potential deletes)
    for col_name in target_fields:
        if col_name not in source_fields:
            missing_in_source.append(col_name)
            print(f"  ⚠️  Column in target but not source: {col_name}")
    
    return new_columns, type_mismatches, missing_in_source

def execute_snowflake_query(query):
    """Execute SQL query in Snowflake"""
    spark.createDataFrame([(1, "test")], ["id", "name"]).write \
    .format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .option("dbtable", "Dummy")\
    .option("postactions", query) \
    .mode("append") \
    .save()


def create_snowflake_table_with_metadata(table_name, schema):
    """Create Snowflake table with audit columns"""
    print(f"\n🔨 Creating Snowflake table: {table_name}")
    
    columns = []
    for field in schema.fields:
        col_name = field.name.upper()
        snowflake_type = spark_to_snowflake_type(field.dataType)
        nullable = "" if field.nullable else "NOT NULL"
        columns.append(f"  {col_name} {snowflake_type} {nullable}".strip())
    
    # Add audit columns
    columns.append("SNFLK_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
    columns.append("SOURCE_SYSTEM VARCHAR(100) DEFAULT 'MYSQL'")
    
    # if ENABLE_SOFT_DELETE:
    #     columns.append("  _IS_DELETED BOOLEAN DEFAULT FALSE")
    #     columns.append("  _DELETED_AT TIMESTAMP_NTZ")
    
    columns_sql = ",\n".join(columns)

    create_sql = f"""CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} ({columns_sql})""".strip()

    print(f"  📝 DDL:\n{create_sql}\n")
    
    try:
        execute_snowflake_query(create_sql)
        print(f"  ✅ Table created successfully: {create_sql}" )
    except Exception as e:
        print(f"  ❌ Failed to create table: {str(e)}")
        raise e


def alter_snowflake_table(table_name, new_columns):
    """Add new columns to existing Snowflake table"""
    if not new_columns:
        return
    
    print(f"\n🔧 Altering Snowflake table: {table_name}")
    
    for field in new_columns:
        col_name = field.name.upper()
        snowflake_type = spark_to_snowflake_type(field.dataType)
        
        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {snowflake_type}"
        print(f"  📝 {alter_sql}")
        
        try:
            execute_snowflake_query(alter_sql)
            print(f"  ✅ Column {col_name} added")
        except Exception as e:
            print(f"  ❌ Failed to add column {col_name}: {str(e)}")


def load_to_snowflake_with_merge(df, table_name, primary_key):
    """Load data using MERGE (UPSERT) operation"""
    print(f"\n🔀 Loading with MERGE operation")
    print(f"  Primary Key: {primary_key}")
    
    # Add audit timestamp
    df_with_audit = df.withColumn("SNFLK_LOADED_AT", current_timestamp())
    
    # Generate staging table name
    staging_table = f"{table_name}_STAGING_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    staging_table = f"{table_name}_STG"
    try:
        # Step 1: Load to staging table
        print(f"  1️⃣ Loading to staging table: {staging_table}")
        df_with_audit.write \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("dbtable", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_TEMP}.{staging_table}") \
            .mode("overwrite") \
            .save()
        
        # Step 2: Execute MERGE
        print(f"  2️⃣ Executing MERGE operation")
        
        # Build column list (exclude audit columns from update)
        columns = [f.name for f in df.schema.fields]
        update_columns = [c for c in columns if not c.startswith('SNFLK_LOADED') and not c.startswith('SOURCE_SYSTEM')]
        
        # Build MERGE statement
        merge_sql = f"""MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} target
USING {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_TEMP}.{staging_table} source
ON target.{primary_key} = source.{primary_key}
WHEN MATCHED THEN UPDATE SET
  {', '.join([f'target.{c} = source.{c}' for c in update_columns])},
  target.SNFLK_LOADED_AT = source.SNFLK_LOADED_AT
WHEN NOT MATCHED THEN INSERT
  ({', '.join(columns + ['SNFLK_LOADED_AT'])})
VALUES
  ({', '.join([f'source.{c}' for c in columns])}, source.SNFLK_LOADED_AT)
        """
        try:
            execute_snowflake_query(merge_sql)
            print(f"  ✅ MERGE executed successfully with merge SQL: {merge_sql}")
        except Exception as e:
            print(f"  ❌ MERGE failed: {str(e)}")
            raise e
        # Step 3: Get merge statistics
        # stats_query = f"SELECT SYSTEM$LAST_CHANGE_COMMIT_TIME('{table_name}') as last_change"
        print(f"  ✅ MERGE completed successfully")
        
    finally:
        # Cleanup staging table
        try:
            cleanup_sql = f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_TEMP}.{staging_table}"
            # execute_snowflake_query(cleanup_sql)
            # print(f"  🧹 Staging table truncated")
        except:
            pass


def Update_process_control_table( source_table, target_table,incremental_column, process_name):
    """Update process control table with job status"""
    print(f"\n🔄 Updating process control table: ")
    max_date_df = spark.read \
    .format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .option("query", f"SELECT MAX({INCREMENTAL_COLUMN}) AS max_date FROM {target_table}") \
    .load()

    max_date = max_date_df.collect()[0]["MAX_DATE"]

    print(f"✅ Max date in Snowflake table: {max_date}")
    
    update_sql = f"""
    UPDATE PROCESS_CONTROL_TABLE
    SET ENDDATE = TIMESTAMP('{max_date}', '00:00:00'),batch_id = DATE_FORMAT(NOW(), '%Y%m%d%H%i%s')
    WHERE SOURCE_TABLE_NAME = '{source_table}'
      AND TARGET_TABLE_NAME = '{target_table}'
      AND PROCESS_NAME = '{process_name}'
    """
    
    try:
        conn = spark._sc._jvm.java.sql.DriverManager.getConnection(
        MYSQL_JDBC_URL+MYSQL_DATABASE_MTD,
        MYSQL_USER,
        MYSQL_PASSWORD)
        stmt = conn.createStatement()
        stmt.execute(update_sql)
        conn.close()
        # spark.write \
        #     .format("mysql") \
        #     .option("url", MYSQL_JDBC_URL+MYSQL_DATABASE_MTD) \
        #     .option("dbtable", "PROCESS_CONTROL_TABLE") \
        #     .option("user", MYSQL_USER) \
        #     .option("password", MYSQL_PASSWORD) \
        #     .option("driver", "com.mysql.cj.jdbc.Driver") \
        #     .save(mode="append", query=update_sql)

        print(f"  ✅ Process control table updated with status: {update_sql}")
    except Exception as e:
        print(f"  ❌ Failed to update process control table: {update_sql}, error: {str(e)}")
        raise e

print("\n" + "=" * 80)
# main execution
print("=" * 80)
if __name__ == "__main__":
    # Read data from MySQL
    mysql_df=get_mysql_data_with_glue_context(MYSQL_TABLE, INCREMENTAL_COLUMN, str(datetime.fromtimestamp(cutoff_ts)))
    # MySQL schema
    mysql_schema = mysql_df.schema
    print(f"\n📋 MySQL Schema:")
    mysql_df.printSchema()
    # check if target table exists in Snowflake and get schema if exists
    get_snowflake_table_exists(target_table)
    if get_snowflake_table_exists(target_table):
        get_snowflake_schema(target_table)
    else:
        print(f"  ❌ Cannot fetch schema for non-existent table {target_table}")
    get_snowflake_schema(target_table)

    snowflake_exists = get_snowflake_table_exists(target_table)
    snowflake_schema = None
    
    if snowflake_exists:
        snowflake_schema = get_snowflake_schema(target_table)
    
    # Handle schema changes
    if not snowflake_exists:
        create_snowflake_table_with_metadata(target_table, mysql_schema)
    else:
        new_columns, type_mismatches, missing_cols = compare_schemas(mysql_schema, snowflake_schema)
        if new_columns:
            alter_snowflake_table(target_table, new_columns)
        else:
            print("\n✅ Schemas are in sync")
        
        if type_mismatches:
            print("\n⚠️  Type mismatches require manual review")  
     #  Load data based on mode
    if load_type == "INCREMENTAL" and PRIMARY_KEY:
        load_to_snowflake_with_merge(mysql_df, target_table, PRIMARY_KEY)
    else:
        load_to_snowflake_append(mysql_df, target_table)
    Update_process_control_table(MYSQL_TABLE, target_table, INCREMENTAL_COLUMN, process_name)
    
print("\n" + "=" * 80)
print("🎉 Job completed successfully!")
print("=" * 80)

# except Exception as e:
#     print(f"\n❌ Error occurred: {str(e)}")
#     import traceback
#     traceback.print_exc()
#     raise e
# job.exit()
job.commit()
spark.stop()
