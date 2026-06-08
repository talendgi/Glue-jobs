# MySQL to Snowflake Incremental Data Sync (AWS Glue Spark)

An automated, incremental ETL pipeline built with **AWS Glue (PySpark)** that syncs data from **MySQL** to **Snowflake**. 

This solution features * incremental loading**, **automatic schema evolution** , and **MERGE (UPSERT)** capabilities, all orchestrated via a centralized metadata control table.

---

## 🏗️ Architecture & Workflow
```text
SparkContext (sc) 
  │  <-- The core Spark engine
  └── GlueContext (glueContext) 
        │  <-- Adds AWS Glue features (Connections, DynamicFrames)
        │
        ├── SparkSession (spark) 
        │     <-- Adds DataFrame & SQL capabilities (Used for Snowflake)
        │
        └── Job (job) 
              <-- Adds Job tracking, metrics, and Bookmarks
```

**1. sc = SparkContext()**

**What it is:** The entry point to the underlying Apache Spark engine.

**Usage:** It establishes the connection to the Spark cluster (the executor nodes). Without this, Spark cannot run any computations. In AWS Glue, this initializes the distributed computing environment.


**2. glueContext = GlueContext(sc)**

**What it is:** The AWS Glue wrapper around the standard SparkContext.

**Usage:** Standard Spark doesn't know how to talk to AWS services (like the Glue Data Catalog, S3, or Glue JDBC Connections). GlueContext bridges this gap.
It allows you to use Glue-specific features like DynamicFrames (glueContext.create_dynamic_frame...).
It allows you to use Glue Connections (the ones you configure in the AWS Glue Console).


**3. spark = glueContext.spark_session**

**What it is:** The entry point for the Spark DataFrame and SQL API.

**Usage:** While GlueContext is great for Glue-specific things, most modern Spark development (and your Snowflake connector) relies on standard DataFrames. This line extracts the SparkSession so you can use standard Spark commands like:
spark.read.format("snowflake")...
df.filter(), df.withColumn(), df.count()
spark.sql("SELECT * FROM...")


**4. job = Job(glueContext)**

   **What it is:** The AWS Glue Job tracker.

   **Usage:** This object tracks the lifecycle of your specific job run in the AWS Glue Console.
   It is required if you want to use Glue Job Bookmarks (which automatically track incremental data without needing a custom PROCESS_CONTROL_TABLE).
   It handles job metrics (how much data was processed, how long it took).
   You use it to start the job (job.init()) and successfully finish it (job.commit()).

-------------------------------------

The pipeline follows a robust, state-driven ETL workflow:

1. **Fetch Watermark**: Reads the last processed timestamp (`ENDDATE`) from the MySQL `PROCESS_CONTROL_TABLE`.
2. **Extract Incremental Data**: Queries the MySQL source table for records newer than the watermark.
3. **Schema Check & Evolution**: 
   - Checks if the target table exists in Snowflake.
   - If it doesn't exist, **auto-creates** it with audit columns.
   - If it exists, **compares schemas** and automatically adds any new columns found in the MySQL source.
4. **Load to Staging**: Writes the incremental data to a temporary staging table in Snowflake.
5. **MERGE (UPSERT)**: Executes a Snowflake `MERGE` statement to update existing records (based on Primary Key) and insert new ones into the final target table.
6. **Update Watermark**: Updates the `PROCESS_CONTROL_TABLE` in MySQL with the new maximum timestamp for the next run.

---

## 📋 Prerequisites

1. **Compute Environment**: AWS Glue 5.0 (Spark  3.5) (or a local Docker environment with Glue libs).
2. **Snowflake Account**: With a dedicated Warehouse, Database, Schema, and Role.
3. **MySQL Database**: Acting as both the data source and the metadata store.
4. **Snowflake Spark Connector JARs**: 
   - **Crucial**: If using Glue 5.0 (Spark 3.5), you **must** use Snowflake Spark Connector **v3.x** (e.g., `spark-snowflake_2.12-3.1.9.jar`) to ensure Query Pushdown is enabled.

---

## ⚙️ Configuration

### 1. Environment Variables (`.env`)
Create a `.env` file in your workspace root to manage credentials securely.

```env
# --- MySQL Configuration ---
MYSQL_HOST=host.docker.internal
MYSQL_PORT=3306
MYSQL_DATABASE=its_mtd
MYSQL_DATABASE_MTD=its_mtd
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password

# --- Snowflake Configuration ---
SNOWFLAKE_ACCOUNT=iw82827.us-west-2.aws
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_DATABASE=ITS
SNOWFLAKE_SCHEMA=WORKSPACE
SNOWFLAKE_SCHEMA_TEMP=STG
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ITS_WORKSPACE

###  Code flow
```text
**get_mysql_data_with_glue_context** - Reads incremental data from MySQL using Glue DynamicFrames and applies the watermark filter.
**compare_schemas** - Compares the MySQL source schema with the Snowflake target schema to detect new columns.
**create_snowflake_table_with_metadata** - Auto-generates and executes DDL to create the target table in Snowflake, including audit columns (SNFLK_LOADED_AT).
**alter_snowflake_table** - Automatically executes ALTER TABLE ADD COLUMN if new fields are detected in the source.
**load_to_snowflake_with_merge** - Handles the heavy lifting: writes to a staging table, executes the MERGE (UPSERT) SQL, and cleans up.
**Update_process_control_table** - Calculates the new max timestamp from the loaded data and updates the MySQL control table via JDBC.
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


**AWS Glue Studio / S3**
1. Upload the Python script and .env file to an S3 bucket.
2. Create a Glue Job in the AWS Console.
3. Under Job Details, ensure Glue version is set to Glue 5.0 - Supports spark 3.5.
4. Under Advanced properties -> Job parameters, add your environment variables or point to the .env file in S3.
5. Under Dependent JARs path, provide the S3 URI to your spark-snowflake_2.12-3.x.x.jar.

