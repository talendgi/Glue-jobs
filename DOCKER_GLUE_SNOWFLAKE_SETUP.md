
# AWS Glue 5.0 (Spark 3.5) + Snowflake Docker Setup Guide

This document outlines the setup, configuration, and execution steps for running AWS Glue 5.0 (Spark 3.5) locally via Docker, with full connectivity to Snowflake and local AWS credentials.

---

## 📋 Prerequisites

1. **Docker Desktop**: Installed and running on your Windows machine.
2. **AWS CLI**: Configured locally with your credentials (`aws configure`).
3. **Snowflake Spark Connector JARs**: Downloaded and placed in the local `snowflake_jars` directory (see Step 1).

---

## 📁 Expected Local Directory Structure

Ensure your local Windows directory looks like this before running the container:

```text
C:\Users\your_user\
├── .aws/                  # Contains 'credentials' and 'config' files
└── glue/
    ├── workspace/         # Your PySpark scripts and .env files
    ├── snowflake_jars/    # Snowflake connector JARs
```

## Download Snowflake Connector JARs
  Because AWS Glue 5.0 uses Spark 3.5, you must use Snowflake Spark Connector version 3.x to ensure Query Pushdown works correctly. Older versions (2.x) will pull entire tables into Spark memory.
1. Go to Maven** Central - Snowflake Spark Connector**
2. Download the latest **3.x.x** version (e.g., spark-snowflake_2.12-3.1.9.jar).
3. Go to **Maven Central - Snowflake JDBC Driver**
4. Download the latest **3.x.x** version (e.g., snowflake-jdbc-3.16.1.jar).
5. Place both **.jar** files into: **C:\Users\your_user\glue\snowflake_jars\**

## Run the Docker Container
Use the following command to start the interactive Glue 5.0 environment.
```bash
docker run -it --rm --add-host=host.docker.internal:host-gateway -v "C:\Users\your_user\.aws:/home/hadoop/.aws" -v "C:\Users\your_user\glue:/home/hadoop/workspace" -v "C:\Users\your_user\glue\snowflake_jars:/opt/spark/jars" -e AWS_PROFILE=default --name glue5_spark_submit public.ecr.aws/glue/aws-glue-libs:5 pyspark
```


once the container is created
<img width="1523" height="469" alt="image" src="https://github.com/user-attachments/assets/606d07ac-0314-4974-8df2-1785a8686fd7" />

<img width="1601" height="834" alt="image" src="https://github.com/user-attachments/assets/0ccf6c1c-f5c0-4c48-b368-491eec8f7010" />


To attach to a** Docker container**, either select **Dev Containers: Attach to Running Container**... from the Command Palette (**F1**) or use the **Remote Explorer** in the **Activity Bar **and from the **Containers** view, select the Attach to Container inline action on the container you want to connect to.

<img width="917" height="704" alt="image" src="https://github.com/user-attachments/assets/d195210b-6c55-4a1f-9799-300ce1019adc" />



