

import logging
from typing import Optional
from typing_extensions import TypedDict

from langgraph.graph import StateGraph
from langchain_groq import ChatGroq
from awsglue.context import GlueContext
from pyspark.context import SparkContext


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# table_name="blood_donation_registry_ml_ready"
YOUR_GROQ_KEY="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"


# Logging
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

#  Snowflake / Spark config  
# ─────────────────────────────────────────────
SF_OPTIONS = {
    "sfURL":       "https://IW82827.ap-southeast-7.aws.snowflakecomputing.com",
    "sfUser":      "logeshits",
    "sfPassword":  "Andoroktaits321",
    "sfDatabase":  "ITS",
    "sfSchema":    "WORKSPACE",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole":      "ITS_WORKSPACE",
}

MYSQL_OPTIONS = {
    "url":      "jdbc:mysql://host.docker.internal:3306/blood_donor_reg",
    "driver":   "com.mysql.cj.jdbc.Driver",
    "user":     "root",
    "password": "Andorokta!321",
}

# ─────────────────────────────────────────────
# LLM
# ─────────────────────────────────────────────
llm = ChatGroq(
    model="llama-3.1-8b-instant",
    temperature=0,
    api_key=YOUR_GROQ_KEY
)
# ─────────────────────────────────────────────
# Graph State
# ─────────────────────────────────────────────
class PipelineState(TypedDict):
    table_name:       str
    mysql_schema:     list[dict]
    snowflake_schema: list[dict]          
    generated_ddl:    str
    ddl_action:       str                 
    load_status:      str
    error:            Optional[str]
    load_type:        str 


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
TYPE_MAP = {"INT": "NUMBER", "VARCHAR": "STRING", "DATETIME": "TIMESTAMP", "TEXT": "STRING"}

UNSAFE_KEYWORDS = {"DROP", "DELETE", "UPDATE", "TRUNCATE"}

def _validate_ddl(sql: str) -> str:
    upper = sql.upper()
    for kw in UNSAFE_KEYWORDS:
        if kw in upper:
            raise ValueError(f"Unsafe keyword detected in generated DDL: {kw}")
    return sql


def _build_prompt(table_name: str, mysql_schema: list, snowflake_schema: list) -> str:
    sf_exists = bool(snowflake_schema)
    type_rules = "\n".join(f"  {k} -> {v}" for k, v in TYPE_MAP.items())

    return f"""You are a senior data engineer performing schema comparison.

Source (MySQL) schema for table `{table_name}`:
{mysql_schema}

Target (Snowflake) schema (empty = table does not exist yet):
{snowflake_schema}

Tasks:
1. Compare both schemas.
2. Write a single paragraph summarising the differences.
3. {"Generate only the necessary ALTER TABLE statements." if sf_exists else "Generate a CREATE TABLE statement."}
4. If schemas are identical return NO_CHANGE.

Type mapping rules:
{type_rules}

Rules:
- No markdown, no code fences, no trailing explanations.
- Respond in EXACTLY this format:

PARAGRAPH:
<one paragraph>

SQL:
<DDL statement(s) or NO_CHANGE>
"""


#  Node 1 — Compare schemas & generate DDL
# ─────────────────────────────────────────────
def node_generate_ddl(state: PipelineState) -> dict:
    logger.info("▶ [node_generate_ddl] Calling LLM for schema comparison …")
    prompt = _build_prompt(state["table_name"], state["mysql_schema"], state["snowflake_schema"])

    response = llm.invoke(prompt)
    raw = response.content.strip()
    logger.debug("LLM raw output:\n%s", raw)
    logger.info("LLM raw output:\n%s", raw)

    if "SQL:" not in raw:
        raise ValueError("LLM response is missing the SQL: section.")

    sql_section = raw.split("SQL:")[1].strip()

    if sql_section.upper() == "NO_CHANGE":
        logger.info("   Schemas are identical — no DDL needed.")
        return {"generated_ddl": "", "ddl_action": "NO_CHANGE"}

    validated = _validate_ddl(sql_section)
    action = "ALTER" if state["snowflake_schema"] else "CREATE"
    logger.info("   DDL action: %s", action)
    return {"generated_ddl": validated, "ddl_action": action}


# Node 2 — Execute DDL in Snowflake
# ─────────────────────────────────────────────
def node_execute_ddl(state: PipelineState) -> dict:
    if state["ddl_action"] == "NO_CHANGE":
        logger.info("▶ [node_execute_ddl] Skipped — schemas identical.")
        return {}

    ddl = state["generated_ddl"]
    logger.info("▶ [node_execute_ddl] Executing DDL:\n%s", ddl)

    try:
        (
            spark.createDataFrame([(1, "ping")], ["id", "val"])
            .write.format("snowflake")
            .options(**SF_OPTIONS)
            .option("dbtable", "dummy")
            .option("postactions", ddl)
            .mode("append")
            .save()
        )
        logger.info("   DDL executed successfully.")
    except Exception as exc:
        logger.error("   DDL execution failed: %s", exc)
        return {"error": str(exc)}

    return {}


# Node 3 — Load MySQL data → Snowflake staging
# ─────────────────────────────────────────────
def node_load_to_stage(state: PipelineState) -> dict:
    if state.get("error"):
        logger.warning("▶ [node_load_to_stage] Skipped due to earlier error.")
        return {"load_status": "SKIPPED"}

    src_table  = state["table_name"]
    stg_table  = f"{src_table}_STG"
    logger.info("▶ [node_load_to_stage] Loading %s → %s …", src_table, stg_table)

    try:
        df = (
            spark.read.format("jdbc")
            .options(**MYSQL_OPTIONS)
            .option("dbtable", src_table)
            .load()
        )
        row_count = df.count()
        logger.info("   Rows read from MySQL: %d", row_count)
        stg_sf_options = {**SF_OPTIONS, "sfSchema": "STG"}
        (
            df.write.format("snowflake")
            .options(**stg_sf_options)
            .option("dbtable", stg_table)
            .mode("overwrite")
            .save()
        )
        logger.info("   Load complete → STG.%s (%d rows)", stg_table, row_count)
        return {"load_status": f"SUCCESS — {row_count} rows loaded into STG.{stg_table}"}

    except Exception as exc:
        logger.error("   Load failed: %s", exc)
        return {"load_status": "FAILED", "error": str(exc)}

# Node 4 — Load Snowflake staging → Target table
# ─────────────────────────────────────────────
def node_merge_to_target(state: PipelineState) -> dict:
    target_table = state["table_name"]
    staging_table = f"{target_table}_STG"
    LOAD_TYPE = state["load_type"]
    incremental_column = state.get("incremental_column")

    """
    Moves data from Snowflake Staging schema to Target schema based on the chosen strategy.
    """
    target_fqn = f"{SF_OPTIONS['sfSchema']}.{target_table}"
    stg_fqn = f"STG.{staging_table}"
    
    print(f"\n🔀 Moving data from STG to target table and the load type is : {LOAD_TYPE}")
    
    if LOAD_TYPE == "FULL":
        sql = f"""
        TRUNCATE TABLE IF EXISTS {target_fqn};
        INSERT INTO {target_fqn} SELECT * FROM {stg_fqn};
        """
        
    elif LOAD_TYPE == "APPEND":
        sql = f"""
        INSERT INTO {target_fqn} SELECT * FROM {stg_fqn};
        """
        
    elif LOAD_TYPE == "DELETE_INSERT":
        if not incremental_column:
            raise ValueError("❌ DELETE_INSERT strategy requires an incremental_column (e.g., 'updated_at').")
        
        sql = f"""
        DELETE FROM {target_fqn} 
        WHERE {incremental_column} IN (SELECT DISTINCT {incremental_column} FROM {stg_fqn});
        
        INSERT INTO {target_fqn} SELECT * FROM {stg_fqn};
        """
        
    elif LOAD_TYPE == "MERGE":
        # Fallback to your existing MERGE logic if a Primary Key is somehow provided
        print("⚠️ MERGE strategy selected, but this function expects no PK. Use your existing MERGE function.")
        return
        
    else:
        raise ValueError(f"❌ Unknown LOAD_STRATEGY: {LOAD_TYPE}")

    # Execute the generated SQL
    try:   
        """Execute SQL query in Snowflake"""
        spark.createDataFrame([(1, "test")], ["id", "name"]).write \
        .format("snowflake") \
        .options(**SF_OPTIONS) \
        .option("dbtable", "Dummy")\
        .option("postactions", sql) \
        .mode("append") \
        .save()
        print(f"✅ Successfully loaded {staging_table} to {target_table} , SQL : {sql}")
    except Exception as e:
        print(f"❌ Failed to load staging to target: {str(e)}")
        raise e

#  Conditional edge — skip DDL execution when NO_CHANGE
# ─────────────────────────────────────────────
def route_after_ddl_gen(state: PipelineState) -> str:
    if state["ddl_action"] == "NO_CHANGE":
        return "load_to_stage"         
    return "execute_ddl"


#  Build & compile the graph
# ─────────────────────────────────────────────
def build_graph() -> StateGraph:
    builder = StateGraph(PipelineState)

    builder.add_node("generate_ddl",   node_generate_ddl)
    builder.add_node("execute_ddl",    node_execute_ddl)
    builder.add_node("load_to_stage",  node_load_to_stage)
    builder.add_node("merge_to_target",   node_merge_to_target)
    builder.set_entry_point("generate_ddl")

    builder.add_conditional_edges(
        "generate_ddl",
        route_after_ddl_gen,
        {
            "execute_ddl":  "execute_ddl",
            "load_to_stage": "load_to_stage",
        },
    )
    builder.add_edge("execute_ddl",   "load_to_stage")
    builder.add_edge("load_to_stage",   "merge_to_target")
    # builder.add_edge("load_to_stage", END)

    return builder.compile()


#  Entry point
# ─────────────────────────────────────────────
def run_pipeline(table_name: str, mysql_schema: list, snowflake_schema: list,load_type: str = "FULL", primary_key: str = "", incremental_col: str = "") -> dict:
    graph = build_graph()

    logger.info("=" * 60)
    logger.info("Pipeline workflow (Mermaid):\n%s", graph.get_graph().draw_mermaid())
    logger.info("=" * 60)

    initial_state: PipelineState = {
        "table_name":       table_name,
        "mysql_schema":     mysql_schema,
        "snowflake_schema": snowflake_schema,
        "generated_ddl":    "",
        "ddl_action":       "",
        "load_status":      "",
        "error":            None,
        "load_type":        load_type,
    }

    result = graph.invoke(initial_state)

    logger.info("─" * 60)
    logger.info("DDL action  : %s", result["ddl_action"])
    logger.info("Generated DDL:\n%s", result["generated_ddl"] or "(none)")
    logger.info("Load status : %s", result["load_status"])
    if result.get("error"):
        logger.error("Pipeline error: %s", result["error"])
    logger.info("=" * 60)

    return result

# ─────────────────────────────────────────────
if __name__ == "__main__":
    table_name = "BLOOD_COMPATIBILITY_LOOKUP"
    LOAD_TYPE = "FULL"

    # ── MySQL: read schema via INFORMATION_SCHEMA ──────────────────
    try:
        mysql_schema_df = (
            spark.read.format("jdbc")
            .options(**MYSQL_OPTIONS)
            .option(
                "dbtable",
                f"""(
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name.lower()}'
                    ORDER BY ORDINAL_POSITION
                ) AS schema_query"""
            )
            .load()
        )
        mysql_schema = [row.asDict() for row in mysql_schema_df.collect()]
        print("=" * 30 + " MySQL Schema " + "=" * 30)
        print(mysql_schema)

    except Exception as e:
        print(f"Error reading MySQL schema: {e}")
        mysql_schema = []

    # ── Snowflake: read schema via INFORMATION_SCHEMA ──────────────
    try:
        sf_schema_df = (
            spark.read.format("snowflake")
            .options(**SF_OPTIONS)
            .option("dbtable", "INFORMATION_SCHEMA.COLUMNS")
            .load()
            .filter(f"TABLE_NAME = '{table_name.upper()}_STG'")
            .select("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
            .orderBy("ORDINAL_POSITION")
        )
        snowflake_schema = [row.asDict() for row in sf_schema_df.collect()]
        print("=" * 30 + " Snowflake Schema " + "=" * 30)
        print(snowflake_schema)

    except Exception as e:
        # Table likely doesn't exist yet — that's fine, pipeline will CREATE it
        print(f"Snowflake table not found or error: {e}")
        snowflake_schema = []

    # ── Run pipeline ───────────────────────────────────────────────
    if not mysql_schema:
        print("MySQL schema is empty — aborting pipeline.")
    else:
        run_pipeline(
            table_name=table_name,
            mysql_schema=mysql_schema,
            snowflake_schema=snowflake_schema,  # [] triggers CREATE TABLE
        )
    print("pipeline execution complete.")
    spark.stop()
