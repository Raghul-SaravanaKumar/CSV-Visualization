import io
import os
import re
import asyncio
import traceback
import pandas as pd
from typing import Optional
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

from kafka_engine import kafka_stream_manager, run_kafka_pipeline, KAFKA_PYTHON_AVAILABLE

# Load environment variables from .env if present
load_dotenv()

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

app = FastAPI(
    title="CSV Executer, Snowflake & Apache Kafka API",
    description="High-performance backend API for analyzing, processing, and streaming CSV datasets to Snowflake in 10-record chunks using Apache Kafka.",
    version="3.0.0"
)

# Enable CORS so the frontend can communicate seamlessly
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global in-memory storage for the currently loaded DataFrame
app_state = {
    "current_df": None,
    "file_name": None
}

class QueryRequest(BaseModel):
    query: str

class SnowflakeCredentials(BaseModel):
    account: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema_: Optional[str] = None

class SnowflakeSaveRequest(BaseModel):
    table_name: str
    table_name_2: Optional[str] = None
    credentials: Optional[SnowflakeCredentials] = None
    if_exists: str = "replace"

class KafkaStreamRequest(BaseModel):
    topic_name: str = "sf-csv-stream"
    batch_size: int = 10
    table_name: str
    num_consumers: int = 1
    table_name_2: Optional[str] = None
    use_real_kafka: bool = False
    bootstrap_servers: str = "localhost:9092"
    credentials: Optional[SnowflakeCredentials] = None

def get_snowflake_connection(creds: Optional[SnowflakeCredentials] = None):
    """
    Connects to Snowflake using provided credentials or falls back to environment variables.
    """
    if not SNOWFLAKE_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="snowflake-connector-python is not installed. Please run: pip install snowflake-connector-python[pandas]"
        )

    account = (creds.account if creds and creds.account else os.getenv("SNOWFLAKE_ACCOUNT"))
    if account:
        account = account.replace("https://", "").replace("http://", "")
        if ".snowflakecomputing.com" in account:
            account = account.split(".snowflakecomputing.com")[0]
        account = account.strip("/")

    user = (creds.user if creds and creds.user else os.getenv("SNOWFLAKE_USER"))
    password = (creds.password if creds and creds.password else os.getenv("SNOWFLAKE_PASSWORD"))
    role = (creds.role if creds and creds.role else os.getenv("SNOWFLAKE_ROLE"))
    warehouse = (creds.warehouse if creds and creds.warehouse else os.getenv("SNOWFLAKE_WAREHOUSE"))
    database = (creds.database if creds and creds.database else os.getenv("SNOWFLAKE_DATABASE"))
    schema = (creds.schema_ if creds and creds.schema_ else os.getenv("SNOWFLAKE_SCHEMA"))

    if not account or not user or not password:
        raise HTTPException(
            status_code=400,
            detail="Missing Snowflake required credentials (Account, User, Password). Provide them via UI settings or .env file."
        )

    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            role=role if role else None,
            warehouse=warehouse if warehouse else None,
            database=database if database else None,
            schema=schema if schema else "PUBLIC"
        )
        return conn, {
            "account": account,
            "user": user,
            "role": role or "DEFAULT",
            "warehouse": warehouse or "DEFAULT",
            "database": database or "DEFAULT",
            "schema": schema or "PUBLIC"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to connect to Snowflake: {str(e)}")

@app.get("/")
def read_root():
    return {
        "status": "online",
        "service": "CSV Executer, Snowflake & Apache Kafka API",
        "loaded_file": app_state["file_name"],
        "rows": len(app_state["current_df"]) if app_state["current_df"] is not None else 0,
        "snowflake_driver_ready": SNOWFLAKE_AVAILABLE,
        "kafka_driver_ready": KAFKA_PYTHON_AVAILABLE
    }

@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only .csv files are supported.")

    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        app_state["current_df"] = df
        app_state["file_name"] = file.filename

        df_clean = df.fillna("")
        preview_rows = df_clean.head(10).to_dict(orient="records")
        dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
        
        return {
            "status": "success",
            "message": f"Successfully parsed '{file.filename}' on Python backend.",
            "metadata": {
                "file_name": file.filename,
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "data_types": dtypes
            },
            "preview": preview_rows
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process CSV file: {str(e)}")

@app.post("/api/execute")
async def execute_query(payload: QueryRequest):
    df = app_state["current_df"]
    if df is None:
        raise HTTPException(status_code=400, detail="No CSV dataset loaded. Please upload a file first via /api/upload.")

    query_str = payload.query.strip()
    if not query_str:
        raise HTTPException(status_code=400, detail="Query string cannot be empty.")

    try:
        if query_str.startswith("df."):
            local_vars = {"df": df, "pd": pd}
            result_obj = eval(query_str, {"__builtins__": {}}, local_vars)
            if isinstance(result_obj, pd.DataFrame):
                result = result_obj.fillna("").to_dict(orient="records")
            elif isinstance(result_obj, pd.Series):
                result = result_obj.fillna("").to_dict()
            else:
                result = result_obj
        else:
            filtered_df = df.query(query_str)
            result = {
                "matched_rows": len(filtered_df),
                "records": filtered_df.fillna("").head(100).to_dict(orient="records")
            }

        return {
            "status": "success",
            "query": query_str,
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Execution error: {str(e)}")

@app.post("/api/snowflake/connect")
async def test_snowflake_connection(creds: Optional[SnowflakeCredentials] = None):
    conn, info = get_snowflake_connection(creds)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Connected to Snowflake successfully!",
            "session_info": {
                "version": row[0] if row else "Unknown",
                "user": row[1] if row else info["user"],
                "warehouse": row[2] if row else info["warehouse"],
                "database": row[3] if row else info["database"],
                "schema": row[4] if row else info["schema"]
            }
        }
    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=400, detail=f"Snowflake verification failed: {str(e)}")

@app.post("/api/snowflake/save")
async def save_to_snowflake(payload: SnowflakeSaveRequest):
    df = app_state["current_df"]
    if df is None:
        raise HTTPException(status_code=400, detail="No CSV data loaded on server. Please upload your CSV first.")

    table_name = payload.table_name.strip()
    if not table_name:
        raise HTTPException(status_code=400, detail="Target table name #1 is required.")

    clean_table_1 = re.sub(r'[^A-Za-z0-9_]', '_', table_name).upper()
    df_to_save = df.copy()
    df_to_save.columns = [re.sub(r'[^A-Za-z0-9_]', '_', col).upper() for col in df_to_save.columns]

    conn, info = get_snowflake_connection(payload.credentials)
    try:
        overwrite = (payload.if_exists == "replace")
        success_1, nchunks_1, nrows_1, _ = write_pandas(
            conn=conn,
            df=df_to_save,
            table_name=clean_table_1,
            database=info["database"] if info["database"] != "DEFAULT" else None,
            schema=info["schema"] if info["schema"] != "DEFAULT" else "PUBLIC",
            auto_create_table=True,
            overwrite=overwrite
        )

        if not success_1:
            conn.close()
            raise HTTPException(status_code=500, detail=f"write_pandas failed for Table #1 ('{clean_table_1}').")

        # Check if table_name_2 is requested
        clean_table_2 = None
        nrows_2 = 0
        if payload.table_name_2 and payload.table_name_2.strip():
            clean_table_2 = re.sub(r'[^A-Za-z0-9_]', '_', payload.table_name_2.strip()).upper()
            success_2, nchunks_2, nrows_2, _ = write_pandas(
                conn=conn,
                df=df_to_save,
                table_name=clean_table_2,
                database=info["database"] if info["database"] != "DEFAULT" else None,
                schema=info["schema"] if info["schema"] != "DEFAULT" else "PUBLIC",
                auto_create_table=True,
                overwrite=overwrite
            )
            if not success_2:
                conn.close()
                raise HTTPException(status_code=500, detail=f"write_pandas failed for Table #2 ('{clean_table_2}').")

        conn.close()

        if clean_table_2:
            return {
                "status": "success",
                "message": f"Successfully inserted {nrows_1:,} rows into Table #1 ('{clean_table_1}') and {nrows_2:,} rows into Table #2 ('{clean_table_2}')!",
                "details": {
                    "table_name_1": clean_table_1,
                    "rows_inserted_1": nrows_1,
                    "table_name_2": clean_table_2,
                    "rows_inserted_2": nrows_2,
                    "database": info["database"],
                    "schema": info["schema"]
                }
            }
        else:
            return {
                "status": "success",
                "message": f"Successfully inserted {nrows_1:,} rows into Snowflake table '{clean_table_1}'.",
                "details": {
                    "table_name": clean_table_1,
                    "rows_inserted": nrows_1,
                    "chunks_written": nchunks_1,
                    "database": info["database"],
                    "schema": info["schema"]
                }
            }
    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Snowflake write error: {str(e)}")

@app.post("/api/kafka/start-stream")
async def start_kafka_stream(payload: KafkaStreamRequest):
    """
    Starts the asynchronous Apache Kafka streaming & batch upload pipeline (10 records/batch).
    Returns immediately while the background task publishes and consumes batches dynamically.
    """
    df = app_state["current_df"]
    if df is None:
        raise HTTPException(status_code=400, detail="No CSV data loaded on server. Please upload your CSV first.")

    if kafka_stream_manager.is_running:
        raise HTTPException(status_code=409, detail="A Kafka streaming pipeline is already in progress. Please wait for it to finish.")

    table_name = payload.table_name.strip()
    if not table_name:
        raise HTTPException(status_code=400, detail="Consumer #1 target table name in Snowflake is required.")

    # Launch streaming task asynchronously
    asyncio.create_task(
        run_kafka_pipeline(
            df=df,
            topic_name=payload.topic_name,
            batch_size=payload.batch_size,
            table_name=table_name,
            use_real_kafka=payload.use_real_kafka,
            bootstrap_servers=payload.bootstrap_servers,
            get_conn_fn=get_snowflake_connection,
            creds_payload=payload.credentials,
            num_consumers=payload.num_consumers,
            table_name_2=payload.table_name_2
        )
    )

    msg_str = f"Apache Kafka stream initiated ({payload.num_consumers} Consumer{'s' if payload.num_consumers>1 else ''})! Publishing {len(df)} records in batches of {payload.batch_size} to Snowflake."
    return {
        "status": "started",
        "message": msg_str,
        "topic": payload.topic_name,
        "batch_size": payload.batch_size,
        "num_consumers": payload.num_consumers
    }

@app.get("/api/kafka/status")
def get_kafka_status():
    """
    Returns live progress metrics and logs for the active/recent Kafka streaming pipeline.
    """
    return kafka_stream_manager.get_status_dict()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
