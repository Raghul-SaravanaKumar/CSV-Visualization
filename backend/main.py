import io
import os
import re
import traceback
import pandas as pd
from typing import Optional
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

app = FastAPI(
    title="CSV Executer & Snowflake Sync API",
    description="High-performance backend API for analyzing, processing, and saving CSV datasets to Snowflake using Pandas.",
    version="2.0.0"
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
    schema_: Optional[str] = None  # Using schema_ to avoid keyword conflict

class SnowflakeSaveRequest(BaseModel):
    table_name: str
    credentials: Optional[SnowflakeCredentials] = None
    if_exists: str = "replace"  # "replace" or "append"

def get_snowflake_connection(creds: Optional[SnowflakeCredentials] = None):
    """
    Connects to Snowflake using provided credentials or falls back to environment variables.
    """
    if not SNOWFLAKE_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="snowflake-connector-python is not installed. Please run: pip install snowflake-connector-python[pandas]"
        )

    # Resolve connection details (payload override takes priority over .env)
    account = (creds.account if creds and creds.account else os.getenv("SNOWFLAKE_ACCOUNT"))
    if account:
        # Smart cleanup: if user pastes full URL like https://bttwlpr-xv06554.snowflakecomputing.com
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
        "service": "CSV Executer & Snowflake Sync API",
        "loaded_file": app_state["file_name"],
        "rows": len(app_state["current_df"]) if app_state["current_df"] is not None else 0,
        "snowflake_driver_ready": SNOWFLAKE_AVAILABLE
    }

@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)):
    """
    Receives a CSV file upload, parses it using Pandas, stores it in memory,
    and returns comprehensive statistical metadata and preview data.
    """
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only .csv files are supported.")

    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))

        # Store globally
        app_state["current_df"] = df
        app_state["file_name"] = file.filename

        # Clean NaN values for JSON serialization
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
    """
    Executes a Pandas filter/query expression or dataframe operation on the loaded CSV.
    """
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
    """
    Tests and validates Snowflake connection using provided credentials or .env defaults.
    """
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
    """
    Saves the currently loaded CSV DataFrame directly into a Snowflake database table using write_pandas.
    Automatically formats column names to uppercase SQL-compatible identifiers.
    """
    df = app_state["current_df"]
    if df is None:
        raise HTTPException(
            status_code=400,
            detail="No CSV data loaded on server. Please upload your CSV first before saving to Snowflake."
        )

    table_name = payload.table_name.strip()
    if not table_name:
        raise HTTPException(status_code=400, detail="Target table name is required.")

    # Clean and uppercase table name (Snowflake standard)
    clean_table_name = re.sub(r'[^A-Za-z0-9_]', '_', table_name).upper()

    # Clean column names for Snowflake SQL compatibility
    df_to_save = df.copy()
    df_to_save.columns = [
        re.sub(r'[^A-Za-z0-9_]', '_', col).upper() for col in df_to_save.columns
    ]

    conn, info = get_snowflake_connection(payload.credentials)
    try:
        # If replace mode and table exists, drop it or use write_pandas overwrite
        # write_pandas auto_create_table=True creates table if it doesn't exist
        overwrite = (payload.if_exists == "replace")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df_to_save,
            table_name=clean_table_name,
            database=info["database"] if info["database"] != "DEFAULT" else None,
            schema=info["schema"] if info["schema"] != "DEFAULT" else "PUBLIC",
            auto_create_table=True,
            overwrite=overwrite
        )

        conn.close()

        if not success:
            raise HTTPException(status_code=500, detail="write_pandas reported failure when inserting into Snowflake.")

        return {
            "status": "success",
            "message": f"Successfully inserted {nrows:,} rows into Snowflake table '{clean_table_name}'.",
            "details": {
                "table_name": clean_table_name,
                "rows_inserted": nrows,
                "chunks_written": nchunks,
                "database": info["database"],
                "schema": info["schema"]
            }
        }
    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Snowflake write error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
