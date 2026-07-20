import asyncio
import json
import time
import re
import traceback
import pandas as pd
from typing import Optional, Dict, Any, List

try:
    from kafka import KafkaProducer, KafkaConsumer
    KAFKA_PYTHON_AVAILABLE = True
except ImportError:
    KAFKA_PYTHON_AVAILABLE = False

try:
    from snowflake.connector.pandas_tools import write_pandas
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

class KafkaStreamManager:
    """
    Manages state and logs for the real-time Apache Kafka CSV-to-Snowflake streaming pipeline.
    """
    def __init__(self):
        self.is_running = False
        self.status = "idle"  # idle, streaming, completed, error
        self.mode = "Simulated Kafka Broker"
        self.topic_name = "sf-csv-stream"
        self.batch_size = 10
        self.total_rows = 0
        self.messages_produced = 0
        self.messages_consumed = 0
        self.batches_uploaded = 0
        self.logs: List[str] = []
        self.error_message = None

    def reset(self, total_rows: int, topic_name: str, batch_size: int, mode: str):
        self.is_running = True
        self.status = "streaming"
        self.mode = mode
        self.topic_name = topic_name
        self.batch_size = batch_size
        self.total_rows = total_rows
        self.messages_produced = 0
        self.messages_consumed = 0
        self.batches_uploaded = 0
        self.logs = []
        self.error_message = None
        self.add_log(f"[System] Initializing Apache Kafka Stream (`{mode}`)...")
        self.add_log(f"[System] Topic: '{topic_name}' | Batch Chunk Size: {batch_size} records | Total Dataset: {total_rows} rows.")

    def add_log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.logs.append(log_entry)
        # Keep last 150 logs in memory
        if len(self.logs) > 150:
            self.logs = self.logs[-150:]

    def get_status_dict(self):
        return {
            "is_running": self.is_running,
            "status": self.status,
            "mode": self.mode,
            "topic_name": self.topic_name,
            "batch_size": self.batch_size,
            "total_rows": self.total_rows,
            "messages_produced": self.messages_produced,
            "messages_consumed": self.messages_consumed,
            "batches_uploaded": self.batches_uploaded,
            "logs": self.logs,
            "error_message": self.error_message
        }

# Global instance
kafka_stream_manager = KafkaStreamManager()

async def run_kafka_pipeline(
    df: pd.DataFrame,
    topic_name: str,
    batch_size: int,
    table_name: str,
    use_real_kafka: bool,
    bootstrap_servers: str,
    get_conn_fn,
    creds_payload: Any
):
    """
    Executes the complete Kafka streaming pipeline:
      1. Producer publishes DataFrame rows to the Kafka topic.
      2. Consumer chunks messages into exact `batch_size` (e.g. 10) records.
      3. Consumer executes Snowflake `write_pandas` for each 10-record chunk.
    """
    mode_str = f"Real Kafka Server ({bootstrap_servers})" if use_real_kafka else "Async Kafka Broker Simulator"
    kafka_stream_manager.reset(len(df), topic_name, batch_size, mode_str)

    # Clean target table name & column names to uppercase SQL identifiers
    clean_table_name = re.sub(r'[^A-Za-z0-9_]', '_', table_name).upper()
    df_clean = df.copy()
    df_clean.columns = [re.sub(r'[^A-Za-z0-9_]', '_', col).upper() for col in df_clean.columns]
    records = df_clean.fillna("").to_dict(orient="records")

    try:
        # Check Snowflake connectivity before starting stream
        conn, info = get_conn_fn(creds_payload)
        kafka_stream_manager.add_log(f"[Snowflake DB] Connected to database '{info['database']}', schema '{info['schema']}'.")
        
        if use_real_kafka:
            if not KAFKA_PYTHON_AVAILABLE:
                raise Exception("kafka-python library not available. Install with `pip install kafka-python`.")
            kafka_stream_manager.add_log(f"[Kafka Broker] Connecting to external broker at {bootstrap_servers}...")
            # Run real Kafka producer/consumer in threads/executor
            await run_real_kafka_stream(records, topic_name, batch_size, clean_table_name, conn, info, bootstrap_servers)
        else:
            # Run simulated in-memory Async Kafka broker engine
            await run_simulated_kafka_stream(records, topic_name, batch_size, clean_table_name, conn, info)

        conn.close()
        kafka_stream_manager.status = "completed"
        kafka_stream_manager.is_running = False
        kafka_stream_manager.add_log(f"[Kafka Pipeline] 🏁 Stream completed successfully! All {kafka_stream_manager.total_rows} records inserted into Snowflake table '{clean_table_name}' over {kafka_stream_manager.batches_uploaded} batches.")
    except Exception as e:
        kafka_stream_manager.status = "error"
        kafka_stream_manager.is_running = False
        kafka_stream_manager.error_message = str(e)
        kafka_stream_manager.add_log(f"[Error] ❌ Pipeline halted: {str(e)}")
        traceback.print_exc()

async def run_simulated_kafka_stream(
    records: List[Dict[str, Any]],
    topic_name: str,
    batch_size: int,
    table_name: str,
    conn,
    info: Dict[str, Any]
):
    """
    Simulates Kafka topic partitioning, offsets, and consumer chunking asynchronously.
    """
    queue = asyncio.Queue()

    # Producer task: emit rows to queue
    async def producer_task():
        for idx, row in enumerate(records):
            offset = idx + 1
            await queue.put((offset, row))
            kafka_stream_manager.messages_produced = offset
            if offset % batch_size == 0 or offset == len(records):
                kafka_stream_manager.add_log(f"[Kafka Producer] Published records #1..#{offset} to topic '{topic_name}'")
            await asyncio.sleep(0.04)  # Slight delay to visualize streaming animation
        await queue.put(None)  # Sentinel for end of stream

    # Consumer task: buffer into chunks of `batch_size` (10) and write to Snowflake
    async def consumer_task():
        batch_buffer = []
        batch_index = 0

        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            offset, row = item
            batch_buffer.append(row)

            # When batch reaches target size (10) or at the end
            if len(batch_buffer) == batch_size or (offset == len(records) and len(batch_buffer) > 0):
                batch_index += 1
                batch_df = pd.DataFrame(batch_buffer)
                kafka_stream_manager.add_log(f"[Kafka Consumer] Collected Batch #{batch_index} ({len(batch_buffer)} records). Uploading to Snowflake table '{table_name}'...")

                # Write to Snowflake
                # Overwrite on Batch #1 (so table is cleanly initialized), then append on subsequent batches
                overwrite = (batch_index == 1)
                
                # Execute write_pandas synchronously in thread pool to avoid blocking async loop
                loop = asyncio.get_running_loop()
                success, nchunks, nrows, _ = await loop.run_in_executor(
                    None,
                    lambda: write_pandas(
                        conn=conn,
                        df=batch_df,
                        table_name=table_name,
                        database=info["database"] if info["database"] != "DEFAULT" else None,
                        schema=info["schema"] if info["schema"] != "DEFAULT" else "PUBLIC",
                        auto_create_table=True,
                        overwrite=overwrite
                    )
                )

                if not success:
                    raise Exception(f"Failed to write Batch #{batch_index} to Snowflake (`write_pandas` returned false).")

                kafka_stream_manager.messages_consumed += len(batch_buffer)
                kafka_stream_manager.batches_uploaded = batch_index
                kafka_stream_manager.add_log(f"[Snowflake DB] ✔️ Batch #{batch_index} inserted successfully ({nrows} rows added to {table_name}).")
                batch_buffer = []
                await asyncio.sleep(0.25)  # Pacing between batches for clear visualization

            queue.task_done()

    # Run producer and consumer concurrently
    await asyncio.gather(producer_task(), consumer_task())

async def run_real_kafka_stream(
    records: List[Dict[str, Any]],
    topic_name: str,
    batch_size: int,
    table_name: str,
    conn,
    info: Dict[str, Any],
    bootstrap_servers: str
):
    """
    Connects to a real Apache Kafka broker using `kafka-python`.
    """
    loop = asyncio.get_running_loop()

    # Initialize Producer
    def init_producer():
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    producer = await loop.run_in_executor(None, init_producer)
    kafka_stream_manager.add_log(f"[Kafka Producer] Connected to broker {bootstrap_servers}. Publishing {len(records)} records...")

    # Publish all records
    for idx, row in enumerate(records):
        producer.send(topic_name, value=row)
        kafka_stream_manager.messages_produced = idx + 1
        if (idx + 1) % batch_size == 0 or (idx + 1) == len(records):
            kafka_stream_manager.add_log(f"[Kafka Producer] Sent record #{idx + 1} to topic '{topic_name}'")
        await asyncio.sleep(0.02)
    
    producer.flush()
    producer.close()
    kafka_stream_manager.add_log(f"[Kafka Producer] All {len(records)} records flushed to topic '{topic_name}'.")

    # Initialize Consumer and consume in batches of 10
    kafka_stream_manager.add_log(f"[Kafka Consumer] Starting consumer on topic '{topic_name}' with `auto_offset_reset='earliest'`...")
    
    def consume_batches():
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers.split(","),
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000, # Stop when topic is drained for 5 seconds
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        batch_buffer = []
        batch_index = 0
        total_consumed = 0

        for message in consumer:
            batch_buffer.append(message.value)
            total_consumed += 1

            if len(batch_buffer) == batch_size or total_consumed == len(records):
                batch_index += 1
                batch_df = pd.DataFrame(batch_buffer)
                kafka_stream_manager.add_log(f"[Kafka Consumer] Collected Batch #{batch_index} ({len(batch_buffer)} records). Pushing to Snowflake table '{table_name}'...")

                overwrite = (batch_index == 1)
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=batch_df,
                    table_name=table_name,
                    database=info["database"] if info["database"] != "DEFAULT" else None,
                    schema=info["schema"] if info["schema"] != "DEFAULT" else "PUBLIC",
                    auto_create_table=True,
                    overwrite=overwrite
                )

                if not success:
                    raise Exception(f"Snowflake write error on Batch #{batch_index}")

                kafka_stream_manager.messages_consumed += len(batch_buffer)
                kafka_stream_manager.batches_uploaded = batch_index
                kafka_stream_manager.add_log(f"[Snowflake DB] ✔️ Batch #{batch_index} inserted successfully ({nrows} rows).")
                batch_buffer = []
                if total_consumed >= len(records):
                    break
        
        consumer.close()

    await loop.run_in_executor(None, consume_batches)
