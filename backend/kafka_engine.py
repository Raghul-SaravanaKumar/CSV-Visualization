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
    Supports both Single Consumer (`1`) and Multi-Consumer Fan-Out (`2` independent consumers to 2 tables).
    """
    def __init__(self):
        self.is_running = False
        self.status = "idle"  # idle, streaming, completed, error
        self.mode = "Simulated Kafka Broker"
        self.topic_name = "sf-csv-stream"
        self.batch_size = 10
        self.total_rows = 0
        self.num_consumers = 1
        
        self.messages_produced = 0
        self.consumer_1_messages = 0
        self.consumer_1_batches = 0
        self.consumer_1_table = ""
        
        self.consumer_2_messages = 0
        self.consumer_2_batches = 0
        self.consumer_2_table = ""
        
        self.logs: List[str] = []
        self.error_message = None

    def reset(self, total_rows: int, topic_name: str, batch_size: int, mode: str, num_consumers: int, table_1: str, table_2: str):
        self.is_running = True
        self.status = "streaming"
        self.mode = mode
        self.topic_name = topic_name
        self.batch_size = batch_size
        self.total_rows = total_rows
        self.num_consumers = num_consumers
        
        self.messages_produced = 0
        self.consumer_1_messages = 0
        self.consumer_1_batches = 0
        self.consumer_1_table = table_1
        
        self.consumer_2_messages = 0
        self.consumer_2_batches = 0
        self.consumer_2_table = table_2 if num_consumers > 1 else ""
        
        self.logs = []
        self.error_message = None
        self.add_log(f"[System] Initializing Apache Kafka Stream (`{mode}`)...")
        if num_consumers == 1:
            self.add_log(f"[System] 1 Producer -> 1 Consumer (`{table_1}`) | Batch Size: {batch_size} | Total Rows: {total_rows}")
        else:
            self.add_log(f"[System] 🚀 Fan-Out Architecture: 1 Producer -> 2 Independent Consumers (`{table_1}` & `{table_2}`) | Batch Size: {batch_size}")

    def add_log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.logs.append(log_entry)
        if len(self.logs) > 200:
            self.logs = self.logs[-200:]

    def get_status_dict(self):
        return {
            "is_running": self.is_running,
            "status": self.status,
            "mode": self.mode,
            "topic_name": self.topic_name,
            "batch_size": self.batch_size,
            "total_rows": self.total_rows,
            "num_consumers": self.num_consumers,
            "messages_produced": self.messages_produced,
            # Backwards compatibility fields
            "messages_consumed": self.consumer_1_messages,
            "batches_uploaded": self.consumer_1_batches,
            # Consumer 1 details
            "consumer_1_messages": self.consumer_1_messages,
            "consumer_1_batches": self.consumer_1_batches,
            "consumer_1_table": self.consumer_1_table,
            # Consumer 2 details
            "consumer_2_messages": self.consumer_2_messages,
            "consumer_2_batches": self.consumer_2_batches,
            "consumer_2_table": self.consumer_2_table,
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
    creds_payload: Any,
    num_consumers: int = 1,
    table_name_2: Optional[str] = None
):
    """
    Executes the complete Kafka streaming pipeline with fan-out support:
      - 1 Producer emitting DataFrame records to topic.
      - 1 or 2 Independent Consumers chunking records into `batch_size` (10) and inserting into Snowflake tables.
    """
    mode_str = f"Real Kafka Server ({bootstrap_servers})" if use_real_kafka else "Async Kafka Broker Simulator"
    clean_table_1 = re.sub(r'[^A-Za-z0-9_]', '_', table_name).upper()
    clean_table_2 = re.sub(r'[^A-Za-z0-9_]', '_', table_name_2).upper() if table_name_2 else ""
    
    if num_consumers == 2 and not clean_table_2:
        clean_table_2 = f"{clean_table_1}_2"

    kafka_stream_manager.reset(len(df), topic_name, batch_size, mode_str, num_consumers, clean_table_1, clean_table_2)

    df_clean = df.copy()
    df_clean.columns = [re.sub(r'[^A-Za-z0-9_]', '_', col).upper() for col in df_clean.columns]
    records = df_clean.fillna("").to_dict(orient="records")

    try:
        conn, info = get_conn_fn(creds_payload)
        kafka_stream_manager.add_log(f"[Snowflake DB] Connected to database '{info['database']}', schema '{info['schema']}'.")
        
        if use_real_kafka:
            if not KAFKA_PYTHON_AVAILABLE:
                raise Exception("kafka-python library not available. Install with `pip install kafka-python`.")
            kafka_stream_manager.add_log(f"[Kafka Broker] Connecting to external broker at {bootstrap_servers}...")
            await run_real_kafka_stream(records, topic_name, batch_size, clean_table_1, clean_table_2, num_consumers, conn, info, bootstrap_servers)
        else:
            await run_simulated_kafka_stream(records, topic_name, batch_size, clean_table_1, clean_table_2, num_consumers, conn, info)

        conn.close()
        kafka_stream_manager.status = "completed"
        kafka_stream_manager.is_running = False
        
        if num_consumers == 1:
            kafka_stream_manager.add_log(f"[Kafka Pipeline] 🏁 Stream completed! All {kafka_stream_manager.total_rows} records inserted into Snowflake table '{clean_table_1}'.")
        else:
            kafka_stream_manager.add_log(f"[Kafka Pipeline] 🏁 Fan-Out Stream completed! All {kafka_stream_manager.total_rows} records inserted independently into BOTH '{clean_table_1}' ({kafka_stream_manager.consumer_1_batches} batches) and '{clean_table_2}' ({kafka_stream_manager.consumer_2_batches} batches).")
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
    table_1: str,
    table_2: str,
    num_consumers: int,
    conn,
    info: Dict[str, Any]
):
    """
    Simulates Kafka topic partitioning, offsets, and multi-consumer fan-out asynchronously.
    """
    queue_1 = asyncio.Queue()
    queue_2 = asyncio.Queue() if num_consumers > 1 else None

    # Producer task: emit rows to all active consumer subscriber queues
    async def producer_task():
        for idx, row in enumerate(records):
            offset = idx + 1
            await queue_1.put((offset, row))
            if queue_2:
                await queue_2.put((offset, row))
                
            kafka_stream_manager.messages_produced = offset
            if offset % batch_size == 0 or offset == len(records):
                kafka_stream_manager.add_log(f"[Kafka Producer] Published records #1..#{offset} to topic '{topic_name}'")
            await asyncio.sleep(0.04)
            
        await queue_1.put(None)
        if queue_2:
            await queue_2.put(None)

    # Consumer task: buffer into chunks of `batch_size` (10) and write to Snowflake
    async def consumer_task(queue: asyncio.Queue, target_table: str, consumer_id: int):
        batch_buffer = []
        batch_index = 0

        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            offset, row = item
            batch_buffer.append(row)

            if len(batch_buffer) == batch_size or (offset == len(records) and len(batch_buffer) > 0):
                batch_index += 1
                batch_df = pd.DataFrame(batch_buffer)
                
                c_tag = f"[Consumer #{consumer_id} -> {target_table}]"
                kafka_stream_manager.add_log(f"{c_tag} Collected Batch #{batch_index} ({len(batch_buffer)} records). Uploading to Snowflake...")

                overwrite = (batch_index == 1)
                loop = asyncio.get_running_loop()
                success, nchunks, nrows, _ = await loop.run_in_executor(
                    None,
                    lambda: write_pandas(
                        conn=conn,
                        df=batch_df,
                        table_name=target_table,
                        database=info["database"] if info["database"] != "DEFAULT" else None,
                        schema=info["schema"] if info["schema"] != "DEFAULT" else "PUBLIC",
                        auto_create_table=True,
                        overwrite=overwrite
                    )
                )

                if not success:
                    raise Exception(f"Failed to write Batch #{batch_index} for Consumer #{consumer_id} to Snowflake.")

                if consumer_id == 1:
                    kafka_stream_manager.consumer_1_messages += len(batch_buffer)
                    kafka_stream_manager.consumer_1_batches = batch_index
                else:
                    kafka_stream_manager.consumer_2_messages += len(batch_buffer)
                    kafka_stream_manager.consumer_2_batches = batch_index

                db_tag = f"[Snowflake DB - Table {consumer_id}]"
                kafka_stream_manager.add_log(f"{db_tag} ✔️ Batch #{batch_index} inserted successfully ({nrows} rows added to {target_table}).")
                batch_buffer = []
                await asyncio.sleep(0.25)

            queue.task_done()

    # Run producer and 1 or 2 consumers concurrently
    tasks = [producer_task(), consumer_task(queue_1, table_1, consumer_id=1)]
    if num_consumers > 1 and queue_2:
        tasks.append(consumer_task(queue_2, table_2, consumer_id=2))
        
    await asyncio.gather(*tasks)

async def run_real_kafka_stream(
    records: List[Dict[str, Any]],
    topic_name: str,
    batch_size: int,
    table_1: str,
    table_2: str,
    num_consumers: int,
    conn,
    info: Dict[str, Any],
    bootstrap_servers: str
):
    """
    Connects to a real Apache Kafka broker and runs 1 or 2 independent consumer groups.
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

    for idx, row in enumerate(records):
        producer.send(topic_name, value=row)
        kafka_stream_manager.messages_produced = idx + 1
        if (idx + 1) % batch_size == 0 or (idx + 1) == len(records):
            kafka_stream_manager.add_log(f"[Kafka Producer] Sent record #{idx + 1} to topic '{topic_name}'")
        await asyncio.sleep(0.02)
    
    producer.flush()
    producer.close()
    kafka_stream_manager.add_log(f"[Kafka Producer] All {len(records)} records flushed to topic '{topic_name}'.")

    # Initialize Consumer function
    def consume_batches(target_table: str, consumer_id: int, group_id: str):
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
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
                
                c_tag = f"[Consumer #{consumer_id} -> {target_table}]"
                kafka_stream_manager.add_log(f"{c_tag} Collected Batch #{batch_index} ({len(batch_buffer)} records). Pushing to Snowflake...")

                overwrite = (batch_index == 1)
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=batch_df,
                    table_name=target_table,
                    database=info["database"] if info["database"] != "DEFAULT" else None,
                    schema=info["schema"] if info["schema"] != "DEFAULT" else "PUBLIC",
                    auto_create_table=True,
                    overwrite=overwrite
                )

                if not success:
                    raise Exception(f"Snowflake write error on Batch #{batch_index} for Consumer #{consumer_id}")

                if consumer_id == 1:
                    kafka_stream_manager.consumer_1_messages += len(batch_buffer)
                    kafka_stream_manager.consumer_1_batches = batch_index
                else:
                    kafka_stream_manager.consumer_2_messages += len(batch_buffer)
                    kafka_stream_manager.consumer_2_batches = batch_index

                db_tag = f"[Snowflake DB - Table {consumer_id}]"
                kafka_stream_manager.add_log(f"{db_tag} ✔️ Batch #{batch_index} inserted successfully ({nrows} rows).")
                batch_buffer = []
                if total_consumed >= len(records):
                    break
        
        consumer.close()

    tasks = [loop.run_in_executor(None, lambda: consume_batches(table_1, 1, "sf-fanout-group-1"))]
    if num_consumers > 1:
        tasks.append(loop.run_in_executor(None, lambda: consume_batches(table_2, 2, "sf-fanout-group-2")))
        
    await asyncio.gather(*tasks)
