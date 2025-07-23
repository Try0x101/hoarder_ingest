import os
import orjson
import ijson
import asyncio
import aiosqlite
import time
import psutil
import redis as sync_redis
from celery import shared_task
from app.services import job_manager, stream_processor
from app.processing import validate_data, orjson_decimal_default
from app.services.timestamp_calculator import calculate_ingestion_timestamps, format_for_db
from app.database import save_telemetry_batch, DB_PATH
from datetime import datetime
from app.utils.async_helpers import AsyncGeneratorReader
from typing import Dict, List, Tuple, Any
from collections import defaultdict

TARGET_DB_SIZE_MB = 900
MAX_DB_SIZE_MB = 1000
CHUNK_SIZE = 1000
REDIS_METRICS_URL = "redis://localhost:6378/2"

async def _async_cleanup_db():
    if not os.path.exists(DB_PATH): return
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    if db_size < MAX_DB_SIZE_MB: return
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            while db_size > TARGET_DB_SIZE_MB:
                cursor = await db.execute("DELETE FROM telemetry WHERE id IN (SELECT id FROM telemetry ORDER BY id ASC LIMIT 1000)")
                if cursor.rowcount == 0: break
                await db.commit()
                await db.execute("VACUUM")
                await db.commit()
                db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    except Exception as e:
        print(f"Error during DB cleanup: {e}")

@shared_task(name='tasks.cleanup_db')
def cleanup_db():
    asyncio.run(_async_cleanup_db())

async def _process_and_save_chunk(records, headers, client_ip, headers_json, received_at, request_id, bts_context, start_index, avg_record_size):
    valid_payloads, valid_device_ids = [], []
    for payload in records:
        is_valid, _, device_id = validate_data(payload, headers, client_ip)
        if is_valid:
            valid_payloads.append(payload)
            valid_device_ids.append(device_id)

    if not valid_payloads: return 0, len(records), bts_context

    event_times, new_bts_context = calculate_ingestion_timestamps(valid_payloads, received_at, bts_context, start_index)
    
    db_records = [(valid_device_ids[i], orjson.dumps(p, default=orjson_decimal_default).decode(), headers_json, format_for_db(event_times[i]), request_id, avg_record_size) for i, p in enumerate(valid_payloads)]

    if db_records: await save_telemetry_batch(db_records)

    return len(db_records), len(records) - len(db_records), new_bts_context

async def _async_bulk_processor(job_context: dict):
    job_id, temp_file_path, metadata = job_context["job_id"], job_context["temp_file_path"], job_context["metadata"]
    total_processed, total_skipped, total_records_in_file = 0, 0, 0
    bts_context = defaultdict(list)
    try:
        await job_manager.set_job_status(job_id, {"status": "PROCESSING", "details": "Analyzing file..."})
        total_file_size = os.path.getsize(temp_file_path)

        count_stream = await stream_processor.get_decompressed_stream(temp_file_path, metadata.get("headers", {}).get("content-encoding"))
        async for _ in ijson.items_async(AsyncGeneratorReader(count_stream), 'item'):
            total_records_in_file += 1
        
        if total_records_in_file == 0:
            await job_manager.set_job_status(job_id, {"status": "COMPLETE", "processed": 0, "skipped": 0, "total_records": 0})
            return

        avg_record_size = total_file_size // total_records_in_file if total_records_in_file > 0 else 0
        
        await job_manager.set_job_status(job_id, {"status": "PROCESSING", "details": "Starting file stream processing."})
        headers, client_ip, request_id = metadata.get("headers", {}), metadata.get("client_ip", "Unknown"), job_id
        headers_json = orjson.dumps({"client_ip": client_ip, "headers": headers}).decode()
        received_at = datetime.fromisoformat(metadata["received_at"])
        
        process_stream = await stream_processor.get_decompressed_stream(temp_file_path, headers.get("content-encoding"))
        reader = AsyncGeneratorReader(process_stream)
        chunk = []
        current_record_index = 0
        async for record in ijson.items_async(reader, 'item'):
            chunk.append(record)
            if len(chunk) >= CHUNK_SIZE:
                processed, skipped, bts_context = await _process_and_save_chunk(chunk, headers, client_ip, headers_json, received_at, request_id, bts_context, current_record_index, avg_record_size)
                total_processed += processed; total_skipped += skipped; current_record_index += len(chunk)
                await job_manager.set_job_status(job_id, {"status": "PROCESSING", "processed": total_processed, "skipped": total_skipped})
                chunk = []
        if chunk:
            processed, skipped, bts_context = await _process_and_save_chunk(chunk, headers, client_ip, headers_json, received_at, request_id, bts_context, current_record_index, avg_record_size)
            total_processed += processed; total_skipped += skipped
        
        await job_manager.set_job_status(job_id, {"status": "COMPLETE", "processed": total_processed, "skipped": total_skipped, "total_records": total_records_in_file})
    except Exception as e:
        await job_manager.set_job_status(job_id, {"status": "FAILED", "error": f"{type(e).__name__}: {e}", "processed": total_processed})
        raise
    finally:
        if os.path.exists(temp_file_path): os.remove(temp_file_path)

@shared_task(name='ingest.process_file')
def process_bulk_file_task(job_context: dict):
    asyncio.run(_async_bulk_processor(job_context))

def _get_app_processes():
    procs = []
    try:
        for p in psutil.process_iter(['pid', 'cmdline']):
            if p.info['cmdline']:
                cmd = " ".join(p.info['cmdline'])
                if 'uvicorn app.main:app' in cmd or 'celery -A celery_app worker' in cmd or 'celery -A celery_app beat' in cmd:
                    procs.append(p)
    except psutil.Error: pass
    return procs

@shared_task(name='tasks.monitor_system')
def monitor_system():
    try:
        processes = _get_app_processes()
        if not processes: return
        for p in processes: p.cpu_percent()
        time.sleep(0.5)
        total_cpu = sum(p.cpu_percent() for p in processes)
        total_mem = sum(p.memory_info().rss for p in processes)
        r = sync_redis.from_url(REDIS_METRICS_URL)
        data = orjson.dumps({"ts": time.time(), "cpu_percent": total_cpu, "mem_rss_bytes": total_mem})
        pipe = r.pipeline()
        pipe.lpush("system_stats", data)
        pipe.ltrim("system_stats", 0, 399)
        pipe.execute()
        r.close()
    except (psutil.Error, sync_redis.RedisError): pass
