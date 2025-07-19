import os
import orjson
import ijson
import asyncio
from celery import shared_task
from app.services import job_manager, stream_processor
from app.processing import validate_data
from app.services.timestamp_calculator import calculate_ingestion_timestamps, format_for_db
from app.database import save_telemetry_batch
from datetime import datetime
from app.utils.async_helpers import AsyncGeneratorReader

async def _process_and_save_data(all_records, headers, client_ip, headers_json, received_at, request_id):
    valid_payloads, valid_device_ids, skipped_count = [], [], 0
    for payload in all_records:
        is_valid, _, device_id = validate_data(payload, headers, client_ip)
        if is_valid:
            valid_payloads.append(payload)
            valid_device_ids.append(device_id)
        else:
            skipped_count += 1
    
    if not valid_payloads:
        return 0, skipped_count

    event_times = calculate_ingestion_timestamps(valid_payloads, received_at)
    
    db_records = []
    for i, p in enumerate(valid_payloads):
        db_records.append((valid_device_ids[i], orjson.dumps(p).decode(), headers_json, format_for_db(event_times[i]), request_id))
    
    if db_records:
        await save_telemetry_batch(db_records)
        
    return len(db_records), skipped_count

async def _async_bulk_processor(job_context: dict):
    job_id, temp_file_path, metadata = job_context["job_id"], job_context["temp_file_path"], job_context["metadata"]
    
    try:
        await job_manager.set_job_status(job_id, {"status": "PROCESSING", "details": "Reading and parsing file."})
        
        try:
            headers, client_ip, request_id = metadata.get("headers", {}), metadata.get("client_ip", "Unknown"), job_id
            headers_json = orjson.dumps({"client_ip": client_ip, "headers": headers}).decode()
            received_at = datetime.fromisoformat(metadata["received_at"])
            
            stream_generator = await stream_processor.get_decompressed_stream(temp_file_path, headers.get("content-encoding"))
            async_file_reader = AsyncGeneratorReader(stream_generator)

            all_records = [record async for record in ijson.items_async(async_file_reader, 'item')]

            await job_manager.set_job_status(job_id, {"status": "PROCESSING", "details": f"Processing {len(all_records)} records."})

            processed, skipped = await _process_and_save_data(all_records, headers, client_ip, headers_json, received_at, request_id)

            await job_manager.set_job_status(job_id, {"status": "COMPLETE", "processed": processed, "skipped": skipped, "total_records": len(all_records)})

        except Exception as e:
            await job_manager.set_job_status(job_id, {"status": "FAILED", "error": f"{type(e).__name__}: {e}"})
            raise
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

@shared_task(name='ingest.process_file')
def process_bulk_file_task(job_context: dict):
    asyncio.run(_async_bulk_processor(job_context))
