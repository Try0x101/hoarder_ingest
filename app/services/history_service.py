import orjson
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from app.database import get_historical_telemetry
from app.utils.formatters import format_utc_timestamp
from .timestamp_calculator import format_display_timestamp

def _check_for_warnings(record: Dict[str, Any]) -> List[str]:
    warnings = []
    calculated_ts_str = record.get("calculated_event_timestamp")
    received_ts_str = record.get("received_at")

    if calculated_ts_str and received_ts_str:
        try:
            if '.' in calculated_ts_str:
                calculated_dt = datetime.strptime(calculated_ts_str, "%Y-%m-%d %H:%M:%S.%f")
            else:
                calculated_dt = datetime.strptime(calculated_ts_str, "%Y-%m-%d %H:%M:%S")

            received_dt = datetime.strptime(received_ts_str, "%Y-%m-%d %H:%M:%S")
            
            if calculated_dt > received_dt + timedelta(seconds=5):
                time_diff = round((calculated_dt - received_dt).total_seconds())
                warnings.append(f"client_clock_skew_detected: event_time is {time_diff}s after receive_time")
        except (ValueError, TypeError):
            pass
            
    return warnings

async def process_device_history(device_id: Optional[str], limit: int, cursor: Optional[str], base_url: str) -> Dict[str, Any]:
    cursor_ts: Optional[str] = None
    cursor_id: Optional[int] = None
    if cursor:
        try:
            parts = cursor.split(',')
            if len(parts) == 2:
                cursor_ts = parts[0]
                cursor_id = int(parts[1])
        except (ValueError, IndexError):
            pass

    history_data = await get_historical_telemetry(device_id, limit, cursor_ts, cursor_id)
    history_data.sort(key=lambda r: (r.get('calculated_event_timestamp', '0'), r.get('id', 0)), reverse=True)

    base_params = [f"limit={limit}"]
    if device_id:
        base_params.append(f"device_id={device_id}")

    self_params = base_params[:]
    if cursor:
        self_params.append(f"cursor={quote_plus(cursor)}")
    self_url = f"{base_url}/data/history?{'&'.join(self_params)}"
    
    request_block = {"self_url": self_url}
    navigation_block = {"root": f"{base_url}/"}

    if device_id:
        navigation_block["latest"] = f"{base_url}/data/latest/{device_id}"

    if not history_data:
        return {
            "request": request_block,
            "navigation": navigation_block,
            "pagination": {"limit": limit, "records_returned": 0, "next_cursor": None},
            "data": [],
            "message": f"No data found for device '{device_id}'" if device_id else "No data found for the given query."
        }

    processed_data = []
    for record in history_data:
        try:
            payload = orjson.loads(record["payload"]) if isinstance(record["payload"], str) else record["payload"]
        except orjson.JSONDecodeError:
            payload = {"error": "invalid json stored in db"}
        request_info = orjson.loads(record.get("request_headers")) if record.get("request_headers") else None
        
        processed_record = {
            "id": record["id"],
            "request_id": record.get("request_id"),
            "device_id": record.get("device_id"),
            "payload": payload,
            "data_timestamp_calculated": format_display_timestamp(record.get("calculated_event_timestamp")),
            "received_at": format_utc_timestamp(record.get("received_at")),
            "request_info": request_info,
            "warnings": _check_for_warnings(record)
        }
        processed_data.append(processed_record)

    next_cursor_obj, next_cursor_str = None, None
    if len(history_data) == limit:
        last_record = history_data[-1]
        next_cursor_str = f"{last_record['calculated_event_timestamp']},{last_record['id']}"
        next_cursor_obj = {
            "raw": next_cursor_str,
            "timestamp": format_display_timestamp(last_record.get("calculated_event_timestamp")),
            "id": last_record['id']
        }
        navigation_block["next_page"] = f"{base_url}/data/history?{'&'.join(base_params + [f'cursor={quote_plus(next_cursor_str)}'])}"
        
    if cursor:
        navigation_block["first_page"] = f"{base_url}/data/history?{'&'.join(base_params)}"

    pagination_block = {
        "limit": limit,
        "records_returned": len(processed_data),
        "next_cursor": next_cursor_obj,
        "time_range": {
            "start": format_display_timestamp(history_data[0].get("calculated_event_timestamp")),
            "end": format_display_timestamp(history_data[-1].get("calculated_event_timestamp")),
        }
    }

    response = {
        "request": request_block,
        "navigation": navigation_block,
        "pagination": pagination_block,
        "data": processed_data
    }
    
    if device_id:
        response["device_id"] = device_id
        
    return response

async def get_latest_device_record(device_id: str, base_url: str) -> Dict[str, Any]:
    history_data = await get_historical_telemetry(device_id, 1)
    if not history_data:
        return None
    
    record = history_data[0]
    
    try:
        payload = orjson.loads(record["payload"]) if isinstance(record["payload"], str) else record["payload"]
    except orjson.JSONDecodeError:
        payload = {"error": "invalid json stored in db"}

    request_info = None
    headers_str = record.get("request_headers")
    if headers_str:
        try:
            request_info = orjson.loads(headers_str)
        except orjson.JSONDecodeError:
            request_info = {"error": "invalid headers json in db"}

    data_payload = {
        "id": record["id"],
        "request_id": record.get("request_id"),
        "device_id": device_id,
        "payload": payload,
        "data_timestamp_calculated": format_display_timestamp(record.get("calculated_event_timestamp")),
        "received_at": format_utc_timestamp(record.get("received_at"))
    }
    
    if request_info:
        data_payload["request_info"] = request_info
        
    warnings = _check_for_warnings(record)
    if warnings:
        data_payload["warnings"] = warnings
        
    return {
        "request": {"self_url": f"{base_url}/data/latest/{device_id}"},
        "navigation": {
            "root": f"{base_url}/",
            "history": f"{base_url}/data/history?device_id={device_id}&limit=50"
        },
        "data": data_payload
    }

async def backfill_timestamps():
    import aiosqlite
    import asyncio
    from app.database import DB_PATH
    from app.services.timestamp_calculator import calculate_ingestion_timestamps, format_for_db
    from datetime import datetime, timezone

    print("Starting backfill process for calculated_event_timestamp...")
    conn = None
    try:
        conn = await aiosqlite.connect(DB_PATH)
        conn.row_factory = aiosqlite.Row
        cursor = await conn.cursor()
        
        await cursor.execute("""
            SELECT request_id, received_at FROM telemetry 
            WHERE calculated_event_timestamp IS NULL AND request_id IS NOT NULL
            GROUP BY request_id ORDER BY id
        """)
        batch_groups = await cursor.fetchall()
        total_groups = len(batch_groups)
        print(f"Found {total_groups} request groups to process.")
        
        processed_count = 0
        for i, group in enumerate(batch_groups):
            try:
                received_at_dt = datetime.strptime(group['received_at'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                
                await cursor.execute("""
                    SELECT id, payload FROM telemetry
                    WHERE request_id = ? AND calculated_event_timestamp IS NULL
                    ORDER BY id
                """, (group['request_id'],))
                
                records = await cursor.fetchall()
                if not records: continue

                payload_list = [orjson.loads(rec['payload']) for rec in records]
                record_ids = [rec['id'] for rec in records]
                
                calculated_times, _ = calculate_ingestion_timestamps(payload_list, received_at_dt)
                
                update_data = [(format_for_db(ts), r_id) for ts, r_id in zip(calculated_times, record_ids)]
                
                await conn.executemany("UPDATE telemetry SET calculated_event_timestamp = ? WHERE id = ?", update_data)
                await conn.commit()
                
                processed_count += len(records)
                print(f"  Processed group {i+1}/{total_groups} (request_id: {group['request_id']}). Records updated: {len(records)}")
            except Exception as e:
                print(f"  Skipping group {i+1} (request_id: {group['request_id']}) due to error: {e}")
            await asyncio.sleep(0.01)

        print(f"\nBackfill complete. Updated {processed_count} total records.")
    except Exception as e:
        print(f"An error occurred during backfill: {e}")
    finally:
        if conn: await conn.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(backfill_timestamps())
