import zlib
import orjson
from typing import Dict, Any, Tuple
from fastapi import HTTPException
import decimal

MAX_PAYLOAD_SIZE = 50 * 1024 * 1024
MAX_DECOMPRESSED_SIZE = 100 * 1024 * 1024
MAX_BATCH_SIZE = 1000
MAX_RESPONSE_RECORDS = 500

def orjson_decimal_default(obj: Any) -> Any:
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

async def process_payload(raw_body: bytes) -> Tuple[bool, Dict[str, Any]]:
    if not raw_body:
        return False, {"error": "Empty payload"}
    
    if len(raw_body) > MAX_PAYLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"Payload too large. Max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB.")

    try:
        decompressed_data = zlib.decompress(raw_body, wbits=-15)
        if len(decompressed_data) > MAX_DECOMPRESSED_SIZE:
            raise HTTPException(status_code=413, detail=f"Decompressed payload too large. Max {MAX_DECOMPRESSED_SIZE // 1024 // 1024}MB.")
        
        payload = orjson.loads(decompressed_data)
        del decompressed_data
        
        if isinstance(payload, list) and len(payload) > MAX_BATCH_SIZE:
            raise HTTPException(status_code=413, detail=f"Batch too large. Max {MAX_BATCH_SIZE:,} records.")
            
        return True, payload
    except zlib.error:
        pass
    except orjson.JSONDecodeError:
        return False, {"error": "Invalid JSON in decompressed payload"}
    except MemoryError:
        raise HTTPException(status_code=413, detail="Payload too large for available memory")
    
    try:
        payload = orjson.loads(raw_body)
        
        if isinstance(payload, list) and len(payload) > MAX_BATCH_SIZE:
            raise HTTPException(status_code=413, detail=f"Batch too large. Max {MAX_BATCH_SIZE:,} records.")
            
        return True, payload
    except orjson.JSONDecodeError:
        return False, {"error": "Payload is not valid JSON or deflated JSON"}
    except MemoryError:
        raise HTTPException(status_code=413, detail="Payload too large for available memory")

def validate_data(payload: Dict[str, Any], headers: Dict[str, Any], client_ip: str) -> Tuple[bool, str, str]:
    if not isinstance(payload, dict):
        return False, "Payload is not a JSON object.", None

    device_id = payload.get('i') or payload.get('id')
    if device_id:
        return True, "Validation successful.", str(device_id)

    user_agent = headers.get('user-agent')
    if user_agent and client_ip:
        generated_id = f"ip:{client_ip}|ua:{user_agent[:50]}"
        return True, "Validation successful (ID generated).", generated_id

    return False, "Missing device identifier ('i' or 'id') and not enough info to generate one.", None
