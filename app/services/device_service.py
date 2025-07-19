from typing import List, Dict, Any
from app.database import get_latest_telemetry
from app.utils.device_extractors import extract_device_info
from app.utils.formatters import format_utc_timestamp, create_device_links

async def get_device_list(base_url: str, limit: int = 20) -> List[Dict[str, Any]]:
    latest_devices = await get_latest_telemetry(limit=limit)
    device_list = []
    for device in latest_devices:
        device_info = extract_device_info(device)
        device_list.append({
            "device_id": device.get("device_id"),
            "device_name": device_info["device_name"],
            "client_ip": device_info["client_ip"],
            "last_seen": format_utc_timestamp(device.get("received_at")),
            "links": create_device_links(base_url, device.get("device_id"))
        })
    return device_list
