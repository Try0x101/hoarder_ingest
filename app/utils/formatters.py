from datetime import datetime
from typing import Optional
from urllib.parse import quote_plus

def format_utc_timestamp(ts_string: Optional[str]) -> str:
    if not ts_string:
        return "N/A"
    try:
        dt_obj = datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S")
        return dt_obj.strftime("%d.%m.%Y %H:%M:%S UTC")
    except (ValueError, TypeError):
        return ts_string

def build_base_url(request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

def create_device_links(base_url: str, device_id: str, limit: int = 50) -> dict:
    return {
        "latest": f"{base_url}/data/latest/{device_id}",
        "history": f"{base_url}/data/history?device_id={device_id}&limit={limit}"
    }

def create_pagination_links(base_url: str, device_id: str, limit: int, next_cursor: Optional[str] = None) -> dict:
    links = {
        "self": f"{base_url}/data/history?device_id={device_id}&limit={limit}",
        "up": f"{base_url}/data/history",
        **create_device_links(base_url, device_id, limit)
    }
    
    if next_cursor:
        links["next_page"] = f"{base_url}/data/history?device_id={device_id}&limit={limit}&cursor={quote_plus(next_cursor)}"
    
    return links
