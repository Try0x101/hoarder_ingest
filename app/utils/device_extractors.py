import orjson
import re
from typing import Dict

def extract_device_name_from_ua(user_agent: str) -> str:
    if not user_agent:
        return "Unknown"
    
    device_patterns = [
        r'Android \d+; ([^)]+)\)',
        r'iPhone OS [^;]+ like Mac OS X\) AppleWebKit[^(]+\(KHTML, like Gecko\) Version[^M]+Mobile[^S]+Safari',
        r'(\w+\s+\w+(?:\s+\w+)?)\s+Build'
    ]
    
    for pattern in device_patterns:
        match = re.search(pattern, user_agent)
        if match:
            device_name = match.group(1).strip()
            if device_name and device_name != "U":
                return device_name
    
    if "iPhone" in user_agent:
        return "iPhone"
    elif "iPad" in user_agent:
        return "iPad"
    elif "Android" in user_agent:
        return "Android Device"
    
    return "Unknown"

def extract_device_info(device_data: dict) -> dict:
    device_name = "Unknown"
    client_ip = "Unknown"
    
    try:
        payload = orjson.loads(device_data.get("payload", "{}"))
        device_name = payload.get("n") or payload.get("name") or payload.get("device_name")
    except:
        pass
    
    try:
        headers_info = orjson.loads(device_data.get("request_headers", "{}"))
        client_ip = headers_info.get("client_ip", "Unknown")
        
        if device_name in [None, "Unknown", ""]:
            headers = headers_info.get("headers", {})
            user_agent = headers.get("user-agent", "")
            device_name = extract_device_name_from_ua(user_agent)
    except:
        pass
    
    return {"device_name": device_name or "Unknown", "client_ip": client_ip}
