from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
import bisect
from collections import defaultdict
import copy

def format_display_timestamp(ts_string: Optional[str]) -> str:
    if not ts_string:
        return "N/A"
    try:
        if '.' in ts_string:
            dt_obj = datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S.%f")
        else:
            dt_obj = datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S")
        
        return dt_obj.replace(tzinfo=timezone.utc).strftime("%d.%m.%Y %H:%M:%S UTC")
    except (ValueError, TypeError):
        return ts_string

def format_for_db(dt_obj: datetime) -> str:
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f")

def calculate_ingestion_timestamps(
    payloads: List[Dict[str, Any]], 
    received_at: datetime,
    prev_context: Optional[Dict[str, List[Tuple[int, datetime]]]] = None,
    start_index: int = 0
) -> Tuple[List[datetime], Dict[str, List[Tuple[int, datetime]]]]:
    
    if not payloads:
        return [], prev_context or defaultdict(list)

    calculated_times = [received_at] * len(payloads)
    device_bts_map = copy.deepcopy(prev_context) if prev_context else defaultdict(list)
    
    new_bts_found_in_chunk = False
    for i, p in enumerate(payloads):
        device_id = p.get('i') or p.get('id')
        if "bts" in p and isinstance(p.get("bts"), (int, float)) and device_id:
            try:
                ts = datetime.fromtimestamp(p["bts"], tz=timezone.utc)
                current_global_index = start_index + i
                device_bts_map[device_id].append((current_global_index, ts))
                new_bts_found_in_chunk = True
            except (ValueError, TypeError):
                pass
    
    if not device_bts_map:
        valid_tsos = [p.get('tso') for p in payloads if isinstance(p.get('tso'), (int, float))]
        if valid_tsos:
            max_tso = max(valid_tsos)
            base_time = received_at - timedelta(seconds=max_tso)
            for i, p in enumerate(payloads):
                if 'tso' in p and isinstance(p.get('tso'), (int, float)):
                    calculated_times[i] = base_time + timedelta(seconds=p['tso'])
        return calculated_times, device_bts_map

    if new_bts_found_in_chunk:
        for device_id in device_bts_map:
            device_bts_map[device_id].sort(key=lambda x: x[0])

    for i, p in enumerate(payloads):
        try:
            device_id = p.get('i') or p.get('id')
            if not device_id: continue
            
            current_global_index = start_index + i
            if "bts" in p and any(r[0] == current_global_index for r in device_bts_map.get(device_id, [])):
                calculated_times[i] = next(r[1] for r in device_bts_map[device_id] if r[0] == current_global_index)
                continue
            
            if "tso" in p and isinstance(p.get('tso'), (int, float)):
                if device_id not in device_bts_map: continue

                device_bts_records = device_bts_map[device_id]
                bts_indices = [r[0] for r in device_bts_records]
                
                insertion_point = bisect.bisect_left(bts_indices, current_global_index)
                if insertion_point == 0: continue
                
                context_index = insertion_point - 1
                context_ts = device_bts_records[context_index][1]
                calculated_times[i] = context_ts + timedelta(seconds=p['tso'])

        except (ValueError, TypeError, IndexError):
            pass

    return calculated_times, device_bts_map
