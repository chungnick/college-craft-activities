import json
import os
from datetime import datetime

FAILED_FILE = "failed_rows.json"

def log_failure(row_id, step_name, error_message):
    """Logs a failed row and the step it failed on."""
    data = {}
    if os.path.exists(FAILED_FILE):
        try:
            with open(FAILED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = {}
    
    if row_id not in data:
        data[row_id] = {}
    
    data[row_id][step_name] = {
        "error": str(error_message),
        "timestamp": datetime.now().isoformat()
    }
    
    with open(FAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def clear_failure(row_id, step_name):
    """Removes a failure entry once it succeeds."""
    if not os.path.exists(FAILED_FILE):
        return
    try:
        with open(FAILED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return
    
    if row_id in data and step_name in data[row_id]:
        del data[row_id][step_name]
        if not data[row_id]:
            del data[row_id]
        with open(FAILED_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

def get_failed_ids(step_name):
    """Returns a list of IDs that failed on a specific step."""
    if not os.path.exists(FAILED_FILE):
        return []
    try:
        with open(FAILED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return [row_id for row_id, steps in data.items() if step_name in steps]
    except json.JSONDecodeError:
        return []

