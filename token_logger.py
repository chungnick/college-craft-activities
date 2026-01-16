import csv
import os
from datetime import datetime

TOKENS_FILE = "tokens.csv"

# Pricing per 1,000,000 tokens (Estimated 2026 pricing)
PRICING = {
    "gemini-3-flash-preview": {"input": 0.10, "output": 0.40},
    "gemini-2.5-flash-lite-preview-09-2025": {"input": 0.05, "output": 0.20},
    "gemini-1.5-flash": {"input": 0.075, "output": 0.30},
    "default": {"input": 0.10, "output": 0.40}
}

def log_tokens(step_name, model_name, input_tokens, output_tokens):
    file_exists = os.path.isfile(TOKENS_FILE)
    is_empty = not file_exists or os.path.getsize(TOKENS_FILE) == 0
    
    header = ["step", "timestamp", "model_name", "input_tokens", "output_tokens"]
    
    with open(TOKENS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_empty:
            writer.writerow(header)
        writer.writerow([step_name, datetime.now().isoformat(), model_name, input_tokens, output_tokens])

def get_total_usage(since=None):
    """Returns a dictionary of usage and cost broken down by model.
    If 'since' is provided (datetime object), only returns usage after that time.
    """
    usage = {} # model_name -> {input: 0, output: 0, cost: 0}
    
    if not os.path.exists(TOKENS_FILE):
        return usage
        
    with open(TOKENS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                timestamp_str = row.get("timestamp")
                if since and timestamp_str:
                    row_time = datetime.fromisoformat(timestamp_str)
                    if row_time < since:
                        continue

                model = row.get("model_name", "unknown")
                in_t = int(row["input_tokens"])
                out_t = int(row["output_tokens"])
                
                if model not in usage:
                    usage[model] = {"input": 0, "output": 0, "cost": 0.0}
                
                usage[model]["input"] += in_t
                usage[model]["output"] += out_t
                
                # Calculate cost
                rates = PRICING.get(model, PRICING["default"])
                cost = (in_t / 1_000_000 * rates["input"]) + (out_t / 1_000_000 * rates["output"])
                usage[model]["cost"] += cost
                
            except (ValueError, KeyError):
                continue
    return usage

def get_total_tokens():
    # Legacy support for main.py if needed, but we'll update main.py
    total_input = 0
    total_output = 0
    usage = get_total_usage()
    for m in usage.values():
        total_input += m["input"]
        total_output += m["output"]
    return total_input, total_output
