import json
import csv
import os
import sys

# Configuration
INPUT_FILE = "results.json"
OUTPUT_FILE = "supabase_import.csv"

# Field Mapping: Map results.json keys to Supabase column names
# Key: JSON field name, Value: (CSV column name, type)
# types: 'text', 'boolean', 'integer', 'float', 'jsonb', 'text[]'
FIELD_MAPPING = {
    "id": ("id", "text"),
    "name": ("title", "text"), # Assuming name in json maps to title
    "url": ("url", "text"),
    "description": ("description", "text"),
    
    # Metadata fields (Step 5) - Multi-selects -> text[]
    "mode": ("mode", "text[]"), # Assuming multi-select or single but array in DB? Options imply single, but user said text[] for multiselect.
                                # Actually options.py says mode is multi-select: False. 
                                # But if we want text[] for flexibility or user request "text[] for multiselect".
                                # Let's check results.json structure. 
                                # If it's a list in JSON, map to Postgres array syntax {val1,val2}
    "price": ("price", "text"), # Single select
    "eligibility": ("eligibility", "text[]"), # Multi
    "grade_level": ("grade_level", "text[]"), # Multi
    "location": ("location", "text[]"), # Multi
    "tags": ("tags", "text[]"), # Multi (Subject areas) - JSON key is 'tags'
    "financial_aid": ("financial_aid", "text"), # Single/Bool? Options says "Yes"/"No"
    "application_requirements": ("application_requirements", "text[]"), # Multi
    
    # Nested JSONs (Step 4 & 7) -> jsonb
    "deadlines": ("deadlines", "jsonb"),
    "decisions_date": ("decisions_date", "jsonb"),
    "program_dates": ("program_dates", "jsonb"),
    
    # Booleans/Strings
    "deadlines_found": ("deadlines_found", "boolean"),
    "program_dates_found": ("program_dates_found", "boolean"),
}

def format_postgres_array(value):
    """
    Convert a Python list to Postgres array string format: {val1,val2}
    Escaping: " -> \" and \ -> \\ inside double quotes if needed, but for simple strings:
    If value contains comma, quotes, whitespace, it should be quoted.
    Standard CSV writer handles the CSV cell quoting. We just need the internal array string.
    Postgres format: {"item 1","item 2"}
    """
    if not value:
        return "{}"
    
    if not isinstance(value, list):
        # Treat single value as 1-item array
        value = [str(value)]
        
    items = []
    for item in value:
        if item is None:
            continue
        # Escape backslashes and double quotes
        s = str(item).replace('\\', '\\\\').replace('"', '\\"')
        items.append(f'"{s}"')
        
    return "{" + ",".join(items) + "}"

def format_jsonb(value):
    """
    Convert Python object to JSON string.
    """
    if value is None:
        return "[]" # Or null, but empty list for arrays usually safer
    return json.dumps(value)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        sys.exit(1)
        
    print(f"Reading {INPUT_FILE}...")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            # Handle list or dict wrapper
            rows = data.get("results", []) if isinstance(data, dict) else data
        except json.JSONDecodeError:
            print(f"Error: {INPUT_FILE} is not valid JSON.")
            sys.exit(1)
            
    if not rows:
        print("No rows to process.")
        sys.exit(0)

    print(f"Converting {len(rows)} rows...")
    
    # Prepare CSV headers based on mapping
    # We want columns in specific order or just all mapped ones?
    # Let's use the order defined in FIELD_MAPPING
    csv_headers = [m[0] for m in FIELD_MAPPING.values()]
    
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)
        
        for row in rows:
            csv_row = []
            for json_key, (csv_col, col_type) in FIELD_MAPPING.items():
                val = row.get(json_key)
                
                # Handling transformations
                if col_type == "text[]":
                    # Expecting list, format as Postgres array
                    # If val is None, empty array
                    # If val is string, wrap in list
                    if val is None:
                        val = []
                    elif isinstance(val, str):
                        val = [val]
                    csv_row.append(format_postgres_array(val))
                    
                elif col_type == "jsonb":
                    csv_row.append(format_jsonb(val))
                    
                elif col_type == "boolean":
                    # Convert string "true"/"false" or bool to CSV friendly
                    if isinstance(val, str):
                        csv_row.append(val.lower())
                    else:
                        csv_row.append(str(val).lower())
                        
                else: # text / default
                    csv_row.append(str(val) if val is not None else "")
            
            writer.writerow(csv_row)
            
    print(f"Successfully wrote {OUTPUT_FILE}")
    
    # Data Quality Report
    print("\n" + "="*50)
    print(f"{'DATA QUALITY REPORT (Supabase Import)':^50}")
    print("="*50)
    print(f"Total Rows: {len(rows)}")
    
    # Calculate fill rates per column
    column_counts = {col: 0 for col in csv_headers}
    
    for row in rows:
        for json_key, (csv_col, col_type) in FIELD_MAPPING.items():
            val = row.get(json_key)
            # Check if "filled" (not None, not empty string, not empty list/dict, not "[]" string if we parsed it back?)
            # We check the raw JSON value state
            is_filled = False
            if val is not None:
                if isinstance(val, (list, dict)):
                    if len(val) > 0: is_filled = True
                elif isinstance(val, str):
                    if val.strip(): is_filled = True
                else: # boolean, numbers
                    is_filled = True
            
            if is_filled:
                column_counts[csv_col] += 1

    print("-" * 50)
    print(f"{'COLUMN':<30} {'FILLED':>8} {'PERCENT':>8}")
    print("-" * 50)
    
    for col in csv_headers:
        count = column_counts[col]
        pct = (count / len(rows)) if rows else 0
        print(f"{col:<30} {count:>8} {pct:>8.1%}")
    
    print("="*50 + "\n")

if __name__ == "__main__":
    main()

