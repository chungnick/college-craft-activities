import json
import os
from collections import Counter
from datetime import datetime

RESULTS_FILE = "results.json"
FAILED_FILE = "failed_rows.json"

def analyze_quality():
    if not os.path.exists(RESULTS_FILE):
        print(f"Error: {RESULTS_FILE} not found.")
        return

    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            results = data.get("results", [])
        except json.JSONDecodeError:
            print(f"Error: {RESULTS_FILE} is not a valid JSON.")
            return

    total = len(results)
    if total == 0:
        print("No entries found in results.json.")
        return

    # Counts
    deadlines_count = sum(1 for r in results if str(r.get("deadlines_found")).lower() == "true")
    dates_count = sum(1 for r in results if str(r.get("program_dates_found")).lower() == "true")
    desc_count = sum(1 for r in results if r.get("description") and len(r.get("description")) > 50)
    title_count = sum(1 for r in results if r.get("title"))
    subtitle_count = sum(1 for r in results if r.get("subtitle"))
    tags_count = sum(1 for r in results if r.get("tags") and len(r.get("tags")) > 0)
    
    # Check for current activities (at least one deadline later than today)
    today = datetime.now().date()
    current_activities_count = 0
    
    for r in results:
        deadlines = r.get("deadlines", [])
        if not deadlines:
            continue
        
        has_future_deadline = False
        for deadline_entry in deadlines:
            dates = deadline_entry.get("dates", [])
            for date_str in dates:
                if not date_str:
                    continue
                try:
                    # Try parsing YYYY-MM-DD format
                    deadline_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if deadline_date > today:
                        has_future_deadline = True
                        break
                except (ValueError, TypeError):
                    # If parsing fails, skip this date
                    continue
            if has_future_deadline:
                break
        
        if has_future_deadline:
            current_activities_count += 1

    # Metadata completeness
    meta_fields = ["mode", "price", "eligibility", "grade_level", "location", "program_type"]
    meta_completeness = {field: sum(1 for r in results if r.get(field) and r.get(field) != "Not Specified" and r.get(field) != []) for field in meta_fields}

    # Failure tracking
    failed_count = 0
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE, "r", encoding="utf-8") as f:
            try:
                failed_data = json.load(f)
                failed_count = len(failed_data)
            except: pass

    # Terminal Output
    print("\n" + "="*50)
    print(f"{'DATA QUALITY REPORT':^50}")
    print("="*50)
    print(f"Total Entries in results.json: {total:,}")
    print(f"Total Entries in failed_rows.json: {failed_count:,}")
    print("-" * 50)
    
    print(f"{'METRIC':<30} {'COUNT':>8} {'PERCENT':>8}")
    print(f"{'Deadlines Found':<30} {deadlines_count:>8} {deadlines_count/total:>8.1%}")
    print(f"{'Program Dates Found':<30} {dates_count:>8} {dates_count/total:>8.1%}")
    print(f"{'Current Activities':<30} {current_activities_count:>8} {current_activities_count/total:>8.1%}")
    print(f"{'Valid Description (>50 chars)':<30} {desc_count:>8} {desc_count/total:>8.1%}")
    print(f"{'Title Present':<30} {title_count:>8} {title_count/total:>8.1%}")
    print(f"{'Subtitle (Inst.) Present':<30} {subtitle_count:>8} {subtitle_count/total:>8.1%}")
    print(f"{'Has at least 1 Tag':<30} {tags_count:>8} {tags_count/total:>8.1%}")
    
    print("-" * 50)
    print(f"{'METADATA COMPLETENESS':^50}")
    print("-" * 50)
    for field, count in meta_completeness.items():
        print(f"{field.replace('_', ' ').title():<30} {count:>8} {count/total:>8.1%}")
    
    print("="*50)
    print("\n")

if __name__ == "__main__":
    analyze_quality()

