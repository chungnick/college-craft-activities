import os
import sys
import subprocess
from datetime import datetime
from dotenv import load_dotenv
from token_logger import get_total_usage

load_dotenv('.env.local')

# --- CONFIGURATION ---
NUMBER_OF_ROWS = 10  # Set to None to run all rows
REPEAT = False      # If False, skip rows already in results.json. If True, re-process them.
TARGET_ID = None    # Set a specific ID to process only that one (e.g. "f443c083...")

# Pipeline Steps: Comment out steps to skip them
PIPELINE = [
    # "step1_valid_url.py",
    # "step2_create_md.py",
    # "step3_sister_md.py",
    "step4_6_orchestrator.py",
]
# ---------------------

def run_step(script_name, limit=None, target_id=None):
    print(f"--- Running {script_name} ---")
    cmd = [sys.executable, script_name]
    
    # Assign specific API keys based on the step
    step_keys = {
        "step2_create_md.py": os.environ.get("GEMINI_API_KEY_STEP_4"), # Reusing step 4 key for fetching
        "step3_sister_md.py": os.environ.get("GEMINI_API_KEY_STEP_4"),
    }
    
    key = step_keys.get(script_name)
    if key:
        cmd.extend(["--api-key", key])
    
    # Pass target ID if specified
    if target_id:
        cmd.extend(["--id", target_id])
    
    if script_name == "step1_valid_url.py":
        # step1 args: --input ec_bank_rows.csv --output ec_bank_rows_with_valid_url.csv (defaults work fine)
        # It doesn't support --limit. We'll just run it as is.
        pass
    elif script_name == "step2_create_md.py":
        # Step 2 needs --limit passed if set
        if limit is not None:
             cmd.extend(["--limit", str(limit)])
        
        if REPEAT:
            cmd.append("--repeat")
    else:
        if limit is not None:
            cmd.extend(["--limit", str(limit)])
        
        if REPEAT:
            cmd.append("--repeat")
    
    try:
        subprocess.check_call(cmd)
        print(f"--- {script_name} completed successfully ---\n")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")
        # Continue to next step or exit? User probably wants pipeline to continue if possible, 
        # but usually a failure in step 3 affects 4/5. 
        # For simplicity, we'll exit on error.
        sys.exit(1)

def main():
    start_time = datetime.now()
    
    # Pipeline sequence
    for script in PIPELINE:
        run_step(script, NUMBER_OF_ROWS, TARGET_ID)

    print("Pipeline completed.")
    
    # Calculate and display usage breakdown for THIS RUN ONLY
    usage = get_total_usage(since=start_time)
    
    print("\n" + "="*60)
    print(f"{'CURRENT RUN TOKEN USAGE':^60}")
    print("="*60)
    print(f"{'MODEL BREAKDOWN':<30} {'INPUT':>10} {'OUTPUT':>10} {'COST':>8}")
    print("-" * 60)
    
    grand_total_input = 0
    grand_total_output = 0
    grand_total_cost = 0.0
    
    for model, data in usage.items():
        grand_total_input += data['input']
        grand_total_output += data['output']
        grand_total_cost += data['cost']
        print(f"{model[:30]:<30} {data['input']:>10,} {data['output']:>10,} ${data['cost']:>8.4f}")
    
    print("-" * 60)
    print(f"{'TOTAL':<30} {grand_total_input:>10,} {grand_total_output:>10,} ${grand_total_cost:>8.4f}")
    print("="*60)

if __name__ == "__main__":
    main()

