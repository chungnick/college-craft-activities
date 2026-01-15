import os
import sys
import subprocess
from dotenv import load_dotenv

load_dotenv('.env.local')

# --- CONFIGURATION ---
NUMBER_OF_ROWS = 20  # Set to None to run all rows
REPEAT = False      # If False, skip rows already in results.json. If True, re-process them.

# Pipeline Steps: Comment out steps to skip them
PIPELINE = [
    # "step1_valid_url.py",
    "step2_create_md.py",
    "step3_sister_md.py",
    "step4_extract_dates.py",
    "step5_extract_metadata.py",
    "step6_extract_details.py",
]
# ---------------------

def run_step(script_name, limit=None):
    print(f"--- Running {script_name} ---")
    cmd = [sys.executable, script_name]
    
    # Step 1 logic is unique (doesn't use --limit in the same way, but let's see if we can just skip args for it)
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
    # Pipeline sequence
    for script in PIPELINE:
        # Step 1 (formerly Step 0) might not take --limit in the same way or uses different args.
        # Checking if script is step1 to adapt arguments if necessary, or assuming standard args.
        # step0_valid_url.py uses argparse but might not have --limit or --repeat the same way.
        # Let's check step1 source code if needed. Assuming for now it needs adjustment or we wrap it.
        # Actually, let's just run it. If it fails due to args, we'll need to fix step1.
        
        # Step 1 likely doesn't support --limit in the same way (it processes a CSV). 
        # But wait, step0_valid_url.py logic is about validating URLs in a CSV. 
        # It doesn't use "limit" typically, it runs on the whole file or chunks.
        # Let's pass arguments carefully.
        
        run_step(script, NUMBER_OF_ROWS)

    print("Pipeline completed.")

if __name__ == "__main__":
    main()

