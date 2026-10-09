# --- AFTER (PATCHED CODE | File: scripts/validate_backfill.py | Lines 1 to 125) ---
import os
import pandas as pd
import re
from dotenv import load_dotenv
from supabase import create_client

# [MODULE/CLASS SSOT ROLE]: Database State Mathematical Validator
# [FUNCTION CONTRACT & MATH]: Executes a read-only cross-reference between raw CSV output and Supabase table states, asserting structural 1:1 parity for Set DNA columns.

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def run_validation():
    target_csv = "31300070_Sanvali Nifty Weekly Directional Positional.csv"
    strat_id = "31300070"
    source_folder = os.getenv("SOURCE_FOLDER", ".")
    file_path = os.path.join(source_folder, target_csv)

    if not os.path.exists(file_path):
        print(f"❌ CRITICAL ERROR: Could not find {target_csv} in {source_folder}")
        return

    print(f"📊 INGESTING TRUTH DATA: {target_csv}")
    
    # [TECHNICAL]: Read CSV and extract ground-truth mappings using the absolute composite key.
    # [BUSINESS / DOMAIN LOGIC]: Binds the raw time, quantity, and side to the Tradetron Set identifiers.
    expected_state = {}
    df = pd.read_csv(file_path)
    
    for _, row in df.iterrows():
        if pd.isna(row.iloc[4]): continue
        try:
            raw_dt = pd.to_datetime(f"{row.iloc[14]} {row.iloc[15]}")
            iso_date = raw_dt.strftime('%Y-%m-%d')
            hour_code = "%#I" if os.name == "nt" else "%-I"
            formatted_time = raw_dt.strftime(f'%Y-%m-%d {hour_code}:%M:%S %p')
            
            qty_val = float(re.sub(r'[^\d.-]', '', str(row.iloc[16])))
            txn_type = str(row.iloc[11]).strip()
            
            c_type = str(row.iloc[12]).strip() if not pd.isna(row.iloc[12]) else None
            c_parent = str(row.iloc[24]).strip() if not pd.isna(row.iloc[24]) else None
            c_id = str(row.iloc[25]).strip() if not pd.isna(row.iloc[25]) else None

            # Composite Key: (Date, Time, Type, Quantity)
            key = (iso_date, formatted_time, txn_type, qty_val)
            expected_state[key] = {
                "condition_type": c_type,
                "condition_parent_id": c_parent,
                "condition_id": c_id
            }
        except Exception: continue

    print(f"✅ Mapped {len(expected_state)} ground-truth trades from CSV.")
    print("────────────────────────────────────────────────────────")

    tables_to_check = ["strategy_trades_verification", "strategy_trades_audit"]

    for table in tables_to_check:
        print(f"🔍 SCANNING TABLE: {table}")
        
        # [TECHNICAL]: Fetch all relevant rows for the strategy ID from the current table.
        # [BUSINESS / DOMAIN LOGIC]: A read-only pull to prevent accidental state mutation during validation.
        db_rows = []
        offset = 0
        while True:
            res = supabase.table(table).select("id, trade_date, txn_time, txn_type, quantity, condition_type, condition_parent_id, condition_id").eq("strategy_id", strat_id).range(offset, offset + 999).execute()
            if not res.data: break
            db_rows.extend(res.data)
            offset += 1000
            
        print(f"   📥 Fetched {len(db_rows)} total rows for ID {strat_id}.")
        
        matches = 0
        mismatches = 0
        missing = 0
        
        mismatch_log = []

        for r in db_rows:
            try:
                # [TECHNICAL]: Normalize floating point quantities to match pandas output formatting.
                key = (str(r['trade_date']), str(r['txn_time']), str(r['txn_type']), float(r['quantity']))
            except Exception:
                continue
                
            if key in expected_state:
                expected = expected_state[key]
                
                # Treat "None", "", and None as equivalent blanks for strict validation.
                def clean_val(v):
                    if v is None or str(v).lower() in ["none", "nan", ""]: return None
                    return str(v).strip()

                db_c_type = clean_val(r.get('condition_type'))
                db_c_parent = clean_val(r.get('condition_parent_id'))
                db_c_id = clean_val(r.get('condition_id'))

                ex_c_type = clean_val(expected['condition_type'])
                ex_c_parent = clean_val(expected['condition_parent_id'])
                ex_c_id = clean_val(expected['condition_id'])

                if (db_c_type == ex_c_type) and (db_c_parent == ex_c_parent) and (db_c_id == ex_c_id):
                    # [BUSINESS / DOMAIN LOGIC]: Strict 1:1 mathematical parity achieved.
                    if ex_c_id is not None:
                        matches += 1
                else:
                    if db_c_id is None and ex_c_id is not None:
                        missing += 1
                    else:
                        mismatches += 1
                        mismatch_log.append(f"Row {r['id']} | DB: {db_c_id} vs CSV: {ex_c_id}")

        print(f"   ✅ PERFECT PARITY: {matches} rows correctly mapped.")
        print(f"   ⚠️ MISSING/NULL:   {missing} rows missing Set DNA.")
        print(f"   ❌ MISMATCHES:     {mismatches} rows contain incorrect Set DNA.")
        
        if mismatch_log:
            print("   --- MISMATCH SAMPLES ---")
            for log in mismatch_log[:5]:
                print(f"   {log}")
        print("────────────────────────────────────────────────────────")

if __name__ == "__main__":
    run_validation()