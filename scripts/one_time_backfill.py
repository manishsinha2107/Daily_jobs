import os
import glob
import pandas as pd
import re
from dotenv import load_dotenv
from supabase import create_client

# [MODULE/CLASS SSOT ROLE]: One-Time Database Retrofitter
# [FUNCTION CONTRACT & MATH]: Reads local CSVs, maps exact trades using absolute mathematical uniqueness (ID + Time + Qty), and backfills Set DNA into the Verification table.
load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def run_backfill():
    print("📡 Fetching existing verification rows...")
    all_db_rows = []
    offset = 0
    while True:
        res = supabase.table("strategy_trades_verification").select("id, strategy_id, trade_date, txn_time, txn_type, quantity").range(offset, offset + 999).execute()
        if not res.data: break
        all_db_rows.extend(res.data)
        offset += 1000

    # [TECHNICAL]: Creates an ultra-rigid dictionary mapping to guarantee we only update the exact row, preventing duplicates.
    db_map = {}
    for r in all_db_rows:
        try:
            # We use float formatting to normalize decimal discrepancies between pandas and supabase
            key = (str(r['strategy_id']), str(r['trade_date']), str(r['txn_time']), str(r['txn_type']), float(r['quantity']))
            db_map[key] = r['id']
        except Exception: pass
        
    print(f"🗺️ Mapped {len(db_map)} existing verification rows.")

    updates = []
    source_folder = os.getenv("SOURCE_FOLDER", ".")
    
    for file_path in glob.glob(os.path.join(source_folder, "*.csv")):
        file_name = os.path.basename(file_path)
        match = re.match(r'^(\d+)_', file_name)
        if not match: continue
        strat_id = match.group(1)
        
        try:
            df = pd.read_csv(file_path)
            for _, row in df.iterrows():
                if pd.isna(row.iloc[4]): continue
                try:
                    raw_dt = pd.to_datetime(f"{row.iloc[14]} {row.iloc[15]}")
                    iso_date = raw_dt.strftime('%Y-%m-%d')
                    
                    # Account for %I stripping leading zeros on some platforms
                    hour_code = "%#I" if os.name == "nt" else "%-I"
                    formatted_time = raw_dt.strftime(f'%Y-%m-%d {hour_code}:%M:%S %p')
                    
                    qty_val = float(re.sub(r'[^\d.-]', '', str(row.iloc[16])))
                    txn_type = str(row.iloc[11]).strip()
                    
                    # [BUSINESS / DOMAIN LOGIC]: Looks up the exact row ID using the rigid mapping key.
                    key = (strat_id, iso_date, formatted_time, txn_type, qty_val)
                    if key in db_map:
                        db_id = db_map[key]
                        
                        updates.append({
                            "id": db_id,
                            "condition_type": str(row.iloc[12]).strip() if not pd.isna(row.iloc[12]) else None,
                            "condition_parent_id": str(row.iloc[24]).strip() if not pd.isna(row.iloc[24]) else None,
                            "condition_id": str(row.iloc[25]).strip() if not pd.isna(row.iloc[25]) else None
                        })
                except Exception: continue
        except Exception as e:
            print(f"⚠️ Failed to parse {file_name}: {e}")
    
    if updates:
        print(f"📤 Pushing {len(updates)} specific updates to Supabase...")
        for i in range(0, len(updates), 500):
            supabase.table("strategy_trades_verification").upsert(updates[i:i+500]).execute()
        print("✅ Backfill Complete.")
    else:
        print("⚠️ No matching rows found to backfill.")

if __name__ == "__main__":
    run_backfill()