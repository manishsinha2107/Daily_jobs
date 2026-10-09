import os
import io
import json
import pandas as pd
import re
from dotenv import load_dotenv
from supabase import create_client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# [MODULE/CLASS SSOT ROLE]: One-Time Database Retrofitter
# [FUNCTION CONTRACT & MATH]: Reads GDrive CSVs once per table loop, mapping exact trades to their independent table primary keys (ID + Time + Qty).

# [TECHNICAL]: Load env vars and init Supabase client.
# [BUSINESS / DOMAIN LOGIC]: Establishes core connection to the database for dual-schema updates.
load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def run_backfill():
    # [TECHNICAL]: Authenticate with Google Drive API.
    # [BUSINESS / DOMAIN LOGIC]: Grants secure access to the cloud CSV directory.
    creds_info = json.loads(os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON"))
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=['https://www.googleapis.com/auth/drive.readonly'])
    drive_service = build('drive', 'v3', credentials=creds)

    source_folder = os.getenv("SOURCE_FOLDER")
    results = drive_service.files().list(q=f"'{source_folder}' in parents and mimeType='text/csv'", fields="files(id, name)").execute()
    files = results.get('files', [])

    # [TECHNICAL]: Defines the target tables array for sequential, isolated processing.
    # [BUSINESS / DOMAIN LOGIC]: Guarantees that audit table primary keys are mapped completely independently from verification table primary keys.
    tables_to_sync = ["strategy_trades_verification", "strategy_trades_audit"]

    for target_table in tables_to_sync:
        print(f"\n📡 Fetching existing rows for {target_table}...")
        all_db_rows = []
        offset = 0
        while True:
            res = supabase.table(target_table).select("id, strategy_id, trade_date, txn_time, txn_type, quantity").range(offset, offset + 999).execute()
            if not res.data: break
            all_db_rows.extend(res.data)
            offset += 1000

        # [TECHNICAL]: Creates an ultra-rigid dictionary mapping specifically keyed to the current table's auto-incrementing ID.
        # [BUSINESS / DOMAIN LOGIC]: Prevents fatal cross-contamination between table schemas.
        db_map = {}
        for r in all_db_rows:
            try:
                key = (str(r['strategy_id']), str(r['trade_date']), str(r['txn_time']), str(r['txn_type']), float(r['quantity']))
                db_map[key] = r['id']
            except Exception: pass
            
        print(f"🗺️ Mapped {len(db_map)} existing rows in {target_table}.")

        updates = []
        
        for f in files:
            file_name = f['name']
            file_id = f['id']
            match = re.match(r'^(\d+)_', file_name)
            if not match: continue
            strat_id = match.group(1)
            
            try:
                # [TECHNICAL]: Download the file into a volatile RAM buffer.
                # [BUSINESS / DOMAIN LOGIC]: Iterates over the raw files cleanly without storing redundant local payload data.
                request = drive_service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                fh.seek(0)
                
                df = pd.read_csv(fh)
                for _, row in df.iterrows():
                    if pd.isna(row.iloc[4]): continue
                    try:
                        raw_dt = pd.to_datetime(f"{row.iloc[14]} {row.iloc[15]}")
                        iso_date = raw_dt.strftime('%Y-%m-%d')
                        hour_code = "%#I" if os.name == "nt" else "%-I"
                        formatted_time = raw_dt.strftime(f'%Y-%m-%d {hour_code}:%M:%S %p')
                        
                        qty_val = float(re.sub(r'[^\d.-]', '', str(row.iloc[16])))
                        txn_type = str(row.iloc[11]).strip()
                        
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
        
        # [MODULE/CLASS SSOT ROLE]: One-Time Database Retrofitter Persistence Layer
        # [FUNCTION CONTRACT & MATH]: Executes targeted in-place column updates specifically scoped to the current active loop's table.
        if updates:
            print(f"📤 Pushing {len(updates)} specific updates to {target_table} via targeted updates...")
            success_count = 0
            for item in updates:
                # [TECHNICAL]: Extract target row ID non-destructively for the specific active table loop.
                # [BUSINESS / DOMAIN LOGIC]: Bypasses Postgres table INSERT constraints cleanly.
                row_id = item["id"]
                payload = {k: v for k, v in item.items() if k != "id"}
                
                try:
                    supabase.table(target_table).update(payload).eq("id", row_id).execute()
                    success_count += 1
                except Exception as upd_err:
                    print(f"⚠️ Failed to update row ID {row_id} in {target_table}: {upd_err}")
            print(f"✅ Backfill Complete for {target_table}. Successfully updated {success_count}/{len(updates)} rows.")
            
            # [TECHNICAL]: Force-clear the array before shifting to the second table in the loop.
            # [BUSINESS / DOMAIN LOGIC]: Ensures zero cross-contamination of payloads between Verification and Audit states.
            updates.clear()
        else:
            print(f"⚠️ No matching rows found to backfill for {target_table}.")

if __name__ == "__main__":
    run_backfill()