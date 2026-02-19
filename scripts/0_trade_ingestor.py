import os
import re
import json
import pandas as pd
from datetime import datetime
import requests
from google.oauth2 import service_account
import google.auth.transport.requests
import io
import platform
from supabase import create_client, Client

# --- CONFIGURATION ---
# --- CONFIGURATION (SECURED FOR PUBLIC REPO) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Move folder IDs to Environment Variables
SOURCE_FOLDER = os.environ.get("GDRIVE_SOURCE_ID")
DEST_FOLDER = os.environ.get("GDRIVE_DEST_ID")

# Initialize Supabase Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Safety Check: Stop if environment is not set up
if not all([SUPABASE_URL, SUPABASE_KEY, SOURCE_FOLDER, DEST_FOLDER]):
    print("❌ ERROR: Missing Critical Environment Variables.")
    exit(1)

SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON"))
SCOPES = ['https://www.googleapis.com/auth/drive']

# --- STABLE HEARTBEAT REPORTER (GitHub Secret Safe) ---
def update_heartbeat(status, msg):
    try:
        # Using the supabase client avoids 'Invalid Header' errors in GitHub Actions
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": datetime.now().isoformat()
        }).eq("step_id", "step0").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat Update Failed: {e}")

def get_drive_token():
    creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
    auth_request = google.auth.transport.requests.Request()
    creds.refresh(auth_request)
    return creds.token

def get_active_strategies():
    # Using supabase client for data fetch as well for consistency
    res = supabase.table("strategies").select("strategy_id,strategy_name").eq("status", "Active").execute()
    data = res.data
    return {str(i['strategy_name']).strip(): str(i['strategy_id']) for i in data}, \
           {str(i['strategy_id']).strip(): str(i['strategy_id']) for i in data}

def move_drive_file(file_id, file_name, token):
    try:
        url = f"https://www.googleapis.com/drive/v3/files/{file_id}?fields=parents"
        headers = {"Authorization": f"Bearer {token}"}
        file_data = requests.get(url, headers=headers, timeout=15).json()
        prev_parents = ",".join(file_data.get('parents', []))
        move_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?addParents={DEST_FOLDER}&removeParents={prev_parents}"
        res = requests.patch(move_url, headers=headers, timeout=15)
        if res.status_code == 200:
            print(f"📦 MOVED: '{file_name}' to Processed Folder.")
            return True
        return False
    except Exception as e:
        print(f"⚠️ MOVE ERROR: {e}")
        return False

def run_ingestion():
    update_heartbeat("running", "📡 Scanning Drive...")
    print("📡 [1/4] Authenticating...")
    
    try:
        token = get_drive_token()
        name_map, id_map = get_active_strategies()
        headers = {"Authorization": f"Bearer {token}"}

        query = f"'{SOURCE_FOLDER}' in parents and trashed = false and mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        files = requests.get(f"https://www.googleapis.com/drive/v3/files?q={query}&fields=files(id,name)", headers=headers).json().get('files', [])
        
        # --- FIX: CLEAR UI IF NO FILES FOUND ---
        if not files:
            print("🏁 No files found in Source Folder.")
            update_heartbeat("success", "✅ Idle: No files found")
            return

        hour_code = "%#I" if platform.system() == "Windows" else "%-I"
        total_files = len(files)
        update_heartbeat("running", f"📂 Found {total_files} files...")

        for idx, f in enumerate(files):
            file_id, file_name = f['id'], f['name']
            print(f"📂 [Processing {idx+1}/{total_files}]: {file_name}")
            update_heartbeat("running", f"🔄 File {idx+1}/{total_files}: {file_name[:15]}...")

            strategy_id = None
            s_match = re.match(r'^(\d{8})_', file_name)
            if s_match and s_match.group(1) in id_map:
                strategy_id = id_map[s_match.group(1)]
            
            if not strategy_id:
                clean_name = re.sub(r'\.xlsx$', '', file_name, flags=re.I)
                clean_name = re.sub(r'\s*\(\d+\)$', '', clean_name).strip() 
                strategy_id = name_map.get(clean_name)

            if not strategy_id:
                print(f"❌ SKIPPED: '{file_name}' (No ID found)")
                continue

            file_res = requests.get(f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media", headers=headers)
            df = pd.read_excel(io.BytesIO(file_res.content), engine='openpyxl')
            
            payload = []
            for _, row in df.iterrows():
                if pd.isna(row.iloc[4]): continue
                try:
                    raw_dt = pd.to_datetime(f"{row.iloc[14]} {row.iloc[15]}")
                    iso_date = raw_dt.strftime('%Y-%m-%d')
                    formatted_time = raw_dt.strftime(f'%Y-%m-%d {hour_code}:%M:%S %p')

                    qty_val = float(re.sub(r'[^\d.-]', '', str(row.iloc[16]))) if not pd.isna(row.iloc[16]) else 0.0
                    px_val = float(re.sub(r'[^\d.-]', '', str(row.iloc[18]))) if not pd.isna(row.iloc[18]) else 0.0

                    payload.append({
                        "strategy_id": int(strategy_id),
                        "strategy_name": file_name.replace('.xlsx',''),
                        "trade_date": iso_date,
                        "instrument": str(row.iloc[4]).strip(),
                        "txn_time": formatted_time,
                        "txn_type": str(row.iloc[11]).strip(),
                        "quantity": qty_val,
                        "price": px_val,
                        "run_counter": int(row.iloc[27]) if not pd.isna(row.iloc[27]) else 0,
                        "status": "pending_ohlc"
                    })
                except: continue

            if payload:
                # Optimized deduplication and upsert via client
                seen = set()
                unique_payload = [p for p in payload if not (key := (p['strategy_id'], p['trade_date'], p['instrument'], p['txn_time'], p['txn_type'], p['quantity'], p['price'])) in seen and not seen.add(key)]
                
                res = supabase.table("strategy_trades_audit").upsert(unique_payload, on_conflict="strategy_id,trade_date,instrument,txn_time,txn_type,quantity,price").execute()
                
                if res.data:
                    print(f"✅ Ingested {len(unique_payload)} rows.")
                    move_drive_file(file_id, file_name, token)

        update_heartbeat("success", f"✅ Completed: {total_files} files processed")

    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        update_heartbeat("error", f"❌ Fatal: {str(e)[:30]}")

if __name__ == "__main__":
    run_ingestion()
