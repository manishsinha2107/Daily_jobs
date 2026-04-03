import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# --- INITIALIZATION ---
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("❌ Error: Supabase credentials missing.")
    sys.exit(1)

supabase: Client = create_client(url, key)

# --- HEARTBEAT REPORTER (Surgical Addition) ---
def report_progress(status, msg):
    """Updates the real-time heartbeat in Supabase for Step 1"""
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": datetime.now().isoformat()
        }).eq("step_id", "step1").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

def sync_fyers_tokens():
    print("🔄 Syncing Native Fyers Tokens (Daily Update)...")
    report_progress("running", "📡 Downloading Fyers Master CSV...")
    
    # 1. Download Fyers NSE F&O Master CSV
    csv_url = "https://public.fyers.in/sym_details/NSE_FO.csv"
    try:
        df = pd.read_csv(csv_url, header=None)
    except Exception as e:
        err_msg = f"❌ Failed to download Fyers CSV: {e}"
        print(err_msg)
        report_progress("error", err_msg)
        return

    # 2. Filter for Options (Index 14 represents Options in Fyers schema)
    target_indices = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']
    options_df = df[
        (df[13].isin(target_indices)) & 
        (df[9].str.endswith('CE') | df[9].str.endswith('PE'))
    ]

    found_count = len(options_df)
    print(f"📥 Found {found_count} Active Options Contracts.")
    report_progress("running", f"📥 Found {found_count} contracts. Upserting...")

    # 3. Prepare payload for Supabase
    payload = []
    for _, row in options_df.iterrows():
        payload.append({
            "token_id": str(row[0]),
            "tsym": str(row[9]),
            "symbol": str(row[13]),
            "last_validated": datetime.now().isoformat(),
            "is_historical": False
        })

    # 4. Bulk Upsert in batches of 1000
    try:
        print(f"🚀 Upserting {len(payload)} native Fyers tokens...")
        for i in range(0, len(payload), 1000):
            batch = payload[i:i+1000]
            supabase.table("broker_tokens").upsert(batch, on_conflict="token_id").execute()
            
            progress_msg = f"⚡ Synced {min(i+1000, found_count)}/{found_count} tokens..."
            print(f"    - {progress_msg}")
            report_progress("running", progress_msg)

        print("✅ Daily Broker Token Sync Complete!")
        report_progress("success", f"✅ Successfully synced {found_count} tokens.")
        
    except Exception as e:
        err_msg = f"❌ Upsert Failed: {str(e)[:50]}"
        print(err_msg)
        report_progress("error", err_msg)

if __name__ == "__main__":
    sync_fyers_tokens()
