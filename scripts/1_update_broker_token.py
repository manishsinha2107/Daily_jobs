import os
import sys
import pandas as pd
import requests
import io
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. INITIALIZATION ---
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("❌ Error: Supabase credentials missing.")
    sys.exit(1)

supabase: Client = create_client(url, key)

def report_progress(status, msg):
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": datetime.now().isoformat()
        }).eq("step_id", "step1").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

# --- 2. CONFIGURATION ---
# We use partial matches to catch variants like 'NIFTY50', 'NIFTYBANK', etc.
TARGET_KEYWORDS = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']

SYMBOL_URLS = {
    "NSE_FO": "https://public.fyers.in/sym_details/NSE_FO.csv",
    "NSE_CM": "https://public.fyers.in/sym_details/NSE_CM.csv"
}

def sync_fyers_tokens():
    print("🌐 Connecting to Fyers Symbol Master...")
    report_progress("running", "🌐 Scanning Fyers Masters...")
    
    all_filtered_rows = []

    for key_name, csv_url in SYMBOL_URLS.items():
        try:
            print(f"📥 Fetching {key_name}...")
            response = requests.get(csv_url, timeout=30)
            if response.status_code != 200:
                continue
            
            # Read without headers first to find the right columns
            df = pd.read_csv(io.StringIO(response.text), header=None)
            
            # Fyers V3 Typical Columns:
            # Col 0: fyToken, Col 9: Full Symbol (NSE:NIFTY...), Col 13: Short Name (NIFTY)
            # We filter by checking if any of the key columns contain our keywords
            mask = df.stack().str.contains('|'.join(TARGET_KEYWORDS), na=False).unstack().any(axis=1)
            df_filtered = df[mask].copy()

            print(f"🔍 Found {len(df_filtered)} symbols in {key_name}")

            for _, row in df_filtered.iterrows():
                # Extracting values based on 2026 V3 positions
                fy_token = str(row[0])
                full_sym = str(row[9]) # NSE:NIFTY26APR25500CE
                
                # TRANSLATION BRIDGE: NSE:NIFTY26APR25500CE -> NIFTY26APR25500CE
                clean_tsym = full_sym.split(':')[-1] if ':' in full_sym else full_sym
                
                # Determine Index Group (NIFTY, BANKNIFTY, etc.)
                parent_index = next((idx for idx in TARGET_KEYWORDS if idx in clean_tsym), "OTHER")

                all_filtered_rows.append({
                    "token_id": fy_token,
                    "tsym": clean_tsym,
                    "symbol": parent_index,
                    "expiry": str(pd.to_datetime(row[8], unit='s').date()) if pd.notnull(row[8]) and int(row[8]) > 0 else None,
                    "strike": float(row[15]) if len(row) > 15 and pd.notnull(row[15]) and row[15] != -1 else None,
                    "option_type": str(row[16]) if len(row) > 16 and pd.notnull(row[16]) and str(row[16]) != 'XX' else None,
                    "last_validated": datetime.now().isoformat()
                })
        except Exception as e:
            print(f"❌ Error processing {key_name}: {e}")

    if all_filtered_rows:
        print(f"🚀 Upserting {len(all_filtered_rows)} tokens to Supabase...")
        for i in range(0, len(all_filtered_rows), 1000):
            batch = all_filtered_rows[i:i+1000]
            supabase.table("broker_tokens").upsert(batch).execute()
        
        report_progress("success", f"✅ {len(all_filtered_rows)} Fyers tokens synced.")
        print("🏁 Sync Complete.")
    else:
        report_progress("error", "❌ No tokens found. Check Fyers URL/Keywords.")

if __name__ == "__main__":
    sync_fyers_tokens()
