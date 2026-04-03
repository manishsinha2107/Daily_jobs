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
    """Updates the real-time heartbeat in Supabase for Step 1"""
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": datetime.now().isoformat()
        }).eq("step_id", "step1").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

# --- 2. CONFIGURATION ---
ALLOWED_INDICES = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']

# Fyers V3 Master URLs
SYMBOL_URLS = {
    "NSE_FO": "https://public.fyers.in/sym_details/NSE_FO.csv",
    "NSE_CM": "https://public.fyers.in/sym_details/NSE_CM.csv"
}

# Official Fyers V3 Column Map
FYERS_HEADER = [
    'fytoken', 'name', 'instrument_type', 'lot', 'tick', 'isin', 'trad_ses', 
    'last_upd', 'expiry_dt', 'symbol', 'exchange', 'segment', 'script_code', 
    'short_sym', 'strike', 'opt', 'fytoken_dup'
]

def sync_fyers_tokens():
    print("🌐 Connecting to Fyers Symbol Master...")
    report_progress("running", "🌐 Downloading Fyers Masters...")
    
    all_filtered_rows = []

    for key_name, csv_url in SYMBOL_URLS.items():
        try:
            print(f"📥 Fetching {key_name}...")
            response = requests.get(csv_url, timeout=30)
            if response.status_code != 200:
                print(f"⚠️ Failed to fetch {key_name}: {response.status_code}")
                continue
                
            df = pd.read_csv(io.StringIO(response.text), names=FYERS_HEADER, header=None)
            
            # Filter for indices we care about
            df_filtered = df[df['short_sym'].isin(ALLOWED_INDICES)].copy()
            print(f"🔍 Found {len(df_filtered)} matching symbols in {key_name}")

            for _, row in df_filtered.iterrows():
                # TRANSLATION BRIDGE: NSE:NIFTY26APR25500CE -> NIFTY26APR25500CE
                raw_fyers_sym = str(row['symbol'])
                clean_tsym = raw_fyers_sym.split(':')[-1] if ':' in raw_fyers_sym else raw_fyers_sym

                all_filtered_rows.append({
                    "token_id": str(row['fytoken']),
                    "tsym": clean_tsym,
                    "symbol": str(row['short_sym']),
                    "expiry": str(pd.to_datetime(row['expiry_dt'], unit='s').date()) if pd.notnull(row['expiry_dt']) and row['expiry_dt'] > 0 else None,
                    "strike": float(row['strike']) if row['strike'] != -1 else None,
                    "option_type": str(row['opt']) if str(row['opt']) != 'XX' else None,
                    "last_validated": datetime.now().isoformat()
                })
        except Exception as e:
            print(f"❌ Error processing {key_name}: {e}")

    if all_filtered_rows:
        print(f"🚀 Upserting {len(all_filtered_rows)} tokens to Supabase...")
        # Chunked upsert to prevent payload size errors
        for i in range(0, len(all_filtered_rows), 1000):
            batch = all_filtered_rows[i:i+1000]
            supabase.table("broker_tokens").upsert(batch).execute()
        
        report_progress("success", f"✅ {len(all_filtered_rows)} Fyers tokens synced.")
        print("🏁 Sync Complete.")
    else:
        report_progress("error", "❌ No tokens found for allowed indices.")

if __name__ == "__main__":
    sync_fyers_tokens()
