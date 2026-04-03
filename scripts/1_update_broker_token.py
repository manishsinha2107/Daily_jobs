import os
import sys
import subprocess
import pandas as pd
import requests
import io
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. THE "BOOTSTRAP" BLOCK ---
def bootstrap():
    """Ensures environment is ready for Fyers and Supabase."""
    required = ["pandas", "requests", "supabase", "python-dotenv"]
    try:
        import supabase
        import dotenv
    except ImportError:
        print("📦 Installing dependencies for Fyers migration...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", *required])

bootstrap()

# --- 2. INITIALIZATION ---
load_dotenv()
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def report_progress(status, msg):
    """Updates Step 1 heartbeat."""
    try:
        supabase.table("engine_heartbeat").update({
            "status": status, "last_msg": msg, "updated_at": datetime.now().isoformat()
        }).eq("step_id", "step1").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat failed: {e}")

# --- 3. CONFIGURATION ---
ALLOWED_INDICES = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
# Fyers Master File URLs (2026 V3)
SYMBOL_URLS = {
    "NSE_FO": "https://public.fyers.in/sym_details/NSE_FO.csv",
    "NSE_CM": "https://public.fyers.in/sym_details/NSE_CM.csv"
}

# The Header format for Fyers V3 Symbol Master
FYERS_HEADER = [
    'fytoken', 'name', 'instrument_type', 'lot', 'tick', 'isin', 'trad_ses', 
    'last_upd', 'expiry_dt', 'symbol', 'exchange', 'segment', 'script_code', 
    'short_sym', 'strike', 'opt', 'fytoken_dup'
]

def sync_fyers_tokens():
    print("🌐 Fetching Symbol Masters from Fyers...")
    report_progress("running", "🌐 Downloading Fyers Masters...")
    
    all_filtered_rows = []

    for key_name, csv_url in SYMBOL_URLS.items():
        try:
            response = requests.get(csv_url, timeout=30)
            df = pd.read_csv(io.StringIO(response.text), names=FYERS_HEADER, header=None)
            
            # Filter for our target indices
            # Fyers 'short_sym' usually contains the base index name
            mask = df['short_sym'].isin(ALLOWED_INDICES)
            df_filtered = df[mask].copy()
            
            for _, row in df_filtered.iterrows():
                # TRANSLATION BRIDGE: 
                # Fyers symbol: 'NSE:NIFTY26APR25500CE' 
                # Your tsym: 'NIFTY26APR25500CE' (Stripping the Exchange prefix)
                raw_fyers_sym = str(row['symbol'])
                internal_tsym = raw_fyers_sym.split(':')[-1] if ':' in raw_fyers_sym else raw_fyers_sym

                all_filtered_rows.append({
                    "token_id": str(row['fytoken']),
                    "tsym": internal_tsym,
                    "symbol": str(row['short_sym']),
                    "expiry": str(pd.to_datetime(row['expiry_dt'], unit='s').date()) if pd.notnull(row['expiry_dt']) else None,
                    "strike": float(row['strike']) if row['strike'] != -1 else None,
                    "option_type": str(row['opt']) if str(row['opt']) != 'XX' else None,
                    "last_validated": datetime.now().isoformat()
                })
            print(f"✅ Processed {key_name}: Found {len(df_filtered)} relevant rows.")
            
        except Exception as e:
            print(f"❌ Error processing {key_name}: {e}")
            continue

    if all_filtered_rows:
        report_progress("running", f"🚀 Upserting {len(all_filtered_rows)} tokens...")
        # Batch upsert to Supabase
        for i in range(0, len(all_filtered_rows), 1000):
            batch = all_filtered_rows[i:i+1000]
            supabase.table("broker_tokens").upsert(batch).execute()
        
        report_progress("success", f"✅ {len(all_filtered_rows)} Fyers tokens synced.")
        print(f"🏁 DONE! Sync complete.")
    else:
        report_progress("error", "❌ No tokens found in master.")

if __name__ == "__main__":
    sync_fyers_tokens()
