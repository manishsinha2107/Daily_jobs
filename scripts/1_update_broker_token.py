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
TARGET_KEYWORDS = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']

# Focused strictly on Options/Futures to avoid NSE_CM formatting errors
SYMBOL_URLS = {
    "NSE_FO": "https://public.fyers.in/sym_details/NSE_FO.csv"
}

def sync_fyers_tokens():
    print("🌐 Connecting to Fyers Symbol Master (NSE_FO)...")
    report_progress("running", "🌐 Scanning Fyers Derivatives Master...")
    
    all_filtered_rows = []

    for key_name, csv_url in SYMBOL_URLS.items():
        try:
            print(f"📥 Fetching {key_name}...")
            response = requests.get(csv_url, timeout=30)
            if response.status_code != 200:
                continue
            
            df = pd.read_csv(io.StringIO(response.text), header=None)
            
            # Smart filter across all columns for target indices
            mask = df.stack().str.contains('|'.join(TARGET_KEYWORDS), na=False).unstack().any(axis=1)
            df_filtered = df[mask].copy()

            print(f"🔍 Found {len(df_filtered)} relevant symbols in {key_name}")

            for _, row in df_filtered.iterrows():
                # Fyers V3 positions: 0:fytoken, 9:symbol_str, 8:expiry, 15:strike, 16:opt_type
                fy_token = str(row[0])
                full_sym = str(row[9])
                
                # TRANSLATION: NSE:NIFTY26APR25500CE -> NIFTY26APR25500CE
                clean_tsym = full_sym.split(':')[-1] if ':' in full_sym else full_sym
                parent_index = next((idx for idx in TARGET_KEYWORDS if idx in clean_tsym), "OTHER")

                # Handle Expiry Safely
                expiry_val = None
                try:
                    raw_expiry = str(row[8]).strip()
                    if raw_expiry and raw_expiry.isdigit() and int(raw_expiry) > 0:
                        expiry_val = str(pd.to_datetime(int(raw_expiry), unit='s').date())
                except:
                    expiry_val = None

                all_filtered_rows.append({
                    "token_id": fy_token,
                    "tsym": clean_tsym,
                    "symbol": parent_index,
                    "expiry": expiry_val,
                    "strike": float(row[15]) if len(row) > 15 and pd.notnull(row[15]) and row[15] != -1 else None,
                    "option_type": str(row[16]) if len(row) > 16 and pd.notnull(row[16]) and str(row[16]) != 'XX' else None,
                    "last_validated": datetime.now().isoformat()
                })
        except Exception as e:
            print(f"❌ Error processing {key_name}: {e}")

    if all_filtered_rows:
        print(f"🚀 Syncing {len(all_filtered_rows)} rows to broker_tokens table...")
        # Batch upsert
        for i in range(0, len(all_filtered_rows), 1000):
            batch = all_filtered_rows[i:i+1000]
            supabase.table("broker_tokens").upsert(batch).execute()
        
        report_progress("success", f"✅ {len(all_filtered_rows)} Fyers FO tokens synced.")
        print("🏁 Sync Complete.")
    else:
        report_progress("error", "❌ No tokens found.")

if __name__ == "__main__":
    sync_fyers_tokens()
