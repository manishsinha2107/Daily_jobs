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

def sync_fyers_tokens():
    print("🔄 Syncing Native Fyers Tokens (Daily Update)...")
    
    # 1. Download Fyers NSE F&O Master CSV
    csv_url = "https://public.fyers.in/sym_details/NSE_FO.csv"
    try:
        df = pd.read_csv(csv_url, header=None)
    except Exception as e:
        print(f"❌ Failed to download Fyers CSV: {e}")
        return

    # 2. Filter for Options (Index 14 represents Options in Fyers schema)
    target_indices = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']
    options_df = df[
        (df[13].isin(target_indices)) & 
        (df[9].str.endswith('CE') | df[9].str.endswith('PE'))
    ]

    print(f"📥 Found {len(options_df)} Active Options Contracts.")

    # 3. Prepare payload for Supabase matching the exact schema
    payload = []
    
    for _, row in options_df.iterrows():
        # row[0] = fyToken (e.g., 101126040740530)
        # row[9] = SymbolTicker (e.g., NSE:NIFTY2640719600CE)
        # row[13] = Underlying (e.g., NIFTY)
        payload.append({
            "token_id": str(row[0]),
            "tsym": str(row[9]),
            "symbol": str(row[13]),
            "last_validated": datetime.now().isoformat(),
            "is_historical": False
        })

    # 4. Bulk Upsert in batches of 1000 using token_id as the primary key
    print(f"🚀 Upserting {len(payload)} native Fyers tokens...")
    for i in range(0, len(payload), 1000):
        batch = payload[i:i+1000]
        supabase.table("broker_tokens").upsert(batch, on_conflict="token_id").execute()
        print(f"   - Synced batch {i//1000 + 1}...")

    print("✅ Daily Broker Token Sync Complete!")

if __name__ == "__main__":
    sync_fyers_tokens()
