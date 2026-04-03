import os
import sys
import pandas as pd
import requests
import io
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

TARGET_KEYWORDS = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
SYMBOL_URLS = {"NSE_FO": "https://public.fyers.in/sym_details/NSE_FO.csv"}

def format_to_legacy(row):
    try:
        # 1. Base Name from Col 13
        base = str(row[13]).strip() # e.g. NIFTY
        
        # 2. Expiry from Col 8 (Epoch)
        dt = pd.to_datetime(int(row[8]), unit='s')
        day = dt.strftime('%d')           # 24
        month = dt.strftime('%b').upper() # FEB
        year = dt.strftime('%y')          # 26
        
        # 3. Strike from Col 15
        strike = str(int(float(row[15]))) # 25550
        
        # 4. Option Type from Col 16
        opt = 'C' if str(row[16]) == 'CE' else 'P' if str(row[16]) == 'PE' else None
        
        if not opt: return None # Skip Futures/Indices
        
        # Result: NIFTY + 24 + FEB + 26 + P + 25550
        return f"{base}{day}{month}{year}{opt}{strike}"
    except:
        return None

def sync_fyers_tokens():
    print("🔄 Re-Syncing with Legacy Format Bridge...")
    response = requests.get(SYMBOL_URLS["NSE_FO"], timeout=30)
    df = pd.read_csv(io.StringIO(response.text), header=None)
    
    # Filter for target indices
    mask = df.stack().str.contains('|'.join(TARGET_KEYWORDS), na=False).unstack().any(axis=1)
    df_filtered = df[mask].copy()

    all_filtered_rows = []
    for _, row in df_filtered.iterrows():
        legacy_tsym = format_to_legacy(row)
        if not legacy_tsym: continue

        all_filtered_rows.append({
            "token_id": str(row[0]),       # Fyers fyToken
            "tsym": legacy_tsym,           # YOUR FORMAT (e.g. NIFTY26APR07P18550)
            "symbol": str(row[13]),        # NIFTY
            "expiry": str(pd.to_datetime(int(row[8]), unit='s').date()),
            "strike": float(row[15]),
            "option_type": str(row[16])
        })

    if all_filtered_rows:
        print(f"🚀 Upserting {len(all_filtered_rows)} standardized tokens...")
        for i in range(0, len(all_filtered_rows), 1000):
            supabase.table("broker_tokens").upsert(all_filtered_rows[i:i+1000]).execute()
        print("✅ Done! Symbols now match your historical data.")

if __name__ == "__main__":
    sync_fyers_tokens()
