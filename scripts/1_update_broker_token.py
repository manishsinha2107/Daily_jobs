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
    """
    Converts Fyers raw data into your historical format:
    Example: NIFTY26APR25500P
    """
    try:
        # 1. Get Base (NIFTY, BANKNIFTY)
        base = str(row[13]) # short_sym column
        
        # 2. Format Expiry (YY + MMM + DD)
        # Fyers uses Epoch for expiry (row[8])
        dt = pd.to_datetime(int(row[8]), unit='s')
        year = dt.strftime('%y')        # 26
        month = dt.strftime('%b').upper() # APR
        day = dt.strftime('%d')         # 07
        
        # 3. Strike (Numeric)
        strike = str(int(float(row[15])))
        
        # 4. Option Type (P or C)
        opt = 'P' if str(row[16]) == 'PE' else 'C' if str(row[16]) == 'CE' else ''
        
        # RECONSTRUCT: NIFTY + 26 + APR + 07 + P + 18550
        return f"{base}{year}{month}{day}{opt}{strike}"
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
