import os
import sys
import math
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# --- NEW: Import Centralized Auth ---
from fyers_auth import get_fyers_access_token

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
    
    # --- 0. AUTHENTICATION INJECTION ---
    report_progress("running", "🔑 Securing Fyers 15-Hour Session Token...")
    token = get_fyers_access_token(silent=False)
    if token:
        report_progress("running", "✅ Fyers session secured in Vault.")
    else:
        report_progress("error", "❌ Failed to secure Fyers session. Subsequent steps may fail.")
        # We allow the script to continue to at least fetch the CSV
    
    # --- 1. Download Fyers NSE F&O Master CSV ---
    report_progress("running", "📡 Downloading Fyers NSE & BSE Master CSVs...")
    urls = [
        ("NSE", "https://public.fyers.in/sym_details/NSE_FO.csv"),
        ("BSE", "https://public.fyers.in/sym_details/BSE_FO.csv")
    ]
    dfs = []
    for exchange, csv_url in urls:
        try:
            print(f"  📥 Downloading {exchange} F&O master...")
            part_df = pd.read_csv(csv_url, header=None)
            dfs.append(part_df)
        except Exception as e:
            print(f"  ⚠️ Warning: Failed to download {exchange} CSV: {e}")

    if not dfs:
        err_msg = "❌ Failed to download both NSE and BSE CSVs."
        print(err_msg)
        report_progress("error", err_msg)
        return

    df = pd.concat(dfs, ignore_index=True)

    # --- 2. Filter for Target Indices (Futures + Options) ---
    target_indices = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX']
    df = df[df[13].isin(target_indices)].copy()

    # Exact Script 13 Expiry Date Parsing
    df['parsed_expiry'] = pd.to_datetime(df[8], unit='s', utc=True).dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d')
    today_str = datetime.now().strftime('%Y-%m-%d')

    # Filter for active Options and Futures contracts
    is_option = df[9].str.endswith('CE') | df[9].str.endswith('PE')
    is_future = df[9].str.endswith('FUT')
    active_df = df[is_option | is_future].copy()

    found_count = len(active_df)
    print(f"📥 Found {found_count} Active Contracts (Options & Futures across NSE + BSE).")
    report_progress("running", f"📥 Found {found_count} contracts. Preparing payloads...")

    # --- 3. Build Payloads for Both Tables ---
    broker_tokens_payload = []
    universe_payload = []

    for _, row in active_df.iterrows():
        tsym_str = str(row[9])
        inst_type = 'FUT' if tsym_str.endswith('FUT') else 'OPT'
        expiry_val = str(row['parsed_expiry'])
        lot_sz = int(row[3]) if pd.notna(row[3]) else None

        # Determine strike and opt_type
        if inst_type == 'OPT':
            strike_val = float(row[15]) if pd.notna(row[15]) else None
            opt_type_val = str(row[16]) if (len(row) > 16 and pd.notna(row[16])) else ('CE' if tsym_str.endswith('CE') else 'PE')
        else:
            strike_val = None
            opt_type_val = None

        # Payload A: broker_tokens
        broker_tokens_payload.append({
            "token_id": str(row[0]),
            "tsym": tsym_str,
            "symbol": str(row[13]),
            "expiry_date": expiry_val,
            "last_validated": datetime.now().isoformat(),
            "is_historical": False
        })

        # Payload B: tracked_options_universe
        universe_payload.append({
            "symbol": tsym_str,
            "expiry_date": expiry_val,
            "discovered_on": today_str,
            "index_name": str(row[13]),
            "instrument_type": inst_type,
            "strike": strike_val,
            "opt_type": opt_type_val,
            "lot_size": lot_sz
        })

    # --- 4. Bulk Upsert to Supabase ---
    try:
        # A. Upsert to broker_tokens
        print(f"🚀 Upserting {len(broker_tokens_payload)} tokens to broker_tokens...")
        chunk_size = 1000
        for i in range(0, len(broker_tokens_payload), chunk_size):
            batch = broker_tokens_payload[i:i + chunk_size]
            supabase.table("broker_tokens").upsert(batch, on_conflict="token_id").execute()
            progress_msg = f"⚡ broker_tokens: {min(i + chunk_size, found_count)}/{found_count} synced..."
            print(f"    - {progress_msg}")
            report_progress("running", progress_msg)

        # B. Upsert to tracked_options_universe
        print(f"🚀 Upserting {len(universe_payload)} contracts to tracked_options_universe...")
        for i in range(0, len(universe_payload), chunk_size):
            batch = universe_payload[i:i + chunk_size]
            supabase.table("tracked_options_universe").upsert(batch, on_conflict="symbol").execute()
            print(f"    - ⚡ tracked_options_universe: {min(i + chunk_size, found_count)}/{found_count} synced...")

        print("✅ Dual Table Sync Complete!")
        report_progress("success", f"✅ Synced {found_count} contracts to broker_tokens & universe.")

    except Exception as e:
        err_msg = f"❌ Upsert Failed: {str(e)[:80]}"
        print(err_msg)
        report_progress("error", err_msg)

if __name__ == "__main__":
    sync_fyers_tokens()
