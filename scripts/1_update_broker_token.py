import os
import sys
import subprocess

# --- 1. THE "BOOTSTRAP" BLOCK (Self-Sufficient Environment) ---
def bootstrap():
    """Checks and installs dependencies before the rest of the script runs."""
    required = ["pandas", "requests", "supabase", "python-dotenv"]
    try:
        import supabase
        import dotenv
    except ImportError:
        print("📦 Missing libraries detected. Bootstrapping environment...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", *required])

# Execute the bootstrap immediately
bootstrap()

# --- 2. NOW IT IS SAFE TO IMPORT EVERYTHING ELSE ---
import pandas as pd
import requests
import zipfile
import io
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Load .env (Local PyCharm) or use OS Environment (GitHub)
load_dotenv()

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("❌ Error: SUPABASE_URL or SUPABASE_KEY not found in environment.")
    sys.exit(1)
  
supabase: Client = create_client(url, key)

# --- HEARTBEAT REPORTER ---
def report_progress(status, msg):
    """Updates the real-time heartbeat in Supabase for Step 1"""
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": datetime.now().isoformat() # Better for Supabase than "now()" string
        }).eq("step_id", "step1").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

# --- CONFIGURATION ---
ALLOWED_INDICES = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
TOKEN_URL = "https://api.shoonya.com/NFO_symbols.txt.zip"

def sync_index_tokens_to_vault():
    print("🌐 Downloading latest tokens from Shoonya...")
    report_progress("running", "🌐 Downloading Shoonya Master...")
    
    try:
        response = requests.get(TOKEN_URL, timeout=30)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f)
    except Exception as e:
        print(f"❌ Error fetching broker tokens: {e}")
        report_progress("error", f"❌ Download Failed: {str(e)[:40]}")
        return

    print(f"🔍 Filtering for indices: {ALLOWED_INDICES}...")
    df_filtered = df[df['Symbol'].isin(ALLOWED_INDICES)].copy()

    print(f"✅ Found {len(df_filtered)} new/active index tokens.")
    report_progress("running", f"🔍 Processing {len(df_filtered)} index tokens...")

    token_payload = []
    for i, row in df_filtered.iterrows():
        token_payload.append({
            "token_id": str(row['Token']),
            "tsym": str(row['TradingSymbol']),
            "symbol": str(row.get('Symbol', '')),
            "expiry": str(pd.to_datetime(row.get('Expiry')).date()) if row.get('Expiry') else None,
            "strike": float(row.get('Strike', 0)) if row.get('Strike') else None,
            "option_type": str(row.get('OptionType', ''))
        })

        if len(token_payload) >= 1000:
            supabase.table("broker_tokens").upsert(token_payload).execute()
            token_payload = []
            print(f"🚀 Upserted batch at row {i}...")

    if token_payload:
        supabase.table("broker_tokens").upsert(token_payload).execute()

    print(f"🏁 DONE! The broker_tokens table is updated.")
    report_progress("success", f"✅ {len(df_filtered)} tokens synced.")

if __name__ == "__main__":
    try:
        sync_index_tokens_to_vault()
    except Exception as e:
        report_progress("error", f"❌ Fatal Error: {str(e)[:50]}")
        sys.exit(1)
