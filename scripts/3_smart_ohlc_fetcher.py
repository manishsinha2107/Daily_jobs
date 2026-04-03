import os
import sys
import pyotp
import requests
import pandas as pd
from datetime import datetime
from fyers_apiv3 import fyersModel
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. INITIALIZATION ---
load_dotenv()
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Fyers Config
CLIENT_ID = os.getenv("FYERS_USERNAME")
APP_ID = os.getenv("FYERS_APP_ID")
SECRET_ID = os.getenv("FYERS_SECRET_ID")
PIN = os.getenv("FYERS_PIN")
TOTP_KEY = os.getenv("FYERS_TOTP_KEY")

def get_fyers_access_token():
    """Headless Auth to bypass browser login on GitHub Actions"""
    try:
        print("🔐 Authenticating with Fyers...")
        # Step 1: Send Client ID
        ses = requests.Session()
        payload1 = {"client_id": APP_ID, "redirect_uri": "https://trade.fyers.in/api-login/redirect-uri/index.html", "response_type": "code", "state": "sample_state"}
        res1 = ses.post("https://api-t1.fyers.in/api/v3/generate-authcode", json=payload1).json()
        
        # Step 2: Send TOTP
        otp = pyotp.TOTP(TOTP_KEY).now()
        payload2 = {"request_key": res1['request_key'], "otp": otp}
        res2 = ses.post("https://api-t1.fyers.in/api/v3/validate-otp", json=payload2).json()
        
        # Step 3: Send PIN
        payload3 = {"request_key": res2['request_key'], "pin": PIN}
        res3 = ses.post("https://api-t1.fyers.in/api/v3/validate-pin", json=payload3).json()
        
        # Step 4: Generate Access Token
        auth_code = res3['data']['authorization_code']
        session = fyersModel.SessionModel(client_id=APP_ID, secret_key=SECRET_ID, redirect_uri="https://trade.fyers.in/api-login/redirect-uri/index.html", response_type="code", grant_type="authorization_code")
        session.set_token(auth_code)
        response = session.generate_token()
        return response["access_token"]
    except Exception as e:
        print(f"❌ Auth Failed: {e}")
        return None

def fetch_ohlc():
    # 1. Login
    access_token = get_fyers_access_token()
    if not access_token: return
    fyers = fyersModel.FyersModel(client_id=APP_ID, token=access_token, is_async=False, log_path="")

    # 2. Get pending tasks
    tasks = supabase.table("strategy_trades_verification").select("*").eq("ohlc_status", "pending_api_search").limit(10).execute().data
    if not tasks:
        print("✅ No pending tasks.")
        return

    for task in tasks:
        sym = task['broker_symbol']
        t_date = task['trade_date']
        fyers_sym = f"NSE:{sym}" # Bridge to Fyers format
        
        print(f"📥 Fetching {sym} for {t_date}...")
        
        data = {"symbol": fyers_sym, "resolution": "1", "date_format": "1", "range_from": t_date, "range_to": t_date, "cont_flag": "1"}
        response = fyers.history(data=data)
        
        if response.get("s") == "ok":
            candles = response.get("candles", [])
            ohlc_batch = []
            for c in candles:
                # Fyers candle: [epoch, o, h, l, c, v]
                dt_obj = datetime.fromtimestamp(c[0])
                # Format to match your legacy AM/PM style: 9:31 AM (no leading zero on hour)
                ts_str = dt_obj.strftime("%Y-%m-%d %-I:%M %p") 
                
                ohlc_batch.append({
                    "token": str(task['token_id']),
                    "ts": ts_str,
                    "symbol": sym, # Saved back in your legacy format
                    "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])
                })
            
            if ohlc_batch:
                supabase.table("market_ohlc_cache").upsert(ohlc_batch).execute()
                supabase.table("strategy_trades_verification").update({"ohlc_status": "success"}).eq("id", task["id"]).execute()
                print(f"✅ Cached {len(ohlc_batch)} mins for {sym}")
        else:
            print(f"⚠️ Error for {sym}: {response.get('message')}")

if __name__ == "__main__":
    fetch_ohlc()
