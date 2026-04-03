import os
import sys
import pyotp
import requests
import hashlib
import pandas as pd
from datetime import datetime
from fyers_apiv3 import fyersModel
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. INITIALIZATION ---
load_dotenv()
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Fyers Config
FY_ID = os.getenv("FYERS_USERNAME")
APP_ID = os.getenv("FYERS_APP_ID")
SECRET_ID = os.getenv("FYERS_SECRET_ID")
PIN = os.getenv("FYERS_PIN")
TOTP_KEY = os.getenv("FYERS_TOTP_KEY")
REDIRECT_URL = "https://trade.fyers.in/api-login/redirect-uri/index.html"

def get_fyers_access_token():
    """Headless Auth using 2026 Vagator V3 Flow"""
    try:
        print("🔐 Authenticating with Fyers (V3 Flow)...")
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        
        # Step 1: Send Client ID (FY_ID) to get the first request_key
        payload1 = {"fy_id": FY_ID, "app_id": APP_ID}
        res1 = requests.post("https://api-t2.fyers.in/vagator/v2/send_login_otp", json=payload1, headers=headers).json()
        req_key = res1.get('request_key')
        
        # Step 2: Verify TOTP
        otp = pyotp.TOTP(TOTP_KEY).now()
        payload2 = {"request_key": req_key, "otp": otp}
        res2 = requests.post("https://api-t2.fyers.in/vagator/v2/verify_otp", json=payload2, headers=headers).json()
        req_key = res2.get('request_key')
        
        # Step 3: Verify PIN
        payload3 = {"request_key": req_key, "identity_type": "pin", "identifier": PIN}
        res3 = requests.post("https://api-t2.fyers.in/vagator/v2/verify_pin", json=payload3, headers=headers).json()
        token_v2 = res3['data']['access_token']
        
        # Step 4: Exchange for Auth Code
        headers_auth = {'Authorization': f'Bearer {token_v2}', 'Content-Type': 'application/json'}
        payload4 = {
            "fyers_id": FY_ID, "app_id": APP_ID, "redirect_uri": REDIRECT_URL, 
            "appType": "100", "response_type": "code", "state": "sample"
        }
        res4 = requests.post("https://api-t1.fyers.in/api/v3/token", json=payload4, headers=headers_auth).json()
        auth_code = res4['Url'].split('auth_code=')[1].split('&')[0]
        
        # Step 5: Final Access Token
        app_id_hash = hashlib.sha256(f"{APP_ID}:{SECRET_ID}".encode()).hexdigest()
        session = fyersModel.SessionModel(
            client_id=APP_ID, secret_key=SECRET_ID, redirect_uri=REDIRECT_URL, 
            response_type="code", grant_type="authorization_code"
        )
        session.set_token(auth_code)
        return session.generate_token()["access_token"]
        
    except Exception as e:
        print(f"❌ Auth Failed: {e}")
        return None

def fetch_ohlc():
    access_token = get_fyers_access_token()
    if not access_token: return
    
    fyers = fyersModel.FyersModel(client_id=APP_ID, token=access_token, is_async=False, log_path="")

    # Fetch 10 pending tasks
    tasks = supabase.table("strategy_trades_verification").select("*").eq("ohlc_status", "pending_api_search").limit(10).execute().data
    if not tasks:
        print("✅ No pending tasks.")
        return

    for task in tasks:
        sym = task['broker_symbol']
        t_date = str(task['trade_date'])
        fyers_sym = f"NSE:{sym}"
        
        print(f"📥 Fetching {sym} for {t_date}...")
        
        data = {"symbol": fyers_sym, "resolution": "1", "date_format": "1", "range_from": t_date, "range_to": t_date, "cont_flag": "1"}
        response = fyers.history(data=data)
        
        if response.get("s") == "ok":
            candles = response.get("candles", [])
            ohlc_batch = []
            for c in candles:
                dt_obj = datetime.fromtimestamp(c[0])
                # Legacy format: 2026-04-03 9:15 AM
                ts_str = dt_obj.strftime("%Y-%m-%d %-I:%M %p")
                
                ohlc_batch.append({
                    "token": str(task['token_id']),
                    "ts": ts_str,
                    "symbol": sym,
                    "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])
                })
            
            if ohlc_batch:
                supabase.table("market_ohlc_cache").upsert(ohlc_batch).execute()
                supabase.table("strategy_trades_verification").update({"ohlc_status": "success"}).eq("id", task["id"]).execute()
                print(f"✅ Success: {len(ohlc_batch)} rows for {sym}")
        else:
            print(f"⚠️ API Error for {sym}: {response.get('message')}")

if __name__ == "__main__":
    fetch_ohlc()
