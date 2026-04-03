import os
import sys
import pyotp
import requests
import base64
import json
import hashlib
from datetime import datetime
from fyers_apiv3 import fyersModel
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. INITIALIZATION ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Fyers Credentials
FY_ID = os.getenv("FYERS_USERNAME")
APP_ID = os.getenv("FYERS_APP_ID")
SECRET_ID = os.getenv("FYERS_SECRET_ID")
PIN = os.getenv("FYERS_PIN")
TOTP_KEY = os.getenv("FYERS_TOTP_KEY")
REDIRECT_URL = "https://trade.fyers.in/api-login/redirect-uri/index.html"

def get_fyers_access_token():
    """Headless Authentication Flow (2026 Fixed)"""
    print("🔐 Starting Secure Fyers Auth...")
    s = requests.Session()
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    
    try:
        # Step 1: Client ID
        payload1 = {"fy_id": base64.b64encode(FY_ID.encode()).decode(), "app_id": "2"}
        r1 = s.post("https://api-t2.fyers.in/vagator/v2/send_login_otp_v2", json=payload1, headers=headers).json()
        req_key = r1.get('request_key')

        # Step 2: TOTP
        otp = pyotp.TOTP(TOTP_KEY).now()
        r2 = s.post("https://api-t2.fyers.in/vagator/v2/verify_otp", json={"request_key": req_key, "otp": otp}, headers=headers).json()
        req_key = r2.get('request_key')

        # Step 3: PIN
        payload3 = {"request_key": req_key, "identity_type": "pin", "identifier": base64.b64encode(PIN.encode()).decode()}
        r3 = s.post("https://api-t2.fyers.in/vagator/v2/verify_pin_v2", json=payload3, headers=headers).json()
        token_v2 = r3['data']['access_token']

        # Step 4: Authorization Code Exchange (FIXED FOR 2026)
        headers_auth = {'Authorization': f'Bearer {token_v2}', 'Content-Type': 'application/json'}
        payload4 = {
            "fyers_id": FY_ID, "app_id": APP_ID.split('-')[0], "redirect_uri": REDIRECT_URL, 
            "appType": "100", "response_type": "code", "state": "abcdefg"
        }
        r4 = s.post("https://api-t1.fyers.in/api/v3/token", json=payload4, headers=headers_auth).json()
        
        # In 2026, the auth_code is inside r4['data']['authorization_code']
        if r4.get('s') == 'ok' and 'data' in r4:
            auth_code = r4['data']['authorization_code']
        else:
            print(f"🛑 Step 4 Failed. Response: {r4}")
            return None

        # Step 5: Final Token Generation
        # We must use the appIdHash (SHA256 of APP_ID:SECRET_ID)
        app_id_hash = hashlib.sha256(f"{APP_ID}:{SECRET_ID}".encode()).hexdigest()
        
        session = fyersModel.SessionModel(
            client_id=APP_ID, secret_key=SECRET_ID, redirect_uri=REDIRECT_URL, 
            response_type="code", grant_type="authorization_code"
        )
        # Using the direct validate_authcode logic to be safe
        response = session.generate_token({"grant_type": "authorization_code", "appIdHash": app_id_hash, "code": auth_code})
        return response["access_token"]

    except Exception as e:
        print(f"⚠️ Auth Exception: {str(e)}")
        return None

def fetch_and_cache_ohlc():
    # 1. Login
    access_token = get_fyers_access_token()
    if not access_token:
        return
    
    fyers = fyersModel.FyersModel(client_id=APP_ID, token=access_token, is_async=False, log_path="")

    # 2. Get 10 pending tasks from verification table
    res = supabase.table("strategy_trades_verification") \
        .select("id, broker_symbol, trade_date, token_id") \
        .eq("ohlc_status", "pending_api_search") \
        .limit(10).execute()
    
    tasks = res.data
    if not tasks:
        print("✅ No pending OHLC tasks.")
        return

    for task in tasks:
        sym = task['broker_symbol']
        t_date = str(task['trade_date'])
        fyers_sym = f"NSE:{sym}" # Use the bridge for API call
        
        print(f"📥 Fetching {sym} for {t_date}...")
        
        data = {
            "symbol": fyers_sym,
            "resolution": "1",
            "date_format": "1",
            "range_from": t_date,
            "range_to": t_date,
            "cont_flag": "1"
        }
        response = fyers.history(data=data)
        
        if response.get("s") == "ok":
            candles = response.get("candles", [])
            ohlc_batch = []
            
            for c in candles:
                # Fyers candle: [epoch, o, h, l, c, v]
                dt_obj = datetime.fromtimestamp(c[0])
                
                # Format to your exact style: 2026-04-03 9:31 AM (No leading zero on hour)
                ts_str = dt_obj.strftime("%Y-%m-%d %-I:%M %p")
                
                ohlc_batch.append({
                    "token": str(task['token_id']),
                    "ts": ts_str,
                    "symbol": sym, # Save in your legacy format
                    "open": float(c[1]), 
                    "high": float(c[2]), 
                    "low": float(c[3]), 
                    "close": float(c[4]), 
                    "volume": int(c[5])
                })
            
            if ohlc_batch:
                # Upsert into cache
                supabase.table("market_ohlc_cache").upsert(ohlc_batch).execute()
                # Mark as success in verification table
                supabase.table("strategy_trades_verification").update({"ohlc_status": "success"}).eq("id", task["id"]).execute()
                print(f"✅ Cached {len(ohlc_batch)} minutes for {sym}")
        else:
            msg = response.get('message', 'Unknown Error')
            print(f"⚠️ API Error for {sym}: {msg}")
            if "not found" in msg.lower():
                 supabase.table("strategy_trades_verification").update({"ohlc_status": "historical_data_unavailable_at_broker"}).eq("id", task["id"]).execute()

if __name__ == "__main__":
    fetch_and_cache_ohlc()
