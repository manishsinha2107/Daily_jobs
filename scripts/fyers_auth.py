import os
import base64
import requests
import pyotp
from datetime import datetime, timezone
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

# --- PATH & ENVIRONMENT CONFIGURATION ---
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(ROOT_DIR, ".env")

if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

# Fyers Credentials
FY_ID = os.getenv("FYERS_USERNAME")
APP_ID = os.getenv("FYERS_APP_ID")
SECRET_ID = os.getenv("FYERS_SECRET_ID")
PIN = os.getenv("FYERS_PIN")
TOTP_KEY = os.getenv("FYERS_TOTP_KEY")
REDIRECT_URL = "https://trade.fyers.in/api-login/redirect-uri/index.html"

# Supabase Credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")
if SUPABASE_URL:
    SUPABASE_URL = SUPABASE_URL.rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
} if SUPABASE_KEY else {}

def get_fyers_access_token(silent=False):
    """
    Centralized Authentication Flow.
    Checks Supabase cache first, falls back to Headless Auth and updates cache.
    Set silent=True to suppress standard terminal outputs (errors still print).
    """
    
    def _log(msg, is_error=False):
        if not silent or is_error:
            print(msg)

    # --- 1. SUPABASE CACHE LOOKUP ---
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            res = requests.get(
                f"{SUPABASE_URL}/rest/v1/broker_sessions?id=eq.1&select=fyers_access_token,updated_at",
                headers=SUPABASE_HEADERS, 
                timeout=5
            )
            if res.status_code == 200:
                data = res.json()
                if data and len(data) > 0:
                    cached_token = data[0].get("fyers_access_token")
                    updated_at_str = data[0].get("updated_at")
                    
                    if cached_token and updated_at_str:
                        if updated_at_str.endswith('Z'):
                            updated_at_str = updated_at_str[:-1] + '+00:00'
                        updated_at = datetime.fromisoformat(updated_at_str)
                        now = datetime.now(timezone.utc)
                        
                        age_hours = (now - updated_at).total_seconds() / 3600.0
                        if age_hours < 15:
                            _log(f" ✅ Found valid Fyers session in Supabase (Age: {age_hours:.1f} hours). Bypassing login.")
                            return cached_token
                        else:
                            _log(f" ♻️ Cached token is {age_hours:.1f} hours old (Expired). Performing fresh login...")
        except Exception as e:
            _log(f" ⚠️ Failed to read from Supabase cache: {e}", is_error=True)

    # --- 2. HEADLESS AUTH FLOW ---
    _log(" 🔑 Performing fresh Fyers Headless Authentication...")
    s = requests.Session()
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    try:
        payload1 = {"fy_id": base64.b64encode(FY_ID.encode()).decode(), "app_id": "2"}
        r1 = s.post("https://api-t2.fyers.in/vagator/v2/send_login_otp_v2", json=payload1, headers=headers).json()
        req_key = r1.get('request_key')

        if not req_key:
             _log(f"❌ OTP Request Error: {r1}", is_error=True)
             return None

        otp = pyotp.TOTP(TOTP_KEY).now()
        r2 = s.post("https://api-t2.fyers.in/vagator/v2/verify_otp", json={"request_key": req_key, "otp": otp}, headers=headers).json()
        req_key = r2.get('request_key')

        payload3 = {"request_key": req_key, "identity_type": "pin", "identifier": base64.b64encode(PIN.encode()).decode()}
        r3 = s.post("https://api-t2.fyers.in/vagator/v2/verify_pin_v2", json=payload3, headers=headers).json()
        token_v2 = r3['data']['access_token']

        short_app_id = APP_ID.split('-')[0]
        headers_auth = {'Authorization': f'Bearer {token_v2}', 'Content-Type': 'application/json'}
        payload4 = {
            "fyers_id": FY_ID, "app_id": short_app_id, "redirect_uri": REDIRECT_URL, 
            "appType": "100", "response_type": "code", "state": "abcdefg"
        }
        r4 = s.post("https://api-t1.fyers.in/api/v3/token", json=payload4, headers=headers_auth).json()
        
        if 'Url' in r4:
            auth_code = r4['Url'].split('auth_code=')[1].split('&')[0]
        else:
            _log(f"❌ Auth Code Error: {r4}", is_error=True)
            return None

        session = fyersModel.SessionModel(
            client_id=APP_ID, secret_key=SECRET_ID, redirect_uri=REDIRECT_URL, 
            response_type="code", grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        
        if response.get("s") == "ok" and "access_token" in response:
            new_token = response["access_token"]
            
            # --- 3. SUPABASE CACHE UPDATE ---
            if SUPABASE_URL and SUPABASE_KEY:
                try:
                    patch_payload = {
                        "fyers_access_token": new_token,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }
                    patch_res = requests.patch(
                        f"{SUPABASE_URL}/rest/v1/broker_sessions?id=eq.1",
                        headers=SUPABASE_HEADERS,
                        json=patch_payload,
                        timeout=5
                    )
                    if patch_res.status_code in [200, 204]:
                        _log(" ✅ Successfully saved new Fyers session to Supabase.")
                    else:
                        _log(f" ⚠️ Failed to update Supabase: {patch_res.text}", is_error=True)
                except Exception as e:
                    _log(f" ⚠️ Network error while updating Supabase cache: {e}", is_error=True)
            
            return new_token
            
        _log(f"❌ Final Token Error: {response}", is_error=True)
        return None
    except Exception as e:
        _log(f"⚠️ Auth Exception: {str(e)}", is_error=True)
        return None
