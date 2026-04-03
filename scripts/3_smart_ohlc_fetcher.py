import os
import pyotp
import base64
import requests
import pandas as pd
from datetime import datetime
import pytz
from supabase import create_client, Client
from dotenv import load_dotenv
from collections import defaultdict
from fyers_apiv3 import fyersModel

# --- MIGRATION FIX: Local vs Cloud Environment ---
if os.path.exists(".env"):
    load_dotenv()

# Initialize Supabase
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
if not url or not key:
    print("❌ Error: Supabase credentials missing.")
    exit(1)
supabase: Client = create_client(url, key)

# --- HEARTBEAT REPORTER (Surgical Patch) ---
def report_progress(status, msg):
    """Updates the real-time heartbeat in Supabase for Step 3"""
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": "now()"
        }).eq("step_id", "step3").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

# Initialize Fyers Constants from Environment Variables
FY_ID = os.getenv("FYERS_USERNAME")
APP_ID = os.getenv("FYERS_APP_ID")
SECRET_ID = os.getenv("FYERS_SECRET_ID")
PIN = os.getenv("FYERS_PIN")
TOTP_KEY = os.getenv("FYERS_TOTP_KEY")
REDIRECT_URL = "https://trade.fyers.in/api-login/redirect-uri/index.html"

def get_fyers_access_token():
    """Headless Authentication Flow for Fyers"""
    s = requests.Session()
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    try:
        payload1 = {"fy_id": base64.b64encode(FY_ID.encode()).decode(), "app_id": "2"}
        r1 = s.post("https://api-t2.fyers.in/vagator/v2/send_login_otp_v2", json=payload1, headers=headers).json()
        req_key = r1.get('request_key')

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
            return None

        session = fyersModel.SessionModel(
            client_id=APP_ID, secret_key=SECRET_ID, redirect_uri=REDIRECT_URL, 
            response_type="code", grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        
        if response.get("s") == "ok" and "access_token" in response:
            return response["access_token"]
        return None
    except Exception as e:
        print(f"⚠️ Auth Exception: {str(e)}")
        return None

def run_smart_fetcher():
    print("🚀 Starting Smart OHLC Fetcher (Date-Symbol-Strategy Grouping)...")
    report_progress("running", "📡 Reading pending OHLC tasks...")
    
    ist = pytz.timezone('Asia/Kolkata')

    # 1. FETCH SNAPSHOT
    pending_tasks = []
    offset = 0
    print("📡 Reading snapshot of trades requiring fresh OHLC verification...")
    while True:
        res = supabase.table("strategy_trades_verification") \
            .select("id, token_id, trade_date, broker_symbol, ohlc_status, strategy_id, strategy_name") \
            .eq("ohlc_status", "pending_api_search") \
            .eq("pnl_status", "pending") \
            .range(offset, offset + 999).execute()

        if not res.data: break
        pending_tasks.extend(res.data)
        if len(res.data) < 1000: break
        offset += 1000

    if not pending_tasks:
        print("✅ No trades pending verification.")
        report_progress("success", "✅ No pending tasks found.")
        return

    # --- ENHANCED LOGIC: GROUP BY DATE + SYMBOL + STRATEGY_ID ---
    task_groups = defaultdict(list)
    for task in pending_tasks:
        key = (task['trade_date'], task['broker_symbol'], task['strategy_id'])
        task_groups[key].append(task)

    print(f"📦 Found {len(pending_tasks)} trade rows across {len(task_groups)} unique Date-Symbol-Strategy groups.")
    report_progress("running", f"📦 Processing {len(task_groups)} OHLC groups...")
    print("-" * 100)

    fyers_api = None
    group_idx = 0
    total_groups = len(task_groups)

    for (t_date, b_sym, s_id), rows in task_groups.items():
        group_idx += 1
        s_name = rows[0].get('strategy_name', 'Unknown')
        valid_token = next((r['token_id'] for r in rows if r['token_id']), None)
        ids_to_update = [r['id'] for r in rows]

        print(f"\n🔄 [{group_idx}/{total_groups}] Strategy: {s_name} (ID: {s_id})")
        print(f"   📍 Target: {b_sym} | Date: {t_date} | Linked Rows: {len(ids_to_update)}")
        report_progress("running", f"🔄 [{group_idx}/{total_groups}] Fetching {b_sym}...")

        # --- STEP 1: SHELF CHECK (Database) ---
        shelf_res = supabase.table("market_ohlc_cache") \
            .select("ts", count="exact") \
            .eq("symbol", b_sym) \
            .like("ts", f"{t_date}%") \
            .execute()

        row_count = shelf_res.count if shelf_res.count else 0

        if row_count >= 300:
            print(f"   ✅ [DATABASE] Shelf Hit: {row_count} candles found. Skipping API.")
        else:
            # --- STEP 2: API FETCH (Fyers) ---
            print(f"   📡 [API] Shelf Miss: Only {row_count} candles. Requesting Fyers...")
            if fyers_api is None:
                report_progress("running", "🔑 Authenticating with Fyers...")
                access_token = get_fyers_access_token()
                if not access_token:
                    print("   ❌ Fyers Login failed.")
                    report_progress("error", "❌ Fyers Login Failed")
                    return
                fyers_api = fyersModel.FyersModel(client_id=APP_ID, token=access_token, is_async=False, log_path="")

            # Local Token Resolution Logic (Fallback if missing in verification table)
            if not valid_token:
                token_lookup = supabase.table("broker_tokens").select("token_id").eq("tsym", b_sym).execute()
                if token_lookup.data:
                    valid_token = token_lookup.data[0]['token_id']
                else:
                    valid_token = 0 # Default fallback to satisfy DB constraints

            # Fyers History Call
            data = {
                "symbol": b_sym,
                "resolution": "1",
                "date_format": "1",
                "range_from": t_date,
                "range_to": t_date,
                "cont_flag": "1"
            }
            response = fyers_api.history(data=data)

            if response.get("s") == "ok":
                candles = response.get("candles", [])
                if candles:
                    ohlc_batch = []
                    for c in candles:
                        # Exact Legacy Time Formatting Logic 
                        dt_obj = datetime.fromtimestamp(c[0], ist)
                        time_part = dt_obj.strftime('%I:%M:%S %p')
                        if time_part.startswith('0'): time_part = time_part[1:]
                        readable_ist_ts = f"{t_date} {time_part}"

                        ohlc_batch.append({
                            "token": str(valid_token),
                            "ts": readable_ist_ts,
                            "symbol": b_sym,
                            "open": float(c[1]),
                            "high": float(c[2]),
                            "low": float(c[3]),
                            "close": float(c[4]),
                            "volume": int(c[5])
                        })

                    if ohlc_batch:
                        supabase.table("market_ohlc_cache").upsert(ohlc_batch).execute()
                        row_count = len(ohlc_batch)
                        print(f"   📥 [SUCCESS] API returned {row_count} candles. Cached successfully.")
                else:
                    print(f"   ⚠️ [EMPTY] Fyers API returned empty candles array for this range.")
            else:
                msg = response.get('message', 'Unknown Error')
                print(f"   ❌ [ERROR] Fyers API response: {msg}")

        # --- STEP 3: STATUS ASSIGNMENT ---
        if row_count >= 300:
            final_ohlc_status = "verified_ohlc_present"
            final_pnl_status = "pending"
            final_pnl_1min_status = "pending"
        else:
            final_ohlc_status = "missing_ohlc_at_vault"
            final_pnl_status = "skipped_no_ohlc"
            final_pnl_1min_status = "skipped_no_ohlc"

        # Bulk update for this specific Group
        supabase.table("strategy_trades_verification").update({
            "ohlc_status": final_ohlc_status,
            "pnl_status": final_pnl_status,
            "pnl_1min_status": final_pnl_1min_status,
            "token_id": valid_token
        }).in_("id", ids_to_update).execute()

        print(f"   📝 [LOG] Records updated to '{final_ohlc_status}'. Processing complete for this group.")

    print("\n" + "="*100)
    print(f"{'SMART FETCHER RUN COMPLETED':^100}")
    print("="*100)
    report_progress("success", f"✅ OHLC Fetching Done for {total_groups} groups.")

if __name__ == "__main__":
    try:
        run_smart_fetcher()
    except Exception as e:
        import traceback
        traceback.print_exc()
        report_progress("error", f"❌ Error: {str(e)[:50]}")
        exit(1)
