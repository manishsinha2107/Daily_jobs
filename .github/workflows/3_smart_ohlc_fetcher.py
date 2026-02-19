import os
import pyotp
import pandas as pd
from datetime import datetime
import pytz
from NorenRestApiPy.NorenApi import NorenApi
from supabase import create_client, Client
from dotenv import load_dotenv
from collections import defaultdict

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

# Initialize Shoonya Constants from GitHub Secrets
SHOONYA_USER    = os.getenv('SHOONYA_USER_ID')
SHOONYA_PWD     = os.getenv('SHOONYA_PASSWORD')
SHOONYA_TOTP    = os.getenv('SHOONYA_TOTP_KEY')
SHOONYA_VC      = os.getenv('SHOONYA_VC')
SHOONYA_API_KEY = os.getenv('SHOONYA_API_KEY')
SHOONYA_IMEI    = os.getenv('SHOONYA_IMEI')

class ShoonyaApiPy(NorenApi):
    def __init__(self):
        super(ShoonyaApiPy, self).__init__(
            host='https://api.shoonya.com/NorenWClientTP/',
            websocket='wss://api.shoonya.com/NorenWClient/'
        )

def run_smart_fetcher():
    print("🚀 Starting Smart OHLC Fetcher (Date-Symbol-Strategy Grouping)...")
    # --- REPORTING START ---
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

    api = None
    group_idx = 0
    total_groups = len(task_groups)

    for (t_date, b_sym, s_id), rows in task_groups.items():
        group_idx += 1
        s_name = rows[0].get('strategy_name', 'Unknown')
        valid_token = next((r['token_id'] for r in rows if r['token_id']), None)
        ids_to_update = [r['id'] for r in rows]

        print(f"\n🔄 [{group_idx}/{total_groups}] Strategy: {s_name} (ID: {s_id})")
        print(f"   📍 Target: {b_sym} | Date: {t_date} | Linked Rows: {len(ids_to_update)}")
        
        # --- REPORTING PROGRESS ---
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
            # --- STEP 2: API FETCH (Shoonya) ---
            print(f"   📡 [API] Shelf Miss: Only {row_count} candles. Requesting Shoonya...")
            if api is None:
                report_progress("running", "🔑 Authenticating with Shoonya...")
                api = ShoonyaApiPy()
                totp_gen = pyotp.TOTP(os.getenv('SHOONYA_TOTP_KEY'))
                login_res = api.login(
                    userid=os.getenv('SHOONYA_USER_ID'), password=os.getenv('SHOONYA_PASSWORD'),
                    twoFA=totp_gen.now(), vendor_code=os.getenv('SHOONYA_VC'),
                    api_secret=os.getenv('SHOONYA_API_KEY'), imei=os.getenv('SHOONYA_IMEI')
                )
                if not login_res or login_res.get('stat') != 'Ok':
                    print(f"   ❌ Login failed: {login_res}")
                    report_progress("error", "❌ Shoonya Login Failed")
                    return

            # Token Resolution Logic
            if not valid_token:
                search_res = api.searchscrip(exchange='NFO', searchtext=b_sym)
                if search_res and 'values' in search_res:
                    for item in search_res['values']:
                        if str(item.get('tsym', '')).strip().upper() == str(b_sym).strip().upper():
                            valid_token = item['token']
                            # Backfill token to mapping table
                            supabase.table("broker_tokens").upsert({"token_id": valid_token, "tsym": b_sym}).execute()
                            break

            if valid_token:
                start_dt = ist.localize(datetime.strptime(f"{t_date} 09:15:00", "%Y-%m-%d %H:%M:%S"))
                end_dt = ist.localize(datetime.strptime(f"{t_date} 15:30:00", "%Y-%m-%d %H:%M:%S"))

                candles = api.get_time_price_series(exchange='NFO', token=valid_token,
                                                   starttime=int(start_dt.timestamp()),
                                                   endtime=int(end_dt.timestamp()), interval=1)

                if candles and isinstance(candles, list):
                    ohlc_batch = []
                    for c in candles:
                        dt_obj = datetime.strptime(c['time'], '%d-%m-%Y %H:%M:%S')
                        time_part = dt_obj.strftime('%I:%M:%S %p')
                        if time_part.startswith('0'): time_part = time_part[1:]
                        readable_ist_ts = f"{t_date} {time_part}"

                        ohlc_batch.append({
                            "token": int(valid_token), "ts": readable_ist_ts, "symbol": b_sym,
                            "open": float(c['into']), "high": float(c['inth']),
                            "low": float(c['intl']), "close": float(c['intc']),
                            "volume": int(c.get('v', 0))
                        })
                    if ohlc_batch:
                        supabase.table("market_ohlc_cache").upsert(ohlc_batch).execute()
                        row_count = len(ohlc_batch)
                        print(f"   📥 [SUCCESS] API returned {row_count} candles. Cached successfully.")
                else:
                    print(f"   ⚠️ [EMPTY] API returned no data for this token/range.")
            else:
                print(f"   ❌ [ERROR] Unable to resolve token for {b_sym}.")

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
    # --- REPORTING SUCCESS ---
    report_progress("success", f"✅ OHLC Fetching Done for {total_groups} groups.")

if __name__ == "__main__":
    try:
        run_smart_fetcher()
    except Exception as e:
        report_progress("error", f"❌ Error: {str(e)[:50]}")
        exit(1)
