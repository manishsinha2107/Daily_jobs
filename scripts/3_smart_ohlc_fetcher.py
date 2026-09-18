import os
import pyotp
import base64
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from supabase import create_client, Client
from dotenv import load_dotenv
from collections import defaultdict
from fyers_apiv3 import fyersModel
import config

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


# --- NEW: Import Centralized Auth ---
from fyers_auth import get_fyers_access_token

# Initialize Fyers Constants from Environment Variables
# (We keep APP_ID because the script still uses it locally to initialize fyersModel)
APP_ID = os.getenv("FYERS_APP_ID")

def get_monthly_fyers_tsym(inst_name):
    """Fallback Translator: Explicitly forces the Fyers Monthly Format"""
    try:
        if not inst_name:
            return None
        parts = inst_name.split('_')
        if parts[0] == 'OPTIDX':
            symbol, expiry_str, opt_type_full, strike = parts[1], parts[2], parts[3], parts[4]
            mmm = expiry_str[2:5].upper()
            yy = expiry_str[-2:]
            fyers_opt = 'CE' if 'C' in opt_type_full else 'PE'
            return f"NSE:{symbol}{yy}{mmm}{strike}{fyers_opt}"
        return inst_name
    except Exception as e:
        return None

def run_smart_fetcher():
    print("🚀 Starting Smart OHLC Fetcher (Stateful Positional + Intraday)...")
    ist = pytz.timezone('Asia/Kolkata')
    today_dt = datetime.now(ist)
    today_str = today_dt.strftime('%Y-%m-%d')
    
    report_progress("running", "📡 Initializing Stateful Positional Fetcher...")

    # 1. FETCH VALID STRATEGIES
    valid_strats_res = supabase.table("strategies").select("strategy_id").in_("deployment_type", config.DEPLOYMENT_TYPES).execute()
    valid_strat_ids = [int(s['strategy_id']) for s in valid_strats_res.data]
    
    if not valid_strat_ids:
        print("✅ No valid strategies for deployment type.")
        return

    # --- 2. BUILD THE UNIFIED TARGET QUEUE ---
    # target_queue[symbol] = {'start_date': 'YYYY-MM-DD', 'instrument': '...', 'linked_ids': set()}
    target_queue = {}
    
    # A. Ingest Active Positional Memory
    mem_res = supabase.table("multi_indices_ohlc_harvest_memory").select("*").gte("expiry_date", today_str).execute()
    for row in mem_res.data:
        sym = row['symbol']
        lh = row.get('last_harvested_date')
        
        if not lh:
            s_date = today_str
        elif lh < today_str:
            # Fetch from the day after the last successful harvest
            lh_dt = datetime.strptime(lh, '%Y-%m-%d')
            s_date = (lh_dt + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            # Even if up to date, we process today to ensure we get the latest intraday candles
            s_date = today_str 
            
        target_queue[sym] = {'start_date': s_date, 'instrument': row.get('instrument_type', ''), 'linked_ids': set()}

    # B. Ingest Pending Verification Trades
    pending_tasks = []
    offset = 0
    while True:
        res = supabase.table("strategy_trades_verification") \
            .select("id, token_id, trade_date, instrument, broker_symbol, ohlc_status, strategy_id") \
            .eq("ohlc_status", "pending_api_search") \
            .eq("pnl_status", "pending") \
            .in_("strategy_id", valid_strat_ids) \
            .range(offset, offset + 999).execute()

        if not res.data: break
        pending_tasks.extend(res.data)
        if len(res.data) < 1000: break
        offset += 1000

    for task in pending_tasks:
        b_sym = task['broker_symbol']
        t_date = task['trade_date']
        
        if b_sym in target_queue:
            # If the trade date is older than the memory start date, stretch the window back
            if t_date < target_queue[b_sym]['start_date']:
                target_queue[b_sym]['start_date'] = t_date
            target_queue[b_sym]['linked_ids'].add(task['id'])
            if not target_queue[b_sym]['instrument']:
                target_queue[b_sym]['instrument'] = task.get('instrument', '')
        else:
            target_queue[b_sym] = {
                'start_date': t_date,
                'instrument': task.get('instrument', ''),
                'linked_ids': {task['id']}
            }

    if not target_queue:
        print("✅ No active memory or pending trades found.")
        report_progress("success", "✅ No pending tasks found.")
        return

    print(f"📦 Assembled {len(target_queue)} unique symbols for Positional/Intraday sync.")
    report_progress("running", f"📦 Processing {len(target_queue)} target symbols...")
    print("-" * 100)

    # --- 3. EXECUTE DELTA FETCH ---
    fyers_api = None
    group_idx = 0
    total_groups = len(target_queue)
    successful_memory_commits = {}

    for b_sym, queue_data in target_queue.items():
        group_idx += 1
        s_date = queue_data['start_date']
        t_inst = queue_data['instrument']
        ids_to_update = list(queue_data['linked_ids'])
        
        print(f"\n🔄 [{group_idx}/{total_groups}] Target: {b_sym}")
        print(f"   📍 Fetch Range: {s_date} to {today_str} | Linked Rows: {len(ids_to_update)}")
        report_progress("running", f"🔄 [{group_idx}/{total_groups}] Fetching {b_sym}...")

        # Shelf Check Optimization (Only if we are just fetching today's intraday)
        row_count = 0
        if s_date == today_str:
            shelf_res = supabase.table("market_ohlc_cache").select("ts", count="exact").eq("symbol", b_sym).like("ts", f"{today_str}%").execute()
            row_count = shelf_res.count if shelf_res.count else 0

        if row_count >= 300:
             print(f"   ✅ [DATABASE] Shelf Hit: {row_count} candles found for today. Skipping API.")
             successful_memory_commits[b_sym] = None # Mark for memory update without API
        else:
            print(f"   📡 [API] Requesting Fyers for Date Range: {s_date} to {today_str}...")
            if fyers_api is None:
                access_token = get_fyers_access_token()
                if not access_token:
                    print("   ❌ Fyers Login failed.")
                    report_progress("error", "❌ Fyers Login Failed")
                    return
                fyers_api = fyersModel.FyersModel(client_id=APP_ID, token=access_token, is_async=False, log_path="")

            # Resolve Token and Expiry from broker_tokens

            token_lookup = supabase.table("broker_tokens").select("token_id, expiry_date").eq("tsym", b_sym).execute()
            if token_lookup.data:
                valid_token = token_lookup.data[0]['token_id']
                sym_expiry = token_lookup.data[0]['expiry_date']
            else:
                valid_token = 0
                sym_expiry = None

            data = {
                "symbol": b_sym,
                "resolution": "1",
                "date_format": "1",
                "range_from": s_date,
                "range_to": today_str,
                "cont_flag": "1"
            }
            
            response = fyers_api.history(data=data)

            # Fallback Logic (Weekly -> Monthly)
            if response.get("s") == "error" and "invalid symbol" in response.get("message", "").lower():
                monthly_sym = get_monthly_fyers_tsym(t_inst)
                if monthly_sym and monthly_sym != b_sym:
                    print(f"   🔄 [RETRY] Fyers rejected Weekly format. Retrying as Monthly: {monthly_sym}...")
                    data["symbol"] = monthly_sym
                    response = fyers_api.history(data=data)
                    
                    if response.get("s") == "ok":
                        b_sym = monthly_sym
                        # Refresh token/expiry for new monthly symbol
                        fb_lookup = supabase.table("broker_tokens").select("token_id, expiry_date").eq("tsym", monthly_sym).execute()
                        if fb_lookup.data:
                            valid_token = fb_lookup.data[0]['token_id']
                            sym_expiry = fb_lookup.data[0]['expiry_date']
                            print(f"   🔍 [TOKEN RECOVERED] Found correct token {valid_token} for {monthly_sym}")
                        else:
                            valid_token = 0
                            sym_expiry = None
                            print(f"   ⚠️ [TOKEN MISSING] Not found in DB. Defaulting to 0 for {monthly_sym}")
                        
                        if ids_to_update:
                            supabase.table("strategy_trades_verification").update({
                                "broker_symbol": monthly_sym,
                                "token_id": valid_token
                            }).in_("id", ids_to_update).execute()

            # Process Response
            if response.get("s") == "ok":
                candles = response.get("candles", [])
                if candles:
                    ohlc_batch = {}
                    for c in candles:
                        dt_obj = datetime.fromtimestamp(c[0], ist)
                        
                        # Dynamic Date & Legacy AM/PM Time Formatting
                        c_date_str = dt_obj.strftime('%Y-%m-%d')
                        time_part = dt_obj.strftime('%I:%M:%S %p')
                        if time_part.startswith('0'): time_part = time_part[1:]
                        readable_ist_ts = f"{c_date_str} {time_part}"

                        ohlc_batch[readable_ist_ts] = {
                            "token": str(valid_token),
                            "ts": readable_ist_ts,
                            "symbol": b_sym,
                            "open": float(c[1]),
                            "high": float(c[2]),
                            "low": float(c[3]),
                            "close": float(c[4]),
                            "volume": int(c[5])
                        }

                    final_payload = list(ohlc_batch.values())
                    supabase.table("market_ohlc_cache").upsert(final_payload).execute()
                    print(f"   📥 [SUCCESS] Cached {len(final_payload)} unique candles across {s_date} to {today_str}.")
                    
                    # Flag this symbol as fully up-to-date for memory commit
                    successful_memory_commits[b_sym] = sym_expiry
                else:
                    print(f"   ⚠️ [EMPTY] Fyers API returned empty candles array for this range.")
            else:
                msg = response.get('message', 'Unknown Error')
                print(f"   ❌ [ERROR] Fyers API response: {msg}")

        # --- 4. VERIFICATION STATUS ASSIGNMENT ---
        if ids_to_update:
            # We simply check if we have data for the original trade date (legacy check)
            verify_res = supabase.table("market_ohlc_cache").select("ts", count="exact").eq("symbol", b_sym).like("ts", f"{queue_data['start_date']}%").execute()
            v_count = verify_res.count if verify_res.count else 0
            
            if v_count >= 300:
                final_ohlc_status = "verified_ohlc_present"
                final_pnl_status = "pending"
            else:
                final_ohlc_status = "missing_ohlc_at_vault"
                final_pnl_status = "skipped_no_ohlc"

            supabase.table("strategy_trades_verification").update({
                "ohlc_status": final_ohlc_status,
                "pnl_status": final_pnl_status,
                "pnl_1min_status": final_pnl_status,
                # Safe fallback if valid_token isn't assigned yet
                "token_id": locals().get('valid_token', 0) 
            }).in_("id", ids_to_update).execute()

            print(f"   📝 [LOG] {len(ids_to_update)} trade(s) updated to '{final_ohlc_status}'.")

    # --- 5. COMMIT POSITIONAL MEMORY ---
    if successful_memory_commits:
        print("\n💾 Committing State to Harvest Memory...")
        mem_payload = []
        for msym, mexp in successful_memory_commits.items():
            payload_item = {
                "symbol": msym,
                "last_harvested_date": today_str
            }
            if mexp: # If we have expiry, update it (useful for newly ingested trades)
                payload_item["expiry_date"] = mexp
            mem_payload.append(payload_item)
            
        if mem_payload:
            supabase.table("multi_indices_ohlc_harvest_memory").upsert(mem_payload, on_conflict="symbol").execute()
            print(f"   ✅ Saved {len(mem_payload)} symbols to active memory.")

    print("\n" + "="*100)
    print(f"{'SMART FETCHER RUN COMPLETED':^100}")
    print("="*100)
    report_progress("success", f"✅ OHLC Fetching Done for {total_groups} targets.")

if __name__ == "__main__":
    try:
        run_smart_fetcher()
    except Exception as e:
        import traceback
        traceback.print_exc()
        report_progress("error", f"❌ Error: {str(e)[:50]}")
        exit(1)
