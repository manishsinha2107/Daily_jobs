import os
import sys
import subprocess

# --- 1. THE "BOOTSTRAP" BLOCK (Self-Sufficient Environment) ---
def bootstrap():
    """Checks and installs dependencies before the rest of the script runs."""
    required = ["pandas", "supabase", "python-dotenv"]
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
from datetime import datetime, timedelta
import calendar
from dotenv import load_dotenv
from supabase import create_client, Client
from collections import defaultdict
import time

# Load .env (Local PyCharm) or use OS Environment (GitHub)
load_dotenv()

# Fetch from Environment
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("❌ Error: SUPABASE_URL or SUPABASE_KEY is missing from environment variables.")
    sys.exit(1)

supabase: Client = create_client(url, key)

# --- HEARTBEAT REPORTER (Surgical Patch) ---
def report_progress(status, msg):
    """Updates the real-time heartbeat in Supabase for Step 2"""
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": "now()"
        }).eq("step_id", "step2").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

# Shoonya OHLC window is ~90 days
DYNAMIC_CUTOFF = (datetime.now() - timedelta(days=88)).strftime('%Y-%m-%d')

def is_monthly_expiry(expiry_date_obj):
    """
    Checks if the given date is the last Thursday of its month.
    Shoonya Monthly symbols usually skip the 'Day' part (e.g., NIFTYOCT25P...)
    """
    year = expiry_date_obj.year
    month = expiry_date_obj.month
    # Find all Thursdays in the month
    month_calendar = calendar.monthcalendar(year, month)
    thursdays = [week[calendar.THURSDAY] for week in month_calendar if week[calendar.THURSDAY] != 0]
    return expiry_date_obj.day == thursdays[-1]

def get_shoonya_tsym(inst_name):
    """
    Enhanced Translator: Handles Monthly vs Weekly Shoonya naming.
    OPTIDX_NIFTY_28OCT2025_PE_24400 -> NIFTYOCT25P24400 (If Monthly)
    OPTIDX_NIFTY_16OCT2025_PE_24400 -> NIFTY16OCT25P24400 (If Weekly)
    """
    try:
        parts = inst_name.split('_')
        if parts[0] == 'OPTIDX':
            symbol, expiry_str, opt_type_full, strike = parts[1], parts[2], parts[3], parts[4]
            opt_type = opt_type_full[0] # P or C

            # Parse expiry string '28OCT2025'
            exp_dt = datetime.strptime(expiry_str, '%d%b%Y')
            day = expiry_str[:2]
            month_abbr = expiry_str[2:5].upper()
            year_short = expiry_str[-2:]

            if is_monthly_expiry(exp_dt):
                # Monthly Format: Symbol + Month + Year + Type + Strike
                return f"{symbol}{month_abbr}{year_short}{opt_type}{strike}"
            else:
                # Weekly Format: Symbol + Day + Month + Year + Type + Strike
                return f"{symbol}{day}{month_abbr}{year_short}{opt_type}{strike}"

        return inst_name
    except Exception as e:
        return None

def sync_audit_to_shadow():
    start_time = time.time()
    print(f"🚀 Starting Targeted Sync Process | Cutoff: {DYNAMIC_CUTOFF}")
    # --- REPORTING START ---
    report_progress("running", "🔍 Scanning verification records...")
    print("-" * 60)

    # 1. Fetch Existing IDs from Verification
    print("🔍 Step 1: Checking existing records in Verification table...")
    existing_ids = set()
    offset = 0
    while True:
        res = supabase.table("strategy_trades_verification").select("id").range(offset, offset + 999).execute()
        if not res.data: break
        for r in res.data: existing_ids.add(r['id'])
        if len(res.data) < 1000: break
        offset += 1000
    print(f"   - Found {len(existing_ids)} IDs already in Verification.")

    # 2. Fetch New Audit Rows (Filtered)
    print("\n📥 Step 2: Fetching Audit rows with status 'pending_ohlc'...")
    # --- REPORTING PROGRESS ---
    report_progress("running", f"📥 Fetching Audit: {len(existing_ids)} exist...")
    
    new_audit_rows = []
    offset = 0
    while True:
        res = supabase.table("strategy_trades_audit") \
            .select("*") \
            .eq("status", "pending_ohlc") \
            .range(offset, offset + 999).execute()

        if not res.data: break

        batch_new = [row for row in res.data if row['id'] not in existing_ids]
        new_audit_rows.extend(batch_new)

        print(f"   - Scanning pending rows {offset} to {offset + len(res.data)}... Found {len(new_audit_rows)} unique.")
        if len(res.data) < 1000: break
        offset += 1000

    if not new_audit_rows:
        print("\n✅ No 'pending_ohlc' trades found. Exiting.")
        # --- REPORTING IDLE COMPLETION ---
        report_progress("success", "✅ No pending trades found.")
        return

    # 3. Targeted OHLC Cache Loading
    print("\n📡 Step 3: Loading Targeted OHLC cache map for relevant dates...")
    # --- REPORTING PROGRESS ---
    report_progress("running", f"📡 Mapping {len(new_audit_rows)} new trades...")
    
    trade_dates_list = [row['trade_date'] for row in new_audit_rows]
    min_date = min(trade_dates_list)
    max_date = max(trade_dates_list)

    existing_cache_map = set()
    offset = 0
    while True:
        c_res = supabase.table("market_ohlc_cache") \
            .select("symbol, ts") \
            .gte("ts", f"{min_date} 00:00:00") \
            .lte("ts", f"{max_date} 11:59:59 PM") \
            .range(offset, offset + 999).execute()

        if not c_res.data: break
        for item in c_res.data:
            existing_cache_map.add(f"{item['symbol']}_{item['ts'][:10]}")
        if len(c_res.data) < 1000: break
        offset += 1000
    print(f"   - Mapped {len(existing_cache_map)} relevant Symbol-Date pairs from Cache.")

    # 4. Process Data & Assign OHLC Status
    print("\n⚙️  Step 4: Processing trades and preparing payload...")
    payload = []
    # stats[strategy_name][trade_date][status] = count
    stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    for row in new_audit_rows:
        b_symbol = get_shoonya_tsym(row['instrument'])
        trade_date = row['trade_date']
        strat_name = row.get('strategy_name', 'Unknown')

        cache_key = f"{b_symbol}_{trade_date}"

        if cache_key in existing_cache_map:
            status = "verified_ohlc_present"
        elif trade_date >= DYNAMIC_CUTOFF:
            status = "pending_api_search"
        else:
            status = "historical_data_unavailable_at_broker"

        # Update Enhanced Stats (Nested by Strategy then Date)
        stats[strat_name][trade_date][status] += 1
        stats[strat_name][trade_date]['total'] += 1

        payload.append({
            "id": row['id'],
            "strategy_id": row['strategy_id'],
            "strategy_name": strat_name,
            "trade_date": trade_date,
            "instrument": row['instrument'],
            "txn_time": row['txn_time'],
            "txn_type": row['txn_type'],
            "quantity": row['quantity'],
            "price": row['price'],
            "run_counter": row['run_counter'],
            "created_at": row['created_at'],
            "broker_symbol": b_symbol,
            "ohlc_status": status,
            "pnl_status": "pending",
            "pnl_1min_status": "pending"
        })

    # 5. Bulk Insert & Update Audit Status
    if payload:
        print(f"\n📤 Step 5: Syncing {len(payload)} trades in batches of 500...")
        # --- REPORTING FINAL SYNC ---
        report_progress("running", f"📤 Syncing {len(payload)} rows...")
        
        for i in range(0, len(payload), 500):
            batch = payload[i:i+500]
            batch_ids = [item['id'] for item in batch]
            supabase.table("strategy_trades_verification").insert(batch).execute()
            supabase.table("strategy_trades_audit") \
                .update({"status": "synced_to_verification"}) \
                .in_("id", batch_ids).execute()
            print(f"   - Processed batch {i//500 + 1}...")

    # --- FINAL STRATEGY REPORT WITH DATES ---
    duration = round(time.time() - start_time, 2)
    print("\n" + "="*95)
    print(f"{'STRATEGY-LEVEL SYNC REPORT':^95}")
    print("="*95)
    print(f"{'Strategy Name':<30} | {'Date':<12} | {'Total':<6} | {'Verified':<10} | {'Pending':<10} | {'NA/Hist':<8}")
    print("-" * 95)

    for s_name, dates_dict in stats.items():
        # Iterate through each date for the current strategy
        for t_date, s_data in sorted(dates_dict.items()):
            v_count = s_data.get('verified_ohlc_present', 0)
            p_count = s_data.get('pending_api_search', 0)
            n_count = s_data.get('historical_data_unavailable_at_broker', 0)

            print(f"{s_name[:28]:<30} | {t_date:<12} | {s_data['total']:<6} | {v_count:<10} | {p_count:<10} | {n_count:<8}")

    print("="*95)
    print(f"🏁 Process Completed in {duration} seconds.")
    print("="*95)
    # --- REPORTING SUCCESS ---
    report_progress("success", f"✅ Synced {len(payload)} trades.")

if __name__ == "__main__":
    try:
        sync_audit_to_shadow()
    except Exception as e:
        report_progress("error", f"❌ Error: {str(e)[:50]}")
        exit(1)
