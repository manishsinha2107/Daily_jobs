import os
import sys
import builtins
from datetime import datetime, timezone, timedelta
try:
    from dotenv import load_dotenv
    if os.path.exists(".env"):
        load_dotenv()
except ImportError:
    pass

from supabase import create_client, Client

# --- FORCE UNBUFFERED LOGGING FOR GITHUB ACTIONS ---
def print(*args, **kwargs):
    kwargs.setdefault('flush', True)
    builtins.print(*args, **kwargs)

# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Primary Supabase credentials not found in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Secondary Client for Universe Table ---
SUPABASE_MANISHSINHA_URL = os.getenv("SUPABASE_MANISHSINHA_URL")
SUPABASE_MANISHSINHA_KEY = os.getenv("SUPABASE_MANISHSINHA_KEY")

supabase_manish = None
if SUPABASE_MANISHSINHA_URL and SUPABASE_MANISHSINHA_KEY:
    supabase_manish = create_client(SUPABASE_MANISHSINHA_URL, SUPABASE_MANISHSINHA_KEY)

# --- HEARTBEAT REPORTER ---
def report_progress(status, msg):
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("step_id", "step8_cleanup").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

def cleanup_old_ohlc():
    # Calculate cutoff dates
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%d")
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    msg_start = f"🧹 Starting Cleanup: OHLC older than {cutoff_date}, Universe prior to {today_date}..."
    print(msg_start)
    report_progress("running", msg_start)

    total_ohlc_deleted = 0
    total_univ_deleted = 0

    try:
        # 1. Purge OHLC Cache (60 days) on Primary Supabase
        res_ohlc = supabase.table("market_ohlc_cache") \
            .delete(returning="minimal", count="exact") \
            .lt("ts", f"{cutoff_date} 00:00:00") \
            .execute()
        
        total_ohlc_deleted = res_ohlc.count if res_ohlc.count is not None else 0

        # 2. Purge Tracked Options Universe (Expired Contracts) on Secondary Supabase
        if supabase_manish:
            res_univ = supabase_manish.table("tracked_options_universe") \
                .delete(returning="minimal", count="exact") \
                .lt("expiry_date", today_date) \
                .execute()
            
            total_univ_deleted = res_univ.count if res_univ.count is not None else 0
        else:
            print("⚠️ ManishSinha Supabase credentials missing. Skipping Universe cleanup.")

    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        report_progress("error", f"❌ Cleanup error: {str(e)[:50]}")
        raise e

    success_msg = f"✅ Cleanup Complete: {total_ohlc_deleted} OHLC records (< {cutoff_date}) and {total_univ_deleted} expired contracts (< {today_date}) removed."
    print(success_msg)
    report_progress("success", success_msg)

if __name__ == "__main__":
    try:
        cleanup_old_ohlc()
    except Exception as e:
        msg_err = f"❌ Step 8 Cleanup Failed: {str(e)[:50]}"
        print(msg_err)
        report_progress("error", msg_err)
        sys.exit(1)
