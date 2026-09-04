import os
import sys
import builtins
from datetime import datetime, timezone
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
    raise ValueError("Supabase credentials not found in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
    # Calculate cutoff date as today's UTC date
    cutoff_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    msg_start = f"🧹 Starting OHLC Cleanup: Removing records older than {cutoff_date} 00:00:00..."
    print(msg_start)
    report_progress("running", msg_start)

    try:
        # Single bulk delete command to let the Postgres database handle it efficiently natively
        res = supabase.table("market_ohlc_cache") \
            .delete() \
            .lt("ts", f"{cutoff_date} 00:00:00") \
            .execute()
        
        # Calculate total deleted based on the returned array
        total_deleted = len(res.data) if res.data else 0

    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        report_progress("error", f"❌ Cleanup error: {str(e)[:50]}")
        raise e

    success_msg = f"✅ Successfully cleaned up {total_deleted} old OHLC records older than {cutoff_date}."
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
