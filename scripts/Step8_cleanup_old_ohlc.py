import os
import sys
import builtins
from datetime import datetime, timedelta, timezone
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
        }).eq("step_id", "step7_cleanup").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

def cleanup_old_ohlc():
    # Calculate cutoff date (30 days ago) based on current UTC date
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    msg_start = f"🧹 Starting OHLC Cleanup: Removing records older than {cutoff_date}..."
    print(msg_start)
    report_progress("running", msg_start)

    total_deleted = 0
    batch_size = 5000  # Delete in safe chunks to prevent DB locks/timeouts

    while True:
        try:
            # Fetch a batch of old rows (fetching only composite keys symbol & ts to minimize payload)
            res = supabase.table("market_ohlc_cache") \
                .select("symbol, ts") \
                .lt("ts", f"{cutoff_date} 00:00:00") \
                .limit(batch_size) \
                .execute()

            rows = res.data
            if not rows:
                print("✅ No more old OHLC records found to delete.")
                break

            # Extract criteria for deletion (using symbol and ts since it's a composite primary key)
            # Supabase delete batching via 'or_' or iterating primary keys safely:
            # To make it robust and clean, we delete matching keys in this batch
            deleted_in_batch = 0
            for row in rows:
                del_res = supabase.table("market_ohlc_cache") \
                    .delete() \
                    .eq("symbol", row['symbol']) \
                    .eq("ts", row['ts']) \
                    .execute()
                deleted_in_batch += 1

            total_deleted += deleted_in_batch
            print(f"🗑️ Deleted batch of {deleted_in_batch} rows (Total cleared so far: {total_deleted})")
            report_progress("running", f"🧹 Cleared {total_deleted} old OHLC rows...")

            if len(rows) < batch_size:
                break

        except Exception as e:
            print(f"❌ Error during cleanup batch: {e}")
            report_progress("error", f"❌ Cleanup error: {str(e)[:50]}")
            raise e

    success_msg = f"✅ Successfully cleaned up {total_deleted} old OHLC records older than {cutoff_date}."
    print(success_msg)
    report_progress("success", success_msg)

if __name__ == "__main__":
    try:
        cleanup_old_ohlc()
    except Exception as e:
        msg_err = f"❌ Step 7 Cleanup Failed: {str(e)[:50]}"
        print(msg_err)
        report_progress("error", msg_err)
        sys.exit(1)
