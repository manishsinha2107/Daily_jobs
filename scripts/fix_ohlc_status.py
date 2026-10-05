import os
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

if os.path.exists(".env"):
    load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("❌ Error: Supabase credentials missing.")
    sys.exit(1)

supabase: Client = create_client(url, key)

def fix_ohlc_verification_status():
    print(f"\n{'='*60}")
    print("🔍 INITIATING OHLC STATUS RE-SYNC & FIXER")
    print(f"{'='*60}\n")

    # 1. Paginated fetch for all records where ohlc_status is 'missing_ohlc_at_vault'
    all_records = []
    offset, limit = 0, 1000
    while True:
        res = supabase.table("strategy_trades_verification") \
            .select("id, strategy_id, trade_date, broker_symbol, ohlc_status") \
            .eq("ohlc_status", "missing_ohlc_at_vault") \
            .range(offset, offset + limit - 1) \
            .execute()
        if not res.data:
            break
        all_records.extend(res.data)
        if len(res.data) < limit:
            break
        offset += limit

    if not all_records:
        print("✅ No records found with 'missing_ohlc_at_vault' status. Exiting.")
        return

    print(f"📦 Found {len(all_records)} record(s) marked as 'missing_ohlc_at_vault'. Cross-referencing cache...")

    updated_count = 0
    for record in all_records:
        rec_id = record['id']
        symbol = record['broker_symbol']
        trade_date = record['trade_date']

        # 2. Check if candles exist in market_ohlc_cache for this symbol and date
        cache_check = supabase.table("market_ohlc_cache") \
            .select("ts", count="exact") \
            .eq("symbol", symbol) \
            .like("ts", f"{trade_date}%") \
            .execute()
        
        candle_count = cache_check.count if cache_check.count else 0

        if candle_count > 0:
            print(f"  ✨ Found {candle_count} candle(s) in cache for {symbol} on {trade_date}. Upgrading status...")
            # Updated to omit 'updated_at' which doesn't exist in strategy_trades_verification schema
            supabase.table("strategy_trades_verification").update({
                "ohlc_status": "verified_ohlc_present",
                "pnl_status": "pending",
                "pnl_1min_status": "pending"
            }).eq("id", rec_id).execute()
            updated_count += 1
        else:
            print(f"  ⏳ No candles found in cache for {symbol} on {trade_date}. Skipping.")

    print(f"\n{'='*60}")
    print(f"✅ OHLC STATUS RE-SYNC COMPLETE. Updated {updated_count} record(s).")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        fix_ohlc_verification_status()
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        sys.exit(1)
