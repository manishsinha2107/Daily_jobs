import os
import json
from supabase import create_client, Client
from datetime import datetime, timezone

# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials not found in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_latest_processed_date():
    """Fetches the maximum date already processed in the enhanced table."""
    response = supabase.table("intraday_pnl_enhanced_data") \
                       .select("trade_date") \
                       .order("trade_date", desc=True) \
                       .limit(1).execute()
    
    if response.data:
        return response.data[0]['trade_date']
    return "2000-01-01" 

def get_active_strategies():
    """Fetches active strategies and returns them as a lookup dictionary."""
    response = supabase.table("strategies") \
                       .select("strategy_id, strategy_name, strategy_full_name, strategy_grouping, capital") \
                       .eq("status", "Active").execute()
    
    return {str(strat['strategy_id']): strat for strat in response.data}

def process_pnl_data():
    latest_date = get_latest_processed_date()
    print(f"Latest processed date: {latest_date}. Fetching newer records...\n")

    # FIXED: Pagination Loop to bypass the 1,000 row API limit
    new_records = []
    fetch_start = 0
    fetch_limit = 1000
    
    while True:
        fetch_end = fetch_start + fetch_limit - 1
        source_response = supabase.table("intraday_pnl_1min_closing") \
                                  .select("*") \
                                  .gt("trade_date", latest_date) \
                                  .range(fetch_start, fetch_end) \
                                  .execute()
        
        chunk = source_response.data
        if not chunk:
            break
            
        new_records.extend(chunk)
        
        # If we received fewer rows than the limit, we've reached the end of the table
        if len(chunk) < fetch_limit:
            break
            
        fetch_start += fetch_limit

    if not new_records:
        print("No new records to process.")
        return

    active_strategies = get_active_strategies()
    enhanced_data_to_insert = []
    
    # Tracking metrics for the final summary
    processed_dates = set()
    processed_strategies = set()
    skipped_records = 0

    print("--- Starting Row Processing ---")

    for record in new_records:
        strat_id_str = str(record['strategy_id'])
        trade_date = record.get('trade_date', 'Unknown Date')
        
        if strat_id_str not in active_strategies:
            continue
            
        strat_info = active_strategies[strat_id_str]
        
        strat_full_name = strat_info.get('strategy_full_name')
        strat_name_basic = strat_info.get('strategy_name')
        
        if strat_full_name and str(strat_full_name).strip():
            strat_name = str(strat_full_name).strip()
        elif strat_name_basic and str(strat_name_basic).strip():
            strat_name = str(strat_name_basic).strip()
        else:
            strat_name = f"Strategy ID {strat_id_str}"
        
        try:
            raw_pnl = record.get('pnl_data')
            
            if not raw_pnl:
                print(f"[SKIP] No PNL data for: {strat_name} on {trade_date}")
                skipped_records += 1
                continue
                
            if isinstance(raw_pnl, list):
                pnl_array = raw_pnl
            elif isinstance(raw_pnl, str):
                pnl_array = json.loads(raw_pnl)
            else:
                print(f"[ERROR] Unexpected data type for: {strat_name} on {trade_date}")
                skipped_records += 1
                continue
            
            if not pnl_array:
                print(f"[SKIP] Empty PNL array for: {strat_name} on {trade_date}")
                skipped_records += 1
                continue
            
            daily_final_pnl = pnl_array[-1]['pnl']
            
            max_profit_obj = max(pnl_array, key=lambda x: x['pnl'])
            max_profit = max_profit_obj['pnl']
            max_profit_time = max_profit_obj['time']
            
            max_loss_obj = min(pnl_array, key=lambda x: x['pnl'])
            max_loss = max_loss_obj['pnl']
            max_loss_time = max_loss_obj['time']

            enhanced_row = {
                "strategy_id": record['strategy_id'],
                "strategy_full_name": strat_name,
                "strategy_grouping": strat_info.get('strategy_grouping'),
                "capital": strat_info.get('capital'),
                "trade_date": trade_date,
                "daily_final_pnl": daily_final_pnl,
                "max_profit": max_profit,
                "max_profit_time": max_profit_time,
                "max_loss": max_loss,
                "max_loss_time": max_loss_time,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            enhanced_data_to_insert.append(enhanced_row)
            processed_dates.add(trade_date)
            processed_strategies.add(strat_name)
            
            print(f"[SUCCESS] Prepared data for: {strat_name} on {trade_date}")
        
        except json.JSONDecodeError:
            print(f"[ERROR] Failed to parse JSON for: {strat_name} on {trade_date}")
            skipped_records += 1
        except KeyError as e:
            print(f"[ERROR] Missing expected key {e} for: {strat_name} on {trade_date}")
            skipped_records += 1

    print("\n--- Processing Summary ---")
    print(f"Total raw records fetched: {len(new_records)}")
    print(f"Total active strategy records ready to push: {len(enhanced_data_to_insert)}")
    print(f"Unique dates processed: {len(processed_dates)}")
    print(f"Unique strategies processed: {len(processed_strategies)} ({', '.join(sorted(processed_strategies)) if processed_strategies else 'None'})")
    print(f"Records skipped/failed: {skipped_records}")
    print("--------------------------\n")

    if enhanced_data_to_insert:
        print(f"Inserting {len(enhanced_data_to_insert)} enhanced records into Supabase in chunks...")
        
        # FIXED: Chunk the inserts to avoid payload limits
        insert_chunk_size = 500
        for i in range(0, len(enhanced_data_to_insert), insert_chunk_size):
            batch = enhanced_data_to_insert[i:i + insert_chunk_size]
            supabase.table("intraday_pnl_enhanced_data").insert(batch).execute()
            print(f"Inserted batch {i // insert_chunk_size + 1} ({len(batch)} records)...")
            
        print("Insertion successfully completed.")
    else:
        print("No valid active strategy records found to insert.")

if __name__ == "__main__":
    process_pnl_data()
