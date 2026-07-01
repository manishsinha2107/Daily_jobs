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
    response = supabase.table("Strategies") \
                       .select("strategy_id, strategy_full_name, strategy_grouping, capital") \
                       .eq("status", "Active").execute()
    
    return {str(strat['strategy_id']): strat for strat in response.data}

def process_pnl_data():
    latest_date = get_latest_processed_date()
    print(f"Latest processed date: {latest_date}. Fetching newer records...")

    source_response = supabase.table("intraday_pnl_1min_closing") \
                              .select("*") \
                              .gt("trade_date", latest_date).execute()
    
    new_records = source_response.data

    if not new_records:
        print("No new records to process.")
        return

    active_strategies = get_active_strategies()
    enhanced_data_to_insert = []

    for record in new_records:
        strat_id_str = str(record['strategy_id'])
        
        if strat_id_str not in active_strategies:
            continue
            
        strat_info = active_strategies[strat_id_str]
        
        try:
            pnl_array = json.loads(record['pnl_data'])
            
            if not pnl_array:
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
                "strategy_full_name": strat_info.get('strategy_full_name'),
                "strategy_grouping": strat_info.get('strategy_grouping'),
                "capital": strat_info.get('capital'),
                "trade_date": record['trade_date'],
                "daily_final_pnl": daily_final_pnl,
                "max_profit": max_profit,
                "max_profit_time": max_profit_time,
                "max_loss": max_loss,
                "max_loss_time": max_loss_time,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            enhanced_data_to_insert.append(enhanced_row)
        
        except json.JSONDecodeError:
            print(f"Error parsing JSON for row ID {record.get('id')}")
        except KeyError as e:
            print(f"Missing expected JSON key {e} in row ID {record.get('id')}")

    if enhanced_data_to_insert:
        print(f"Inserting {len(enhanced_data_to_insert)} enhanced records...")
        supabase.table("intraday_pnl_enhanced_data").insert(enhanced_data_to_insert).execute()
        print("Insertion complete.")
    else:
        print("No valid active strategy records found to insert.")

if __name__ == "__main__":
    process_pnl_data()
