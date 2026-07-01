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

def fetch_all_paginated(table_name, select_query="*"):
    """Fetches entire tables dynamically bypassing the 1,000 row API limit."""
    all_data = []
    offset = 0
    limit = 1000
    while True:
        res = supabase.table(table_name).select(select_query).range(offset, offset + limit - 1).execute()
        chunk = res.data
        if not chunk:
            break
        all_data.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit
    return all_data

def build_lot_size_lookup(lot_data):
    """Builds an in-memory lookup grouped by instrument and sorted newest to oldest."""
    lookup = {}
    for row in lot_data:
        idx = row['instrument']
        if idx not in lookup:
            lookup[idx] = []
        lookup[idx].append(row)
    
    for idx in lookup:
        lookup[idx].sort(key=lambda x: x['effective_date'], reverse=True)
    return lookup

def get_historical_lot_size(lookup, index_name, target_date_str):
    """Instantly retrieves the active lot size for a specific past date."""
    if index_name not in lookup or not lookup[index_name]:
        return 1 
        
    target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    first_of_month = target_dt.replace(day=1).strftime("%Y-%m-%d")
    
    valid_lots = [lot for lot in lookup[index_name] if lot['effective_date'] <= first_of_month]
    
    if not valid_lots:
        return lookup[index_name][-1]['lot_size'] 
    return valid_lots[0]['lot_size']

def build_deployment_lookup(deploy_data):
    """Builds an in-memory lookup mapping (Strategy ID + Month) directly to Multiplier."""
    lookup = {}
    for row in deploy_data:
        try:
            sid = str(row['strategy_id'])
            month_str_raw = str(row['month']).split('T')[0]
            month_dt = datetime.strptime(month_str_raw, "%Y-%m-%d")
            month_str = month_dt.replace(day=1).strftime("%Y-%m-%d")
            lookup[f"{sid}_{month_str}"] = int(float(row['multiplier']))
        except Exception:
            pass
    return lookup

def get_active_strategies():
    """Fetches active strategies and returns them as a lookup dictionary."""
    response = supabase.table("strategies") \
                       .select("strategy_id, strategy_name, strategy_full_name, strategy_grouping, capital, index_name, deployment_type") \
                       .eq("status", "Active").execute()
    
    return {str(strat['strategy_id']): strat for strat in response.data}

def get_latest_date_for_strategy(strategy_id):
    """Fetches the maximum date already processed for a SPECIFIC strategy."""
    response = supabase.table("intraday_pnl_enhanced_data") \
                       .select("trade_date") \
                       .eq("strategy_id", strategy_id) \
                       .order("trade_date", desc=True) \
                       .limit(1).execute()
    
    if response.data:
        return response.data[0]['trade_date']
    return "2000-01-01" 

def process_pnl_data():
    active_strategies = get_active_strategies()
    
    if not active_strategies:
        print("No active strategies found to process.")
        return

    print(f"Found {len(active_strategies)} active strategies. Loading memory tables...\n")

    raw_lots = fetch_all_paginated("lot_sizes")
    raw_deployments = fetch_all_paginated("live_deployments")
    
    lot_lookup = build_lot_size_lookup(raw_lots)
    deploy_lookup = build_deployment_lookup(raw_deployments)
    
    unit_cap_map = {}
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for sid, strat in active_strategies.items():
        curr_lot = get_historical_lot_size(lot_lookup, strat.get('index_name'), today_str)
        try:
            unit_cap_map[sid] = float(strat.get('capital', 0)) / curr_lot if curr_lot else 0
        except TypeError:
            unit_cap_map[sid] = 0

    enhanced_data_to_insert = []
    processed_dates = set()
    processed_strategies = set()
    skipped_records = 0
    total_raw_fetched = 0

    print("--- Starting Row Processing ---")

    for strat_id_str, strat_info in active_strategies.items():
        strat_full_name = strat_info.get('strategy_full_name')
        strat_name_basic = strat_info.get('strategy_name')
        
        if strat_full_name and str(strat_full_name).strip():
            strat_name = str(strat_full_name).strip()
        elif strat_name_basic and str(strat_name_basic).strip():
            strat_name = str(strat_name_basic).strip()
        else:
            strat_name = f"Strategy ID {strat_id_str}"
            
        latest_date = get_latest_date_for_strategy(strat_id_str)
        
        fetch_start = 0
        fetch_limit = 1000
        strategy_records = []
        
        while True:
            fetch_end = fetch_start + fetch_limit - 1
            source_response = supabase.table("intraday_pnl_1min_closing") \
                                      .select("*") \
                                      .eq("strategy_id", strat_id_str) \
                                      .gt("trade_date", latest_date) \
                                      .range(fetch_start, fetch_end) \
                                      .execute()
            
            chunk = source_response.data
            if not chunk:
                break
            strategy_records.extend(chunk)
            if len(chunk) < fetch_limit:
                break
            fetch_start += fetch_limit

        if not strategy_records:
            print(f"[INFO] No new data pending for: {strat_name} (Latest: {latest_date})")
            continue
            
        total_raw_fetched += len(strategy_records)
        
        for record in strategy_records:
            trade_date = record.get('trade_date', 'Unknown Date')
            
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

                trade_dt = datetime.strptime(trade_date, "%Y-%m-%d")
                target_month_iso = trade_dt.replace(day=1).strftime("%Y-%m-%d")
                
                multiplier_key = f"{strat_id_str}_{target_month_iso}"
                deployment_type = strat_info.get('deployment_type', '')
                
                if multiplier_key in deploy_lookup:
                    multiplier = deploy_lookup[multiplier_key]
                else:
                    if deployment_type == 'Live Auto':
                        error_msg = f"❌ FATAL ERROR: Live Auto Strategy ID {strat_id_str} ({strat_name}) has no multiplier defined for {target_month_iso} in live_deployments."
                        print(error_msg)
                        raise KeyError(error_msg)
                    else:
                        multiplier = 1
                
                hist_lot = get_historical_lot_size(lot_lookup, strat_info.get('index_name'), trade_date)
                base_unit_cap = unit_cap_map.get(strat_id_str, 0)
                eff_capital = base_unit_cap * hist_lot * multiplier
                
                pnl_percent = round((daily_final_pnl / eff_capital * 100), 4) if eff_capital > 0 else 0.0

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
                    "effective_capital": round(eff_capital, 2),
                    "multiplier": multiplier,
                    "pnl_percentage": pnl_percent,
                    "deployment_type": deployment_type,
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
                
                enhanced_data_to_insert.append(enhanced_row)
                processed_dates.add(trade_date)
                processed_strategies.add(strat_name)
                
                print(f"[SUCCESS] Prepared data for: {strat_name} on {trade_date} (Cap: {round(eff_capital, 2)}, Mult: {multiplier}, Type: {deployment_type})")
            
            except json.JSONDecodeError:
                print(f"[ERROR] Failed to parse JSON for: {strat_name} on {trade_date}")
                skipped_records += 1
            except KeyError as e:
                if "FATAL ERROR" in str(e):
                    raise
                print(f"[ERROR] Missing expected key {e} for: {strat_name} on {trade_date}")
                skipped_records += 1

    print("\n--- Processing Summary ---")
    print(f"Total raw records fetched across all strategies: {total_raw_fetched}")
    print(f"Total records successfully prepared to push: {len(enhanced_data_to_insert)}")
    print(f"Unique dates processed: {len(processed_dates)}")
    print(f"Unique strategies processed: {len(processed_strategies)} ({', '.join(sorted(processed_strategies)) if processed_strategies else 'None'})")
    print(f"Records skipped/failed: {skipped_records}")
    print("--------------------------\n")

    if enhanced_data_to_insert:
        print(f"Inserting {len(enhanced_data_to_insert)} enhanced records into Supabase in chunks of 500...")
        insert_chunk_size = 500
        for i in range(0, len(enhanced_data_to_insert), insert_chunk_size):
            batch = enhanced_data_to_insert[i:i + insert_chunk_size]
            supabase.table("intraday_pnl_enhanced_data").insert(batch).execute()
            print(f"Inserted batch {i // insert_chunk_size + 1} ({len(batch)} records)...")
        print("Insertion successfully completed.")
    else:
        print("No new data to insert to the database.")

if __name__ == "__main__":
    process_pnl_data()
