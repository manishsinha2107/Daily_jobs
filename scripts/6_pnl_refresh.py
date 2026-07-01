import os
import json
import builtins
import pandas as pd
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
    """Overrides the default print function to instantly flush to the console."""
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
        }).eq("step_id", "step6").execute()
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

# --- DATA FETCHERS & LOOKUPS ---
def fetch_all_paginated(table_name, select_query="*"):
    all_data = []
    offset, limit = 0, 1000
    while True:
        res = supabase.table(table_name).select(select_query).range(offset, offset + limit - 1).execute()
        chunk = res.data
        if not chunk: break
        all_data.extend(chunk)
        if len(chunk) < limit: break
        offset += limit
    return all_data

def build_lot_size_lookup(lot_data):
    lookup = {}
    for row in lot_data:
        idx = row['instrument']
        if idx not in lookup: lookup[idx] = []
        lookup[idx].append(row)
    for idx in lookup:
        lookup[idx].sort(key=lambda x: x['effective_date'], reverse=True)
    return lookup

def get_historical_lot_size(lookup, index_name, target_date_str):
    if index_name not in lookup or not lookup[index_name]: return 1 
    target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    first_of_month = target_dt.replace(day=1).strftime("%Y-%m-%d")
    valid_lots = [lot for lot in lookup[index_name] if lot['effective_date'] <= first_of_month]
    if not valid_lots: return lookup[index_name][-1]['lot_size'] 
    return valid_lots[0]['lot_size']

def build_deployment_lookup(deploy_data):
    lookup = {}
    for row in deploy_data:
        try:
            sid = str(row['strategy_id'])
            month_str = datetime.strptime(str(row['month']).split('T')[0], "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")
            lookup[f"{sid}_{month_str}"] = int(float(row['multiplier']))
        except Exception: pass
    return lookup

# --- PRE-FLIGHT METADATA SYNC ---
def sync_strategy_metadata():
    """Ensures historical PNL rows reflect the current master strategy status and name."""
    print("🔄 Running Pre-Flight Metadata Sync...")
    
    # 1. Fetch Master Metadata
    master_res = supabase.table("strategies").select("strategy_id, strategy_name, strategy_full_name, status").execute()
    if not master_res.data:
        print("   - No strategies found to sync.")
        return
        
    master_meta = {
        int(s['strategy_id']): {
            'status': s['status'], 
            'name': s.get('strategy_full_name') or s.get('strategy_name') or f"ID {s['strategy_id']}"
        } 
        for s in master_res.data
    }
    
    synced_status_count = 0
    synced_name_count = 0
    
    # 2. Targeted Independent Updates (Safe from comma crashes)
    for sid, meta in master_meta.items():
        correct_status = meta['status']
        correct_name = meta['name']
        
        # Action A: Sync Status
        try:
            res_status = supabase.table("daily_strategy_pnl").update({"status": correct_status}) \
                                 .eq("strategy_id", sid).neq("status", correct_status).execute()
            if res_status.data:
                print(f"   -> [STATUS SYNC] ID {sid} updated to '{correct_status}'. ({len(res_status.data)} rows aligned)")
                synced_status_count += 1
        except Exception as e:
            print(f"   -> [ERROR] Failed to sync status for ID {sid}: {e}")

        # Action B: Sync Name
        try:
            res_name = supabase.table("daily_strategy_pnl").update({"strategy_name": correct_name}) \
                               .eq("strategy_id", sid).neq("strategy_name", correct_name).execute()
            if res_name.data:
                print(f"   -> [NAME SYNC] ID {sid} updated to '{correct_name}'. ({len(res_name.data)} rows aligned)")
                synced_name_count += 1
        except Exception as e:
            print(f"   -> [ERROR] Failed to sync name for ID {sid}: {e}")
            
    if synced_status_count == 0 and synced_name_count == 0:
        print("   ✅ All historical metadata (Names & Statuses) is perfectly aligned.")
    else:
        print(f"   ✅ Synchronized {synced_status_count} statuses and {synced_name_count} names successfully.")
    print("----------------------------------------\n")

# --- CORE LOGIC ---
def run_pnl_refresh():
    msg_start = "🔄 Starting Zero-History State-Based P&L Refresh..."
    print(msg_start)
    report_progress("running", msg_start)
    
    # Run the metadata synchronizer before any math begins
    sync_strategy_metadata()
    
    # 1. Fetch Active Strategies (Only active ones get daily math updates)
    strategies_res = supabase.table("strategies").select("strategy_id, strategy_name, strategy_full_name, strategy_grouping, capital, index_name, deployment_type, user_name, status").eq("status", "Active").execute()
    active_strategies = {str(s['strategy_id']): s for s in strategies_res.data}
    active_ids_int = [int(sid) for sid in active_strategies.keys()]
    
    if not active_strategies:
        msg_none = "✅ No active strategies found."
        print(msg_none)
        report_progress("success", msg_none)
        return

    # 2. Bulk Memory Load (Lookups)
    print("📦 Fetching lot sizes and deployments...")
    lot_lookup = build_lot_size_lookup(fetch_all_paginated("lot_sizes"))
    deploy_lookup = build_deployment_lookup(fetch_all_paginated("live_deployments"))
    
    unit_cap_map = {}
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for sid, strat in active_strategies.items():
        curr_lot = get_historical_lot_size(lot_lookup, strat.get('index_name'), today_str)
        unit_cap_map[sid] = float(strat.get('capital', 0)) / curr_lot if curr_lot else 0

    # 3. State Extraction (1 Fast Query per Strategy)
    print(f"📦 Fetching latest state for {len(active_ids_int)} active strategies...")
    strategy_states = {}
    for sid in active_strategies.keys():
        res = supabase.table("daily_strategy_pnl").select("trade_date, cumulative_pnl, peak_cumulative_pnl").eq("strategy_id", int(sid)).order("trade_date", desc=True).limit(1).execute()
        if res.data:
            row = res.data[0]
            strategy_states[sid] = {
                'latest_date': row['trade_date'],
                'cum_pnl': float(row.get('cumulative_pnl') or 0.0),
                'peak_pnl': float(row.get('peak_cumulative_pnl') or 0.0)
            }
        else:
            strategy_states[sid] = {
                'latest_date': '2000-01-01',
                'cum_pnl': 0.0,
                'peak_pnl': 0.0
            }
            
    global_min_date = min([state['latest_date'] for state in strategy_states.values()])

    # 4. Bulk Intraday Fetch (Only fetching data newer than the furthest behind strategy)
    print(f"🔎 Bulk fetching new intraday data since {global_min_date}...")
    new_intraday_data = []
    offset, limit = 0, 1000
    while True:
        res = supabase.table("intraday_pnl_1min_closing").select("*").in_("strategy_id", active_ids_int).gt("trade_date", global_min_date).range(offset, offset + limit - 1).execute()
        chunk = res.data
        if not chunk: break
        new_intraday_data.extend(chunk)
        if len(chunk) < limit: break
        offset += limit

    # Group new intraday data by strategy in memory
    intraday_grouped = {sid: [] for sid in active_strategies.keys()}
    for row in new_intraday_data:
        sid_str = str(row['strategy_id'])
        if sid_str in intraday_grouped:
            intraday_grouped[sid_str].append(row)

    # Global tracking for final summary
    total_upserted = 0
    skipped_records = 0
    processed_strategies = set()
    processed_dates = set()

    print("\n--- Starting Assembly Line Processing ---")

    # 5. Assembly Line Processing (Per Strategy)
    for strat_id_str, strat_meta in active_strategies.items():
        strat_name = strat_meta.get('strategy_full_name') or strat_meta.get('strategy_name') or f"ID {strat_id_str}"
        strat_state = strategy_states[strat_id_str]
        
        # A. Filter and SORT raw records specifically for this strategy
        strategy_records = [row for row in intraday_grouped[strat_id_str] if row['trade_date'] > strat_state['latest_date']]
        strategy_records.sort(key=lambda x: x['trade_date']) # CRITICAL: Sort chronologically for math
        
        if not strategy_records:
            print(f"[INFO] No new data for: {strat_name} (Latest: {strat_state['latest_date']})")
            continue

        processed_strategies.add(strat_name)
        
        running_cum_pnl = strat_state['cum_pnl']
        running_peak = strat_state['peak_pnl']
        final_payload = []

        # B. Parse, Calculate Math, and Prepare New Trades
        for row in strategy_records:
            t_date_str = row.get('trade_date', 'Unknown')
            try:
                raw_pnl = row.get('pnl_data')
                if not raw_pnl:
                    print(f"[SKIP] No PNL data for: {strat_name} on {t_date_str}")
                    skipped_records += 1
                    continue
                    
                pnl_json = raw_pnl if isinstance(raw_pnl, list) else json.loads(raw_pnl)
                if not pnl_json:
                    print(f"[SKIP] Empty PNL array for: {strat_name} on {t_date_str}")
                    skipped_records += 1
                    continue
                
                daily_final_pnl = float(pnl_json[-1]['pnl'])
                max_profit_obj = max(pnl_json, key=lambda x: float(x['pnl']))
                max_loss_obj = min(pnl_json, key=lambda x: float(x['pnl']))
                
                t_date_obj = datetime.strptime(t_date_str, "%Y-%m-%d")
                target_month_iso = t_date_obj.replace(day=1).strftime("%Y-%m-%d")
                
                multiplier_key = f"{strat_id_str}_{target_month_iso}"
                deploy_type = strat_meta.get('deployment_type', '')
                
                if multiplier_key in deploy_lookup:
                    multiplier = deploy_lookup[multiplier_key]
                elif deploy_type == 'Live Auto':
                    error_msg = f"❌ FATAL ERROR: Live Auto ID {strat_id_str} ({strat_name}) missing multiplier for {target_month_iso}."
                    print(error_msg)
                    report_progress("error", error_msg)
                    raise KeyError(error_msg) 
                else:
                    multiplier = 1
                
                hist_lot = get_historical_lot_size(lot_lookup, strat_meta['index_name'], t_date_str)
                eff_cap = unit_cap_map.get(strat_id_str, 0) * hist_lot * multiplier
                pnl_pct = round((daily_final_pnl / eff_cap * 100), 4) if eff_cap > 0 else 0.0
                
                # Dynamic Cumulative Math
                running_cum_pnl += daily_final_pnl
                if running_cum_pnl > running_peak:
                    running_peak = running_cum_pnl
                max_dd_amount = running_peak - running_cum_pnl
                
                final_payload.append({
                    "trade_date": t_date_str,
                    "trade_year": t_date_obj.year,
                    "trade_month": t_date_obj.month,
                    "trade_month_name": t_date_obj.strftime("%b"),
                    "month_year": t_date_obj.strftime("%b %Y"),
                    "strategy_id": int(strat_id_str),
                    "strategy_name": strat_name,
                    "index_name": strat_meta['index_name'],
                    "user_name": strat_meta.get('user_name'),
                    "strategy_grouping": strat_meta.get('strategy_grouping'),
                    "status": strat_meta['status'],
                    "deployment_type": deploy_type,
                    "pnl": round(daily_final_pnl, 2),
                    "max_profit": max_profit_obj['pnl'],
                    "max_profit_time": max_profit_obj['time'],
                    "max_loss": max_loss_obj['pnl'],
                    "max_loss_time": max_loss_obj['time'],
                    "eff_capital": round(eff_cap, 2),
                    "multiplier": int(multiplier),
                    "is_win": 1 if daily_final_pnl > 0 else 0,
                    "pnl_percent": pnl_pct,
                    "cumulative_pnl": round(running_cum_pnl, 2),
                    "peak_cumulative_pnl": round(running_peak, 2),
                    "max_dd_amount": round(max_dd_amount, 2),
                    "updated_at": datetime.now(timezone.utc).isoformat()
                })
                
                # REINSTATED DETAILED LOGGING
                print(f"[SUCCESS] Prepared data for: {strat_name} on {t_date_str} (Cap: {round(eff_cap, 2)}, Mult: {multiplier}, Type: {deploy_type}, Status: {strat_meta['status']})")
                processed_dates.add(t_date_str)
                
            except Exception as e:
                if "FATAL ERROR" in str(e):
                    raise
                print(f"[ERROR] Failed to process {strat_name} on {t_date_str}: {str(e)[:50]}")
                skipped_records += 1
                continue

        if not final_payload:
            continue

        # C. Immediate Upsert for this specific strategy
        try:
            for i in range(0, len(final_payload), 500):
                supabase.table("daily_strategy_pnl").upsert(final_payload[i:i+500]).execute()
            
            print(f"[UPSERT SUCCESS] Pushed {len(final_payload)} rows to database for {strat_name}.\n")
            total_upserted += len(final_payload)
        except Exception as e:
            error_msg = f"❌ UPSERT CRASH for {strat_name}: {str(e)[:100]}"
            print(error_msg)
            report_progress("error", error_msg)
            raise e

    # 6. Final Summary
    print("\n--- Processing Summary ---")
    print(f"Total raw intraday records fetched: {len(new_intraday_data)}")
    print(f"Total records successfully upserted: {total_upserted}")
    print(f"Unique dates processed: {len(processed_dates)}")
    print(f"Unique strategies updated: {len(processed_strategies)}")
    print(f"Records skipped/failed: {skipped_records}")
    print("--------------------------\n")

    if total_upserted > 0:
        msg_final = f"✅ Successfully upserted {total_upserted} daily trades."
        print(msg_final)
        report_progress("success", msg_final)
    else:
        msg_done = "✅ Already up to date. No new records were inserted."
        print(msg_done)
        report_progress("success", msg_done)

if __name__ == "__main__":
    try:
        run_pnl_refresh()
    except Exception as e:
        msg_err = f"❌ Step 6 Failed: {str(e)[:50]}"
        print(msg_err)
        report_progress("error", msg_err)
        raise e
