import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
try:
    from dotenv import load_dotenv
    if os.path.exists(".env"):
        load_dotenv()
except ImportError:
    pass

from supabase import create_client, Client

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
        print(msg)
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

# --- DATA FETCHERS & LOOKUPS ---
def fetch_all_paginated(table_name, select_query="*"):
    """Fetches entire tables dynamically bypassing the 1,000 row API limit."""
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

def get_latest_date_for_strategy(strategy_id):
    """Fetches the maximum date already processed in daily_strategy_pnl."""
    response = supabase.table("daily_strategy_pnl").select("trade_date").eq("strategy_id", strategy_id).order("trade_date", desc=True).limit(1).execute()
    if response.data: return response.data[0]['trade_date']
    return "2000-01-01" 

# --- CORE LOGIC ---
def run_pnl_refresh():
    report_progress("running", "🔄 Starting Unified P&L Refresh...")
    
    # 1. Fetch Active Strategies (Live Auto & Live Offline)
    strategies_res = supabase.table("strategies").select("strategy_id, strategy_name, strategy_full_name, strategy_grouping, capital, index_name, deployment_type, user_name, status").eq("status", "Active").execute()
    active_strategies = {str(s['strategy_id']): s for s in strategies_res.data}
    
    if not active_strategies:
        report_progress("success", "✅ No active strategies found.")
        return

    # 2. Upfront Memory Load
    report_progress("running", "📦 Loading historical lots and multipliers into memory...")
    lot_lookup = build_lot_size_lookup(fetch_all_paginated("lot_sizes"))
    deploy_lookup = build_deployment_lookup(fetch_all_paginated("live_deployments"))
    
    # Pre-calculate base unit capital mapping
    unit_cap_map = {}
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for sid, strat in active_strategies.items():
        curr_lot = get_historical_lot_size(lot_lookup, strat.get('index_name'), today_str)
        unit_cap_map[sid] = float(strat.get('capital', 0)) / curr_lot if curr_lot else 0

    raw_new_trades = []
    skipped_records = 0
    total_fetched = 0

    report_progress("running", f"🔎 Fetching incremental intraday data for {len(active_strategies)} strategies...")

    # 3. Strategy-Level Incremental Fetch & Parse
    for strat_id_str, strat_meta in active_strategies.items():
        strat_name = strat_meta.get('strategy_full_name') or strat_meta.get('strategy_name') or f"ID {strat_id_str}"
        latest_date = get_latest_date_for_strategy(strat_id_str)
        
        fetch_start, fetch_limit = 0, 1000
        strategy_records = []
        
        while True:
            res = supabase.table("intraday_pnl_1min_closing").select("*").eq("strategy_id", strat_id_str).gt("trade_date", latest_date).range(fetch_start, fetch_start + fetch_limit - 1).execute()
            chunk = res.data
            if not chunk: break
            strategy_records.extend(chunk)
            if len(chunk) < fetch_limit: break
            fetch_start += fetch_limit

        total_fetched += len(strategy_records)

        for row in strategy_records:
            t_date_str = row.get('trade_date', 'Unknown')
            try:
                raw_pnl = row.get('pnl_data')
                pnl_json = raw_pnl if isinstance(raw_pnl, list) else json.loads(raw_pnl)
                if not pnl_json: continue
                
                daily_final_pnl = pnl_json[-1]['pnl']
                max_profit_obj = max(pnl_json, key=lambda x: x['pnl'])
                max_loss_obj = min(pnl_json, key=lambda x: x['pnl'])
                
                t_date_obj = datetime.strptime(t_date_str, "%Y-%m-%d")
                target_month_iso = t_date_obj.replace(day=1).strftime("%Y-%m-%d")
                
                # Multiplier Logic + Guardrails
                multiplier_key = f"{strat_id_str}_{target_month_iso}"
                deploy_type = strat_meta.get('deployment_type', '')
                
                if multiplier_key in deploy_lookup:
                    multiplier = deploy_lookup[multiplier_key]
                elif deploy_type == 'Live Auto':
                    error_msg = f"❌ FATAL ERROR: Live Auto ID {strat_id_str} missing multiplier for {target_month_iso}."
                    report_progress("error", error_msg)
                    raise KeyError(error_msg)
                else:
                    multiplier = 1
                
                # Capital Math
                hist_lot = get_historical_lot_size(lot_lookup, strat_meta['index_name'], t_date_str)
                eff_cap = unit_cap_map.get(strat_id_str, 0) * hist_lot * multiplier
                pnl_pct = round((daily_final_pnl / eff_cap * 100), 4) if eff_cap > 0 else 0.0
                
                raw_new_trades.append({
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
                    "updated_at": datetime.now(timezone.utc).isoformat()
                })
            except Exception:
                skipped_records += 1
                continue

    if not raw_new_trades:
        report_progress("success", "✅ Already up to date. No new records to process.")
        return

    # 4. Cumulative Pandas Math
    report_progress("running", f"🧮 Computing cumulative metrics for {len(raw_new_trades)} new trades...")
    df_new = pd.DataFrame(raw_new_trades)
    
    # We only need existing data for the strategies we are actively updating
    updated_sids = df_new['strategy_id'].astype(str).unique().tolist()
    existing_data = []
    
    # Fetch existing data for these specific strategies to continue the cumulative sum
    for sid in updated_sids:
        existing_data.extend(fetch_all_paginated("daily_strategy_pnl", f"*").loc[lambda x: True] if False else supabase.table("daily_strategy_pnl").select("*").eq("strategy_id", sid).execute().data) # Optimized query

    # Correct fetch for existing data
    df_ui_existing = pd.DataFrame(existing_data) if existing_data else pd.DataFrame()
    df_work = pd.concat([df_ui_existing, df_new], ignore_index=True)
    
    final_payload = []
    for sid in df_new['strategy_id'].unique():
        strat_df = df_work[df_work['strategy_id'] == sid].sort_values('trade_date').copy()
        
        # Calculate cumulatives across entire history
        strat_df['cumulative_pnl'] = strat_df['pnl'].cumsum()
        strat_df['peak_cumulative_pnl'] = strat_df['cumulative_pnl'].cummax()
        strat_df['max_dd_amount'] = strat_df['peak_cumulative_pnl'] - strat_df['cumulative_pnl']
        
        # We only want to upsert the NEW rows (to save DB write limits)
        new_dates = df_new[df_new['strategy_id'] == sid]['trade_date'].tolist()
        strat_new_only = strat_df[strat_df['trade_date'].isin(new_dates)]
        
        for _, r in strat_new_only.iterrows():
            row_dict = r.replace({np.nan: None}).to_dict()
            row_dict.pop('id', None)  # Let Supabase handle the ID on upsert/insert
            final_payload.append(row_dict)

    # 5. Bulk Upsert
    report_progress("running", f"📤 Upserting {len(final_payload)} rows to daily_strategy_pnl...")
    for i in range(0, len(final_payload), 500):
        supabase.table("daily_strategy_pnl").upsert(final_payload[i:i+500]).execute()
    
    report_progress("success", f"✅ Successfully processed {total_fetched} raw records and updated {len(final_payload)} daily trades.")

if __name__ == "__main__":
    try:
        run_pnl_refresh()
    except Exception as e:
        report_progress("error", f"❌ Step 6 Failed: {str(e)[:50]}")
        raise e
