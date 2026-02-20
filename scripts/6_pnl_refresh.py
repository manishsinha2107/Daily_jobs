import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
# --- MIGRATION FIX: Environment Awareness ---
try:
    from dotenv import load_dotenv
    if os.path.exists(".env"):
        load_dotenv()
except ImportError:
    pass

from supabase import create_client, Client

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- HEARTBEAT REPORTER (Surgical Patch for Step 6) ---
def report_progress(status, msg):
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": datetime.now().isoformat()
        }).eq("step_id", "step6").execute() # Targets Step 6 slot
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

def fetch_all_paginated(table_name):
    all_data = []
    offset, limit = 0, 1000
    while True:
        res = supabase.table(table_name).select("*").range(offset, offset + limit - 1).execute()
        data = res.data
        all_data.extend(data)
        if len(data) < limit: break
        offset += limit
    return pd.DataFrame(all_data)

def get_lot_size(lot_df, instrument, target_date):
    if isinstance(target_date, str):
        target_date = datetime.strptime(target_date, "%Y-%m-%d")
    first_of_month = target_date.replace(day=1).strftime("%Y-%m-%d")
    valid_lots = lot_df[(lot_df['instrument'] == instrument) & (lot_df['effective_date'] <= first_of_month)]
    if valid_lots.empty:
        return lot_df[lot_df['instrument'] == instrument].sort_values('effective_date').iloc[0]['lot_size']
    return valid_lots.sort_values('effective_date', ascending=False).iloc[0]['lot_size']

def run_pnl_refresh():
    print("🔄 Starting P&L Refresh...") 
    report_progress("running", "🔄 Syncing source tables...")

    # LOAD SOURCES
    df_strategies = fetch_all_paginated("strategies")
    df_lots = fetch_all_paginated("lot_sizes")
    df_deploy = fetch_all_paginated("live_deployments")
    df_intraday = fetch_all_paginated("intraday_pnl_1min_closing")
    
    try:
        df_ui_existing = fetch_all_paginated("daily_strategy_pnl")
        if not df_ui_existing.empty:
            existing_keys = set((df_ui_existing['strategy_id'].astype(str) + "_" + df_ui_existing['trade_date'].astype(str)).tolist())
        else:
            existing_keys = set()
    except:
        existing_keys = set()

    # FILTER: ACTIVE AND LIVE AUTO
    active_strats = df_strategies[(df_strategies['status'] == 'Active') & (df_strategies['deployment_type'] == 'Live Auto')]
    active_ids = active_strats['strategy_id'].astype(int).tolist()

    # CAPITAL MAP
    unit_cap_map = {}
    today_dt = datetime.now()
    for _, s in active_strats.iterrows():
        curr_lot = get_lot_size(df_lots, s['index_name'], today_dt)
        unit_cap_map[s['strategy_id']] = float(s['capital']) / curr_lot

    raw_new_trades = []
    total_potential = len(df_intraday)
    
    report_progress("running", f"🧐 Analyzing {total_potential} intraday records...")

    for _, row in df_intraday.iterrows():
        sid = int(row['strategy_id'])
        t_date_str = str(row['trade_date'])
        
        if sid in active_ids and f"{sid}_{t_date_str}" not in existing_keys:
            try:
                pnl_json = json.loads(row['pnl_data']) if isinstance(row['pnl_data'], str) else row['pnl_data']
                if not pnl_json: continue
                last_pnl = float(pnl_json[-1]['pnl'])
                
                s_meta = active_strats[active_strats['strategy_id'] == sid].iloc[0]
                t_date_obj = datetime.strptime(t_date_str, "%Y-%m-%d")
                
                m_deploy = df_deploy[(df_deploy['strategy_id'] == sid) & (df_deploy['month'] == t_date_obj.strftime("%B %Y"))]
                multiplier = int(m_deploy.iloc[0]['multiplier']) if not m_deploy.empty else 1
                
                hist_lot = get_lot_size(df_lots, s_meta['index_name'], t_date_obj)
                eff_cap = unit_cap_map[sid] * hist_lot * multiplier

                raw_new_trades.append({
                    "trade_date": t_date_str,
                    "trade_year": t_date_obj.year,
                    "trade_month": t_date_obj.month,
                    "trade_month_name": t_date_obj.strftime("%b"),
                    "month_year": t_date_obj.strftime("%b %Y"),
                    "strategy_id": sid,
                    "strategy_name": s_meta['strategy_name'],
                    "index_name": s_meta['index_name'],
                    "user_name": s_meta['user_name'],
                    "strategy_grouping": s_meta['strategy_grouping'],
                    "status": s_meta['status'],
                    "deployment_type": s_meta['deployment_type'],
                    "pnl": round(last_pnl, 2),
                    "eff_capital": round(eff_cap, 2),
                    "multiplier": multiplier,
                    "is_win": 1 if last_pnl > 0 else 0,
                    "pnl_percent": round((last_pnl / eff_cap * 100), 4) if eff_cap > 0 else 0
                })
            except: continue

    if not raw_new_trades:
        print("✅ Already up to date.")
        report_progress("success", "✅ Already up to date.")
        return

    # COMPUTE AND UPSERT
    report_progress("running", f"🧮 Computing metrics for {len(raw_new_trades)} new trades...")
    df_new = pd.DataFrame(raw_new_trades)
    df_work = pd.concat([df_ui_existing, df_new], ignore_index=True)
    final_payload = []

    for sid in df_new['strategy_id'].unique():
        strat_df = df_work[df_work['strategy_id'] == sid].sort_values('trade_date').copy()
        strat_df['cumulative_pnl'] = strat_df['pnl'].cumsum()
        strat_df['peak_cumulative_pnl'] = strat_df['cumulative_pnl'].cummax()
        strat_df['max_dd_amount'] = strat_df['peak_cumulative_pnl'] - strat_df['cumulative_pnl']
        
        for _, r in strat_df.iterrows():
            row_dict = r.replace({np.nan: None}).to_dict()
            row_dict.pop('id', None)
            final_payload.append(row_dict)

    report_progress("running", "📤 Upserting to daily_strategy_pnl...")
    for i in range(0, len(final_payload), 500):
        supabase.table("daily_strategy_pnl").upsert(final_payload[i:i+500]).execute()
    
    msg = f"✅ Successfully updated {len(raw_new_trades)} trades."
    print(msg)
    report_progress("success", msg)

if __name__ == "__main__":
    try:
        run_pnl_refresh()
    except Exception as e:
        report_progress("error", f"❌ Step 6 Failed: {str(e)[:50]}")
        raise e
