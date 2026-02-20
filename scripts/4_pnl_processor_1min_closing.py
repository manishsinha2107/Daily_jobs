import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client

# --- MIGRATION FIX: Environment Awareness ---
if os.path.exists(".env"):
    load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("❌ Error: Supabase credentials missing.")
    exit(1)

supabase: Client = create_client(url, key)

# --- HEARTBEAT REPORTER (Surgical Patch) ---
def report_progress(status, msg):
    try:
        supabase.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": datetime.now().isoformat() # Use dynamic ISO time
        }).eq("step_id", "step4").execute() # CHANGED: step4.1 -> step4
    except Exception as e:
        print(f"⚠️ Heartbeat update failed: {e}")

def fetch_all_verified_records():
    """Pagination-aware fetch for ONLY pending records."""
    all_data = []
    limit = 1000
    offset = 0
    while True:
        # ✅ UPDATE: We filter for 'pending' directly in the query
        res = supabase.table("strategy_trades_verification") \
            .select("strategy_id, trade_date, pnl_1min_status") \
            .eq("pnl_1min_status", "pending") \
            .in_("ohlc_status", ["verified_ohlc_present", "partial_ohlc_data"]) \
            .range(offset, offset + limit - 1) \
            .execute()

        all_data.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
    return all_data

def fetch_ohlc_data_paginated(symbols, t_date):
    """PATCH 1: Paginated fetch for OHLC data to bypass 1000-row limit."""
    all_ohlc = []
    limit = 1000
    offset = 0
    while True:
        res = supabase.table("market_ohlc_cache") \
            .select("symbol, ts, close") \
            .in_("symbol", symbols) \
            .like("ts", f"{t_date}%") \
            .range(offset, offset + limit - 1) \
            .execute()

        if not res.data: break
        all_ohlc.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
    return all_ohlc

def calculate_intraday_pnl_1min_closing():
    print("🔍 Fetching database state for 1-Min Closing Audit...")
    # --- REPORTING START ---
    report_progress("running", "🔍 Analyzing P&L queue...")

    # --- LOGGING POINT 1: Total Records ---
    print(f"📊 Total records fetched from Supabase: {len(raw_audit_data)}")
    
    raw_audit_data = fetch_all_verified_records()

        # --- LOGGING POINT 1: Total Records ---
    print(f"📊 Total records fetched from Supabase: {len(raw_audit_data)}")

    if not raw_audit_data: 
        print("✅ No records found with 'pending' status and valid OHLC data.")
        report_progress("success", "✅ No pending 1-min P&L tasks.")
        return

    df_audit = pd.DataFrame(raw_audit_data)

    # --- LOGGING POINT 2: Filtered Pending ---
    df_pending = df_audit[df_audit['pnl_1min_status'] == 'pending']
    print(f"📥 Pending P&L calculations: {len(df_pending)}")
    
    if df_pending.empty:
        print("🏁 All fetched records are already processed or skipped. Exiting.")
        report_progress("success", "🏁 All P&L tasks completed.")
        return

    strategy_map = {}
    for _, row in df_pending.iterrows():
        sid = row['strategy_id']
        if sid not in strategy_map: strategy_map[sid] = []
        strategy_map[sid].append(row['trade_date'])

    if not strategy_map:
        print("🏁 Processing Finished: No pending 1-min closing tasks left.")
        report_progress("success", "🏁 All P&L tasks completed.")
        return

    # Tracking total progress for the dashboard
    total_dates = sum(len(set(dates)) for dates in strategy_map.values())
    processed_count = 0

    for strat_id in sorted(strategy_map.keys()):
        date_list = sorted(list(set(strategy_map[strat_id])))
        for t_date in date_list:
            processed_count += 1
            # --- REPORTING PROGRESS ---
            report_progress("running", f"📈 [{processed_count}/{total_dates}] Processing Strat {strat_id}...")

            # --- NEW: PRE-CALCULATION CHECK ---
            check = supabase.table("intraday_pnl_1min_closing") \
                .select("strategy_id") \
                .eq("strategy_id", strat_id) \
                .eq("trade_date", t_date) \
                .execute()

            if check.data:
                supabase.table("strategy_trades_verification").update({"pnl_1min_status": "completed"}) \
                    .eq("strategy_id", strat_id).eq("trade_date", t_date).execute()
                print(f"  ⏭️ {t_date} | Already calculated. Status synced.")
                continue

            res = supabase.table("strategy_trades_verification").select("*") \
                .eq("strategy_id", strat_id).eq("trade_date", t_date).execute()

            if not res.data: continue

            df_all = pd.DataFrame(res.data)
            df_all['dt_obj'] = pd.to_datetime(df_all['txn_time'], format='mixed')
            df_all = df_all.sort_values(by='dt_obj')

            instruments = df_all['broker_symbol'].unique().tolist()
            ohlc_data = fetch_ohlc_data_paginated(instruments, t_date)

            ohlc_lookup = {}
            for row in ohlc_data:
                ohlc_lookup[(row['symbol'], row['ts'])] = float(row['close'])

            inventory = {}
            realized_pnl_bucket = 0.0
            pnl_series = []

            current_time = df_all['dt_obj'].min().replace(second=0, microsecond=0)
            market_close = datetime.strptime(f"{t_date} 15:30:00", "%Y-%m-%d %H:%M:%S")

            if current_time > market_close:
                print(f"⚠️ SKIPPED: Trades after 3:30 PM ({df_all['txn_time'].min()})")
                supabase.table("strategy_trades_verification").update({"pnl_1min_status": "skipped_invalid_time"}) \
                    .eq("strategy_id", strat_id).eq("trade_date", t_date).execute()
                continue

            print(f"  📈 {t_date} | Strategy {strat_id} | Calculating...", end=" ", flush=True)

            while current_time <= market_close:
                next_minute = current_time + timedelta(minutes=1)
                minute_txns = df_all[(df_all['dt_obj'] >= current_time) & (df_all['dt_obj'] < next_minute)]

                for _, txn in minute_txns.iterrows():
                    inst, t_type, t_price = txn['broker_symbol'], txn['txn_type'], float(txn['price'])
                    t_qty = int(abs(txn['quantity']))

                    if inst not in inventory or inventory[inst]['qty'] == 0:
                        inventory[inst] = {'qty': t_qty, 'avg_price': t_price, 'side': 'LONG' if t_type == 'B' else 'SHORT'}
                    else:
                        inv = inventory[inst]
                        if (inv['side'] == 'LONG' and t_type == 'B') or (inv['side'] == 'SHORT' and t_type == 'S'):
                            new_total = inv['qty'] + t_qty
                            inv['avg_price'] = ((inv['avg_price'] * inv['qty']) + (t_price * t_qty)) / new_total
                            inv['qty'] = new_total
                        else:
                            if t_qty > inv['qty']:
                                excess_qty = t_qty - inv['qty']
                                pnl_mult = 1 if inv['side'] == 'LONG' else -1
                                realized_pnl_bucket += (t_price - inv['avg_price']) * inv['qty'] * pnl_mult
                                inv['side'] = 'SHORT' if inv['side'] == 'LONG' else 'LONG'
                                inv['qty'] = excess_qty
                                inv['avg_price'] = t_price
                            else:
                                pnl_mult = 1 if inv['side'] == 'LONG' else -1
                                realized_pnl_bucket += (t_price - inv['avg_price']) * t_qty * pnl_mult
                                inv['qty'] -= t_qty

                m_close = 0.0
                has_active_inventory = False
                time_str_db = current_time.strftime('%I:%M:%S %p').lstrip('0')
                lookup_ts = f"{t_date} {time_str_db}"

                for inst, data in inventory.items():
                    if data['qty'] > 0:
                        has_active_inventory = True
                        close_val = ohlc_lookup.get((inst, lookup_ts))
                        if close_val:
                            pnl_mult = 1 if data['side'] == 'LONG' else -1
                            m_close += (close_val - data['avg_price']) * data['qty'] * pnl_mult

                total_pnl = round(realized_pnl_bucket + m_close, 2)
                pnl_series.append({
                    "pnl": total_pnl,
                    "time": current_time.strftime('%I:%M %p').lstrip('0')
                })

                if not has_active_inventory and current_time > df_all['dt_obj'].max():
                    break
                current_time = next_minute

            if pnl_series:
                supabase.table("intraday_pnl_1min_closing").upsert(
                    {
                        "strategy_id": int(strat_id),
                        "trade_date": t_date,
                        "pnl_data": pnl_series,
                        "updated_at": datetime.now().isoformat()
                    },
                    on_conflict="strategy_id, trade_date"
                ).execute()

                supabase.table("strategy_trades_verification").update({"pnl_1min_status": "completed"}) \
                    .eq("strategy_id", strat_id).eq("trade_date", t_date).execute()

                print(f"✅ Final P&L: {pnl_series[-1]['pnl']}")

    # --- REPORTING SUCCESS ---
    report_progress("success", f"✅ Processed {processed_count} P&L dates.")

if __name__ == "__main__":
    try:
        calculate_intraday_pnl_1min_closing()
    except Exception as e:
        report_progress("error", f"❌ P&L Error: {str(e)[:50]}")
        exit(1)
