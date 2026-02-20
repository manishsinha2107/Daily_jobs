import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client

# --- MIGRATION FIX: Local vs Cloud Environment ---
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
            "updated_at": datetime.now().isoformat()
        }).eq("step_id", "step5").execute() # Ensure this is step5
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
            .select("strategy_id, trade_date, pnl_status") \
            .eq("pnl_status", "pending") \
            .in_("ohlc_status", ["verified_ohlc_present", "partial_ohlc_data"]) \
            .range(offset, offset + limit - 1) \
            .execute()

        all_data.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
    return all_data

def fetch_ohlc_data_paginated(symbols, t_date):
    """PATCH 1: Paginated fetch to bypass 1000-row limit (Ensures no legs are dropped)"""
    all_ohlc = []
    limit = 1000
    offset = 0
    while True:
        res = supabase.table("market_ohlc_cache") \
            .select("symbol, ts, close, high, low") \
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
    report_progress("running", "🔍 Analyzing P&L queue...")
    
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
        print("🏁 Processing Finished: No pending tasks left.")
        report_progress("success", "🏁 No pending OHLC P&L tasks.")
        return

    # Progress Tracking
    total_dates = sum(len(set(dates)) for dates in strategy_map.values())
    processed_count = 0

    for strat_id in sorted(strategy_map.keys()):
        date_list = sorted(list(set(strategy_map[strat_id])))

        for t_date in date_list:
            processed_count += 1
            # --- REPORTING PROGRESS ---
            report_progress("running", f"📊 [{processed_count}/{total_dates}] High-Fi P&L: Strat {strat_id}...")

            # Check for existing P&L
            check = supabase.table("intraday_pnl_1min_ohlc") \
                .select("strategy_id").eq("strategy_id", strat_id).eq("trade_date", t_date).execute()

            if check.data:
                supabase.table("strategy_trades_verification").update({"pnl_status": "completed"}) \
                    .eq("strategy_id", strat_id).eq("trade_date", t_date).execute()
                print(f"  ⏭️ {t_date} | Already calculated. Status synced.")
                continue

            res = supabase.table("strategy_trades_verification").select("*") \
                .eq("strategy_id", strat_id).eq("trade_date", t_date).execute()

            if not res.data: continue

            # --- PRE-PROCESS TRADES ---
            df_all = pd.DataFrame(res.data)
            df_all['dt_obj'] = pd.to_datetime(df_all['txn_time'], format='mixed')
            df_all = df_all.sort_values(by='dt_obj')

            # --- PRE-PROCESS OHLC ---
            instruments = df_all['broker_symbol'].unique().tolist()
            ohlc_data = fetch_ohlc_data_paginated(instruments, t_date)
            ohlc_lookup = {(row['symbol'], row['ts']): row for row in ohlc_data}

            inventory = {}
            realized_pnl_bucket = 0.0
            pnl_series = []

            current_time = df_all['dt_obj'].min().replace(second=0, microsecond=0)
            market_close = datetime.strptime(f"{t_date} 15:30:00", "%Y-%m-%d %H:%M:%S")

            # --- PRE-FLIGHT VALIDATION ---
            if current_time > market_close:
                print(f"⚠️ SKIPPED: Trades after 3:30 PM ({df_all['txn_time'].min()})")
                supabase.table("strategy_trades_verification").update({"pnl_status": "skipped_invalid_time"}) \
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
                                excess = t_qty - inv['qty']
                                pnl_mult = 1 if inv['side'] == 'LONG' else -1
                                realized_pnl_bucket += (t_price - inv['avg_price']) * inv['qty'] * pnl_mult
                                inv['side'] = 'SHORT' if inv['side'] == 'LONG' else 'LONG'
                                inv['qty'] = excess
                                inv['avg_price'] = t_price
                            else:
                                pnl_mult = 1 if inv['side'] == 'LONG' else -1
                                realized_pnl_bucket += (t_price - inv['avg_price']) * t_qty * pnl_mult
                                inv['qty'] -= t_qty

                m_c, m_h, m_l = 0.0, 0.0, 0.0
                has_active = False
                time_str_db = current_time.strftime('%I:%M:%S %p').lstrip('0')
                lookup_ts = f"{t_date} {time_str_db}"

                for inst, data in inventory.items():
                    if data['qty'] > 0:
                        has_active = True
                        ohlc = ohlc_lookup.get((inst, lookup_ts))
                        if ohlc:
                            o_c, o_h, o_l = float(ohlc['close']), float(ohlc['high']), float(ohlc['low'])
                            if data['side'] == 'LONG':
                                m_c += (o_c - data['avg_price']) * data['qty']
                                m_h += (o_h - data['avg_price']) * data['qty']
                                m_l += (o_l - data['avg_price']) * data['qty']
                            else: # SHORT
                                m_c += (data['avg_price'] - o_c) * data['qty']
                                m_h += (data['avg_price'] - o_l) * data['qty']
                                m_l += (data['avg_price'] - o_h) * data['qty']

                pnl_series.append({
                    "time": current_time.strftime('%I:%M %p').lstrip('0'),
                    "c": str(round(realized_pnl_bucket + m_c, 2)),
                    "h": str(round(realized_pnl_bucket + m_h, 2)),
                    "l": str(round(realized_pnl_bucket + m_l, 2))
                })

                if not has_active and current_time > df_all['dt_obj'].max():
                    break
                current_time = next_minute

            if pnl_series:
                supabase.table("intraday_pnl_1min_ohlc").upsert({
                    "strategy_id": int(strat_id), "trade_date": t_date,
                    "pnl_data": pnl_series, "updated_at": datetime.now().isoformat()
                }).execute()
                supabase.table("strategy_trades_verification").update({"pnl_status": "completed"}) \
                    .eq("strategy_id", strat_id).eq("trade_date", t_date).execute()
                print(f"✅ Done")

    # --- REPORTING SUCCESS ---
    report_progress("success", "🏁 Full P&L OHLC Audit sequence finished.")

if __name__ == "__main__":
    try:
        calculate_intraday_pnl_1min()
    except Exception as e:
        report_progress("error", f"❌ High-Fi P&L Error: {str(e)[:50]}")
        exit(1)
