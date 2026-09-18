import sys
import os
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
import config

# =====================================================================
# SECURE SUPABASE INITIALIZATION
# =====================================================================
if os.path.exists(".env"):
    load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("❌ Error: Supabase credentials missing.")
    sys.exit(1)

supabase: Client = create_client(url, key)

# =====================================================================
# HELPERS: PAGINATION & LOOKUPS
# =====================================================================
def fetch_all_paginated(table_name, select_query="*", filters=None):
    all_data = []
    offset, limit = 0, 1000
    while True:
        query = supabase.table(table_name).select(select_query)
        if filters:
            for f in filters:
                if f['type'] == 'eq': query = query.eq(f['col'], f['val'])
                elif f['type'] == 'in': query = query.in_(f['col'], f['val'])
                
        res = query.range(offset, offset + limit - 1).execute()
        chunk = res.data
        if not chunk: break
        all_data.extend(chunk)
        if len(chunk) < limit: break
        offset += limit
    return all_data

def fetch_ohlc_data_paginated(symbols, t_date):
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
    target_dt = datetime.strptime(str(target_date_str).split(' ')[0], "%Y-%m-%d")
    first_of_month = target_dt.replace(day=1).strftime("%Y-%m-%d")
    valid_lots = [lot for lot in lookup[index_name] if lot['effective_date'] <= first_of_month]
    if not valid_lots: return valid_lots[0]['lot_size'] if valid_lots else 1
    return valid_lots[0]['lot_size']

def build_tax_lookup(tax_data):
    lookup = {}
    for row in tax_data:
        seg = row['segment']
        if seg not in lookup: lookup[seg] = []
        lookup[seg].append(row)
    for seg in lookup:
        lookup[seg].sort(key=lambda x: x['effective_date'], reverse=True)
    return lookup

def get_historical_tax(lookup, segment, target_date_str):
    target_dt_str = str(target_date_str).split(' ')[0]
    valid_taxes = [t for t in lookup[segment] if t['effective_date'] <= target_dt_str]
    if not valid_taxes: return lookup[segment][-1] 
    return valid_taxes[0]

# =====================================================================
# LIVE MEMORY ENGINE: 1-MINUTE MAE/MFE CALCULATOR & DAILY SNAPSHOTS
# =====================================================================
def extract_memory_extremes(strat_id, entry_date_str, exit_date_str, entry_time_str, exit_time_str):
    
    # 1. PERFECT TIMEZONE STRIPPING
    entry_dt = pd.to_datetime(entry_time_str).replace(tzinfo=None)
    exit_dt = pd.to_datetime(exit_time_str).replace(tzinfo=None)

    # 2. FETCH TRADES BY DATE (SAFE), THEN FILTER IN PANDAS (SAFE)
    all_trades = []
    offset, limit = 0, 1000
    while True:
        res = supabase.table("strategy_trades_verification") \
            .select("broker_symbol, txn_time, txn_type, quantity, price") \
            .eq("strategy_id", strat_id) \
            .eq("ohlc_status", "verified_ohlc_present") \
            .gte("trade_date", entry_date_str) \
            .lte("trade_date", exit_date_str) \
            .range(offset, offset + limit - 1) \
            .execute()
        if not res.data: break
        all_trades.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
        
    trades_df = pd.DataFrame(all_trades)
    if trades_df.empty:
        return 0.0, str(exit_time_str), 0.0, str(entry_time_str), {}

    # Implement Exact Intraday Architecture for Trade Timestamps
    trades_df['dt_obj'] = pd.to_datetime(trades_df['txn_time'], format='mixed').apply(lambda x: x.replace(tzinfo=None))
    trades_df = trades_df[(trades_df['dt_obj'] >= entry_dt) & (trades_df['dt_obj'] <= exit_dt)]
    trades_df = trades_df.sort_values(by='dt_obj')

    if trades_df.empty:
        return 0.0, str(exit_time_str), 0.0, str(entry_time_str), {}

    symbols = trades_df['broker_symbol'].unique().tolist()

    # 3. FETCH OHLC DAY-BY-DAY (Bypasses AM/PM Bugs & SKIPS WEEKENDS)
    current_d = entry_dt.date()
    end_d = exit_dt.date()
    ohlc_lookup = {}
    
    while current_d <= end_d:
        if current_d.weekday() < 5:  # 0-4 represents Monday-Friday
            d_str = current_d.strftime('%Y-%m-%d')
            day_ohlc = fetch_ohlc_data_paginated(symbols, d_str)
            for row in day_ohlc:
                # Implement Exact Intraday Architecture for OHLC lookup
                ohlc_lookup[(row['symbol'], row['ts'])] = float(row['close'])
        current_d += timedelta(days=1)

    # 4. GENERATE CONTINUOUS MARKET HOURS TIMELINE (SKIPS WEEKENDS & AFTER HOURS)
    current_time = entry_dt.replace(second=0, microsecond=0)
    end_time = exit_dt.replace(second=0, microsecond=0)
    unique_times = []
    
    while current_time <= end_time:
        # Skip Weekends entirely
        if current_time.weekday() >= 5:
            current_time = (current_time + timedelta(days=1)).replace(hour=9, minute=15)
            continue
            
        if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute > 30):
            current_time = (current_time + timedelta(days=1)).replace(hour=9, minute=15)
            continue
        if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 15):
            current_time = current_time.replace(hour=9, minute=15)
            continue
            
        unique_times.append(current_time)
        current_time += timedelta(minutes=1)
        
    for t in trades_df['dt_obj']:
        # Ensure execution times are in the list
        if t.weekday() < 5:
            unique_times.append(t.replace(second=0, microsecond=0))
        
    unique_times = sorted(list(set(unique_times)))

    # 5. SIMULATE ENGINE
    trade_idx = 0
    total_trades = len(trades_df)
    inventory = {}
    realized_pnl = 0.0
    
    max_pnl, min_pnl = -float('inf'), float('inf')
    max_pnl_time, min_pnl_time = None, None
    daily_mtm_snapshots = {}

    for current_ts in unique_times:
        date_str = current_ts.strftime('%Y-%m-%d')
        time_str = current_ts.strftime('%I:%M %p').lstrip('0')
        
        if date_str not in daily_mtm_snapshots:
            daily_mtm_snapshots[date_str] = {
                'eod_pnl': 0.0, 'max_pnl': -float('inf'), 'max_time': time_str, 
                'min_pnl': float('inf'), 'min_time': time_str
            }

        while trade_idx < total_trades and trades_df.iloc[trade_idx]['dt_obj'] <= current_ts + timedelta(seconds=59):
            txn = trades_df.iloc[trade_idx]
            sym, t_price, t_type = txn['broker_symbol'], float(txn['price']), txn['txn_type']
            t_qty = int(abs(txn['quantity']))

            if sym not in inventory or inventory[sym]['qty'] == 0:
                inventory[sym] = {'qty': t_qty, 'avg_price': t_price, 'side': 'LONG' if t_type == 'B' else 'SHORT', 'last_price': t_price}
            else:
                inv = inventory[sym]
                if (inv['side'] == 'LONG' and t_type == 'B') or (inv['side'] == 'SHORT' and t_type == 'S'):
                    new_total = inv['qty'] + t_qty
                    inv['avg_price'] = ((inv['avg_price'] * inv['qty']) + (t_price * t_qty)) / new_total
                    inv['qty'] = new_total
                else:
                    if t_qty > inv['qty']:
                        excess = t_qty - inv['qty']
                        mult = 1 if inv['side'] == 'LONG' else -1
                        realized_pnl += (t_price - inv['avg_price']) * inv['qty'] * mult
                        inv['side'] = 'SHORT' if inv['side'] == 'LONG' else 'LONG'
                        inv['qty'] = excess
                        inv['avg_price'] = t_price
                        inv['last_price'] = t_price 
                    else:
                        mult = 1 if inv['side'] == 'LONG' else -1
                        realized_pnl += (t_price - inv['avg_price']) * t_qty * mult
                        inv['qty'] -= t_qty
            trade_idx += 1

        # MTM Evaluation utilizing EXACT INTRADAY STRING LOOKUP
        m_close = 0.0
        time_str_db = current_ts.strftime('%I:%M:%S %p').lstrip('0')
        lookup_ts = f"{date_str} {time_str_db}"
        
        for sym, inv in inventory.items():
            if inv['qty'] > 0:
                close_val = ohlc_lookup.get((sym, lookup_ts))
                if close_val is not None:
                    inv['last_price'] = close_val
                
                pnl_mult = 1 if inv['side'] == 'LONG' else -1
                m_close += (inv['last_price'] - inv['avg_price']) * inv['qty'] * pnl_mult

        total_live_pnl = realized_pnl + m_close
        
        daily_mtm_snapshots[date_str]['eod_pnl'] = total_live_pnl
        
        if total_live_pnl > daily_mtm_snapshots[date_str]['max_pnl']:
            daily_mtm_snapshots[date_str]['max_pnl'] = total_live_pnl
            daily_mtm_snapshots[date_str]['max_time'] = time_str
            
        if total_live_pnl < daily_mtm_snapshots[date_str]['min_pnl']:
            daily_mtm_snapshots[date_str]['min_pnl'] = total_live_pnl
            daily_mtm_snapshots[date_str]['min_time'] = time_str

        if total_live_pnl > max_pnl: max_pnl, max_pnl_time = total_live_pnl, time_str
        if total_live_pnl < min_pnl: min_pnl, min_pnl_time = total_live_pnl, time_str

    for d_str, stats in daily_mtm_snapshots.items():
        if stats['max_pnl'] == -float('inf'): stats['max_pnl'] = stats['eod_pnl']
        if stats['min_pnl'] == float('inf'): stats['min_pnl'] = stats['eod_pnl']

    return (
        round(max_pnl, 2) if max_pnl != -float('inf') else 0.0, max_pnl_time, 
        round(min_pnl, 2) if min_pnl != float('inf') else 0.0, min_pnl_time,
        daily_mtm_snapshots
    )

# =====================================================================
# LIVE CURVE REBUILDER
# =====================================================================
def run_live_positional_curve_rebuilder():
    print(f"\n{'='*60}")
    print("🔄 INITIATING LIVE POSITIONAL CURVE REBUILDER")
    print(f"{'='*60}\n")
    
    print("📦 Fetching live positional cycles from ledger...")
    cycles_data = fetch_all_paginated("positional_trade_ledger")
    
    if not cycles_data:
        print("✅ No closed live positional cycles found to process.")
        return
        
    cycles_df = pd.DataFrame(cycles_data)
    
    cycles_df['exit_time_sort'] = pd.to_datetime(cycles_df['exit_time'])
    cycles_df = cycles_df.sort_values(['strategy_id', 'exit_time_sort']).drop(columns=['exit_time_sort'])

    print("📦 Fetching metadata, lot sizes, and taxes from Supabase...")
    
    master_res = supabase.table("strategies").select("*") \
        .eq("position_type", "Positional") \
        .in_("deployment_type", config.DEPLOYMENT_TYPES) \
        .execute()
    
    master_meta = {int(s['strategy_id']): s for s in master_res.data}
    
    lot_lookup = build_lot_size_lookup(fetch_all_paginated("lot_sizes"))
    tax_lookup = build_tax_lookup(fetch_all_paginated("market_tax_rates"))
    
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for strat_id, strat_cycles in cycles_df.groupby('strategy_id'):
        meta = master_meta.get(int(strat_id))
        
        if not meta:
            print(f"⚠️ ERROR: Strategy {strat_id} metadata missing or not authorized. Skipping.")
            continue
            
        strat_name = meta.get('strategy_full_name') or meta.get('strategy_name') or f"ID {strat_id}"
        
        index_name = meta.get('index_name')
        if not index_name:
            raise KeyError(f"❌ 'index_name' missing in database metadata for Strategy ID {strat_id}")
            
        base_capital = float(meta.get('capital', 0.0))
        
        print(f"\n⚙️ Rebuilding Positional Curve For: {strat_name} (ID: {strat_id})")
        
        global_running_cum_pnl = 0.0
        global_running_peak = 0.0
        
        final_daily_payload = []
        final_summary_payload = []
        
        for _, cycle in strat_cycles.iterrows():
            entry_date_str = str(cycle['entry_date']).split(' ')[0]
            exit_date_str = str(cycle['exit_date']).split(' ')[0]
            
            entry_dt_obj = pd.to_datetime(cycle['entry_time']).replace(tzinfo=None)
            exit_dt_obj = pd.to_datetime(cycle['exit_time']).replace(tzinfo=None)
            broker_entry_time = entry_dt_obj.strftime('%I:%M %p').lstrip('0')
            broker_exit_time = exit_dt_obj.strftime('%I:%M %p').lstrip('0')
            
            gross_pnl = float(cycle['gross_pnl'])
            turnover = float(cycle['premium_turnover'])
            order_count = int(cycle['order_count'])
            
            try:
                cycle_qtys = json.loads(cycle['base_qtys']) if isinstance(cycle['base_qtys'], str) else cycle['base_qtys']
                if not isinstance(cycle_qtys, list): 
                    cycle_qtys = []
            except Exception:
                cycle_qtys = []
            
            mid_qty_idx = len(cycle_qtys) // 2
            
            tax_rates = get_historical_tax(tax_lookup, 'NFO_OPT', exit_date_str)
            half_turnover = turnover / 2.0
            
            exchange_fee = turnover * (float(tax_rates['exchange_fee_pct']) / 100)
            stt = half_turnover * (float(tax_rates['stt_sell_pct']) / 100)
            stamp_duty = half_turnover * (float(tax_rates['stamp_duty_buy_pct']) / 100)
            sebi_fee = (turnover / 10000000) * float(tax_rates['sebi_fee_per_crore'])
            brokerage = order_count * float(tax_rates['default_brokerage_per_order'])
            
            total_taxable = brokerage + exchange_fee + sebi_fee
            gst = total_taxable * (float(tax_rates['gst_pct']) / 100)
            
            estimated_costs = exchange_fee + stt + stamp_duty + sebi_fee + brokerage + gst
            net_pnl = gross_pnl - estimated_costs
            
            # Pass correct dates to safely query Supabase
            max_profit, mp_time, max_loss, ml_time, daily_snapshots = extract_memory_extremes(
                strat_id, entry_date_str, exit_date_str, cycle['entry_time'], cycle['exit_time']
            )
            
            valid_trading_dates = set(daily_snapshots.keys())
            valid_trading_dates.add(entry_date_str)
            valid_trading_dates.add(exit_date_str)
            
            # Strip out any weekends from valid_trading_dates fallback
            valid_trading_dates = {d for d in valid_trading_dates if datetime.strptime(d, "%Y-%m-%d").weekday() < 5}
            sorted_dates = sorted(list(valid_trading_dates))
            
            prev_snap = 0.0
            
            for d_str in sorted_dates:
                day_stat = daily_snapshots.get(d_str)
                curr_dt = datetime.strptime(d_str, "%Y-%m-%d")
                
                if d_str == exit_date_str:
                    curr_snap = gross_pnl
                else:
                    curr_snap = day_stat['eod_pnl'] if day_stat else prev_snap
                    
                daily_gross = curr_snap - prev_snap
                
                if day_stat:
                    daily_max = day_stat['max_pnl'] - prev_snap
                    daily_max_time = day_stat['max_time']
                    daily_min = day_stat['min_pnl'] - prev_snap
                    daily_min_time = day_stat['min_time']
                else:
                    daily_max = daily_gross
                    daily_min = daily_gross
                    if d_str == entry_date_str:
                        daily_max_time = broker_entry_time
                        daily_min_time = broker_entry_time
                    else:
                        daily_max_time = broker_exit_time
                        daily_min_time = broker_exit_time
                
                if d_str == exit_date_str and d_str == entry_date_str:
                    daily_cost = estimated_costs
                    daily_buy = int(cycle['buy_fills'])
                    daily_sell = int(cycle['sell_fills'])
                    daily_orders = int(cycle['order_count'])
                    daily_turnover = float(cycle['premium_turnover'])
                    daily_qtys = cycle_qtys
                elif d_str == entry_date_str:
                    daily_cost = estimated_costs / 2.0
                    daily_buy = int(cycle['buy_fills']) // 2
                    daily_sell = int(cycle['sell_fills']) // 2
                    daily_orders = int(cycle['order_count']) // 2
                    daily_turnover = float(cycle['premium_turnover']) / 2.0
                    daily_qtys = cycle_qtys[:mid_qty_idx]
                elif d_str == exit_date_str:
                    daily_cost = estimated_costs / 2.0
                    daily_buy = int(cycle['buy_fills']) - (int(cycle['buy_fills']) // 2)
                    daily_sell = int(cycle['sell_fills']) - (int(cycle['sell_fills']) // 2)
                    daily_orders = int(cycle['order_count']) - (int(cycle['order_count']) // 2)
                    daily_turnover = float(cycle['premium_turnover']) / 2.0
                    daily_qtys = cycle_qtys[mid_qty_idx:]
                else:
                    daily_cost = 0.0
                    daily_buy = 0
                    daily_sell = 0
                    daily_orders = 0
                    daily_turnover = 0.0
                    daily_qtys = []
                    
                daily_net = daily_gross - daily_cost
                
                hist_lot_daily = get_historical_lot_size(lot_lookup, index_name, d_str)
                curr_lot = get_historical_lot_size(lot_lookup, index_name, today_str)
                unit_cap = base_capital / curr_lot if curr_lot else 0
                eff_cap_daily = unit_cap * hist_lot_daily
                
                daily_pnl_pct = round((daily_net / eff_cap_daily * 100), 4) if eff_cap_daily > 0 else 0.0
                
                global_running_cum_pnl += daily_net
                if global_running_cum_pnl > global_running_peak:
                    global_running_peak = global_running_cum_pnl
                daily_max_dd_amount = global_running_peak - global_running_cum_pnl
                
                daily_cum_pnl_pct = round((global_running_cum_pnl / eff_cap_daily * 100), 4) if eff_cap_daily > 0 else 0.0
                daily_max_dd_pct = round((daily_max_dd_amount / eff_cap_daily * 100), 4) if eff_cap_daily > 0 else 0.0
                
                final_daily_payload.append({
                    "trade_date": d_str,
                    "month_year": curr_dt.strftime("%b %Y"),
                    "strategy_id": strat_id,
                    "strategy_name": strat_name,
                    "index_name": index_name,
                    "user_name": meta.get('user_name', ''),
                    "strategy_grouping": meta.get('strategy_grouping', ''),
                    "status": meta.get('status', 'Active'),
                    "deployment_type": meta.get('deployment_type', 'Live Offline'),
                    "pnl": round(daily_gross, 2),
                    "eff_capital": round(eff_cap_daily, 2),
                    "multiplier": 1,
                    "is_win": 1 if daily_net > 0 else 0,
                    "pnl_percent": daily_pnl_pct,
                    "cumulative_pnl": round(global_running_cum_pnl, 2),
                    "max_dd_percent": daily_max_dd_pct,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "cumulative_pnl_percent": daily_cum_pnl_pct,
                    "max_dd_amount": round(daily_max_dd_amount, 2),
                    "peak_cumulative_pnl": round(global_running_peak, 2),
                    "trade_year": curr_dt.year,
                    "trade_month": curr_dt.month,
                    "trade_month_name": curr_dt.strftime("%b"),
                    "max_profit": round(daily_max, 2),
                    "max_profit_time": daily_max_time,
                    "max_loss": round(daily_min, 2),
                    "max_loss_time": daily_min_time,
                    "base_capital": base_capital,
                    "trades_type": meta.get('trades_type', ''),
                    "buy_fills": daily_buy,
                    "sell_fills": daily_sell,
                    "order_count": daily_orders,
                    "premium_turnover": round(daily_turnover, 2),
                    "estimated_costs": round(daily_cost, 2),
                    "net_pnl": round(daily_net, 2),
                    "base_qtys": json.dumps(daily_qtys)
                })
                
                prev_snap = curr_snap

            hist_lot_exit = get_historical_lot_size(lot_lookup, index_name, exit_date_str)
            curr_lot_exit = get_historical_lot_size(lot_lookup, index_name, today_str)
            unit_cap_exit = base_capital / curr_lot_exit if curr_lot_exit else 0
            eff_cap_exit = unit_cap_exit * hist_lot_exit 
            
            cycle_pnl_pct = round((net_pnl / eff_cap_exit * 100), 4) if eff_cap_exit > 0 else 0.0
            cycle_cum_pnl_pct = round((global_running_cum_pnl / eff_cap_exit * 100), 4) if eff_cap_exit > 0 else 0.0
            cycle_max_dd_amount = global_running_peak - global_running_cum_pnl
            cycle_max_dd_pct = round((cycle_max_dd_amount / eff_cap_exit * 100), 4) if eff_cap_exit > 0 else 0.0
            
            exit_date_obj = datetime.strptime(exit_date_str, "%Y-%m-%d")
            
            final_summary_payload.append({
                "trade_date": exit_date_str,
                "month_year": exit_date_obj.strftime("%b %Y"),
                "strategy_id": strat_id,
                "strategy_name": strat_name,
                "index_name": index_name,
                "user_name": meta.get('user_name', ''),
                "strategy_grouping": meta.get('strategy_grouping', ''),
                "status": meta.get('status', 'Active'),
                "deployment_type": meta.get('deployment_type', 'Live Offline'),
                "pnl": round(gross_pnl, 2),
                "eff_capital": round(eff_cap_exit, 2),
                "multiplier": 1,
                "is_win": 1 if net_pnl > 0 else 0,
                "pnl_percent": cycle_pnl_pct,
                "cumulative_pnl": round(global_running_cum_pnl, 2),
                "max_dd_percent": cycle_max_dd_pct,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "cumulative_pnl_percent": cycle_cum_pnl_pct,
                "max_dd_amount": round(cycle_max_dd_amount, 2),
                "peak_cumulative_pnl": round(global_running_peak, 2),
                "trade_year": exit_date_obj.year,
                "trade_month": exit_date_obj.month,
                "trade_month_name": exit_date_obj.strftime("%b"),
                "max_profit": max_profit,
                "max_profit_time": mp_time,
                "max_loss": max_loss,
                "max_loss_time": ml_time,
                "base_capital": base_capital,
                "trades_type": meta.get('trades_type', ''),
                "buy_fills": int(cycle['buy_fills']),
                "sell_fills": int(cycle['sell_fills']),
                "order_count": order_count,
                "premium_turnover": round(turnover, 2),
                "estimated_costs": round(estimated_costs, 2),
                "net_pnl": round(net_pnl, 2),
                "base_qtys": cycle['base_qtys']
            })
            
            print(f"   📅 Anchored Cycle: {exit_date_str} | Gross: ₹{gross_pnl:.2f} | Net: ₹{net_pnl:.2f} | MAE: ₹{max_loss:.2f} | MFE: ₹{max_profit:.2f}")

        chunk_size = 250
        if final_daily_payload:
            print(f"   🚀 Upserting {len(final_daily_payload)} Daily MTM rows...")
            for i in range(0, len(final_daily_payload), chunk_size):
                chunk = final_daily_payload[i:i+chunk_size]
                try:
                    supabase.table("daily_strategy_pnl").upsert(chunk).execute()
                except Exception as e:
                    print(f"   ❌ Supabase Daily MTM upsert failed: {e}")
                    
        if final_summary_payload:
            print(f"   🚀 Upserting {len(final_summary_payload)} Cycle Summary rows...")
            for i in range(0, len(final_summary_payload), chunk_size):
                chunk = final_summary_payload[i:i+chunk_size]
                try:
                    supabase.table("positional_cycle_summary").upsert(chunk).execute()
                except Exception as e:
                    print(f"   ❌ Supabase Cycle Summary upsert failed: {e}")

    print(f"\n{'='*60}")
    print("🏁 LIVE POSITIONAL CURVE REBUILD COMPLETE.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    run_live_positional_curve_rebuilder()
