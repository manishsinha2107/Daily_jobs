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
    if index_name not in lookup or not lookup[index_name]:
        raise ValueError(f"❌ Missing lot size data in DB for index {index_name}")
    target_dt = datetime.strptime(str(target_date_str).split(' ')[0], "%Y-%m-%d")
    first_of_month = target_dt.replace(day=1).strftime("%Y-%m-%d")
    valid_lots = [lot for lot in lookup[index_name] if lot['effective_date'] <= first_of_month]
    if not valid_lots:
        raise ValueError(f"❌ No valid historical lot size found for {index_name} on or before {first_of_month}")
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
def extract_memory_extremes(strat_id, entry_time, exit_time):
    """Calculates Max Profit, Max Loss, and dynamic daily extremes using Live Supabase tables."""
    # 1. Paginated fetch of exact legs executed during this specific cycle
    all_trades = []
    offset, limit = 0, 1000
    while True:
        res = supabase.table("strategy_trades_verification") \
            .select("broker_symbol, txn_time, txn_type, quantity, price") \
            .eq("strategy_id", strat_id) \
            .eq("ohlc_status", "verified_ohlc_present") \
            .gte("txn_time", entry_time) \
            .lte("txn_time", exit_time) \
            .order("txn_time") \
            .range(offset, offset + limit - 1) \
            .execute()
        if not res.data: break
        all_trades.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
        
    trades_df = pd.DataFrame(all_trades)
    if trades_df.empty:
        return 0.0, str(exit_time), 0.0, str(entry_time), {}

    # Map broker_symbol to 'symbol' to align with the core mathematical loop
    trades_df = trades_df.rename(columns={'broker_symbol': 'symbol'})
    symbols = trades_df['symbol'].unique().tolist()

    # 2. Paginated fetch of the massive 1-minute OHLC block strictly for this cycle's duration
    all_ohlc = []
    offset = 0
    while True:
        res = supabase.table("market_ohlc_cache") \
            .select("symbol, ts, close") \
            .in_("symbol", symbols) \
            .gte("ts", entry_time) \
            .lte("ts", exit_time) \
            .order("ts") \
            .range(offset, offset + limit - 1) \
            .execute()
        if not res.data: break
        all_ohlc.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
        
    ohlc_df = pd.DataFrame(all_ohlc)
    if ohlc_df.empty:
        return 0.0, str(exit_time), 0.0, str(entry_time), {}

    # Align the time column name for the loop
    ohlc_df = ohlc_df.rename(columns={'ts': 'timestamp'})

    # 3. Simulate the timeline
    trades_df['txn_time'] = pd.to_datetime(trades_df['txn_time'])
    ohlc_df['timestamp'] = pd.to_datetime(ohlc_df['timestamp'])
    
    unique_times = sorted(ohlc_df['timestamp'].unique())
    trade_idx = 0
    total_trades = len(trades_df)
    
    inventory = {}
    realized_pnl = 0.0
    
    max_pnl = -float('inf')
    max_pnl_time = None
    min_pnl = float('inf')
    min_pnl_time = None
    
    daily_mtm_snapshots = {}

    # Step through every minute data point organically
    for current_ts in unique_times:
        date_str = current_ts.strftime('%Y-%m-%d')
        time_str = current_ts.strftime('%I:%M %p').lstrip('0')
        
        # Initialize daily trackers dynamically
        if date_str not in daily_mtm_snapshots:
            daily_mtm_snapshots[date_str] = {
                'eod_pnl': 0.0,
                'max_pnl': -float('inf'),
                'max_time': time_str,
                'min_pnl': float('inf'),
                'min_time': time_str
            }

        # Process any fills that occurred exactly at or just before this minute
        while trade_idx < total_trades and trades_df.iloc[trade_idx]['txn_time'] <= current_ts:
            txn = trades_df.iloc[trade_idx]
            sym = txn['symbol']
            t_price = float(txn['price'])
            t_qty = int(abs(txn['quantity']))
            t_type = txn['txn_type']

            if sym not in inventory or inventory[sym]['qty'] == 0:
                inventory[sym] = {'qty': t_qty, 'avg_price': t_price, 'side': 'LONG' if t_type == 'B' else 'SHORT'}
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
                    else:
                        mult = 1 if inv['side'] == 'LONG' else -1
                        realized_pnl += (t_price - inv['avg_price']) * t_qty * mult
                        inv['qty'] -= t_qty
            trade_idx += 1

        # Calculate Unrealized MTM using this minute's closing prices
        unrealized_pnl = 0.0
        current_closes = ohlc_df[ohlc_df['timestamp'] == current_ts].set_index('symbol')['close'].to_dict()
        
        for sym, inv in inventory.items():
            if inv['qty'] > 0 and sym in current_closes:
                mult = 1 if inv['side'] == 'LONG' else -1
                unrealized_pnl += (current_closes[sym] - inv['avg_price']) * inv['qty'] * mult

        total_live_pnl = realized_pnl + unrealized_pnl
        
        # Continuously overwrite EOD snapshot
        daily_mtm_snapshots[date_str]['eod_pnl'] = total_live_pnl
        
        # Track DAILY watermarks
        if total_live_pnl > daily_mtm_snapshots[date_str]['max_pnl']:
            daily_mtm_snapshots[date_str]['max_pnl'] = total_live_pnl
            daily_mtm_snapshots[date_str]['max_time'] = time_str
            
        if total_live_pnl < daily_mtm_snapshots[date_str]['min_pnl']:
            daily_mtm_snapshots[date_str]['min_pnl'] = total_live_pnl
            daily_mtm_snapshots[date_str]['min_time'] = time_str

        # Track CYCLE watermarks
        if total_live_pnl > max_pnl:
            max_pnl = total_live_pnl
            max_pnl_time = time_str
        if total_live_pnl < min_pnl:
            min_pnl = total_live_pnl
            min_pnl_time = time_str

    # Final sweep to catch flat days
    for d_str, stats in daily_mtm_snapshots.items():
        if stats['max_pnl'] == -float('inf'):
            stats['max_pnl'] = stats['eod_pnl']
        if stats['min_pnl'] == float('inf'):
            stats['min_pnl'] = stats['eod_pnl']

    return (
        round(max_pnl, 2) if max_pnl != -float('inf') else 0.0, 
        max_pnl_time, 
        round(min_pnl, 2) if min_pnl != float('inf') else 0.0, 
        min_pnl_time,
        daily_mtm_snapshots
    )

# =====================================================================
# LIVE CURVE REBUILDER
# =====================================================================
def run_live_positional_curve_rebuilder():
    print(f"\n{'='*60}")
    print("🔄 INITIATING LIVE POSITIONAL CURVE REBUILDER")
    print(f"{'='*60}\n")
    
    # 1. Fetch completed cycles directly from the live ledger table
    print("📦 Fetching live positional cycles from ledger...")
    cycles_data = fetch_all_paginated("positional_trade_ledger")
    
    if not cycles_data:
        print("✅ No closed live positional cycles found to process.")
        return
        
    cycles_df = pd.DataFrame(cycles_data)
    
    # Sort natively by strategy and exit time to ensure correct cumulative PnL progression
    cycles_df['exit_time_sort'] = pd.to_datetime(cycles_df['exit_time'])
    cycles_df = cycles_df.sort_values(['strategy_id', 'exit_time_sort']).drop(columns=['exit_time_sort'])

    # 2. Memory Load Lookups
    print("📦 Fetching metadata, lot sizes, and taxes from Supabase...")
    
    # Fetch valid live positional strategies based on config
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
            print(f"⚠️ ERROR: Strategy {strat_id} metadata missing or not authorized by config deployment rules. Skipping.")
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
            # Slice off any trailing timestamp zeroes
            entry_date_str = str(cycle['entry_date']).split(' ')[0]
            exit_date_str = str(cycle['exit_date']).split(' ')[0]
            
            # Extract precise broker execution times for the zero-fallback logic
            entry_dt_obj = pd.to_datetime(cycle['entry_time'])
            exit_dt_obj = pd.to_datetime(cycle['exit_time'])
            broker_entry_time = entry_dt_obj.strftime('%I:%M %p').lstrip('0')
            broker_exit_time = exit_dt_obj.strftime('%I:%M %p').lstrip('0')
            
            gross_pnl = float(cycle['gross_pnl'])
            turnover = float(cycle['premium_turnover'])
            order_count = int(cycle['order_count'])
            
            # Extract and parse base_qtys for dynamic splitting
            try:
                cycle_qtys = json.loads(cycle['base_qtys']) if isinstance(cycle['base_qtys'], str) else cycle['base_qtys']
                if not isinstance(cycle_qtys, list): 
                    cycle_qtys = []
            except Exception:
                cycle_qtys = []
            
            mid_qty_idx = len(cycle_qtys) // 2
            
            # --- STRICT HISTORICAL TAX CALCULATIONS ---
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
            
            # --- EXTRACT MEMORY EXTREMES & SNAPSHOTS ---
            max_profit, mp_time, max_loss, ml_time, daily_snapshots = extract_memory_extremes(
                strat_id, cycle['entry_time'], cycle['exit_time']
            )
            
            # --- DAILY MTM UNPACKING (UI Curve) ---
            valid_trading_dates = set(daily_snapshots.keys())
            valid_trading_dates.add(entry_date_str)
            valid_trading_dates.add(exit_date_str)
            sorted_dates = sorted(list(valid_trading_dates))
            
            prev_snap = 0.0
            
            for d_str in sorted_dates:
                day_stat = daily_snapshots.get(d_str)
                curr_dt = datetime.strptime(d_str, "%Y-%m-%d")
                
                # Retrieve EOD snapshot or final gross PnL
                if d_str == exit_date_str:
                    curr_snap = gross_pnl
                else:
                    curr_snap = day_stat['eod_pnl'] if day_stat else prev_snap
                    
                daily_gross = curr_snap - prev_snap
                
                # Calculate True Daily Extremes from 1-min data
                if day_stat:
                    daily_max = day_stat['max_pnl'] - prev_snap
                    daily_max_time = day_stat['max_time']
                    daily_min = day_stat['min_pnl'] - prev_snap
                    daily_min_time = day_stat['min_time']
                else:
                    # Safe broker-fill fallback if OHLC was fully missing for this specific date
                    daily_max = daily_gross
                    daily_min = daily_gross
                    if d_str == entry_date_str:
                        daily_max_time = broker_entry_time
                        daily_min_time = broker_entry_time
                    else:
                        daily_max_time = broker_exit_time
                        daily_min_time = broker_exit_time
                
                # --- PRECISE TRADE AND COST ALLOCATION FOR UI MATRICES ---
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
                
                # Capital calculation for daily row
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
                    # Excluded overlap_live_pnl and overlap_slippage_amount to match schema
                })
                
                prev_snap = curr_snap

            # --- CYCLE SUMMARY UNPACKING (Journal Curve) ---
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

        # --- SUPABASE UPSERT (Batched) ---
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
