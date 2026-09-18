import sys
import os
import pandas as pd
import math
import json
from datetime import datetime, timezone
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
# HELPER: FETCH PAGINATED DATA
# =====================================================================
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

# =====================================================================
# HELPER: HISTORICAL FREEZE LIMIT LOOKUP
# =====================================================================
def build_lot_size_lookup(lot_data):
    lookup = {}
    for row in lot_data:
        idx = row['instrument']
        if idx not in lookup: lookup[idx] = []
        lookup[idx].append(row)
    for idx in lookup:
        lookup[idx].sort(key=lambda x: x['effective_date'], reverse=True)
    return lookup

def get_historical_freeze_limit(lookup, index_name, target_date_str):
    if index_name not in lookup or not lookup[index_name]: 
        raise ValueError(f"❌ Missing lot size/freeze limit data in DB for index {index_name}")
    
    target_dt_str = str(target_date_str).split(' ')[0]
    first_of_month = datetime.strptime(target_dt_str, "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")
    
    valid_lots = [lot for lot in lookup[index_name] if lot['effective_date'] <= first_of_month]
    if not valid_lots: 
        raise ValueError(f"❌ No valid historical freeze limit found for {index_name} on or before {first_of_month}")
        
    limit = valid_lots[0].get('freeze_limit')
    if not limit or int(limit) <= 0:
        raise ValueError(f"❌ Invalid or missing freeze limit in DB for {index_name} on or before {first_of_month}")
        
    return int(limit)

# =====================================================================
# LIVE POSITIONAL LEDGER BUILDER
# =====================================================================
def run_live_positional_ledger():
    print(f"\n{'='*60}")
    print("🚀 INITIATING LIVE POSITIONAL TRADE LEDGER ENGINE")
    print(f"{'='*60}\n")

    print("📡 Fetching strategy metadata & lot sizes from Supabase...")
    strat_res = supabase.table("strategies").select("strategy_id, index_name, position_type, deployment_type") \
        .eq("position_type", "Positional") \
        .in_("deployment_type", config.DEPLOYMENT_TYPES) \
        .execute()
    
    if not strat_res.data:
        print("✅ No active Live Positional strategies found in Supabase. Exiting.")
        return
        
    strategy_lookup = {int(row['strategy_id']): row['index_name'] for row in strat_res.data}
    valid_strat_ids = list(strategy_lookup.keys())
    
    lot_data = fetch_all_paginated("lot_sizes")
    lot_lookup = build_lot_size_lookup(lot_data)

    print("📡 Querying verified positional trade logs from Supabase...")
    all_trades = []
    offset, limit = 0, 1000
    while True:
        res = supabase.table("strategy_trades_verification") \
            .select("strategy_id, trade_date, broker_symbol, txn_time, txn_type, quantity, price") \
            .in_("strategy_id", valid_strat_ids) \
            .eq("ohlc_status", "verified_ohlc_present") \
            .order("txn_time") \
            .range(offset, offset + limit - 1) \
            .execute()
        
        if not res.data: break
        all_trades.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
    
    if not all_trades:
        print("✅ No verified positional trade logs exist for active strategies. Exiting.")
        return

    raw_trades = pd.DataFrame(all_trades)
    raw_trades['txn_time'] = pd.to_datetime(raw_trades['txn_time'], format='mixed').apply(lambda x: x.replace(tzinfo=None))
    
    all_cycles = []
    
    for strat_id, strat_trades in raw_trades.groupby('strategy_id'):
        print(f"🔍 Sweeping fills for Strategy ID: {strat_id}...")
        
        index_name = strategy_lookup.get(strat_id, 'NIFTY')
        
        inventory = {}
        realized_pnl = 0.0
        
        cycle_start_time = None
        buy_fills = 0
        sell_fills = 0
        order_count = 0
        premium_turnover = 0.0
        base_qtys = []
        
        strat_trades = strat_trades.sort_values(by='txn_time')
        
        for _, txn in strat_trades.iterrows():
            sym = txn['broker_symbol']
            t_price = float(txn['price'])
            t_qty = int(abs(txn['quantity']))
            t_type = txn['txn_type']
            t_time = txn['txn_time']
            
            if not cycle_start_time:
                cycle_start_time = t_time
                
            buy_fills += 1 if t_type == 'B' else 0
            sell_fills += 1 if t_type == 'S' else 0
            premium_turnover += (t_price * t_qty)
            base_qtys.append(t_qty)
            
            freeze_limit = get_historical_freeze_limit(lot_lookup, index_name, t_time)
            order_count += math.ceil(t_qty / freeze_limit)

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
                        excess_qty = t_qty - inv['qty']
                        pnl_mult = 1 if inv['side'] == 'LONG' else -1
                        realized_pnl += (t_price - inv['avg_price']) * inv['qty'] * pnl_mult
                        
                        inv['side'] = 'SHORT' if inv['side'] == 'LONG' else 'LONG'
                        inv['qty'] = excess_qty
                        inv['avg_price'] = t_price
                    else:
                        pnl_mult = 1 if inv['side'] == 'LONG' else -1
                        realized_pnl += (t_price - inv['avg_price']) * t_qty * pnl_mult
                        inv['qty'] -= t_qty

            if all(v['qty'] == 0 for k, v in inventory.items()):
                exit_time = t_time
                duration = (exit_time.date() - cycle_start_time.date()).days
                
                cycle_id = f"CYC-{strat_id}-{cycle_start_time.strftime('%Y%m%d%H%M')}-{exit_time.strftime('%Y%m%d%H%M')}"
                
                cycle_record = {
                    'cycle_id': cycle_id,
                    'strategy_id': strat_id,
                    'entry_date': str(cycle_start_time.date()),
                    'exit_date': str(exit_time.date()),
                    'entry_time': str(cycle_start_time),
                    'exit_time': str(exit_time),
                    'duration_days': duration,
                    'gross_pnl': round(realized_pnl, 2),
                    'buy_fills': buy_fills,
                    'sell_fills': sell_fills,
                    'order_count': order_count,
                    'premium_turnover': round(premium_turnover, 2),
                    'base_qtys': json.dumps(base_qtys),
                    'updated_at': datetime.now(timezone.utc).isoformat()
                }
                all_cycles.append(cycle_record)
                
                inventory = {}
                realized_pnl = 0.0
                cycle_start_time = None
                buy_fills = 0
                sell_fills = 0
                order_count = 0
                premium_turnover = 0.0
                base_qtys = []

    if all_cycles:
        print(f"📤 Pushing {len(all_cycles)} completed positional cycles to database...")
        
        chunk_size = 250
        upsert_errors = 0
        
        for i in range(0, len(all_cycles), chunk_size):
            chunk = all_cycles[i:i + chunk_size]
            try:
                supabase.table("positional_trade_ledger").upsert(chunk).execute()
            except Exception as e:
                print(f"   ❌ Supabase upsert failed for chunk {i}: {e}")
                upsert_errors += 1
                
        if upsert_errors == 0:
            print(f"✅ SUCCESS: {len(all_cycles)} cycles pushed to Supabase cloud.")
    else:
        print("⚠️ No fully closed positional cycles found.")

    print(f"\n{'='*60}")
    print("🏁 LIVE POSITIONAL LEDGER BUILD COMPLETE.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    run_live_positional_ledger()
