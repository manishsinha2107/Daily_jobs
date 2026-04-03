import os
import re
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment
load_dotenv()
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Fyers Single-Character Month Map for Weeklies
FYERS_MONTH_MAP = {
    'JAN': '1', 'FEB': '2', 'MAR': '3', 'APR': '4',
    'MAY': '5', 'JUN': '6', 'JUL': '7', 'AUG': '8',
    'SEP': '9', 'OCT': 'O', 'NOV': 'N', 'DEC': 'D'
}

def translate_to_fyers(legacy_sym):
    """
    Translates Shoonya: NIFTY17FEB26P24700 -> Fyers: NSE:NIFTY2621724700PE
    """
    # Group 2 = DD, Group 3 = MMM, Group 4 = YY
    match = re.match(r'^([A-Z]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)$', legacy_sym)
    if not match: 
        return None
        
    symbol, dd, mmm, yy, opt_type, strike = match.groups()
    
    fyers_month = FYERS_MONTH_MAP.get(mmm.upper(), '')
    fyers_opt = 'CE' if opt_type == 'C' else 'PE'
    
    # Correct Fyers Order: YY + M + DD
    return f"NSE:{symbol}{yy}{fyers_month}{dd}{strike}{fyers_opt}"

def get_all_unique_symbols(table_name, column_name):
    """Fetches all unique legacy symbols, bypassing the 1000-row limit."""
    unique_syms = set()
    limit = 1000
    offset = 0
    while True:
        res = supabase.table(table_name).select(column_name).range(offset, offset + limit - 1).execute()
        if not res.data: break
        for r in res.data:
            sym = r.get(column_name)
            # Only grab symbols that haven't been migrated to NSE: yet
            if sym and not str(sym).startswith("NSE:"):
                unique_syms.add(sym)
        if len(res.data) < limit: break
        offset += limit
    return list(unique_syms)

def run_migration():
    print("🚀 Starting Full Database Migration to Fyers Native Format...")
    
    # --- 1. UPDATE VERIFICATION TABLE ---
    print("\n📊 Scanning strategy_trades_verification...")
    unique_ver = get_all_unique_symbols("strategy_trades_verification", "broker_symbol")
    
    if not unique_ver:
        print("✅ No legacy symbols found in verification table.")
    else:
        for old_sym in unique_ver:
            new_sym = translate_to_fyers(old_sym)
            if new_sym:
                print(f"🔄 Migrating Trades: {old_sym} -> {new_sym}")
                supabase.table("strategy_trades_verification").update({"broker_symbol": new_sym}).eq("broker_symbol", old_sym).execute()

    # --- 2. UPDATE OHLC CACHE TABLE ---
    print("\n📈 Scanning market_ohlc_cache...")
    unique_ohlc = get_all_unique_symbols("market_ohlc_cache", "symbol")
    
    if not unique_ohlc:
        print("✅ No legacy symbols found in OHLC cache.")
    else:
        for old_sym in unique_ohlc:
            new_sym = translate_to_fyers(old_sym)
            if new_sym:
                print(f"🔄 Migrating OHLC Data: {old_sym} -> {new_sym}")
                supabase.table("market_ohlc_cache").update({"symbol": new_sym}).eq("symbol", old_sym).execute()
                
    print("\n🏆 MIGRATION COMPLETE. Your entire database is now 100% Fyers Native.")

if __name__ == "__main__":
    run_migration()
