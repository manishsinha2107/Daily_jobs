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
    Translates Shoonya: NIFTY26APR07C18100 -> Fyers: NSE:NIFTY2640718100CE
    Or Shoonya: NIFTY26JUN29P38000 -> Fyers: NSE:NIFTY2662938000PE
    """
    # Regex to extract parts: (SYMBOL)(YY)(MMM)(DD)(C/P)(STRIKE)
    match = re.match(r'^([A-Z]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)$', legacy_sym)
    
    if not match:
        return f"UNMATCHED: {legacy_sym}"
        
    symbol, yy, mmm, dd, opt_type, strike = match.groups()
    
    fyers_month = FYERS_MONTH_MAP.get(mmm.upper(), '')
    fyers_opt = 'CE' if opt_type == 'C' else 'PE'
    
    return f"NSE:{symbol}{yy}{fyers_month}{dd}{strike}{fyers_opt}"

def run_preview():
    print("🔍 Fetching unique symbols from Market OHLC Cache...")
    
    # We fetch a chunk of records just to get a good sample
    res = supabase.table("market_ohlc_cache").select("symbol").limit(5000).execute()
    
    if not res.data:
        print("❌ No data found.")
        return
        
    unique_symbols = list(set([row['symbol'] for row in res.data]))
    
    print("\n✅ MIGRATION DRY RUN PREVIEW:")
    print("-" * 65)
    print(f"{'LEGACY (Shoonya)':<25} | {'NEW NATIVE (Fyers)':<30}")
    print("-" * 65)
    
    # Print the first 20 unique transformations
    for sym in unique_symbols[:20]:
        new_sym = translate_to_fyers(sym)
        print(f"{sym:<25} | {new_sym:<30}")
        
    print("-" * 65)
    print(f"Total Unique Symbols Found: {len(unique_symbols)}")

if __name__ == "__main__":
    run_preview()
