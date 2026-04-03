import pandas as pd

print("📥 Fetching the official Fyers NSE F&O Master Database...")
url = "https://public.fyers.in/sym_details/NSE_FO.csv"

# Load the Fyers CSV (It has no headers, so we use indices)
df = pd.read_csv(url, header=None)

# Fyers CSV Schema:
# Index 1 = Human Readable Name (e.g., NIFTY 10 APR 22500 CE)
# Index 9 = SymbolTicker / API Format (e.g., NSE:NIFTY2441022500CE)
# Index 13 = Underlying Asset (e.g., NIFTY)

# Filter for NIFTY Options
nifty_options = df[
    (df[13] == 'NIFTY') & 
    (df[9].str.endswith('CE') | df[9].str.endswith('PE'))
]

print("\n✅ EXACT FYERS FORMATS (Direct from their servers today):")
print("-" * 80)
print(f"{'Human Readable Name':<30} | {'Fyers API Ticker (What we need)':<40}")
print("-" * 80)

# Print the first 10 results to see the pattern
for _, row in nifty_options.head(10).iterrows():
    print(f"{str(row[1]):<30} | {str(row[9]):<40}")

print("-" * 80)
