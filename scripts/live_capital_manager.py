# --- AFTER (NEW FILE | File: live_capital_manager.py | Lines 1 to 53) ---
"""
# [MODULE/CLASS SSOT ROLE]: Centralized Live Capital & Regime Manager
# [FUNCTION CONTRACT & MATH]: Provides the Single Source of Truth for fetching historical lot sizes, taxes, and freeze limits across all live processors.
"""
from datetime import datetime

def build_lot_size_lookup(lot_data):
    lookup = {}
    for row in lot_data:
        idx = row['instrument']
        if idx not in lookup: lookup[idx] = []
        lookup[idx].append(row)
    for idx in lookup:
        lookup[idx].sort(key=lambda x: x['effective_date'], reverse=True)
    return lookup

# [MODULE/CLASS SSOT ROLE]: Point-in-Time Historical Lot Size & Physical Multiplier Authority (Law 1 SSOT).
# [FUNCTION CONTRACT & MATH]: Resolves historical lot size strictly on or before target_date_str and derives traded multiplier = max(1, round(max(|q|) / hist_lot)).
def get_historical_lot_size(lookup, index_name, target_date_str):
    # [TECHNICAL]: Fail loudly with ValueError if index_name is absent from lookup instead of returning magic fallback 1.
    # [BUSINESS / DOMAIN LOGIC]: Prevents silent capital and multiplier corruption if an index is missing from the lot_sizes table.
    if index_name not in lookup or not lookup[index_name]:
        raise ValueError(f"❌ Missing lot size data in DB for index {index_name}")
    # [TECHNICAL]: Strip timestamp suffix and filter sorted effective_date records <= target_dt_str.
    # [BUSINESS / DOMAIN LOGIC]: Applies the exact SEBI exchange lot size active on the trade date.
    target_dt_str = str(target_date_str).split(' ')[0]
    valid_lots = [lot for lot in lookup[index_name] if lot['effective_date'] <= target_dt_str]
    if not valid_lots:
        raise ValueError(f"❌ No historical lot size found for {index_name} on or before {target_dt_str}")
    lot_val = int(valid_lots[0]['lot_size'])
    if lot_val <= 0:
        raise ValueError(f"❌ Invalid non-positive lot size ({lot_val}) for {index_name} on {target_dt_str}")
    return lot_val

def resolve_traded_multiplier(base_qtys, hist_lot):
    # [TECHNICAL]: Parse base_qtys from JSON string or list and divide maximum absolute leg quantity by hist_lot.
    # [BUSINESS / DOMAIN LOGIC]: Automatically detects the account's live traded multiplier (e.g. 130 qty / 65 lot = 2x) directly from broker fills without needing a manual live_deployments table.
    if not hist_lot or int(hist_lot) <= 0:
        raise ValueError(f"❌ Invalid historical lot size ({hist_lot}) passed to resolve_traded_multiplier.")
    parsed_qtys = base_qtys
    if isinstance(base_qtys, str):
        import json
        parsed_qtys = json.loads(base_qtys) if base_qtys.strip() else []
    if not isinstance(parsed_qtys, list) or len(parsed_qtys) == 0:
        return 1
    max_qty = max(abs(int(float(q))) for q in parsed_qtys)
    if max_qty <= 0:
        return 1
    return max(1, int(round(max_qty / float(hist_lot))))

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
    if segment not in lookup or not lookup[segment]:
        raise ValueError(f"❌ Missing tax data in DB for segment {segment}")
    target_dt_str = str(target_date_str).split(' ')[0]
    valid_taxes = [t for t in lookup[segment] if t['effective_date'] <= target_dt_str]
    if not valid_taxes:
        return lookup[segment][-1] 
    return valid_taxes[0]

def build_freeze_limit_lookup(freeze_data):
    lookup = {}
    for row in freeze_data:
        idx = row['instrument']
        if idx not in lookup: lookup[idx] = []
        lookup[idx].append(row)
    for idx in lookup:
        lookup[idx].sort(key=lambda x: x['effective_date'], reverse=True)
    return lookup

def get_historical_freeze_limit(lookup, index_name, target_date_str):
    if index_name not in lookup or not lookup[index_name]: 
        raise ValueError(f"❌ Missing freeze limit data in DB for index {index_name}")
    target_dt_str = str(target_date_str).split(' ')[0]
    valid_limits = [limit for limit in lookup[index_name] if limit['effective_date'] <= target_dt_str]
    if not valid_limits: 
        raise ValueError(f"❌ No valid historical freeze limit found for {index_name} on or before {target_dt_str}")
    limit = valid_limits[0].get('freeze_limit')
    if not limit or int(limit) <= 0:
        raise ValueError(f"❌ Invalid or missing freeze limit in DB for {index_name} on or before {target_dt_str}")
    return int(limit)
