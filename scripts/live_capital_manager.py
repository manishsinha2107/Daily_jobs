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

def get_historical_lot_size(lookup, index_name, target_date_str):
    # [TECHNICAL]: Stripped the flawed `.replace(day=1)` to enforce exact point-in-time date evaluation.
    # [TRADING DOMAIN]: Instantly resolves mid-month exchange regime hallucinations.
    if index_name not in lookup or not lookup[index_name]: return 1 
    target_dt_str = str(target_date_str).split(' ')[0]
    valid_lots = [lot for lot in lookup[index_name] if lot['effective_date'] <= target_dt_str]
    if not valid_lots: return lookup[index_name][-1]['lot_size'] 
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
