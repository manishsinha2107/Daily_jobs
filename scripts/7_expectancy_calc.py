import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client

# Authentication
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_all_paginated(table_name):
    print(f"📡 Fetching data from: {table_name}...")
    all_data = []
    offset = 0
    limit = 1000
    while True:
        res = supabase.table(table_name).select("*").range(offset, offset + limit - 1).execute()
        data = res.data
        all_data.extend(data)
        if len(data) < limit: break
        offset += limit
    return pd.DataFrame(all_data)

def report_heartbeat(status, msg):
    try:
        url = f"{SUPABASE_URL}/rest/v1/engine_heartbeat?step_id=eq.step7"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        data = {"status": status, "last_msg": msg, "updated_at": datetime.utcnow().isoformat()}
        import requests
        requests.patch(url, headers=headers, json=data, timeout=10)
    except Exception as e:
        print(f"⚠️ Heartbeat Log Failed: {e}")

def downsample_series(series, count):
    if len(series) <= count: return series.tolist()
    indices = np.linspace(0, len(series) - 1, count).astype(int)
    return series.iloc[indices].tolist()

def run_expectancy_calc():
    print("🚀 INITIALIZING STEP 7: EXPECTANCY UI REFRESH (Live Auto Only)")
    report_heartbeat("running", "📊 Computing Live Auto Stats...")

    # 1. Fetching Data
    df_strategies = fetch_all_paginated("strategies")
    df_daily = fetch_all_paginated("daily_strategy_pnl") 
    
    # 2. Strict Filtering: Active AND Live Auto
    eligible_strats = df_strategies[
        (df_strategies['status'] == 'Active') & 
        (df_strategies['deployment_type'] == 'Live Auto')
    ]
    
    print(f"🔍 Filtered {len(eligible_strats)} 'Active & Live Auto' strategies from {len(df_strategies)} total.")

    all_expectancy_payloads = []
    now_iso = datetime.utcnow().isoformat()

    for _, strat in eligible_strats.iterrows():
        sid = int(strat['strategy_id'])
        sname = str(strat['strategy_name'])
        
        # Filter daily rows for this specific ID from the UI P&L table
        s_df = df_daily[df_daily['strategy_id'] == sid].sort_values('trade_date').reset_index(drop=True)
        
        if s_df.empty:
            print(f"⚠️  Skipping {sname} ({sid}): No entries in daily_strategy_pnl.")
            continue

        # Data Extraction
        pnls = s_df['pnl'].astype(float).values
        eff_cap_series = s_df['eff_capital'].astype(float).values
        cum_series = s_df['cumulative_pnl'].astype(float) 
        current_capital = eff_cap_series[-1] 

        # 3. Core Math (Non-Capital Dependent)
        wins = pnls[pnls > 0]
        losses = np.abs(pnls[pnls < 0])
        nonzero_pnls = pnls[pnls != 0]
        nonzero_count = len(nonzero_pnls)
        
        win_rate = round(float(len(wins) / nonzero_count), 6) if nonzero_count > 0 else 0.0
        loss_rate = round(float(len(losses) / nonzero_count), 6) if nonzero_count > 0 else 0.0
        avg_gain = round(float(np.mean(wins)), 6) if len(wins) > 0 else 0.0
        avg_loss = round(float(np.mean(losses)), 6) if len(losses) > 0 else 0.0
        rr_ratio = round(float(avg_gain / avg_loss), 6) if avg_loss > 0 else 0.0
        
        exp_per_day = (win_rate * avg_gain) - (loss_rate * avg_loss)
        monthly_exp = round(float(exp_per_day * 22), 6) 
        monthly_exp_pct = round(float(monthly_exp / current_capital), 6) if current_capital > 0 else 0.0
        total_ret_pct = round(float(cum_series.iloc[-1] / current_capital), 6) if current_capital > 0 else 0.0
        
        days_count = len(s_df)
        years = max(days_count / 252.0, 0.001) # Avoid div by zero
        cagr = round(float((1 + total_ret_pct)**(1/years) - 1), 6) if (1 + total_ret_pct) > 0 else 0.0

        # 4. Drawdowns
        peaks = cum_series.cummax()
        drawdowns = peaks - cum_series
        max_dd = round(float(drawdowns.max()), 2)
        max_dd_percent = round(float(max_dd / current_capital), 6) if current_capital > 0 else 0.0
        
        # Duration calc
        duration = 0
        if max_dd > 0:
            trough_idx = drawdowns.idxmax()
            peak_val = peaks.iloc[trough_idx]
            post_trough = cum_series.iloc[trough_idx:]
            recovery = post_trough[post_trough >= peak_val]
            peak_idx = cum_series.iloc[:trough_idx][cum_series == peak_val].index[-1]
            duration = int(recovery.index[0] - peak_idx) if not recovery.empty else int((len(s_df) - 1) - peak_idx)

        # 5. DYNAMIC ROI MATH (Correcting for Lot Size/Capital shifts)
        daily_rets = pnls / eff_cap_series
        vol = round(float(np.std(daily_rets, ddof=1) * np.sqrt(252)), 6) if len(daily_rets) > 1 else 0.0
        sharpe = round(float(cagr / vol), 6) if vol > 0 else 0.0
        
        downside_rets = daily_rets[daily_rets < 0]
        down_vol = round(float(np.std(downside_rets, ddof=1) * np.sqrt(252)), 6) if len(downside_rets) > 1 else 0.0
        sortino = round(float(cagr / down_vol), 6) if down_vol > 0 else 0.0
        calmar = round(float(cagr / max_dd_percent), 6) if max_dd_percent > 0 else 0.0

        # 6. Sparkline & Monthly PnL
        spark_data = downsample_series(cum_series / eff_cap_series, 25)
        spark_json = [round(float(x * 100), 6) for x in spark_data]

        s_df_monthly = s_df.copy()
        s_df_monthly['trade_date'] = pd.to_datetime(s_df_monthly['trade_date'])
        monthly_pnl = s_df_monthly.set_index('trade_date').resample('ME')['pnl'].sum()
        m_pnl_json = [{"month": k.strftime('%Y-%m'), "pnl": round(float(v), 2)} for k, v in monthly_pnl.items()]

        all_expectancy_payloads.append({
            "strategy_id": sid,
            "strategy_name": sname,
            "win_rate": win_rate,
            "loss_rate": loss_rate,
            "average_gain": avg_gain,
            "average_loss": avg_loss,
            "risk_reward_ratio": rr_ratio,
            "monthly_expectancy": monthly_exp,
            "monthly_expectancy_percent": monthly_exp_pct,
            "max_dd": max_dd,
            "max_dd_percent": max_dd_percent,
            "last_calculated_at": now_iso,
            "trade_days_count": int(days_count),
            "first_trade_date": str(s_df['trade_date'].iloc[0]),
            "last_trade_date": str(s_df['trade_date'].iloc[-1]),
            "total_return_pct": total_ret_pct,
            "cagr_pct": cagr,
            "last30d_return_pct": round(float((cum_series.iloc[-1] - cum_series.iloc[-min(30, days_count)]) / current_capital), 6) if days_count > 0 else 0.0,
            "last90d_return_pct": round(float((cum_series.iloc[-1] - cum_series.iloc[-min(90, days_count)]) / current_capital), 6) if days_count > 0 else 0.0,
            "annual_volatility_pct": vol,
            "sharpe_ratio": sharpe,
            "calmar_ratio": calmar,
            "max_dd_duration_days": int(max(0, duration)),
            "sparkline_compact": spark_json,
            "positive_months_pct": round(float(len(monthly_pnl[monthly_pnl > 0]) / len(monthly_pnl)), 6) if not monthly_pnl.empty else 0.0,
            "monthly_pnl_json": m_pnl_json,
            "low_sample_flag": bool(days_count < 60),
            "sortino_ratio": sortino,
            "annual_downside_volatility_pct": down_vol,
            "strategy_capital": current_capital,
            "deployment_status": str(strat['status']),
            "deployment_type": str(strat['deployment_type'])
        })

    # 7. Upsert to Supabase
    if all_expectancy_payloads:
        print(f"📤 Uploading {len(all_expectancy_payloads)} records to expectancy...")
        try:
            supabase.table("expectancy").upsert(
                all_expectancy_payloads, 
                on_conflict="strategy_id"
            ).execute()
            print(f"✅ SUCCESS: {len(all_expectancy_payloads)} records written to expectancy.")
            report_heartbeat("success", f"Live Auto Sync Complete: {len(all_expectancy_payloads)} rows.")
        except Exception as e:
            print(f"❌ API Error: {e}")
            report_heartbeat("error", f"API Fail: {str(e)}")
    else:
        print("⚠️ No valid Live Auto payloads generated.")

if __name__ == "__main__":
    run_expectancy_calc()
