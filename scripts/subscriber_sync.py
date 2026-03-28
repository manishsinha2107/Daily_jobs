import os
import asyncio
import re
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from playwright.async_api import async_playwright

# Load configuration
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TT_EMAIL = os.environ.get("TT_USER_EMAIL")
TT_PASSWORD = os.environ.get("TT_USER_PASSWORD")

MONTH_MAP = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}

def log(msg):
    print(f"DEBUG: {msg}", flush=True)

def update_heartbeat(status, msg):
    try:
        sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        sb.table("engine_heartbeat").upsert({
            "step_id": "sub_pnl_sync",
            "status": status,
            "last_msg": msg,
            "updated_at": "now()"
        }, on_conflict="step_id").execute()
    except Exception as e:
        log(f"⚠️ Heartbeat Update Failed: {e}")

async def run_subscriber_pnl_sync():
    update_heartbeat("running", "📡 Initializing Delta Sync...")
    
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. Fetch Active Strategies and their Last Synced Counter
    # We join the ledger with our new View to get the 'Frontier'
    ledger_res = (
        supabase.table("strategy_ledger")
        .select("strategy_id, user_email")
        .eq("sub_status", "Active")
        .execute()
    )
    
    sync_res = supabase.table("latest_strategy_sync").select("*").execute()
    
    if not ledger_res.data:
        update_heartbeat("success", "🏁 No active subscribers found.")
        return

    # Mappings
    sid_to_user = {str(row['strategy_id']): row['user_email'] for row in ledger_res.data}
    # { 'SID': last_counter_int }
    last_counters = {str(row['strategy_id']): int(row['last_synced_counter']) for row in sync_res.data}
    
    target_sids = list(sid_to_user.keys())
    log(f"✅ Delta Sync: Tracking {len(target_sids)} strategies.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(viewport={'width': 1280, 'height': 720})
        page = await context.new_page()

        try:
            log(f"🔑 Logging into Tradetron...")
            await page.goto("https://tradetron.tech/deployed-strategies", wait_until="load", timeout=90000)
            
            login_area = page.locator('#main')
            await login_area.locator('input[name="email"]').fill(TT_EMAIL)
            await login_area.locator('input[name="password"]').fill(TT_PASSWORD)
            await login_area.locator('button:has-text("Sign In")').click()
            await page.wait_for_selector('.strategy__filter-btn', timeout=60000)

            # Apply Filters
            await page.locator('a[data-target="#deployedFilterModal"]').first.click()
            await page.wait_for_selector('#deployedFilterModal', state="visible")
            await page.locator('#react-select-4-input').fill("LIVE AUTO")
            await page.keyboard.press("Enter")
            await page.locator('select#modalFilterSelect8').select_option("Shared with me")
            await page.locator('.modal-body-submit:has-text("Filter")').click()
            await asyncio.sleep(5) 

            cards = page.locator('.strategy__section.deployed__archived')
            card_count = await cards.count()

            for i in range(card_count):
                card = cards.nth(i)
                sid_badge = card.locator('a[data-tip^="SID"]')
                if await sid_badge.count() == 0: continue
                
                raw_sid_text = await sid_badge.first.get_attribute("data-tip")
                sid = re.search(r'\d+', raw_sid_text).group()

                if sid not in target_sids: continue

                # Get current "Max Counter" in DB for this SID (default to 0 if new)
                db_max_counter = last_counters.get(sid, 0)
                strat_name = await card.locator('.deployed__archived-head a').first.inner_text()
                
                # Check year for date formatting
                deployed_info = await card.locator('.deployed__archived-info').inner_text()
                year_match = re.search(r'20\d{2}', deployed_info)
                deployed_year = year_match.group() if year_match else str(datetime.now().year)

                counter_select = card.locator(f'select#run_counter_{sid}')
                options = await counter_select.locator('option').all()
                
                new_data_found = False
                for opt in options:
                    val_str = await opt.get_attribute("value")
                    if val_str == "All": continue
                    
                    val_int = int(val_str)
                    
                    # DELTA LOGIC: Only process if the counter is higher than what we have
                    if val_int <= db_max_counter:
                        continue 
                    
                    new_data_found = True
                    txt = await opt.inner_text()
                    pnl_match = re.search(r'\((?:₹\s*)?([\d\s,.-]+)\)', txt)
                    pnl_value = float(pnl_match.group(1).replace(',', '').replace(' ', '')) if pnl_match else 0.0

                    await counter_select.select_option(val_str)
                    await asyncio.sleep(1.5)
                    
                    # Scrape Date from Modal
                    await card.locator('.deployed__archived-table a[data-target="#notificationLog"]').first.click()
                    await page.wait_for_selector('#notificationLog.show', state="visible")
                    
                    raw_date = await page.locator('#notificationLog td.no_wrap').first.inner_text()
                    day, month_str = raw_date.split()
                    formatted_date = f"{deployed_year}-{MONTH_MAP[month_str]}-{day.zfill(2)}"
                    
                    # Upsert to Supabase
                    payload = {
                        "strategy_id": int(sid),
                        "strategy_name": strat_name,
                        "trade_date": formatted_date,
                        "counter": val_int,
                        "pnl_value": pnl_value,
                        "user_email": sid_to_user.get(sid),
                        "tt_email_id": TT_EMAIL
                    }
                    
                    supabase.table("subscriber_daily_pnl").upsert(
                        payload, on_conflict="strategy_id, trade_date, counter"
                    ).execute()
                    
                    await page.locator('#notificationLog .modal__close').click()
                    await page.wait_for_selector('#notificationLog', state="hidden")
                    log(f"✅ Synced Counter {val_int} for {sid}")

                if not new_data_found:
                    log(f"⏭️ SID {sid}: No new counters. Already at {db_max_counter}.")

                supabase.table("strategy_ledger").update({"last_updated_at": "now()"}).eq("strategy_id", sid).execute()

            update_heartbeat("success", "✨ Delta Sync Finished.")

        except Exception as e:
            update_heartbeat("error", f"❌ Error: {str(e)[:100]}")
            log(f"❌ Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run_subscriber_pnl_sync())
