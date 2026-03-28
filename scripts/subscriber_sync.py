import os
import asyncio
import re
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from playwright.async_api import async_playwright

# Load configuration from GitHub Secrets / .env
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TT_EMAIL = os.environ.get("TT_USER_EMAIL")
TT_PASSWORD = os.environ.get("TT_USER_PASSWORD")

# Month mapping for Tradetron date parsing
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
    update_heartbeat("running", "📡 Initializing Subscriber PnL Sync...")
    
    # 1. Fetch active SIDs and their corresponding mappings from ledger
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    ledger_res = (
        supabase.table("strategy_ledger")
        .select("strategy_id, user_email, tt_email_id")
        .eq("sub_status", "Active")
        .execute()
    )
    
    if not ledger_res.data:
        update_heartbeat("success", "🏁 No active subscribers in ledger.")
        return

    # Fetch latest synced counters from the View to enable Delta Sync
    sync_res = supabase.table("latest_strategy_sync").select("*").execute()
    last_counters = {str(row['strategy_id']): int(row['last_synced_counter']) for row in sync_res.data}

    # Create a nested mapping: { 'SID': {'user': '...', 'tt': '...'} }
    sid_map = {
        str(row['strategy_id']): {
            'user': row['user_email'], 
            'tt': row['tt_email_id']
        } for row in ledger_res.data
    }
    target_sids = list(sid_map.keys())
    log(f"✅ Found {len(target_sids)} active strategies. Delta Sync + Capital Scraping enabled.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(viewport={'width': 1280, 'height': 720})
        page = await context.new_page()

        try:
            # 2. Login to Tradetron (Your working logic preserved)
            log(f"🔑 Logging into Tradetron as {TT_EMAIL}...")
            await page.goto("https://tradetron.tech/deployed-strategies", wait_until="load", timeout=90000)
            
            login_area = page.locator('#main')
            await login_area.locator('input[name="email"]').fill(TT_EMAIL)
            await login_area.locator('input[name="password"]').fill(TT_PASSWORD)

            altcha = login_area.locator('altcha-widget')
            if await altcha.is_visible():
                await altcha.locator('.altcha-checkbox').click()
                await altcha.locator('text=Verified').wait_for(state="visible", timeout=30000)

            await login_area.locator('button:has-text("Sign In")').click()
            await page.wait_for_selector('.strategy__filter-btn', timeout=60000)

            # Close Invoice Alert if present
            invoice_alert = page.locator('.alert__box .alert__close')
            if await invoice_alert.is_visible():
                await invoice_alert.click()
                log("🚫 Closed invoice alert overlay.")

            # 3. Apply Filters
            log("⚡ Applying Filters: LIVE AUTO & Shared with me...")
            await page.locator('a[data-target="#deployedFilterModal"]').first.click()
            await page.wait_for_selector('#deployedFilterModal', state="visible")

            await page.locator('#react-select-4-input').fill("LIVE AUTO")
            await page.keyboard.press("Enter")
            await page.locator('select#modalFilterSelect8').select_option("Shared with me")
            await page.locator('.modal-body-submit:has-text("Filter")').click()
            await asyncio.sleep(5) 

            # 4. Process Strategy Cards
            cards = page.locator('.strategy__section.deployed__archived')
            card_count = await cards.count()
            log(f"📋 Visible cards after filtering: {card_count}")

            for i in range(card_count):
                card = cards.nth(i)
                
                sid_badge = card.locator('a[data-tip^="SID"]')
                if await sid_badge.count() == 0: continue
                
                raw_sid_text = await sid_badge.first.get_attribute("data-tip")
                sid = re.search(r'\d+', raw_sid_text).group() if re.search(r'\d+', raw_sid_text) else None

                if not sid or sid not in target_sids: continue

                db_max_counter = last_counters.get(sid, 0)
                strat_name = await card.locator('.deployed__archived-head a').first.inner_text()
                
                # --- NEW CAPITAL SCRAPING LOGIC ---
                # Scrape Capital (e.g., "₹ 65.00 k" or "₹ 1.30 L")
                cap_element = card.locator('.deployed_archived-info p:has-text("Capital:") span')
                raw_cap_text = await cap_element.inner_text() if await cap_element.count() > 0 else "0"
                
                # Parse value and multiplier
                clean_cap = re.sub(r'[^\d\.kKLl]', '', raw_cap_text)
                cap_value = float(re.search(r'[\d\.]+', clean_cap).group()) if re.search(r'[\d\.]+', clean_cap) else 0.0
                
                if 'k' in clean_cap.lower():
                    cap_value *= 1000
                elif 'l' in clean_cap.lower():
                    cap_value *= 100000
                
                log(f"🎯 Strategy: {strat_name} (SID: {sid}) | Cap: {cap_value} | DB Max: {db_max_counter}")
                # ----------------------------------

                deployed_info = await card.locator('.deployed__archived-info').inner_text()
                year_match = re.search(r'20\d{2}', deployed_info)
                deployed_year = year_match.group() if year_match else str(datetime.now().year)

                counter_select = card.locator(f'select#run_counter_{sid}')
                options = await counter_select.locator('option').all()
                
                processed_count = 0
                for opt in options:
                    val_str = await opt.get_attribute("value")
                    if val_str == "All": continue
                    val_int = int(val_str)
                    
                    if val_int <= db_max_counter: continue
                    
                    processed_count += 1
                    txt = await opt.inner_text()
                    pnl_match = re.search(r'\((?:₹\s*)?([\d\s,.-]+)\)', txt)
                    pnl_value = float(pnl_match.group(1).replace(',', '').replace(' ', '')) if pnl_match else 0.0

                    await counter_select.select_option(val_str)
                    await asyncio.sleep(2)
                    
                    await card.locator('.deployed__archived-table a[data-target="#notificationLog"]').first.click()
                    await page.wait_for_selector('#notificationLog.show', state="visible")
                    
                    raw_date = await page.locator('#notificationLog td.no_wrap').first.inner_text()
                    day, month_str = raw_date.split()
                    formatted_date = f"{deployed_year}-{MONTH_MAP[month_str]}-{day.zfill(2)}"
                    
                    mapping = sid_map.get(sid, {})
                    payload = {
                        "strategy_id": int(sid),
                        "strategy_name": strat_name,
                        "trade_date": formatted_date,
                        "counter": val_int,
                        "pnl_value": pnl_value,
                        "capital_at_sync": cap_value, # New Field
                        "user_email": mapping.get('user'),
                        "tt_email_id": mapping.get('tt')
                    }
                    
                    supabase.table("subscriber_daily_pnl").upsert(
                        payload, on_conflict="strategy_id, trade_date, counter"
                    ).execute()
                    
                    await page.locator('#notificationLog .modal__close').click()
                    await page.wait_for_selector('#notificationLog', state="hidden")

                if processed_count > 0:
                    log(f"✅ Synced {processed_count} new counters for {sid}")
                else:
                    log(f"⏭️ No new data for {sid}")

                supabase.table("strategy_ledger").update({"last_updated_at": "now()"}).eq("strategy_id", sid).execute()

            update_heartbeat("success", f"✨ Successfully synced {len(target_sids)} strategies.")

        except Exception as e:
            update_heartbeat("error", f"❌ Error: {str(e)[:100]}")
            log(f"❌ Critical Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run_subscriber_pnl_sync())
