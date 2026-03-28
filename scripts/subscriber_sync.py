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
        sb.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": "now()"
        }).eq("step_id", "sub_pnl_sync").execute()
    except Exception as e:
        log(f"⚠️ Heartbeat Update Failed: {e}")

async def run_subscriber_pnl_sync():
    update_heartbeat("running", "📡 Initializing Subscriber PnL Sync...")
    
    # 1. Fetch active SIDs from strategy_ledger
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    ledger_res = supabase.table("strategy_ledger").select("strategy_id, strategy_name").eq("sub_status", "Active").execute()
    
    if not ledger_res.data:
        update_heartbeat("success", "🏁 No active subscribers in ledger.")
        return

    target_sids = [str(row['strategy_id']) for row in ledger_res.data]
    log(f"✅ Found {len(target_sids)} active strategies in ledger.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(viewport={'width': 1280, 'height': 720})
        page = await context.new_page()

        try:
            # 2. Login to Tradetron
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

            # Execution Filter (React-Select)
            await page.locator('#react-select-4-input').fill("LIVE AUTO")
            await page.keyboard.press("Enter")
            
            # Type Filter (Standard Select)
            await page.locator('select#modalFilterSelect8').select_option("Shared with me")
            
            # Submit Filter
            await page.locator('.modal-body-submit:has-text("Filter")').click()
            await asyncio.sleep(5) 

            # 4. Process Strategy Cards
            cards = page.locator('.strategy__section.deployed__archived')
            card_count = await cards.count()
            log(f"📋 Visible cards after filtering: {card_count}")

            for i in range(card_count):
                card = cards.nth(i)
                
                # FIX: Use attribute selector [data-tip^="SID"] to target the unique badge
                # This avoids the strict mode error by ignoring the text-only 'SID' link
                sid_badge = card.locator('a[data-tip^="SID"]')
                
                if await sid_badge.count() == 0:
                    log(f"⚠️ Card {i}: Could not find SID badge. Skipping.")
                    continue
                
                # Extract the attribute value (e.g., "SID: 28594262")
                raw_sid_text = await sid_badge.first.get_attribute("data-tip")
                sid_match = re.search(r'\d+', raw_sid_text)
                sid = sid_match.group() if sid_match else None

                if not sid or sid not in target_sids:
                    log(f"⏭️ Skipping SID {sid}: Not in ledger or sub_status not Active.")
                    continue

                # Get Strategy Name from the first anchor in the head section
                strat_name_element = card.locator('.deployed__archived-head a').first
                strat_name = await strat_name_element.inner_text()
                log(f"🎯 Processing Strategy: {strat_name} (SID: {sid})")
                
                # Capture Deployed Year
                deployed_info = await card.locator('.deployed__archived-info').inner_text()
                year_match = re.search(r'20\d{2}', deployed_info)
                deployed_year = year_match.group() if year_match else str(datetime.now().year)

                # 5. Iterate through Counters
                counter_select = card.locator(f'select#run_counter_{sid}')
                options = await counter_select.locator('option').all()
                
                for opt in options:
                    val = await opt.get_attribute("value")
                    txt = await opt.inner_text() # "5 (₹ 2,005)"
                    
                    if val == "All": continue
                    
                    # Clean PnL Value
                    pnl_match = re.search(r'\((?:₹\s*)?([\d\s,.-]+)\)', txt)
                    pnl_raw = pnl_match.group(1).replace(',', '').replace(' ', '') if pnl_match else "0"
                    pnl_value = float(pnl_raw)

                    # Select Counter and Open Modal
                    await counter_select.select_option(val)
                    await asyncio.sleep(2)
                    
                    # Click first instrument link to trigger modal
                    await card.locator('.deployed__archived-table a[data-target="#notificationLog"]').first.click()
                    await page.wait_for_selector('#notificationLog.show', state="visible")
                    
                    # Scrape Date from Modal
                    date_cell = page.locator('#notificationLog td.no_wrap').first
                    raw_date = await date_cell.inner_text() # "25 Mar"
                    
                    day, month_str = raw_date.split()
                    formatted_date = f"{deployed_year}-{MONTH_MAP[month_str]}-{day.zfill(2)}"
                    
                    # 6. Upsert to Supabase
                    payload = {
                        "strategy_id": int(sid),
                        "strategy_name": strat_name,
                        "trade_date": formatted_date,
                        "counter": int(val),
                        "pnl_value": pnl_value,
                        "tt_email_id": TT_EMAIL
                    }
                    
                    supabase.table("subscriber_daily_pnl").upsert(
                        payload, on_conflict="strategy_id, trade_date, counter"
                    ).execute()
                    
                    # Close Modal
                    await page.locator('#notificationLog .modal__close').click()
                    await page.wait_for_selector('#notificationLog', state="hidden")

                # Update Ledger last_updated_at
                supabase.table("strategy_ledger").update({"last_updated_at": "now()"}).eq("strategy_id", sid).execute()
                log(f"✅ Sync complete for {strat_name}")

            update_heartbeat("success", f"✨ Successfully synced {len(target_sids)} strategies.")

        except Exception as e:
            update_heartbeat("error", f"❌ Error: {str(e)[:50]}")
            log(f"❌ Critical Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run_subscriber_pnl_sync())
