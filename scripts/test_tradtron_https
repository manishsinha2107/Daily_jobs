import os
import sys
import asyncio
import json
import io
import pandas as pd
from supabase import create_client, Client
from playwright.async_api import async_playwright

def log(msg):
    print(f"DEBUG_VALIDATION: {msg}", flush=True)

async def test_tradetron_filename():
    log("🚀 Starting Tradetron Filename Validation Test...")

    # 1. Fetch GitHub Secrets mapped to Environment Variables
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("❌ ERROR: Missing SUPABASE_URL or SUPABASE_KEY in environment.")
        sys.exit(1)

    # 2. Query target strategy details from Supabase
    log("📡 Querying target strategy from Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    target_strategy = "Vaidehi Nifty Intraday Option Selling"
    res = supabase.table("strategies") \
        .select("user_email, email_password, strategy_name, strategy_id") \
        .eq("strategy_name", target_strategy) \
        .execute()

    if not res.data:
        log(f"❌ ERROR: Target strategy '{target_strategy}' not found in database.")
        return

    row = res.data[0]
    email = row['user_email']
    password = row['email_password']
    db_strat_name = str(row['strategy_name']).strip()
    db_strat_id = str(row['strategy_id']).strip()

    log(f"📋 DB Reference -> ID: {db_strat_id} | Name: '{db_strat_name}' | Email: {email}")

    # 3. Launch Playwright to capture the download filename
    async with async_playwright() as p:
        log("🌐 Launching Chromium browser...")
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        context = await browser.new_context(viewport={'width': 1280, 'height': 720})
        page = await context.new_page()

        try:
            log("🔑 Navigating to Tradetron Login...")
            await page.goto("https://tradetron.tech/deployed-strategies", wait_until="load", timeout=90000)

            login_area = page.locator('#main')
            await login_area.locator('input[name="email"]').fill(email)
            await login_area.locator('input[name="password"]').fill(password)

            altcha = login_area.locator('altcha-widget')
            if await altcha.is_visible():
                log("🧩 Altcha widget detected. Waiting for verification...")
                await altcha.locator('.altcha-checkbox').click()
                await altcha.locator('text=Verified').wait_for(state="visible", timeout=30000)

            await login_area.locator('button:has-text("Sign In")').click()
            await page.wait_for_selector('#search_input', timeout=60000)
            log("🔒 Login Successful.")

            log(f"🔍 Searching for strategy: {db_strat_name}")
            await page.locator('#search_input').fill("")
            await page.locator('#search_input').type(db_strat_name, delay=50)
            await asyncio.sleep(5)

            container = page.locator(f"div.strategy__section:has(a:text-is('{db_strat_name}'))").first

            if await container.count() > 0:
                log("🎯 Strategy container located on UI. Triggering download context...")
                await container.locator('button[id*="More"]').click()

                async with page.expect_download() as download_info:
                    await page.locator('a:has-text("Download Data")').click()

                download = await download_info.value
                server_filename = download.suggested_filename
                log(f"📥 Server Download Caught! Raw Suggested Filename: '{server_filename}'")

                # 4. Perform String and Mismatch Analysis
                clean_server_name = os.path.splitext(server_filename)[0]
                
                print("\n" + "="*50)
                print("📊 FILENAME VALIDATION ANALYSIS")
                print("="*50)
                print(f"Database Strategy Name : '{db_strat_name}'")
                print(f"Tradetron Filename     : '{clean_server_name}'")
                print(f"File Extension Detected: '{os.path.splitext(server_filename)[1]}'")
                
                if db_strat_name == clean_server_name:
                    print("✅ MATCH SUCCESS: Tradetron's suggested name perfectly matches the database string.")
                else:
                    print("⚠️ MISMATCH DETECTED!")
                    print(f"Length - DB: {len(db_strat_name)} characters | Server: {len(clean_server_name)} characters")
                    
                    # Character conversion analysis
                    has_spaces_in_db = " " in db_strat_name
                    has_underscores_in_server = "_" in clean_server_name
                    print(f"Does DB name contain spaces?: {has_spaces_in_db}")
                    print(f"Does Server name use underscores instead?: {has_underscores_in_server}")
                print("="*50 + "\n")

            else:
                log(f"❓ ERROR: Strategy '{db_strat_name}' not found on Tradetron deployed dashboard.")

        except Exception as e:
            log(f"❌ Error during execution: {e}")
        finally:
            await context.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_tradetron_filename())
