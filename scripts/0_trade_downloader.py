import os
import asyncio
import json
import io
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from playwright.async_api import async_playwright
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

def log(msg):
    print(f"DEBUG: {msg}", flush=True)

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SOURCE_FOLDER = os.environ.get("SOURCE_FOLDER")
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON"))
SCOPES = ['https://www.googleapis.com/auth/drive']

def upload_to_drive(file_path, file_name):
    log(f"📤 Uploading {file_name} to Drive...")
    try:
        creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {'name': file_name, 'parents': [SOURCE_FOLDER]}
        mime = 'text/csv' if file_name.endswith('.csv') else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        with open(file_path, 'rb') as f:
            media = MediaIoBaseUpload(io.BytesIO(f.read()), mimetype=mime)
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            return True
    except Exception as e:
        log(f"⚠️ Drive Upload Failed: {e}")
        return False

async def run_smart_downloader():
    log("📡 Fetching strategies from Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table("strategy_mapping").select("tt_user_id, tt_password, strategy_name").eq("deployment_status", "Active").eq("deployment_type", "Live Offline").execute()
    
    if not res.data:
        log("🏁 No strategies found.")
        return

    df = pd.DataFrame(res.data)
    grouped = df.groupby('tt_user_id')

    async with async_playwright() as p:
        log("🌐 Launching Chromium...")
        # Added slow_mo to give the UI time to react to typing
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        
        account_idx = 0
        total_accounts = len(grouped)

        for email, group in grouped:
            account_idx += 1
            # NEW LOGIC: Create a FRESH context for every account to clear cookies/session
            context = await browser.new_context(viewport={'width': 1280, 'height': 720})
            page = await context.new_page()
            
            try:
                log(f"🔑 [{account_idx}/{total_accounts}] Target Account: {email}")
                
                # Go to Deployed Strategies - this will force login because context is fresh
                await page.goto("https://tradetron.tech/deployed-strategies", wait_until="load", timeout=90000)
                
                # We expect to be on the login page because this is a new context
                login_area = page.locator('#main')
                
                log("🔒 Entering credentials...")
                await login_area.locator('input[name="email"]').fill(email)
                await login_area.locator('input[name="password"]').fill(group.iloc[0]['tt_password'])

                altcha = login_area.locator('altcha-widget')
                if await altcha.is_visible():
                    log("🔘 Solving ALTCHA...")
                    await altcha.locator('.altcha-checkbox').click()
                    await altcha.locator('text=Verified').wait_for(state="visible", timeout=30000)
                    log("✅ Verified.")

                await login_area.locator('button:has-text("Sign In")').click()
                
                log("⏳ Waiting for Dashboard...")
                # Ensure we are actually logged into the CORRECT account
                await page.wait_for_selector('#search_input', timeout=60000)
                log(f"🔓 Dashboard Loaded for {email}")

                for _, row in group.iterrows():
                    strat_name = str(row['strategy_name']).strip()
                    log(f"🔍 Searching Strategy: {strat_name}")
                    
                    # Clear search and type new name
                    await page.locator('#search_input').fill("")
                    await page.locator('#search_input').type(strat_name, delay=50) # Use .type for better AJAX trigger
                    await asyncio.sleep(5) # Give Tradetron extra time to filter the list

                    # Locate the container that contains exactly this name
                    container = page.locator(f"div.strategy__section:has(a:text-is('{strat_name}'))").first
                    
                    if await container.count() > 0:
                        status_text = await container.inner_text()
                        if "Exited" in status_text:
                            log(f"🎯 Match found & EXITED. Downloading...")
                            await container.locator('button[id*="More"]').click()
                            
                            async with page.expect_download() as download_info:
                                await page.locator('a:has-text("Download Data")').click()
                            
                            download = await download_info.value
                            temp_path = await download.path()
                            if upload_to_drive(temp_path, download.suggested_filename):
                                log(f"✅ Drive Upload Success: {download.suggested_filename}")
                        else:
                            log(f"⏭️ {strat_name} found, but status is NOT 'Exited'.")
                    else:
                        log(f"❓ ERROR: Strategy '{strat_name}' not found in {email}'s dashboard.")
                        # Take a screenshot to see what IS on the page
                        await page.screenshot(path=f"not_found_{account_idx}.png")

            except Exception as e:
                log(f"❌ Critical Error for {email}: {e}")
                await page.screenshot(path=f"error_{account_idx}.png")
            finally:
                # NEW LOGIC: Close the context entirely to wipe all data
                await context.close()
                if account_idx < total_accounts:
                    log("🕒 3s Cooldown before next fresh login...")
                    await asyncio.sleep(3)

        await browser.close()
        log("✨ Full Orchestration Complete.")

if __name__ == "__main__":
    asyncio.run(run_smart_downloader())
