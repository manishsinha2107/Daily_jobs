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

# Force logs to show up in GitHub Actions
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
    creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    file_metadata = {'name': file_name, 'parents': [SOURCE_FOLDER]}
    mime = 'text/csv' if file_name.endswith('.csv') else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    with open(file_path, 'rb') as f:
        media = MediaIoBaseUpload(io.BytesIO(f.read()), mimetype=mime)
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

async def run_smart_downloader():
    log("📡 Initializing Supabase Client...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table("strategy_mapping").select(
        "tt_user_id, tt_password, strategy_name"
    ).eq("deployment_status", "Active").eq("deployment_type", "Live Offline").execute()
    
    if not res.data:
        log("🏁 No data found in Supabase.")
        return

    log(f"✅ Found {len(res.data)} strategies to process.")
    df = pd.DataFrame(res.data)
    grouped = df.groupby('tt_user_id')

    async with async_playwright() as p:
        log("🌐 Launching Chromium...")
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        context = await browser.new_context(viewport={'width': 1280, 'height': 720})
        
        for email, group in grouped:
            page = await context.new_page()
            try:
                log(f"🔑 Attempting Login: {email}")
                await page.goto("https://tradetron.tech/login", wait_until="networkidle", timeout=60000)
                
                await page.locator('#main #email').fill(email)
                await page.locator('#main #password').fill(group.iloc[0]['tt_password'])

                altcha = page.locator('altcha-widget')
                if await altcha.is_visible():
                    log("🔘 Solving ALTCHA...")
                    await altcha.locator('.altcha-checkbox').click()
                    await altcha.locator('text=Verified').wait_for(state="visible", timeout=20000)

                await page.locator('#main button:has-text("Sign In")').click()
                log("⏳ Waiting for Dashboard...")
                await page.wait_for_url("**/deployed-strategies", timeout=45000)
                log("🔓 Dashboard Loaded.")

                for _, row in group.iterrows():
                    name = str(row['strategy_name']).strip()
                    log(f"🔍 Searching: {name}")
                    await page.locator('#search_input').fill(name)
                    await asyncio.sleep(3) # Wait for UI to update

                    container = page.locator(".strategy__section", has_text=name).first
                    if await container.count() > 0 and "Exited" in await container.inner_text():
                        log(f"🎯 Strategy {name} is EXITED. Downloading...")
                        await container.locator('button[id*="More"]').click()
                        
                        async with page.expect_download() as download_info:
                            await page.locator('a:has-text("Download Data")').click()
                        
                        download = await download_info.value
                        temp_path = await download.path()
                        drive_id = upload_to_drive(temp_path, download.suggested_filename)
                        log(f"✅ File ID: {drive_id}")
                    else:
                        log(f"⏭️ {name} not found or not 'Exited'.")

            except Exception as e:
                log(f"❌ ERROR for {email}: {e}")
                # Save a screenshot to debug what went wrong
                await page.screenshot(path=f"error_{email}.png")
                log(f"📸 Screenshot saved as error_{email}.png")
            finally:
                await page.close()

        await browser.close()
        log("✨ All tasks complete.")

if __name__ == "__main__":
    asyncio.run(run_smart_downloader())
