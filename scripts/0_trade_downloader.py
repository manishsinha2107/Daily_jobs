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
        
        # 1. Broad Search for Duplicate (including All Drives/Shared)
        query = f"name = '{file_name}' and '{SOURCE_FOLDER}' in parents and trashed = false"
        existing_files = service.files().list(
            q=query, 
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute().get('files', [])
        
        if existing_files:
            log(f"⏭️ File {file_name} already exists. Skipping.")
            return True

        # 2. File Metadata
        file_metadata = {
            'name': file_name, 
            'parents': [SOURCE_FOLDER]
        }
        
        mime = 'text/csv' if file_name.endswith('.csv') else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        
        with open(file_path, 'rb') as f:
            # We use a non-resumable upload for files this small to avoid the quota-check handshake issues
            media = MediaIoBaseUpload(io.BytesIO(f.read()), mimetype=mime)
            
            # 3. CRITICAL: supportsAllDrives=True combined with specific fields
            file = service.files().create(
                body=file_metadata, 
                media_body=media, 
                fields='id, name',
                supportsAllDrives=True 
            ).execute()
            
            if file.get('id'):
                log(f"✅ Drive Upload Success. File ID: {file.get('id')}")
                return True
    except Exception as e:
        log(f"⚠️ Drive Upload Failed: {e}")
        # Detailed log for quota issues
        if "storageQuotaExceeded" in str(e):
            log("💡 TIP: Go to your Google Drive folder, right-click, and ensure the Service Account email is an 'Editor'.")
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
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        
        account_idx = 0
        total_accounts = len(grouped)

        for email, group in grouped:
            account_idx += 1
            context = await browser.new_context(viewport={'width': 1280, 'height': 720})
            page = await context.new_page()
            
            try:
                log(f"🔑 [{account_idx}/{total_accounts}] Target Account: {email}")
                await page.goto("https://tradetron.tech/deployed-strategies", wait_until="load", timeout=90000)
                
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
                await page.wait_for_selector('#search_input', timeout=60000)
                log(f"🔓 Dashboard Loaded for {email}")

                for _, row in group.iterrows():
                    strat_name = str(row['strategy_name']).strip()
                    log(f"🔍 Searching Strategy: {strat_name}")
                    
                    await page.locator('#search_input').fill("")
                    await page.locator('#search_input').type(strat_name, delay=50)
                    await asyncio.sleep(5) 

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
                            upload_to_drive(temp_path, download.suggested_filename)
                        else:
                            log(f"⏭️ {strat_name} found, but status is NOT 'Exited'.")
                    else:
                        log(f"❓ ERROR: Strategy '{strat_name}' not found.")
            except Exception as e:
                log(f"❌ Critical Error for {email}: {e}")
            finally:
                await context.close()
                if account_idx < total_accounts:
                    await asyncio.sleep(3)

        await browser.close()
        log("✨ Full Orchestration Complete.")

if __name__ == "__main__":
    asyncio.run(run_smart_downloader())
