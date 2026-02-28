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

# --- 1. CONFIGURATION ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SOURCE_FOLDER = os.environ.get("SOURCE_FOLDER") # The folder where files go for ingestion
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON"))
SCOPES = ['https://www.googleapis.com/auth/drive']

# --- 2. DRIVE UPLOAD LOGIC ---
def upload_to_drive(file_path, file_name):
    """Uploads the downloaded report directly to your Google Drive Source Folder"""
    creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    
    file_metadata = {
        'name': file_name,
        'parents': [SOURCE_FOLDER]
    }
    
    # Check if file is CSV or Excel based on ext
    mime = 'text/csv' if file_name.endswith('.csv') else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    
    with open(file_path, 'rb') as f:
        media = MediaIoBaseUpload(io.BytesIO(f.read()), mimetype=mime)
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

# --- 3. THE SMART DOWNLOADER ENGINE ---
async def run_smart_downloader():
    # Fetch targets from Supabase
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table("strategy_mapping").select(
        "tt_user_id, tt_password, strategy_name"
    ).eq("deployment_status", "Active").eq("deployment_type", "Live Offline").execute()
    
    if not res.data:
        print("🏁 No active strategies to download.")
        return

    df = pd.DataFrame(res.data)
    grouped = df.groupby('tt_user_id')

    async with async_playwright() as p:
        # Headless=True for GitHub Actions
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        
        for email, group in grouped:
            page = await context.new_page()
            try:
                print(f"\n👤 Account: {email}")
                await page.goto("https://tradetron.tech/login")
                
                # Login Flow
                login_form = page.locator('#main')
                await login_form.locator('#email').fill(email)
                await login_form.locator('#password').fill(group.iloc[0]['tt_password'])

                # Enhanced ALTCHA Solver
                altcha = login_form.locator('altcha-widget')
                if await altcha.locator('.altcha-checkbox').is_visible():
                    print("🔘 Solving ALTCHA...")
                    await altcha.locator('.altcha-checkbox').click()
                    await altcha.locator('text=Verified').wait_for(state="visible", timeout=15000)

                await login_form.locator('button:has-text("Sign In")').click()
                await page.wait_for_url("**/deployed-strategies", timeout=30000)

                for _, row in group.iterrows():
                    name = str(row['strategy_name']).strip()
                    print(f"🔍 Checking: {name}")
                    
                    await page.locator('#search_input').fill(name)
                    await asyncio.sleep(2) # AJAX Wait

                    container = page.locator(".strategy__section", has_text=name).first
                    if await container.count() > 0 and "Exited" in await container.inner_text():
                        # More -> Download
                        await container.locator('button[id*="More"]').click()
                        
                        async with page.expect_download() as download_info:
                            await page.locator('a:has-text("Download Data")').click()
                        
                        download = await download_info.value
                        temp_path = await download.path() # Path on GitHub runner
                        
                        # Upload directly to GDrive
                        drive_id = upload_to_drive(temp_path, download.suggested_filename)
                        print(f"✅ Uploaded to GDrive: {download.suggested_filename} (ID: {drive_id})")
                    else:
                        print(f"⏭️ Skipped {name}")

            except Exception as e:
                print(f"❌ Error in account {email}: {e}")
            finally:
                await page.close()

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_smart_downloader())
