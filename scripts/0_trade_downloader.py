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

# Force logs to show up immediately in GitHub Actions
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
        
        # Determine MimeType
        mime = 'text/csv' if file_name.endswith('.csv') else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        
        with open(file_path, 'rb') as f:
            media = MediaIoBaseUpload(io.BytesIO(f.read()), mimetype=mime)
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            return file.get('id')
    except Exception as e:
        log(f"⚠️ Drive Upload Failed: {e}")
        return None

async def run_smart_downloader():
    log("📡 Initializing Supabase Client...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Fetching active strategies
    res = supabase.table("strategy_mapping").select(
        "tt_user_id, tt_password, strategy_name"
    ).eq("deployment_status", "Active").eq("deployment_type", "Live Offline").execute()
    
    if not res.data:
        log("🏁 No data found in Supabase strategy_mapping.")
        return

    log(f"✅ Found {len(res.data)} strategies across multiple accounts.")
    df = pd.DataFrame(res.data)
    grouped = df.groupby('tt_user_id')

    async with async_playwright() as p:
        log("🌐 Launching Chromium...")
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        
        # Setting a standard user agent to avoid bot detection
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        
        account_count = 0
        total_accounts = len(grouped)

        for email, group in grouped:
            account_count += 1
            page = await context.new_page()
            
            try:
                log(f"🔑 [{account_count}/{total_accounts}] Attempting Login: {email}")
                await page.goto("https://tradetron.tech/login", wait_until="networkidle", timeout=60000)
                
                # Anchor to #main to prevent Strict Mode Violation (resolving 3 elements)
                login_form = page.locator('#main')
                await login_form.locator('#email').fill(email)
                await login_form.locator('#password').fill(group.iloc[0]['tt_password'])

                # Solve ALTCHA
                altcha = login_form.locator('altcha-widget')
                if await altcha.is_visible():
                    log("🔘 Solving ALTCHA...")
                    await altcha.locator('.altcha-checkbox').click()
                    # Wait for specific verified text within the widget
                    await altcha.locator('text=Verified').wait_for(state="visible", timeout=25000)
                    log("✅ ALTCHA Verified.")

                await login_form.locator('button:has-text("Sign In")').click()
                log("⏳ Waiting for Dashboard...")
                await page.wait_for_url("**/deployed-strategies", timeout=45000)
                log("🔓 Dashboard Loaded.")

                # Process strategies for this account
                for _, row in group.iterrows():
                    name = str(row['strategy_name']).strip()
                    log(f"🔍 Searching: {name}")
                    
                    search_box = page.locator('#search_input')
                    await search_box.fill(name)
                    await asyncio.sleep(3) # Wait for AJAX results

                    # Target the specific strategy section
                    container = page.locator(".strategy__section", has_text=name).first
                    
                    if await container.count() > 0:
                        content = await container.inner_text()
                        if "Exited" in content:
                            log(f"🎯 Found 'Exited' status for {name}. Downloading...")
                            # Open the 'More' menu
                            await container.locator('button[id*="More"]').click()
                            
                            async with page.expect_download() as download_info:
                                await page.locator('a:has-text("Download Data")').click()
                            
                            download = await download_info.value
                            temp_path = await download.path()
                            
                            # Upload to GDrive
                            drive_id = upload_to_drive(temp_path, download.suggested_filename)
                            if drive_id:
                                log(f"✅ Successfully uploaded: {download.suggested_filename}")
                        else:
                            log(f"⏭️ {name} status is not 'Exited'.")
                    else:
                        log(f"❓ Strategy '{name}' not found on page.")

            except Exception as e:
                log(f"❌ ERROR for {email}: {e}")
                # Debugging capture
                await page.screenshot(path=f"error_{email.split('@')[0]}.png")
            finally:
                await page.close()
                if account_count < total_accounts:
                    log("🕒 Cooldown: Waiting 3 seconds before next account...")
                    await asyncio.sleep(3)

        await browser.close()
        log("✨ All tasks complete.")

if __name__ == "__main__":
    asyncio.run(run_smart_downloader())
