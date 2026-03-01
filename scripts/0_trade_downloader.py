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

def update_heartbeat(status, msg):
    try:
        sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        sb.table("engine_heartbeat").update({
            "status": status,
            "last_msg": msg,
            "updated_at": "now()"
        }).eq("step_id", "pre_step_tt").execute()
    except Exception as e:
        log(f"⚠️ Heartbeat Update Failed: {e}")

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SOURCE_FOLDER = os.environ.get("SOURCE_FOLDER")
SCOPES = ['https://www.googleapis.com/auth/drive']

def upload_to_drive(file_path, file_name):
    log(f"📤 GitHub Action: Uploading {file_name} (Impersonating Manish)...")
    try:
        service_account_info = json.loads(os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON"))
        folder_id = os.environ.get("SOURCE_FOLDER")
        
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, 
            scopes=SCOPES
        ).with_subject("manish.kumar@ensuringsuccess.in")
        
        service = build('drive', 'v3', credentials=creds)
        
        file_metadata = {
            'name': file_name, 
            'parents': [folder_id]
        }
        
        with open(file_path, 'rb') as f:
            media = MediaIoBaseUpload(io.BytesIO(f.read()), mimetype='text/csv', resumable=False)
            file = service.files().create(
                body=file_metadata, 
                media_body=media, 
                fields='id',
                supportsAllDrives=True 
            ).execute()
            
            if file.get('id'):
                log(f"✅ GitHub Upload Success. ID: {file.get('id')}")
                return True
                
    except Exception as e:
        log(f"⚠️ Drive Upload Failed: {e}")
        return False

async def run_smart_downloader():
    update_heartbeat("running", "📡 Initializing Tradetron Scraper...")
    log("📡 Fetching strategies from Supabase...")
    
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table("strategies").select("user_email, email_password, strategy_name").eq("status", "Active").eq("deployment_type", "Live Offline").execute()    
    
    if not res.data:
        update_heartbeat("success", "🏁 No active strategies found.")
        log("🏁 No strategies found.")
        return

    df = pd.DataFrame(res.data)
    grouped = df.groupby('user_email')
    total_accounts = len(grouped)

    try:
        async with async_playwright() as p:
            log("🌐 Launching Chromium...")
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
            
            account_idx = 0
            for email, group in grouped:
                account_idx += 1
                update_heartbeat("running", f"🔄 Processing: {email} ({account_idx}/{total_accounts})")
                
                context = await browser.new_context(viewport={'width': 1280, 'height': 720})
                page = await context.new_page()
                
                try:
                    log(f"🔑 [{account_idx}/{total_accounts}] Target Account: {email}")
                    await page.goto("https://tradetron.tech/deployed-strategies", wait_until="load", timeout=90000)
                    
                    login_area = page.locator('#main')
                    await login_area.locator('input[name="email"]').fill(email)
                    await login_area.locator('input[name="password"]').fill(group.iloc[0]['email_password'])

                    altcha = login_area.locator('altcha-widget')
                    if await altcha.is_visible():
                        await altcha.locator('.altcha-checkbox').click()
                        await altcha.locator('text=Verified').wait_for(state="visible", timeout=30000)

                    await login_area.locator('button:has-text("Sign In")').click()
                    await page.wait_for_selector('#search_input', timeout=60000)

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
                    log(f"❌ Error for {email}: {e}")
                finally:
                    await context.close()
                    if account_idx < total_accounts:
                        await asyncio.sleep(3)

            await browser.close()
            
        update_heartbeat("success", f"✅ Scrape Complete: {total_accounts} accounts.")
        log("✨ Full Orchestration Complete.")

    except Exception as e:
        update_heartbeat("error", f"❌ Global Error: {str(e)[:50]}")
        log(f"❌ Critical Global Error: {e}")

if __name__ == "__main__":
    asyncio.run(run_smart_downloader())
