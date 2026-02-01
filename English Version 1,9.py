# -*- coding: utf-8 -*-
"""
================================================================================
    GOOGLE DRIVE BACKUP TOOL v1.9.1 - MANUAL RESUME OPTIMIZED
    Optimized for Manual Resume - Stop runtime and resume the next day
================================================================================

VERSION: 1.9.1 FINAL
DATE: February 02, 2026

RECOMMENDED WORKFLOW:
1. Run the backup normally.
2. If a rate limit error occurs → STOP RUNTIME IMMEDIATELY (Runtime → Disconnect and delete runtime).
3. Wait for 24 hours.
4. Restart the notebook → The program will AUTOMATICALLY RESUME.

NEW FEATURES v1.9.1:
✅ Auto-detect resume mode (manual selection not required)
✅ Clear notifications when to stop runtime
✅ Checkpoint saved after every successful file
✅ Smart resume - automatically detects backup state
✅ Recommendation to STOP RUNTIME instead of waiting in a loop

================================================================================
"""

# ============================================================
# STEP 1: INSTALL LIBRARIES
# ============================================================

print("📦 Installing libraries...")
import subprocess
import sys

packages = [
    'google-auth',
    'google-auth-oauthlib', 
    'google-auth-httplib2',
    'google-api-python-client',
    'tqdm',
    'requests',
    'psutil'
]

for package in packages:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', package])

print("✅ Library installation complete!\n")


# ============================================================
# STEP 2: IMPORT LIBRARIES
# ============================================================

import os
import json
import hashlib
import time
from datetime import datetime, timedelta
from pathlib import Path
import io
import logging
import gc
from threading import Lock
import concurrent.futures
import multiprocessing

# Google Drive API
from google.colab import auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.auth import default

# Progress bar
from tqdm.notebook import tqdm

# System monitoring
import psutil

# Disable warnings
logging.getLogger('google_auth_httplib2').setLevel(logging.ERROR)


# ============================================================
# ⚙️  STEP 3: CONFIGURATION - EDIT HERE
# ============================================================

# 📁 FOLDER IDs (REQUIRED - PASTE YOUR IDs HERE)
# Get ID from folder URL: drive.google.com/drive/folders/XXX...
SOURCE_FOLDER_ID = 'YOUR_SOURCE_FOLDER_ID_HERE'      # <--- Paste Source Folder ID here
BACKUP_PARENT_ID = 'YOUR_DESTINATION_FOLDER_ID_HERE' # <--- Paste Destination Folder ID here

# 🏷️  Folder suffix
FOLDER_SUFFIX = '_BACKUP'

# 🚀 Workers
MAX_WORKERS = None  # Auto-detect

# 🛡️  Rate Limit Protection
MAX_CONSECUTIVE_RATE_LIMIT_ERRORS = 3   # Stop after 3 consecutive errors
RATE_LIMIT_COOLDOWN_HOURS = 24          # Cooldown period: 24h

# 📝 Files
LOG_FILE = 'backup_log.json'
STATE_FILE = 'backup_state.json'

# 🎯 MANUAL RESUME MODE (Default)
# True = Recommend STOPPING RUNTIME when rate limit is hit (Best practice)
# False = Auto retry loop (Not recommended)
MANUAL_RESUME_MODE = True

print("="*80)
print("⚙️  CONFIGURATION:")
print("="*80)
print(f"📁 Source: {SOURCE_FOLDER_ID}")
print(f"📁 Backup Parent: {BACKUP_PARENT_ID}")
print(f"🎯 Resume Mode: {'MANUAL (Recommended)' if MANUAL_RESUME_MODE else 'AUTO'}")
print("="*80 + "\n")


# ============================================================
# STEP 4: GOOGLE DRIVE AUTHENTICATION
# ============================================================

print("🔐 Authenticating with Google Drive...")
auth.authenticate_user()
creds, _ = default()
drive_service = build('drive', 'v3', credentials=creds)
print("✅ Authentication successful!\n")


# ============================================================
# STEP 5: CLASS DEFINITIONS
# ============================================================

class BackupState:
    """Manages backup state with optimized manual resume"""
    
    def __init__(self, state_file='backup_state.json'):
        self.state_file = state_file
        self.state = self.load_state()
    
    def load_state(self):
        """Load state from file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    print(f"📂 State loaded from {self.state_file}")
                    return state
            except:
                print(f"⚠️  Could not load state, creating new...")
        
        return {
            'status': 'new',
            'current_folder': None,
            'pending_files': [],
            'failed_files': [],
            'consecutive_rate_limit_errors': 0,
            'last_rate_limit_time': None,
            'backup_folder_id': None,
            'total_files_processed': 0,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
    
    def save_state(self):
        """Save state - Checkpoint after every change"""
        self.state['updated_at'] = datetime.now().isoformat()
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)
    
    def update(self, **kwargs):
        """Update and save immediately"""
        self.state.update(kwargs)
        self.save_state()
    
    def can_resume(self):
        """Check if resume is allowed"""
        if self.state['last_rate_limit_time']:
            try:
                last_error = datetime.fromisoformat(self.state['last_rate_limit_time'])
                now = datetime.now()
                hours_passed = (now - last_error).total_seconds() / 3600
                
                if hours_passed < RATE_LIMIT_COOLDOWN_HOURS:
                    remaining = RATE_LIMIT_COOLDOWN_HOURS - hours_passed
                    next_time = last_error + timedelta(hours=RATE_LIMIT_COOLDOWN_HOURS)
                    
                    print(f"\n⏰ NEED TO WAIT {remaining:.1f} MORE HOURS")
                    print(f"🕐 Try again after: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"💡 Recommendation: Wait for the cooldown, then restart the notebook\n")
                    return False
            except:
                pass
        
        return True
    
    def reset_rate_limit_counter(self):
        """Reset counter"""
        self.state['consecutive_rate_limit_errors'] = 0
        self.save_state()
    
    def increment_rate_limit_error(self):
        """Increment counter"""
        self.state['consecutive_rate_limit_errors'] += 1
        self.state['last_rate_limit_time'] = datetime.now().isoformat()
        self.save_state()
        return self.state['consecutive_rate_limit_errors']
    
    def should_auto_resume(self):
        """Check if auto resume should be triggered"""
        # If status = paused and 24h passed
        if self.state['status'] == 'paused':
            if self.can_resume():
                return True
        return False


class DriveBackupManager:
    """Backup Manager optimized for manual resume"""
    
    def __init__(self, service, log_file='backup_log.json', state_file='backup_state.json', 
                 max_workers=None, manual_mode=True):
        self.service = service
        self.log_file = log_file
        self.backup_log = self.load_log()
        self.backup_state = BackupState(state_file)
        self.local_temp_dir = '/content/temp_backup'
        os.makedirs(self.local_temp_dir, exist_ok=True)
        self.manual_mode = manual_mode
        
        if max_workers is None:
            self.max_workers = self._auto_detect_workers()
        else:
            self.max_workers = max_workers
        
        self.log_lock = Lock()
        self.state_lock = Lock()
        self.download_stats = {'success': 0, 'failed': 0, 'skipped': 0}
        self.upload_stats = {'success': 0, 'failed': 0}
        self.creds, _ = default()
        self.should_stop = False
        
        print(f"🚀 Workers: {self.max_workers}")
        print(f"🎯 Mode: {'MANUAL RESUME (Recommended)' if manual_mode else 'AUTO RESUME'}\n")
    
    def __del__(self):
        """Cleanup"""
        try:
            if hasattr(self, 'local_temp_dir') and os.path.exists(self.local_temp_dir):
                for file in os.listdir(self.local_temp_dir):
                    try:
                        os.remove(os.path.join(self.local_temp_dir, file))
                    except:
                        pass
            gc.collect()
        except:
            pass
    
    def _auto_detect_workers(self):
        """Auto detect workers"""
        try:
            memory = psutil.virtual_memory()
            available_gb = memory.available / (1024 ** 3)
            cpu_count = multiprocessing.cpu_count()
            
            workers_by_ram = int(available_gb / 0.3)
            workers_by_cpu = cpu_count
            optimal_workers = max(3, min(workers_by_ram, workers_by_cpu, 8))
            
            print(f"💾 RAM: {available_gb:.1f}GB | 🖥️  CPU: {cpu_count}")
            return optimal_workers
        except:
            return 4
    
    def _get_thread_local_service(self):
        """Thread-local service"""
        return build('drive', 'v3', credentials=self.creds)
    
    def load_log(self):
        """Load log"""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {'backed_up_files': {}, 'last_run': None}
    
    def save_log(self):
        """Save log"""
        with self.log_lock:
            self.backup_log['last_run'] = datetime.now().isoformat()
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(self.backup_log, f, indent=2, ensure_ascii=False)
    
    def is_rate_limit_error(self, error):
        """Check for rate limit error"""
        if isinstance(error, HttpError):
            return error.resp.status == 403 and 'userRateLimitExceeded' in str(error)
        return False
    
    def _handle_rate_limit_exceeded(self):
        """Handle rate limit - Manual Recommendation"""
        print("\n" + "="*80)
        print("🚫 RATE LIMIT EXCEEDED!")
        print("="*80)
        print(f"❌ Encountered {MAX_CONSECUTIVE_RATE_LIMIT_ERRORS} consecutive rate limit errors")
        print(f"💾 State saved to: {self.backup_state.state_file}")
        
        if self.manual_mode:
            print("\n" + "🎯 RECOMMENDATION - MANUAL RESUME:")
            print("="*80)
            print("1️⃣  STOP RUNTIME IMMEDIATELY:")
            print("   → Click: Runtime → Disconnect and delete runtime")
            print("   → Or: Runtime → Manage sessions → Terminate")
            print("\n2️⃣  WAIT 24 HOURS")
            
            next_run = datetime.now() + timedelta(hours=RATE_LIMIT_COOLDOWN_HOURS)
            print(f"\n3️⃣  RESTART AFTER: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print("   → Open this notebook")
            print("   → Run all cells → Auto-Resume triggers")
            
            print("\n📊 SAVED STATE:")
            print(f"   ✅ Files backed up: {len(self.backup_log['backed_up_files'])}")
            print(f"   ⏳ Files pending: {len(self.backup_state.state['pending_files'])}")
            print(f"   ❌ Files failed: {len(self.backup_state.state['failed_files'])}")
            print("="*80)
            
            print("\n⚠️  NOTE: No further action needed, just stop the runtime!")
            print("💡 State is safely saved, you can close the tab.")
        else:
            print(f"\n⏰ Waiting {RATE_LIMIT_COOLDOWN_HOURS}h before retrying...")
        
        print("="*80 + "\n")
        
        self.should_stop = True
        self.backup_state.update(status='paused')
    
    def get_file_info(self, file_id):
        """Get file info"""
        try:
            return self.service.files().get(
                fileId=file_id,
                fields='id, name, size, md5Checksum, mimeType'
            ).execute()
        except HttpError as e:
            print(f"❌ Error: {e}")
            return None
    
    def download_file(self, file_id, file_name, file_size=None, max_retries=3, service=None):
        """Download file"""
        if self.should_stop:
            return None
        
        if service is None:
            service = self.service
        
        local_path = os.path.join(self.local_temp_dir, file_name)
        
        for attempt in range(max_retries):
            fh = None
            pbar = None
            
            try:
                request = service.files().get_media(fileId=file_id)
                fh = io.FileIO(local_path, 'wb')
                downloader = MediaIoBaseDownload(fh, request, chunksize=10*1024*1024)
                
                done = False
                pbar = tqdm(total=100, desc=f"📥 {file_name[:30]}", unit='%', leave=False)
                
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        pbar.update(int(status.progress() * 100) - pbar.n)
                
                pbar.close()
                pbar = None
                fh.close()
                fh = None
                
                if file_size:
                    local_size = os.path.getsize(local_path)
                    if local_size != int(file_size):
                        raise Exception(f"Size mismatch")
                
                print(f"✅ Downloaded: {file_name}")
                return local_path
            
            except Exception as e:
                if self.is_rate_limit_error(e):
                    print(f"🚫 Rate limit: {file_name}")
                    with self.state_lock:
                        count = self.backup_state.increment_rate_limit_error()
                        if count >= MAX_CONSECUTIVE_RATE_LIMIT_ERRORS:
                            self._handle_rate_limit_exceeded()
                            return None
                
                print(f"⚠️  Download attempt {attempt + 1}/{max_retries} failed")
                
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except:
                        pass
                
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"❌ Failed: {file_name}")
                    return None
            
            finally:
                if pbar:
                    try:
                        pbar.close()
                    except:
                        pass
                if fh:
                    try:
                        fh.close()
                    except:
                        pass
        
        return None
    
    def upload_file(self, local_path, file_name, parent_folder_id, original_md5=None, max_retries=3, service=None):
        """Upload file"""
        if self.should_stop:
            return None
        
        if service is None:
            service = self.service
        
        for attempt in range(max_retries):
            uploaded_file_id = None
            
            try:
                file_metadata = {
                    'name': file_name,
                    'parents': [parent_folder_id]
                }
                
                media = MediaFileUpload(local_path, resumable=True, chunksize=5*1024*1024)
                file = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, name, size, md5Checksum'
                ).execute()
                
                uploaded_file_id = file['id']
                
                if original_md5 and file.get('md5Checksum') != original_md5:
                    try:
                        service.files().delete(fileId=uploaded_file_id).execute()
                    except:
                        pass
                    raise Exception("MD5 mismatch")
                
                print(f"✅ Uploaded: {file_name}")
                
                # Reset counter on success
                with self.state_lock:
                    self.backup_state.reset_rate_limit_counter()
                
                return uploaded_file_id
            
            except Exception as e:
                if self.is_rate_limit_error(e):
                    print(f"🚫 RATE LIMIT: {file_name}")
                    
                    with self.state_lock:
                        count = self.backup_state.increment_rate_limit_error()
                        print(f"⚠️  Error count: {count}/{MAX_CONSECUTIVE_RATE_LIMIT_ERRORS}")
                        
                        if count >= MAX_CONSECUTIVE_RATE_LIMIT_ERRORS:
                            self._handle_rate_limit_exceeded()
                            return None
                    
                    if uploaded_file_id:
                        try:
                            service.files().delete(fileId=uploaded_file_id).execute()
                        except:
                            pass
                    
                    wait_time = min(60 * (2 ** attempt), 300)
                    print(f"⏳ Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                print(f"⚠️  Upload attempt {attempt + 1}/{max_retries} failed")
                
                if uploaded_file_id:
                    try:
                        service.files().delete(fileId=uploaded_file_id).execute()
                    except:
                        pass
                
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"❌ Failed: {file_name}")
                    return None
        
        return None
    
    def create_folder(self, folder_name, parent_id=None):
        """Create folder"""
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            folder = self.service.files().create(
                body=file_metadata,
                fields='id, name'
            ).execute()
            
            print(f"📁 Created: {folder_name}")
            return folder['id']
        except HttpError as e:
            print(f"❌ Error: {e}")
            return None
    
    def list_files_in_folder(self, folder_id):
        """List files"""
        items = []
        page_token = None
        
        try:
            while True:
                response = self.service.files().list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    fields='nextPageToken, files(id, name, mimeType, size, md5Checksum)',
                    pageToken=page_token,
                    pageSize=100
                ).execute()
                
                items.extend(response.get('files', []))
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
            
            return items
        except HttpError as e:
            print(f"❌ Error: {e}")
            return []
    
    def process_single_file(self, item, backup_folder_id):
        """Process 1 file - Save state after each file"""
        if self.should_stop:
            with self.state_lock:
                if item not in self.backup_state.state['pending_files']:
                    self.backup_state.state['pending_files'].append(item)
                    self.backup_state.save_state()
            return False
        
        item_id = item['id']
        item_name = item['name']
        file_size = item.get('size')
        original_md5 = item.get('md5Checksum')
        
        thread_service = None
        local_path = None
        
        try:
            thread_service = self._get_thread_local_service()
            
            # Check if backed up
            with self.log_lock:
                if item_id in self.backup_log['backed_up_files']:
                    print(f"⏭️  Skipped: {item_name}")
                    self.download_stats['skipped'] += 1
                    return True
            
            # Download
            local_path = self.download_file(item_id, item_name, file_size, service=thread_service)
            
            if self.should_stop:
                with self.state_lock:
                    if item not in self.backup_state.state['pending_files']:
                        self.backup_state.state['pending_files'].append(item)
                        self.backup_state.save_state()
                return False
            
            if local_path and os.path.exists(local_path):
                self.download_stats['success'] += 1
                
                # Upload
                uploaded_id = self.upload_file(
                    local_path,
                    item_name,
                    backup_folder_id,
                    original_md5,
                    service=thread_service
                )
                
                if self.should_stop:
                    with self.state_lock:
                        if item not in self.backup_state.state['pending_files']:
                            self.backup_state.state['pending_files'].append(item)
                            self.backup_state.save_state()
                    return False
                
                if uploaded_id:
                    self.upload_stats['success'] += 1
                    
                    # Save log immediately
                    with self.log_lock:
                        self.backup_log['backed_up_files'][item_id] = {
                            'name': item_name,
                            'type': 'file',
                            'size': file_size,
                            'md5': original_md5,
                            'backup_id': uploaded_id,
                            'backup_time': datetime.now().isoformat()
                        }
                    
                    # Cleanup
                    try:
                        os.remove(local_path)
                        local_path = None
                    except:
                        pass
                    
                    # CHECKPOINT: Save log and state after every success
                    self.save_log()
                    with self.state_lock:
                        self.backup_state.state['total_files_processed'] += 1
                        self.backup_state.save_state()
                    
                    return True
                else:
                    self.upload_stats['failed'] += 1
                    with self.state_lock:
                        if item not in self.backup_state.state['failed_files']:
                            self.backup_state.state['failed_files'].append(item)
                            self.backup_state.save_state()
                    return False
            else:
                self.download_stats['failed'] += 1
                with self.state_lock:
                    if item not in self.backup_state.state['failed_files']:
                        self.backup_state.state['failed_files'].append(item)
                        self.backup_state.save_state()
                return False
        
        except Exception as e:
            print(f"❌ Error: {e}")
            with self.state_lock:
                if item not in self.backup_state.state['failed_files']:
                    self.backup_state.state['failed_files'].append(item)
                    self.backup_state.save_state()
            return False
        
        finally:
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except:
                    pass
    
    def _process_files_batch(self, files, backup_folder_id):
        """Process batch"""
        if not files:
            return
        
        print(f"\n🚀 Processing {len(files)} files...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self.process_single_file, file_item, backup_folder_id): file_item
                for file_item in files
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_file, timeout=3600):
                if self.should_stop:
                    print("\n⏸️  Stopping...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                
                completed += 1
                
                try:
                    future.result()
                except:
                    pass
                
                if completed % 20 == 0:
                    mem = psutil.virtual_memory().percent
                    if mem > 80:
                        gc.collect()
        
        if len(files) > 50:
            gc.collect()
    
    def _backup_folder_recursive(self, source_folder_id, backup_folder_id):
        """Recursive backup"""
        if self.should_stop:
            return
        
        items = self.list_files_in_folder(source_folder_id)
        print(f"\n📊 Found {len(items)} items")
        
        folders = [i for i in items if i['mimeType'] == 'application/vnd.google-apps.folder']
        files = [i for i in items if i['mimeType'] != 'application/vnd.google-apps.folder']
        
        # Folders
        for folder_item in folders:
            if self.should_stop:
                break
            
            item_id = folder_item['id']
            item_name = folder_item['name']
            
            if item_id in self.backup_log['backed_up_files']:
                continue
            
            print(f"\n📁 Processing: {item_name}")
            new_folder_id = self.create_folder(item_name, backup_folder_id)
            
            if new_folder_id:
                self._backup_folder_recursive(item_id, new_folder_id)
                
                with self.log_lock:
                    self.backup_log['backed_up_files'][item_id] = {
                        'name': item_name,
                        'type': 'folder',
                        'backup_time': datetime.now().isoformat()
                    }
        
        # Files
        if files and not self.should_stop:
            self._process_files_batch(files, backup_folder_id)
    
    def smart_backup(self):
        """
        SMART BACKUP - Auto detect and resume
        No manual mode selection required
        """
        
        # Check if status is paused
        if self.backup_state.state['status'] == 'paused':
            if not self.backup_state.can_resume():
                print("\n⏰ 24h cooldown has not passed yet.")
                print("💡 Please come back later.\n")
                return None
            
            # Auto resume
            print("\n" + "="*80)
            print("🔄 AUTO RESUME - Detected paused backup")
            print("="*80)
            
            backup_folder_id = self.backup_state.state.get('backup_folder_id')
            
            if not backup_folder_id:
                print("❌ Backup folder ID not found")
                return None
            
            print(f"📁 Backup folder: {backup_folder_id}")
            
            pending = self.backup_state.state.get('pending_files', [])
            failed = self.backup_state.state.get('failed_files', [])
            
            print(f"📊 Pending: {len(pending)} | Failed: {len(failed)}")
            
            all_retry = pending + failed
            
            if all_retry:
                print(f"\n🔄 Retrying {len(all_retry)} files...")
                self._process_files_batch(all_retry, backup_folder_id)
                
                if not self.should_stop:
                    self.backup_state.update(
                        pending_files=[],
                        failed_files=[],
                        status='completed'
                    )
                    print("\n✅ Resume complete!")
            else:
                print("\n✅ No files to retry!")
                self.backup_state.update(status='completed')
            
            return backup_folder_id
        
        # New Backup
        print("\n" + "="*80)
        print("🆕 NEW BACKUP")
        print("="*80)
        
        source_info = self.get_file_info(SOURCE_FOLDER_ID)
        if not source_info:
            print("❌ Cannot retrieve source info")
            return None
        
        backup_folder_name = source_info['name'] + FOLDER_SUFFIX
        backup_folder_id = self.create_folder(backup_folder_name, BACKUP_PARENT_ID)
        
        if not backup_folder_id:
            return None
        
        self.backup_state.update(
            status='in_progress',
            backup_folder_id=backup_folder_id,
            current_folder=SOURCE_FOLDER_ID
        )
        
        self.download_stats = {'success': 0, 'failed': 0, 'skipped': 0}
        self.upload_stats = {'success': 0, 'failed': 0}
        
        self._backup_folder_recursive(SOURCE_FOLDER_ID, backup_folder_id)
        
        self.save_log()
        
        if self.should_stop:
            print(f"\n⏸️  BACKUP PAUSED")
        else:
            self.backup_state.update(status='completed')
            print(f"\n✅ COMPLETED!")
        
        print(f"\n📊 Download: ✅ {self.download_stats['success']} | "
              f"❌ {self.download_stats['failed']} | ⏭️ {self.download_stats['skipped']}")
        print(f"📊 Upload: ✅ {self.upload_stats['success']} | ❌ {self.upload_stats['failed']}")
        
        return backup_folder_id
    
    def get_backup_stats(self):
        """Stats"""
        total = len(self.backup_log['backed_up_files'])
        files = sum(1 for i in self.backup_log['backed_up_files'].values() if i['type'] == 'file')
        folders = sum(1 for i in self.backup_log['backed_up_files'].values() if i['type'] == 'folder')
        
        print("\n" + "="*80)
        print("📊 STATISTICS")
        print("="*80)
        print(f"Total: {total} | Files: {files} | Folders: {folders}")
        print(f"Last run: {self.backup_log.get('last_run', 'N/A')}")
        print(f"Status: {self.backup_state.state['status']}")
        
        if self.backup_state.state.get('pending_files'):
            print(f"Pending: {len(self.backup_state.state['pending_files'])}")
        if self.backup_state.state.get('failed_files'):
            print(f"Failed: {len(self.backup_state.state['failed_files'])}")
        
        print("="*80 + "\n")


# ============================================================
# STEP 6: INITIALIZATION & EXECUTION
# ============================================================

print("🔧 Initializing Backup Manager...")
backup_manager = DriveBackupManager(
    drive_service,
    log_file=LOG_FILE,
    state_file=STATE_FILE,
    max_workers=MAX_WORKERS,
    manual_mode=MANUAL_RESUME_MODE
)

# Current stats
backup_manager.get_backup_stats()

# ============================================================
# 🚀 RUN BACKUP - AUTO SMART MODE
# ============================================================

print("\n" + "="*80)
print("🎯 RECOMMENDED WORKFLOW:")
print("="*80)
print("1. Run backup normally")
print("2. If rate limit error occurs → STOP RUNTIME")
print("3. Wait 24h")
print("4. Restart notebook → AUTO RESUME")
print("="*80 + "\n")

print("🚀 STARTING BACKUP...")
start_time = time.time()

# SMART BACKUP - Auto detect resume or new backup
backup_folder_id = backup_manager.smart_backup()

end_time = time.time()

# ============================================================
# RESULTS
# ============================================================

if backup_folder_id:
    duration = end_time - start_time
    print(f"\n✅ SUCCESS!")
    print(f"⏱️  Duration: {duration:.2f}s ({duration/60:.2f} minutes)")
    print(f"📁 Backup Folder ID: {backup_folder_id}")
    print(f"🔗 Link: https://drive.google.com/drive/folders/{backup_folder_id}")
    
    backup_manager.get_backup_stats()
elif backup_manager.should_stop:
    print(f"\n💡 NEXT STEPS:")
    print("="*80)
    print("✅ State safely saved")
    print("✅ STOP RUNTIME IMMEDIATELY (Runtime → Disconnect)")
    print("✅ Wait 24h")
    print("✅ Open notebook again → Run → Auto resume")
    print("="*80 + "\n")
else:
    print("\n❌ BACKUP FAILED!")

# ============================================================
# UTILITIES
# ============================================================

def view_state():
    """View state"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            print("\n📊 STATE:")
            print(json.dumps(state, indent=2, ensure_ascii=False))

def view_log():
    """View log"""
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            log = json.load(f)
            print(f"\n📊 LOG:")
            print(f"Total: {len(log['backed_up_files'])}")

def download_files():
    """Download files"""
    from google.colab import files
    for filename in [STATE_FILE, LOG_FILE]:
        if os.path.exists(filename):
            files.download(filename)
            print(f"✅ Downloaded: {filename}")

print("""
================================================================================
                        UTILITIES
================================================================================

view_state()      # View backup state
view_log()        # View backup log  
download_files()  # Download state + log to local machine

================================================================================
""")