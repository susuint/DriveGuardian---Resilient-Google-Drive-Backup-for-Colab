# -*- coding: utf-8 -*-
"""
================================================================================
    GOOGLE DRIVE BACKUP TOOL v1.9.1 - MANUAL RESUME OPTIMIZED
    Tối ưu cho Manual Resume - Dừng runtime và chạy lại ngày hôm sau
================================================================================

PHIÊN BẢN: 1.9.1 FINAL
NGÀY: February 02, 2026

WORKFLOW KHUYẾN NGHỊ:
1. Chạy backup bình thường
2. Nếu gặp rate limit → DỪNG RUNTIME NGAY (Runtime → Disconnect and delete runtime)
3. Đợi 24h
4. Khởi động lại notebook → Chương trình TỰ ĐỘNG RESUME

TÍNH NĂNG MỚI v1.9.1:
✅ Auto-detect resume mode (không cần chọn manual)
✅ Thông báo rõ ràng khi nên dừng runtime
✅ Checkpoint sau mỗi file thành công
✅ Smart resume - tự động phát hiện trạng thái
✅ Khuyến nghị DỪNG RUNTIME thay vì chờ

================================================================================
"""

# ============================================================
# BƯỚC 1: CÀI ĐẶT THƯ VIỆN
# ============================================================

print("📦 Đang cài đặt thư viện...")
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

print("✅ Hoàn tất cài đặt thư viện!\n")


# ============================================================
# BƯỚC 2: IMPORT THƯ VIỆN
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

# Tắt warnings
logging.getLogger('google_auth_httplib2').setLevel(logging.ERROR)


# ============================================================
# ⚙️  BƯỚC 3: CẤU HÌNH CHÍNH - CHỈNH SỬA Ở ĐÂY
# ============================================================

# 📁 FOLDER IDs (BẮT BUỘC - THAY THẾ ID CỦA BẠN VÀO ĐÂY)
# Lấy ID từ URL folder: drive.google.com/drive/folders/XXX...
SOURCE_FOLDER_ID = 'YOUR_SOURCE_FOLDER_ID_HERE'    # <--- Thay ID folder nguồn cần backup vào đây
BACKUP_PARENT_ID = 'YOUR_DESTINATION_FOLDER_ID_HERE' # <--- Thay ID folder đích (nơi chứa backup) vào đây

# 🏷️  Folder suffix
FOLDER_SUFFIX = '_BACKUP'

# 🚀 Workers
MAX_WORKERS = None  # Auto-detect

# 🛡️  Rate Limit Protection
MAX_CONSECUTIVE_RATE_LIMIT_ERRORS = 3   # Dừng sau 3 lỗi
RATE_LIMIT_COOLDOWN_HOURS = 24          # Cooldown 24h

# 📝 Files
LOG_FILE = 'backup_log.json'
STATE_FILE = 'backup_state.json'

# 🎯 MANUAL RESUME MODE (Mặc định)
# True = Đề xuất DỪNG RUNTIME khi gặp rate limit
# False = Tự động retry (không khuyến nghị)
MANUAL_RESUME_MODE = True

print("="*80)
print("⚙️  CẤU HÌNH:")
print("="*80)
print(f"📁 Source: {SOURCE_FOLDER_ID}")
print(f"📁 Backup Parent: {BACKUP_PARENT_ID}")
print(f"🎯 Resume Mode: {'MANUAL (Khuyến nghị)' if MANUAL_RESUME_MODE else 'AUTO'}")
print("="*80 + "\n")


# ============================================================
# BƯỚC 4: XÁC THỰC GOOGLE DRIVE
# ============================================================

print("🔐 Đang xác thực với Google Drive...")
auth.authenticate_user()
creds, _ = default()
drive_service = build('drive', 'v3', credentials=creds)
print("✅ Xác thực thành công!\n")


# ============================================================
# BƯỚC 5: ĐỊNH NGHĨA CLASS
# ============================================================

class BackupState:
    """Quản lý trạng thái backup với manual resume tối ưu"""
    
    def __init__(self, state_file='backup_state.json'):
        self.state_file = state_file
        self.state = self.load_state()
    
    def load_state(self):
        """Load trạng thái từ file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    print(f"📂 Đã load state từ {self.state_file}")
                    return state
            except:
                print(f"⚠️  Không thể load state, tạo mới...")
        
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
        """Lưu trạng thái - Checkpoint sau mỗi thay đổi"""
        self.state['updated_at'] = datetime.now().isoformat()
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)
    
    def update(self, **kwargs):
        """Update và lưu ngay"""
        self.state.update(kwargs)
        self.save_state()
    
    def can_resume(self):
        """Kiểm tra có thể resume không"""
        if self.state['last_rate_limit_time']:
            try:
                last_error = datetime.fromisoformat(self.state['last_rate_limit_time'])
                now = datetime.now()
                hours_passed = (now - last_error).total_seconds() / 3600
                
                if hours_passed < RATE_LIMIT_COOLDOWN_HOURS:
                    remaining = RATE_LIMIT_COOLDOWN_HOURS - hours_passed
                    next_time = last_error + timedelta(hours=RATE_LIMIT_COOLDOWN_HOURS)
                    
                    print(f"\n⏰ CẦN ĐỢI THÊM {remaining:.1f} GIỜ")
                    print(f"🕐 Thử lại sau: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"💡 Khuyến nghị: Đợi đủ thời gian rồi khởi động lại notebook\n")
                    return False
            except:
                pass
        
        return True
    
    def reset_rate_limit_counter(self):
        """Reset counter"""
        self.state['consecutive_rate_limit_errors'] = 0
        self.save_state()
    
    def increment_rate_limit_error(self):
        """Tăng counter"""
        self.state['consecutive_rate_limit_errors'] += 1
        self.state['last_rate_limit_time'] = datetime.now().isoformat()
        self.save_state()
        return self.state['consecutive_rate_limit_errors']
    
    def should_auto_resume(self):
        """Kiểm tra xem có nên tự động resume không"""
        # Nếu status = paused và đã qua 24h
        if self.state['status'] == 'paused':
            if self.can_resume():
                return True
        return False


class DriveBackupManager:
    """Backup Manager tối ưu cho manual resume"""
    
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
        print(f"🎯 Mode: {'MANUAL RESUME (Khuyến nghị)' if manual_mode else 'AUTO RESUME'}\n")
    
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
        """Kiểm tra rate limit"""
        if isinstance(error, HttpError):
            return error.resp.status == 403 and 'userRateLimitExceeded' in str(error)
        return False
    
    def _handle_rate_limit_exceeded(self):
        """Xử lý rate limit - Khuyến nghị MANUAL"""
        print("\n" + "="*80)
        print("🚫 VƯỢT QUÁ GIỚI HẠN RATE LIMIT!")
        print("="*80)
        print(f"❌ Đã gặp {MAX_CONSECUTIVE_RATE_LIMIT_ERRORS} lỗi rate limit liên tiếp")
        print(f"💾 Đã lưu trạng thái vào: {self.backup_state.state_file}")
        
        if self.manual_mode:
            print("\n" + "🎯 KHUYẾN NGHỊ - MANUAL RESUME:")
            print("="*80)
            print("1️⃣  DỪNG RUNTIME NGAY:")
            print("   → Click: Runtime → Disconnect and delete runtime")
            print("   → Hoặc: Runtime → Manage sessions → Terminate")
            print("\n2️⃣  ĐỢI 24 GIỜ")
            
            next_run = datetime.now() + timedelta(hours=RATE_LIMIT_COOLDOWN_HOURS)
            print(f"\n3️⃣  KHỞI ĐỘNG LẠI SAU: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print("   → Mở lại notebook này")
            print("   → Chạy toàn bộ → Tự động resume")
            
            print("\n📊 TRẠNG THÁI ĐÃ LƯU:")
            print(f"   ✅ Files đã backup: {len(self.backup_log['backed_up_files'])}")
            print(f"   ⏳ Files đang chờ: {len(self.backup_state.state['pending_files'])}")
            print(f"   ❌ Files thất bại: {len(self.backup_state.state['failed_files'])}")
            print("="*80)
            
            print("\n⚠️  LƯU Ý: Không cần làm gì thêm, chỉ cần dừng runtime!")
            print("💡 State đã được lưu an toàn, bạn có thể tắt máy/đóng tab")
        else:
            print(f"\n⏰ Đợi {RATE_LIMIT_COOLDOWN_HOURS}h rồi chạy lại")
        
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
            print(f"❌ Lỗi: {e}")
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
                
                # Reset counter khi thành công
                with self.state_lock:
                    self.backup_state.reset_rate_limit_counter()
                
                return uploaded_file_id
            
            except Exception as e:
                if self.is_rate_limit_error(e):
                    print(f"🚫 RATE LIMIT: {file_name}")
                    
                    with self.state_lock:
                        count = self.backup_state.increment_rate_limit_error()
                        print(f"⚠️  Lỗi thứ {count}/{MAX_CONSECUTIVE_RATE_LIMIT_ERRORS}")
                        
                        if count >= MAX_CONSECUTIVE_RATE_LIMIT_ERRORS:
                            self._handle_rate_limit_exceeded()
                            return None
                    
                    if uploaded_file_id:
                        try:
                            service.files().delete(fileId=uploaded_file_id).execute()
                        except:
                            pass
                    
                    wait_time = min(60 * (2 ** attempt), 300)
                    print(f"⏳ Chờ {wait_time}s...")
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
        """Tạo folder"""
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
        """Xử lý 1 file - Lưu state sau mỗi file"""
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
            
            # Check đã backup chưa
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
                    
                    # Lưu log ngay
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
                    
                    # CHECKPOINT: Lưu cả log và state sau mỗi file thành công
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
        
        print(f"\n🚀 Đang xử lý {len(files)} files...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self.process_single_file, file_item, backup_folder_id): file_item
                for file_item in files
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_file, timeout=3600):
                if self.should_stop:
                    print("\n⏸️  Đang dừng...")
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
        """Backup đệ quy"""
        if self.should_stop:
            return
        
        items = self.list_files_in_folder(source_folder_id)
        print(f"\n📊 Tìm thấy {len(items)} items")
        
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
        SMART BACKUP - Tự động phát hiện và resume
        Không cần chọn mode manual
        """
        
        # Kiểm tra có state paused không
        if self.backup_state.state['status'] == 'paused':
            if not self.backup_state.can_resume():
                print("\n⏰ Chưa đủ 24h để resume")
                print("💡 Hãy quay lại sau\n")
                return None
            
            # Auto resume
            print("\n" + "="*80)
            print("🔄 TỰ ĐỘNG RESUME - Phát hiện backup đã bị dừng")
            print("="*80)
            
            backup_folder_id = self.backup_state.state.get('backup_folder_id')
            
            if not backup_folder_id:
                print("❌ Không tìm thấy backup folder ID")
                return None
            
            print(f"📁 Backup folder: {backup_folder_id}")
            
            pending = self.backup_state.state.get('pending_files', [])
            failed = self.backup_state.state.get('failed_files', [])
            
            print(f"📊 Pending: {len(pending)} | Failed: {len(failed)}")
            
            all_retry = pending + failed
            
            if all_retry:
                print(f"\n🔄 Retry {len(all_retry)} files...")
                self._process_files_batch(all_retry, backup_folder_id)
                
                if not self.should_stop:
                    self.backup_state.update(
                        pending_files=[],
                        failed_files=[],
                        status='completed'
                    )
                    print("\n✅ Resume hoàn tất!")
            else:
                print("\n✅ Không có file cần retry!")
                self.backup_state.update(status='completed')
            
            return backup_folder_id
        
        # Backup mới
        print("\n" + "="*80)
        print("🆕 BACKUP MỚI")
        print("="*80)
        
        source_info = self.get_file_info(SOURCE_FOLDER_ID)
        if not source_info:
            print("❌ Không thể lấy thông tin source")
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
            print(f"\n⏸️  BACKUP BỊ DỪNG")
        else:
            self.backup_state.update(status='completed')
            print(f"\n✅ HOÀN TẤT!")
        
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
        print("📊 THỐNG KÊ")
        print("="*80)
        print(f"Tổng: {total} | Files: {files} | Folders: {folders}")
        print(f"Lần chạy cuối: {self.backup_log.get('last_run', 'Chưa có')}")
        print(f"Trạng thái: {self.backup_state.state['status']}")
        
        if self.backup_state.state.get('pending_files'):
            print(f"Pending: {len(self.backup_state.state['pending_files'])}")
        if self.backup_state.state.get('failed_files'):
            print(f"Failed: {len(self.backup_state.state['failed_files'])}")
        
        print("="*80 + "\n")


# ============================================================
# BƯỚC 6: KHỞI TẠO & CHẠY
# ============================================================

print("🔧 Khởi tạo Backup Manager...")
backup_manager = DriveBackupManager(
    drive_service,
    log_file=LOG_FILE,
    state_file=STATE_FILE,
    max_workers=MAX_WORKERS,
    manual_mode=MANUAL_RESUME_MODE
)

# Stats hiện tại
backup_manager.get_backup_stats()

# ============================================================
# 🚀 CHẠY BACKUP - TỰ ĐỘNG SMART
# ============================================================

print("\n" + "="*80)
print("🎯 WORKFLOW KHUYẾN NGHỊ:")
print("="*80)
print("1. Chạy backup bình thường")
print("2. Nếu gặp rate limit → DỪNG RUNTIME")
print("3. Đợi 24h")
print("4. Khởi động lại notebook → TỰ ĐỘNG RESUME")
print("="*80 + "\n")

print("🚀 BẮT ĐẦU BACKUP...")
start_time = time.time()

# SMART BACKUP - Tự động phát hiện resume hoặc backup mới
backup_folder_id = backup_manager.smart_backup()

end_time = time.time()

# ============================================================
# KẾT QUẢ
# ============================================================

if backup_folder_id:
    duration = end_time - start_time
    print(f"\n✅ THÀNH CÔNG!")
    print(f"⏱️  Thời gian: {duration:.2f}s ({duration/60:.2f} phút)")
    print(f"📁 Backup Folder ID: {backup_folder_id}")
    print(f"🔗 Link: https://drive.google.com/drive/folders/{backup_folder_id}")
    
    backup_manager.get_backup_stats()
elif backup_manager.should_stop:
    print(f"\n💡 NEXT STEPS:")
    print("="*80)
    print("✅ State đã được lưu an toàn")
    print("✅ DỪNG RUNTIME NGAY (Runtime → Disconnect)")
    print("✅ Đợi 24h")
    print("✅ Mở lại notebook → Chạy lại → Tự động resume")
    print("="*80 + "\n")
else:
    print("\n❌ BACKUP THẤT BẠI!")

# ============================================================
# TIỆN ÍCH
# ============================================================

def view_state():
    """Xem state"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            print("\n📊 STATE:")
            print(json.dumps(state, indent=2, ensure_ascii=False))

def view_log():
    """Xem log"""
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
                        TIỆN ÍCH
================================================================================

view_state()      # Xem backup state
view_log()        # Xem backup log  
download_files()  # Download state + log về máy

================================================================================
""")
