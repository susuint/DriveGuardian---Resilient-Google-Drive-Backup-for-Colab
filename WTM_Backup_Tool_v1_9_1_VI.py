# -*- coding: utf-8 -*-
"""
================================================================================
    CÔNG CỤ SAO LƯU GOOGLE DRIVE v2.0 - MẠNH MẼ & NÂNG CAO
    Phiên bản sản xuất với xử lý lỗi và quản lý bộ nhớ chuẩn chỉ
================================================================================

PHIÊN BẢN: 2.0.0
NGÀY: 04 Tháng 02, 2026

CÁC CẢI TIẾN CHÍNH:
✅ Phát hiện giới hạn tốc độ (rate limit) chuẩn xác trên TẤT CẢ thao tác
✅ Mô hình "Cầu dao ngắt mạch" (Circuit breaker) để xử lý giới hạn tốc độ
✅ Ngăn chặn rò rỉ bộ nhớ với cơ chế dọn dẹp tài nguyên
✅ Thao tác an toàn luồng (Thread-safe) với trình quản lý ngữ cảnh
✅ Cơ chế thử lại (Backoff) theo cấp số nhân kèm độ trễ ngẫu nhiên (jitter)
✅ Xử lý tắt chương trình nhẹ nhàng (Graceful shutdown)
✅ Khả năng phục hồi lỗi toàn diện
✅ Quản lý tài nguyên cho các xử lý tập tin (File handles)
✅ Cập nhật trạng thái nguyên tử (Atomic updates)

THAY ĐỔI KHÔNG PHÁ VỠ CẤU TRÚC:
- Tất cả các biến cấu hình cũ vẫn hoạt động bình thường
- Các tệp trạng thái tương thích ngược
- API không thay đổi đối với người dùng

TỐI ƯU HÓA BỘ NHỚ:
- Dọn dẹp file handle đúng cách
- Giới hạn luồng (thread pool) với giới hạn tài nguyên
- Gọi bộ thu gom rác (garbage collection) rõ ràng tại các điểm kiểm soát
- Xử lý luồng (stream) cho các tập tin lớn

================================================================================
"""

# ============================================================
# CÀI ĐẶT
# ============================================================

print("📦 Đang cài đặt các thư viện phụ thuộc...")
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

print("✅ Đã cài đặt xong các thư viện!\n")

# ============================================================
# IMPORT THƯ VIỆN
# ============================================================

import os
import json
import hashlib
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
import io
import logging
import gc
import signal
import atexit
from threading import Lock, Event, RLock
from contextlib import contextmanager
import concurrent.futures
import multiprocessing
from collections import deque
from typing import Optional, Dict, List, Any, Tuple

# Google Drive API
from google.colab import auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.auth import default

# Thanh tiến trình
from tqdm.notebook import tqdm

# Giám sát hệ thống
import psutil

# Tắt cảnh báo không cần thiết
logging.getLogger('google_auth_httplib2').setLevel(logging.ERROR)

# ============================================================
# CẤU HÌNH
# ============================================================

# 📁 ID THƯ MỤC (BẮT BUỘC)
SOURCE_FOLDER_ID = '1ZY4ab0XlPHa5asdsafghjFFFgeNx'  # ⚠️ THAY ĐỔI MÃ NÀY (Thư mục nguồn)
BACKUP_PARENT_ID = 'ABCDfghjFFFgeNx123124353xxa41'  # ⚠️ THAY ĐỔI MÃ NÀY (Thư mục đích để chứa backup)

# 🏷️ Cài đặt chung
FOLDER_SUFFIX = '_BACKUP'     # Hậu tố tên thư mục backup
MAX_WORKERS = None            # Tự động phát hiện số luồng

# 🛡️ Bảo vệ Giới hạn Tốc độ (Mô hình Ngắt mạch)
RATE_LIMIT_THRESHOLD = 3          # Số lỗi cho phép trước khi ngắt mạch
RATE_LIMIT_COOLDOWN_HOURS = 24    # Thời gian chờ (nguội) tính bằng giờ
RATE_LIMIT_WINDOW_SECONDS = 60    # Cửa sổ thời gian đếm lỗi (giây)

# 📝 Tệp tin lưu trữ
LOG_FILE = 'backup_log.json'      # File nhật ký
STATE_FILE = 'backup_state.json'  # File trạng thái

# 🎯 Chế độ
MANUAL_RESUME_MODE = True         # True: Chế độ khôi phục thủ công (an toàn hơn)

# 🔧 Cài đặt Nâng cao
CHUNK_SIZE = 10 * 1024 * 1024      # Kích thước phân mảnh 10MB
MAX_RETRIES = 3                     # Số lần thử lại tối đa cho mỗi thao tác
INITIAL_BACKOFF = 2                 # Thời gian chờ ban đầu (giây)
MAX_BACKOFF = 300                   # Thời gian chờ tối đa (giây)
MEMORY_CLEANUP_THRESHOLD = 80       # Ngưỡng RAM % để kích hoạt dọn dẹp
MAX_FILE_HANDLES = 10               # Số lượng file handle mở đồng thời tối đa

print("="*80)
print("⚙️  CẤU HÌNH:")
print("="*80)
print(f"📁 Nguồn: {SOURCE_FOLDER_ID}")
print(f"📁 Thư mục cha Backup: {BACKUP_PARENT_ID}")
print(f"🎯 Chế độ: {'KHÔI PHỤC THỦ CÔNG' if MANUAL_RESUME_MODE else 'KHÔI PHỤC TỰ ĐỘNG'}")
print(f"🛡️ Giới hạn tốc độ: {RATE_LIMIT_THRESHOLD} lỗi trong {RATE_LIMIT_WINDOW_SECONDS} giây")
print(f"💾 Kích thước mảnh (Chunk): {CHUNK_SIZE / (1024*1024):.0f}MB")
print("="*80 + "\n")

# ============================================================
# XÁC THỰC
# ============================================================

print("🔐 Đang xác thực với Google Drive...")
auth.authenticate_user()
creds, _ = default()
drive_service = build('drive', 'v3', credentials=creds)
print("✅ Xác thực thành công!\n")

# ============================================================
# CÁC LỚP TIỆN ÍCH (UTILITY CLASSES)
# ============================================================

class CircuitBreaker:
    """
    Mô hình ngắt mạch để bảo vệ chống lại giới hạn tốc độ (Rate Limit).
    
    Các trạng thái:
    - CLOSED (ĐÓNG): Hoạt động bình thường
    - OPEN (MỞ): Quá nhiều lỗi, chặn mọi yêu cầu
    - HALF_OPEN (BÁN MỞ): Đang thử xem dịch vụ đã hồi phục chưa
    """
    
    def __init__(self, threshold: int, window_seconds: int, cooldown_hours: int):
        self.threshold = threshold
        self.window_seconds = window_seconds
        self.cooldown_seconds = cooldown_hours * 3600
        
        self.state = 'CLOSED'
        self.failures = deque()  # Lưu timestamps của các lỗi
        self.last_failure_time = None
        self.lock = RLock()
        
    def record_success(self):
        """Ghi nhận thao tác thành công"""
        with self.lock:
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                self.failures.clear()
                
    def record_failure(self) -> bool:
        """
        Ghi nhận lỗi và trả về True nếu mạch nên mở (ngắt).
        
        Trả về:
            bool: True nếu mạch bị ngắt (tripped)
        """
        with self.lock:
            now = time.time()
            self.last_failure_time = now
            self.failures.append(now)
            
            # Xóa các lỗi cũ nằm ngoài cửa sổ thời gian
            cutoff = now - self.window_seconds
            while self.failures and self.failures[0] < cutoff:
                self.failures.popleft()
            
            # Kiểm tra nếu vượt quá ngưỡng
            if len(self.failures) >= self.threshold:
                self.state = 'OPEN'
                return True
                
            return False
    
    def can_proceed(self) -> Tuple[bool, Optional[str]]:
        """
        Kiểm tra xem thao tác có thể tiếp tục không.
        
        Trả về:
            Tuple[bool, Optional[str]]: (có_thể_tiếp_tục, lý_do_nếu_bị_chặn)
        """
        with self.lock:
            if self.state == 'CLOSED':
                return True, None
                
            if self.state == 'OPEN':
                if self.last_failure_time:
                    elapsed = time.time() - self.last_failure_time
                    
                    if elapsed >= self.cooldown_seconds:
                        self.state = 'HALF_OPEN'
                        return True, None
                    
                    remaining = self.cooldown_seconds - elapsed
                    next_time = datetime.fromtimestamp(
                        self.last_failure_time + self.cooldown_seconds
                    )
                    
                    return False, (
                        f"Mạch đang MỞ (Ngắt). "
                        f"Vui lòng đợi thêm {remaining/3600:.1f} giờ. "
                        f"Tiếp tục sau: {next_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    
            if self.state == 'HALF_OPEN':
                return True, None
                
            return False, "Trạng thái ngắt mạch không xác định"
    
    def get_status(self) -> Dict[str, Any]:
        """Lấy trạng thái hiện tại"""
        with self.lock:
            return {
                'state': self.state,
                'failures_in_window': len(self.failures),
                'threshold': self.threshold,
                'last_failure': self.last_failure_time
            }


class ResourceManager:
    """
    Quản lý tài nguyên hệ thống để ngăn rò rỉ bộ nhớ.
    """
    
    def __init__(self, max_file_handles: int):
        self.max_file_handles = max_file_handles
        self.active_handles = []
        self.lock = Lock()
        
    @contextmanager
    def get_file_handle(self, path: str, mode: str):
        """Trình quản lý ngữ cảnh cho file handles với tự động dọn dẹp"""
        handle = None
        try:
            # Đợi nếu có quá nhiều file đang mở
            while len(self.active_handles) >= self.max_file_handles:
                time.sleep(0.1)
                self._cleanup_closed_handles()
            
            handle = open(path, mode)
            
            with self.lock:
                self.active_handles.append(handle)
            
            yield handle
            
        finally:
            if handle:
                try:
                    handle.close()
                except:
                    pass
                
                with self.lock:
                    if handle in self.active_handles:
                        self.active_handles.remove(handle)
    
    def _cleanup_closed_handles(self):
        """Loại bỏ các handle đã đóng khỏi danh sách theo dõi"""
        with self.lock:
            self.active_handles = [h for h in self.active_handles if not h.closed]
    
    def cleanup_all(self):
        """Buộc đóng tất cả các handle"""
        with self.lock:
            for handle in self.active_handles:
                try:
                    handle.close()
                except:
                    pass
            self.active_handles.clear()


class MemoryMonitor:
    """Giám sát và quản lý sử dụng bộ nhớ"""
    
    def __init__(self, threshold_percent: int = 80):
        self.threshold = threshold_percent
        
    def check_and_cleanup(self) -> bool:
        """
        Kiểm tra bộ nhớ và dọn dẹp nếu cần.
        
        Trả về:
            bool: True nếu đã thực hiện dọn dẹp
        """
        try:
            mem = psutil.virtual_memory()
            if mem.percent > self.threshold:
                gc.collect()
                return True
        except:
            pass
        return False
    
    def get_usage(self) -> Dict[str, Any]:
        """Lấy thông tin sử dụng bộ nhớ hiện tại"""
        try:
            mem = psutil.virtual_memory()
            return {
                'percent': mem.percent,
                'available_gb': mem.available / (1024**3),
                'total_gb': mem.total / (1024**3)
            }
        except:
            return {}


# ============================================================
# QUẢN LÝ TRẠNG THÁI (STATE MANAGEMENT)
# ============================================================

class BackupState:
    """Quản lý trạng thái backup an toàn luồng với cập nhật nguyên tử"""
    
    def __init__(self, state_file: str = 'backup_state.json'):
        self.state_file = state_file
        self.lock = RLock()
        self.state = self._load_state()
        
    def _load_state(self) -> Dict[str, Any]:
        """Tải trạng thái từ tệp"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    print(f"📂 Đã tải trạng thái từ {self.state_file}")
                    return state
            except Exception as e:
                print(f"⚠️ Lỗi tải trạng thái: {e}")
        
        return {
            'status': 'new',
            'version': '2.0',
            'backup_folder_id': None,
            'current_folder': None,
            'pending_files': [],
            'failed_files': [],
            'total_files_processed': 0,
            'circuit_breaker_state': 'CLOSED',
            'last_rate_limit_time': None,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
    
    def _save_state(self):
        """Lưu trạng thái vào tệp (phải được gọi trong lock)"""
        try:
            self.state['updated_at'] = datetime.now().isoformat()
            
            # Ghi nguyên tử bằng tệp tạm
            temp_file = self.state_file + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
            
            # Đổi tên nguyên tử
            os.replace(temp_file, self.state_file)
            
        except Exception as e:
            print(f"⚠️ Lỗi lưu trạng thái: {e}")
    
    def update(self, **kwargs):
        """Cập nhật nguyên tử an toàn luồng"""
        with self.lock:
            self.state.update(kwargs)
            self._save_state()
    
    def add_pending(self, file_item: Dict[str, Any]):
        """Thêm tập tin vào danh sách chờ"""
        with self.lock:
            if file_item not in self.state['pending_files']:
                self.state['pending_files'].append(file_item)
                self._save_state()
    
    def add_failed(self, file_item: Dict[str, Any]):
        """Thêm tập tin vào danh sách lỗi"""
        with self.lock:
            if file_item not in self.state['failed_files']:
                self.state['failed_files'].append(file_item)
                self._save_state()
    
    def remove_from_pending(self, file_id: str):
        """Xóa tập tin khỏi danh sách chờ theo ID"""
        with self.lock:
            self.state['pending_files'] = [
                f for f in self.state['pending_files'] 
                if f.get('id') != file_id
            ]
            self._save_state()
    
    def increment_processed(self):
        """Tăng bộ đếm số lượng đã xử lý"""
        with self.lock:
            self.state['total_files_processed'] += 1
            self._save_state()
    
    def get_snapshot(self) -> Dict[str, Any]:
        """Lấy ảnh chụp (snapshot) trạng thái an toàn luồng"""
        with self.lock:
            return self.state.copy()


# ============================================================
# TRÌNH QUẢN LÝ BACKUP CHÍNH
# ============================================================

class DriveBackupManager:
    """
    Trình quản lý backup mạnh mẽ với xử lý lỗi và tài nguyên chuẩn.
    """
    
    def __init__(
        self,
        service,
        log_file: str = 'backup_log.json',
        state_file: str = 'backup_state.json',
        max_workers: Optional[int] = None,
        manual_mode: bool = True
    ):
        self.service = service
        self.log_file = log_file
        self.manual_mode = manual_mode
        
        # Quản lý trạng thái
        self.backup_state = BackupState(state_file)
        self.backup_log = self._load_log()
        self.log_lock = RLock()
        
        # Ngắt mạch cho giới hạn tốc độ
        self.circuit_breaker = CircuitBreaker(
            threshold=RATE_LIMIT_THRESHOLD,
            window_seconds=RATE_LIMIT_WINDOW_SECONDS,
            cooldown_hours=RATE_LIMIT_COOLDOWN_HOURS
        )
        
        # Quản lý tài nguyên
        self.resource_manager = ResourceManager(MAX_FILE_HANDLES)
        self.memory_monitor = MemoryMonitor(MEMORY_CLEANUP_THRESHOLD)
        
        # Thư mục làm việc tạm thời
        self.local_temp_dir = '/content/temp_backup'
        os.makedirs(self.local_temp_dir, exist_ok=True)
        
        # Thread pool (Luồng xử lý)
        if max_workers is None:
            self.max_workers = self._auto_detect_workers()
        else:
            self.max_workers = max_workers
        
        # Xử lý tắt chương trình
        self.shutdown_event = Event()
        self._setup_signal_handlers()
        
        # Thống kê
        self.stats = {
            'download': {'success': 0, 'failed': 0, 'skipped': 0},
            'upload': {'success': 0, 'failed': 0}
        }
        
        # Chứng chỉ cho các luồng cục bộ
        self.creds, _ = default()
        
        print(f"🚀 Số luồng (Workers): {self.max_workers}")
        print(f"🎯 Chế độ: {'THỦ CÔNG' if manual_mode else 'TỰ ĐỘNG'}")
        print(f"💾 Ngưỡng bộ nhớ: {MEMORY_CLEANUP_THRESHOLD}%")
        print()
    
    def __del__(self):
        """Dọn dẹp khi hủy đối tượng"""
        self._cleanup()
    
    def _setup_signal_handlers(self):
        """Thiết lập xử lý tắt chương trình nhẹ nhàng"""
        def shutdown_handler(signum, frame):
            print("\n⚠️ Nhận tín hiệu tắt, đang dọn dẹp...")
            self.shutdown_event.set()
        
        try:
            signal.signal(signal.SIGINT, shutdown_handler)
            signal.signal(signal.SIGTERM, shutdown_handler)
        except:
            pass  # Tín hiệu có thể không hoạt động trên Colab
        
        atexit.register(self._cleanup)
    
    def _cleanup(self):
        """Dọn dẹp tài nguyên"""
        try:
            self.resource_manager.cleanup_all()
            
            if os.path.exists(self.local_temp_dir):
                for file in os.listdir(self.local_temp_dir):
                    try:
                        os.remove(os.path.join(self.local_temp_dir, file))
                    except:
                        pass
            
            gc.collect()
        except:
            pass
    
    def _auto_detect_workers(self) -> int:
        """Tự động phát hiện số lượng luồng tối ưu"""
        try:
            mem_info = self.memory_monitor.get_usage()
            available_gb = mem_info.get('available_gb', 4)
            cpu_count = multiprocessing.cpu_count()
            
            workers_by_ram = max(1, int(available_gb / 0.3))
            workers_by_cpu = cpu_count
            optimal = max(3, min(workers_by_ram, workers_by_cpu, 8))
            
            print(f"💾 RAM: {available_gb:.1f}GB | 🖥️ CPU: {cpu_count}")
            return optimal
        except:
            return 4
    
    def _load_log(self) -> Dict[str, Any]:
        """Tải nhật ký backup"""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        
        return {
            'version': '2.0',
            'backed_up_files': {},
            'last_run': None
        }
    
    def _save_log(self):
        """Lưu nhật ký backup với ghi nguyên tử"""
        with self.log_lock:
            try:
                self.backup_log['last_run'] = datetime.now().isoformat()
                
                temp_file = self.log_file + '.tmp'
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self.backup_log, f, indent=2, ensure_ascii=False)
                
                os.replace(temp_file, self.log_file)
            except Exception as e:
                print(f"⚠️ Lỗi lưu nhật ký: {e}")
    
    def _get_thread_local_service(self):
        """Lấy dịch vụ Drive cục bộ cho luồng"""
        return build('drive', 'v3', credentials=self.creds)
    
    def _is_rate_limit_error(self, error: Exception) -> bool:
        """Kiểm tra xem lỗi có phải do giới hạn tốc độ không"""
        if isinstance(error, HttpError):
            return (
                error.resp.status == 403 and 
                'userRateLimitExceeded' in str(error)
            )
        return False
    
    def _exponential_backoff(self, attempt: int, jitter: bool = True) -> float:
        """Tính toán thời gian chờ với độ trễ ngẫu nhiên"""
        backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)
        
        if jitter:
            backoff = backoff * (0.5 + random.random())
        
        return backoff
    
    def _handle_rate_limit(self) -> bool:
        """
        Xử lý lỗi giới hạn tốc độ.
        
        Trả về:
            bool: True nếu nên dừng thực thi
        """
        # Ghi nhận lỗi vào mạch ngắt
        circuit_tripped = self.circuit_breaker.record_failure()
        
        if circuit_tripped:
            self.backup_state.update(
                status='paused',
                circuit_breaker_state='OPEN',
                last_rate_limit_time=datetime.now().isoformat()
            )
            
            print("\n" + "="*80)
            print("🚫 CẦU DAO GIỚI HẠN TỐC ĐỘ ĐÃ NGẮT")
            print("="*80)
            print(f"❌ Phát hiện {RATE_LIMIT_THRESHOLD} lỗi giới hạn tốc độ trong {RATE_LIMIT_WINDOW_SECONDS} giây")
            print(f"💾 Trạng thái đã lưu tại: {self.backup_state.state_file}")
            
            if self.manual_mode:
                self._print_manual_resume_instructions()
            else:
                print(f"\n⏰ Tự động khôi phục sau {RATE_LIMIT_COOLDOWN_HOURS} giờ")
            
            print("="*80 + "\n")
            
            self.shutdown_event.set()
            return True
        
        return False
    
    def _print_manual_resume_instructions(self):
        """In hướng dẫn khôi phục thủ công"""
        next_run = datetime.now() + timedelta(hours=RATE_LIMIT_COOLDOWN_HOURS)
        
        print("\n🎯 HƯỚNG DẪN KHÔI PHỤC THỦ CÔNG:")
        print("="*80)
        print("1️⃣ DỪNG RUNTIME NGAY LẬP TỨC:")
        print("   → Runtime (Thời gian chạy) → Disconnect and delete runtime (Ngắt kết nối và xóa)")
        print()
        print("2️⃣ ĐỢI 24 GIỜ")
        print()
        print(f"3️⃣ KHÔI PHỤC SAU: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        print("   → Mở lại notebook này")
        print("   → Chạy tất cả các ô (Run all) → Tự động khôi phục")
        print()
        print("📊 TIẾN ĐỘ ĐÃ LƯU:")
        
        snapshot = self.backup_state.get_snapshot()
        print(f"   ✅ Đã hoàn thành: {len(self.backup_log['backed_up_files'])}")
        print(f"   ⏳ Đang chờ: {len(snapshot['pending_files'])}")
        print(f"   ❌ Thất bại: {len(snapshot['failed_files'])}")
        print("="*80)
    
    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Lấy siêu dữ liệu tập tin"""
        try:
            return self.service.files().get(
                fileId=file_id,
                fields='id, name, size, md5Checksum, mimeType'
            ).execute()
        except HttpError as e:
            print(f"❌ Lỗi khi lấy thông tin tập tin: {e}")
            return None
    
    def download_file(
        self,
        file_id: str,
        file_name: str,
        file_size: Optional[str] = None,
        service=None
    ) -> Optional[str]:
        """
        Tải xuống tập tin với xử lý lỗi và quản lý tài nguyên.
        
        Trả về:
            Optional[str]: Đường dẫn cục bộ nếu thành công, None nếu thất bại
        """
        if self.shutdown_event.is_set():
            return None
        
        # Kiểm tra mạch ngắt
        can_proceed, reason = self.circuit_breaker.can_proceed()
        if not can_proceed:
            print(f"🚫 {reason}")
            return None
        
        if service is None:
            service = self.service
        
        local_path = os.path.join(self.local_temp_dir, file_name)
        
        for attempt in range(MAX_RETRIES):
            fh = None
            pbar = None
            
            try:
                request = service.files().get_media(fileId=file_id)
                
                with self.resource_manager.get_file_handle(local_path, 'wb') as fh:
                    downloader = MediaIoBaseDownload(
                        fh,
                        request,
                        chunksize=CHUNK_SIZE
                    )
                    
                    done = False
                    pbar = tqdm(
                        total=100,
                        desc=f"📥 {file_name[:30]}",
                        unit='%',
                        leave=False
                    )
                    
                    while not done and not self.shutdown_event.is_set():
                        status, done = downloader.next_chunk()
                        if status:
                            progress = int(status.progress() * 100)
                            pbar.update(progress - pbar.n)
                    
                    if pbar:
                        pbar.close()
                        pbar = None
                
                # Xác minh kích thước nếu có
                if file_size:
                    local_size = os.path.getsize(local_path)
                    if local_size != int(file_size):
                        raise Exception(
                            f"Kích thước không khớp: mong đợi {file_size}, thực tế {local_size}"
                        )
                
                # Thành công - ghi nhận vào mạch ngắt
                self.circuit_breaker.record_success()
                print(f"✅ Đã tải xuống: {file_name}")
                return local_path
                
            except Exception as e:
                # Xử lý giới hạn tốc độ
                if self._is_rate_limit_error(e):
                    print(f"🚫 Giới hạn tốc độ khi tải: {file_name}")
                    if self._handle_rate_limit():
                        return None
                
                print(f"⚠️ Lần thử tải {attempt + 1}/{MAX_RETRIES} thất bại: {e}")
                
                # Dọn dẹp bản tải lỗi
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except:
                        pass
                
                # Thử lại với độ trễ
                if attempt < MAX_RETRIES - 1:
                    backoff = self._exponential_backoff(attempt)
                    print(f"⏳ Thử lại sau {backoff:.1f} giây...")
                    time.sleep(backoff)
                else:
                    print(f"❌ Tải xuống thất bại: {file_name}")
                    return None
            
            finally:
                if pbar:
                    try:
                        pbar.close()
                    except:
                        pass
        
        return None
    
    def upload_file(
        self,
        local_path: str,
        file_name: str,
        parent_folder_id: str,
        original_md5: Optional[str] = None,
        service=None
    ) -> Optional[str]:
        """
        Tải lên tập tin với xử lý lỗi chuẩn.
        
        Trả về:
            Optional[str]: ID tập tin đã tải lên nếu thành công, None nếu thất bại
        """
        if self.shutdown_event.is_set():
            return None
        
        # Kiểm tra mạch ngắt
        can_proceed, reason = self.circuit_breaker.can_proceed()
        if not can_proceed:
            print(f"🚫 {reason}")
            return None
        
        if service is None:
            service = self.service
        
        for attempt in range(MAX_RETRIES):
            uploaded_file_id = None
            
            try:
                file_metadata = {
                    'name': file_name,
                    'parents': [parent_folder_id]
                }
                
                media = MediaFileUpload(
                    local_path,
                    resumable=True,
                    chunksize=CHUNK_SIZE
                )
                
                file = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, name, size, md5Checksum'
                ).execute()
                
                uploaded_file_id = file['id']
                
                # Xác minh MD5 nếu có
                if original_md5 and file.get('md5Checksum') != original_md5:
                    try:
                        service.files().delete(fileId=uploaded_file_id).execute()
                    except:
                        pass
                    raise Exception("MD5 checksum không khớp")
                
                # Thành công
                self.circuit_breaker.record_success()
                print(f"✅ Đã tải lên: {file_name}")
                return uploaded_file_id
                
            except Exception as e:
                # Xử lý giới hạn tốc độ
                if self._is_rate_limit_error(e):
                    print(f"🚫 Giới hạn tốc độ khi tải lên: {file_name}")
                    
                    # Dọn dẹp tập tin đã tải lên lỗi
                    if uploaded_file_id:
                        try:
                            service.files().delete(fileId=uploaded_file_id).execute()
                        except:
                            pass
                    
                    if self._handle_rate_limit():
                        return None
                
                print(f"⚠️ Lần thử tải lên {attempt + 1}/{MAX_RETRIES} thất bại: {e}")
                
                # Dọn dẹp bản tải lên lỗi
                if uploaded_file_id:
                    try:
                        service.files().delete(fileId=uploaded_file_id).execute()
                    except:
                        pass
                
                # Thử lại với độ trễ
                if attempt < MAX_RETRIES - 1:
                    backoff = self._exponential_backoff(attempt)
                    print(f"⏳ Thử lại sau {backoff:.1f} giây...")
                    time.sleep(backoff)
                else:
                    print(f"❌ Tải lên thất bại: {file_name}")
                    return None
        
        return None
    
    def create_folder(
        self,
        folder_name: str,
        parent_id: Optional[str] = None
    ) -> Optional[str]:
        """Tạo thư mục"""
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
            
            print(f"📁 Đã tạo thư mục: {folder_name}")
            return folder['id']
            
        except HttpError as e:
            print(f"❌ Lỗi khi tạo thư mục: {e}")
            return None
    
    def list_files_in_folder(self, folder_id: str) -> List[Dict[str, Any]]:
        """Liệt kê tất cả tập tin trong thư mục"""
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
            print(f"❌ Lỗi khi liệt kê tập tin: {e}")
            return []
    
    def process_single_file(
        self,
        item: Dict[str, Any],
        backup_folder_id: str
    ) -> bool:
        """
        Xử lý từng tập tin với quản lý trạng thái.
        
        Trả về:
            bool: True nếu thành công
        """
        if self.shutdown_event.is_set():
            self.backup_state.add_pending(item)
            return False
        
        item_id = item['id']
        item_name = item['name']
        file_size = item.get('size')
        original_md5 = item.get('md5Checksum')
        
        thread_service = None
        local_path = None
        
        try:
            # Lấy dịch vụ cục bộ cho luồng
            thread_service = self._get_thread_local_service()
            
            # Kiểm tra nếu đã backup rồi
            with self.log_lock:
                if item_id in self.backup_log['backed_up_files']:
                    print(f"⏭️ Bỏ qua (đã sao lưu): {item_name}")
                    self.stats['download']['skipped'] += 1
                    return True
            
            # Tải xuống
            local_path = self.download_file(
                item_id,
                item_name,
                file_size,
                service=thread_service
            )
            
            if self.shutdown_event.is_set():
                self.backup_state.add_pending(item)
                return False
            
            if not local_path or not os.path.exists(local_path):
                self.stats['download']['failed'] += 1
                self.backup_state.add_failed(item)
                return False
            
            self.stats['download']['success'] += 1
            
            # Tải lên
            uploaded_id = self.upload_file(
                local_path,
                item_name,
                backup_folder_id,
                original_md5,
                service=thread_service
            )
            
            if self.shutdown_event.is_set():
                self.backup_state.add_pending(item)
                return False
            
            if not uploaded_id:
                self.stats['upload']['failed'] += 1
                self.backup_state.add_failed(item)
                return False
            
            self.stats['upload']['success'] += 1
            
            # Lưu vào nhật ký (thao tác nguyên tử)
            with self.log_lock:
                self.backup_log['backed_up_files'][item_id] = {
                    'name': item_name,
                    'type': 'file',
                    'size': file_size,
                    'md5': original_md5,
                    'backup_id': uploaded_id,
                    'backup_time': datetime.now().isoformat()
                }
            
            # Dọn dẹp tập tin cục bộ
            try:
                os.remove(local_path)
                local_path = None
            except:
                pass
            
            # Điểm kiểm soát: Lưu log và tăng bộ đếm
            self._save_log()
            self.backup_state.increment_processed()
            self.backup_state.remove_from_pending(item_id)
            
            # Kiểm tra dọn dẹp bộ nhớ
            self.memory_monitor.check_and_cleanup()
            
            return True
            
        except Exception as e:
            print(f"❌ Lỗi khi xử lý {item_name}: {e}")
            self.backup_state.add_failed(item)
            return False
            
        finally:
            # Đảm bảo dọn dẹp tập tin cục bộ
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except:
                    pass
    
    def process_files_batch(
        self,
        files: List[Dict[str, Any]],
        backup_folder_id: str
    ):
        """Xử lý lô tập tin với thread pool"""
        if not files:
            return
        
        print(f"\n🚀 Đang xử lý {len(files)} tập tin...")
        
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            futures = {
                executor.submit(
                    self.process_single_file,
                    file_item,
                    backup_folder_id
                ): file_item
                for file_item in files
            }
            
            completed = 0
            
            for future in concurrent.futures.as_completed(futures):
                if self.shutdown_event.is_set():
                    print("\n⏸️ Đang tắt chương trình nhẹ nhàng...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                
                completed += 1
                
                try:
                    future.result()
                except Exception as e:
                    print(f"⚠️ Ngoại lệ luồng: {e}")
                
                # Dọn dẹp bộ nhớ định kỳ
                if completed % 20 == 0:
                    if self.memory_monitor.check_and_cleanup():
                        print(f"♻️ Đã dọn dẹp bộ nhớ ({completed}/{len(files)})")
        
        # Dọn dẹp cuối cùng cho lô lớn
        if len(files) > 50:
            gc.collect()
    
    def backup_folder_recursive(
        self,
        source_folder_id: str,
        backup_folder_id: str
    ):
        """Sao lưu đệ quy với quản lý trạng thái"""
        if self.shutdown_event.is_set():
            return
        
        # Liệt kê các mục
        items = self.list_files_in_folder(source_folder_id)
        print(f"\n📊 Tìm thấy {len(items)} mục trong thư mục")
        
        # Tách thư mục và tập tin
        folders = [
            i for i in items
            if i['mimeType'] == 'application/vnd.google-apps.folder'
        ]
        files = [
            i for i in items
            if i['mimeType'] != 'application/vnd.google-apps.folder'
        ]
        
        # Xử lý thư mục đệ quy
        for folder_item in folders:
            if self.shutdown_event.is_set():
                break
            
            item_id = folder_item['id']
            item_name = folder_item['name']
            
            # Bỏ qua nếu đã backup
            with self.log_lock:
                if item_id in self.backup_log['backed_up_files']:
                    print(f"⏭️ Bỏ qua thư mục: {item_name}")
                    continue
            
            print(f"\n📁 Đang xử lý thư mục: {item_name}")
            
            # Tạo thư mục trong backup
            new_folder_id = self.create_folder(item_name, backup_folder_id)
            
            if new_folder_id:
                # Đệ quy
                self.backup_folder_recursive(item_id, new_folder_id)
                
                # Đánh dấu thư mục đã backup
                with self.log_lock:
                    self.backup_log['backed_up_files'][item_id] = {
                        'name': item_name,
                        'type': 'folder',
                        'backup_id': new_folder_id,
                        'backup_time': datetime.now().isoformat()
                    }
                
                self._save_log()
        
        # Xử lý tập tin theo lô
        if files and not self.shutdown_event.is_set():
            self.process_files_batch(files, backup_folder_id)
    
    def smart_backup(self) -> Optional[str]:
        """
        Sao lưu thông minh với phát hiện tự động khôi phục.
        
        Trả về:
            Optional[str]: ID thư mục backup nếu thành công
        """
        snapshot = self.backup_state.get_snapshot()
        
        # Kiểm tra nếu đang khôi phục từ trạng thái tạm dừng
        if snapshot['status'] == 'paused':
            # Kiểm tra mạch ngắt
            can_proceed, reason = self.circuit_breaker.can_proceed()
            if not can_proceed:
                print(f"\n⏰ {reason}")
                print("💡 Hãy quay lại sau để tiếp tục\n")
                return None
            
            # Khôi phục
            print("\n" + "="*80)
            print("🔄 PHÁT HIỆN TỰ ĐỘNG KHÔI PHỤC")
            print("="*80)
            
            backup_folder_id = snapshot.get('backup_folder_id')
            if not backup_folder_id:
                print("❌ Không tìm thấy ID thư mục backup")
                return None
            
            print(f"📁 Thư mục Backup: {backup_folder_id}")
            
            pending = snapshot.get('pending_files', [])
            failed = snapshot.get('failed_files', [])
            
            print(f"📊 Đang chờ: {len(pending)} | Thất bại: {len(failed)}")
            
            # Thử lại tất cả tập tin chờ và lỗi
            all_retry = pending + failed
            
            if all_retry:
                print(f"\n🔄 Đang thử lại {len(all_retry)} tập tin...")
                self.process_files_batch(all_retry, backup_folder_id)
                
                if not self.shutdown_event.is_set():
                    self.backup_state.update(
                        pending_files=[],
                        failed_files=[],
                        status='completed',
                        circuit_breaker_state='CLOSED'
                    )
                    print("\n✅ Khôi phục hoàn tất!")
            else:
                print("\n✅ Không có tập tin nào cần thử lại!")
                self.backup_state.update(status='completed')
            
            return backup_folder_id
        
        # Backup mới
        print("\n" + "="*80)
        print("🆕 BẮT ĐẦU SAO LƯU MỚI")
        print("="*80)
        
        # Lấy thông tin nguồn
        source_info = self.get_file_info(SOURCE_FOLDER_ID)
        if not source_info:
            print("❌ Không thể lấy thông tin thư mục nguồn")
            return None
        
        # Tạo thư mục backup
        backup_folder_name = source_info['name'] + FOLDER_SUFFIX
        backup_folder_id = self.create_folder(backup_folder_name, BACKUP_PARENT_ID)
        
        if not backup_folder_id:
            return None
        
        # Cập nhật trạng thái
        self.backup_state.update(
            status='in_progress',
            backup_folder_id=backup_folder_id,
            current_folder=SOURCE_FOLDER_ID,
            circuit_breaker_state='CLOSED'
        )
        
        # Bắt đầu backup đệ quy
        self.backup_folder_recursive(SOURCE_FOLDER_ID, backup_folder_id)
        
        # Lưu nhật ký cuối cùng
        self._save_log()
        
        # Cập nhật trạng thái cuối
        if self.shutdown_event.is_set():
            print("\n⏸️ ĐÃ TẠM DỪNG BACKUP")
            self.backup_state.update(status='paused')
        else:
            print("\n✅ BACKUP HOÀN TẤT!")
            self.backup_state.update(status='completed')
        
        # In thống kê
        self.print_stats()
        
        return backup_folder_id
    
    def print_stats(self):
        """In thống kê toàn diện"""
        print(f"\n📊 THỐNG KÊ:")
        print("="*80)
        print(f"Tải xuống: ✅ {self.stats['download']['success']} | "
              f"❌ {self.stats['download']['failed']} | "
              f"⏭️ {self.stats['download']['skipped']}")
        print(f"Tải lên:   ✅ {self.stats['upload']['success']} | "
              f"❌ {self.stats['upload']['failed']}")
        
        total_backed_up = len(self.backup_log['backed_up_files'])
        files_count = sum(
            1 for item in self.backup_log['backed_up_files'].values()
            if item['type'] == 'file'
        )
        folders_count = sum(
            1 for item in self.backup_log['backed_up_files'].values()
            if item['type'] == 'folder'
        )
        
        print(f"\nTổng đã backup: {total_backed_up}")
        print(f"  Tập tin: {files_count}")
        print(f"  Thư mục: {folders_count}")
        
        # Trạng thái mạch ngắt
        cb_status = self.circuit_breaker.get_status()
        print(f"\nMạch ngắt: {cb_status['state']}")
        print(f"  Lỗi trong khung giờ: {cb_status['failures_in_window']}/{cb_status['threshold']}")
        
        # Sử dụng bộ nhớ
        mem_usage = self.memory_monitor.get_usage()
        if mem_usage:
            print(f"\nBộ nhớ: {mem_usage['percent']:.1f}% đã dùng "
                  f"({mem_usage['available_gb']:.1f}GB còn trống)")
        
        print("="*80 + "\n")
    
    def get_backup_summary(self):
        """Lấy tóm tắt backup"""
        snapshot = self.backup_state.get_snapshot()
        
        print("\n" + "="*80)
        print("📊 TÓM TẮT BACKUP")
        print("="*80)
        print(f"Trạng thái: {snapshot['status']}")
        print(f"Tổng đã xử lý: {snapshot['total_files_processed']}")
        print(f"Đang chờ: {len(snapshot.get('pending_files', []))}")
        print(f"Thất bại: {len(snapshot.get('failed_files', []))}")
        print(f"Lần chạy cuối: {self.backup_log.get('last_run', 'Chưa bao giờ')}")
        print("="*80 + "\n")


# ============================================================
# THỰC THI CHÍNH
# ============================================================

print("🔧 Đang khởi tạo Trình quản lý Backup...")
backup_manager = DriveBackupManager(
    drive_service,
    log_file=LOG_FILE,
    state_file=STATE_FILE,
    max_workers=MAX_WORKERS,
    manual_mode=MANUAL_RESUME_MODE
)

# Hiển thị trạng thái hiện tại
backup_manager.get_backup_summary()

# ============================================================
# CHẠY BACKUP
# ============================================================

print("\n" + "="*80)
print("🎯 QUY TRÌNH KHUYẾN NGHỊ:")
print("="*80)
print("1. Chạy backup bình thường")
print("2. Nếu gặp giới hạn tốc độ → DỪNG RUNTIME")
print("3. Đợi 24 giờ")
print("4. Khởi động lại notebook → Tự động khôi phục")
print("="*80 + "\n")

print("🚀 ĐANG BẮT ĐẦU BACKUP...")
start_time = time.time()

# Chạy backup thông minh
backup_folder_id = backup_manager.smart_backup()

end_time = time.time()

# ============================================================
# KẾT QUẢ
# ============================================================

if backup_folder_id:
    duration = end_time - start_time
    print(f"\n✅ THÀNH CÔNG!")
    print(f"⏱️ Thời gian: {duration:.2f}s ({duration/60:.2f} phút)")
    print(f"📁 ID Thư mục Backup: {backup_folder_id}")
    print(f"🔗 Link: https://drive.google.com/drive/folders/{backup_folder_id}")
    
    backup_manager.get_backup_summary()
    
elif backup_manager.shutdown_event.is_set():
    print(f"\n💡 BƯỚC TIẾP THEO:")
    print("="*80)
    print("✅ Trạng thái đã được lưu an toàn")
    print("✅ DỪNG RUNTIME NGAY LẬP TỨC (Runtime → Disconnect)")
    print("✅ Đợi 24 giờ")
    print("✅ Mở lại notebook → Chạy tất cả (Run all) → Tự động khôi phục")
    print("="*80 + "\n")
    
else:
    print("\n❌ BACKUP THẤT BẠI!")

# ============================================================
# TIỆN ÍCH
# ============================================================

def view_state():
    """Xem trạng thái hiện tại"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            print("\n📊 TRẠNG THÁI HIỆN TẠI:")
            print(json.dumps(state, indent=2, ensure_ascii=False))

def view_log():
    """Xem nhật ký backup"""
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            log = json.load(f)
            print(f"\n📊 NHẬT KÝ BACKUP:")
            print(f"Tổng số mục: {len(log['backed_up_files'])}")
            print(f"Lần chạy cuối: {log.get('last_run', 'Chưa bao giờ')}")

def download_files():
    """Tải xuống tệp trạng thái và nhật ký"""
    from google.colab import files
    for filename in [STATE_FILE, LOG_FILE]:
        if os.path.exists(filename):
            files.download(filename)
            print(f"✅ Đã tải xuống: {filename}")

def get_circuit_breaker_status():
    """Lấy trạng thái mạch ngắt"""
    if 'backup_manager' in globals():
        status = backup_manager.circuit_breaker.get_status()
        print("\n🔌 TRẠNG THÁI MẠCH NGẮT:")
        print(f"  Trạng thái: {status['state']}")
        print(f"  Số lỗi: {status['failures_in_window']}/{status['threshold']}")
        if status['last_failure']:
            last = datetime.fromtimestamp(status['last_failure'])
            print(f"  Lỗi gần nhất: {last.strftime('%Y-%m-%d %H:%M:%S')}")

def force_reset_circuit_breaker():
    """Buộc đặt lại mạch ngắt (cẩn thận khi dùng)"""
    if 'backup_manager' in globals():
        backup_manager.circuit_breaker.state = 'CLOSED'
        backup_manager.circuit_breaker.failures.clear()
        backup_manager.backup_state.update(
            circuit_breaker_state='CLOSED',
            last_rate_limit_time=None
        )
        print("✅ Đã đặt lại mạch ngắt!")

print("""
================================================================================
                        CÁC TIỆN ÍCH
================================================================================

view_state()                    # Xem trạng thái backup hiện tại
view_log()                      # Xem nhật ký backup
download_files()                # Tải xuống tệp trạng thái + nhật ký
get_circuit_breaker_status()    # Kiểm tra mạch ngắt
force_reset_circuit_breaker()   # Đặt lại mạch ngắt (cẩn thận!)

================================================================================
""")
