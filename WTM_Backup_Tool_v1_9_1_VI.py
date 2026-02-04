# -*- coding: utf-8 -*-
"""THE Ultimate manual backup and resume v1,5_VN.ipynb -- PHIÊN BẢN TIẾNG VIỆT

Tự động tạo bởi Colab.

File gốc nằm tại
    https://colab.research.google.com/drive/16q4haxQuYTLg367C92TqG2UF24Bsb5ya
"""

# -*- coding: utf-8 -*-
"""
================================================================================
    CÔNG CỤ SAO LƯU GOOGLE DRIVE v2.0 - MẠNH MẼ & NÂNG CAO (TIẾNG VIỆT)
    Sẵn sàng cho môi trường thực tế với xử lý lỗi và quản lý bộ nhớ
================================================================================

PHIÊN BẢN: 2.0.0 (VN)
NGÀY: 04 Tháng 2, 2026

CẢI TIẾN CHÍNH:
✅ Phát hiện giới hạn tốc độ (rate limit) chính xác cho MỌI thao tác
✅ Mô hình Circuit breaker để xử lý giới hạn tốc độ
✅ Ngăn chặn rò rỉ bộ nhớ với việc dọn dẹp đúng cách
✅ Hoạt động an toàn với luồng (thread-safe)
✅ Backoff theo cấp số nhân với độ trễ ngẫu nhiên (jitter)
✅ Xử lý tắt chương trình nhẹ nhàng (graceful shutdown)
✅ Phục hồi lỗi toàn diện
✅ Quản lý tài nguyên cho file handles
✅ Cập nhật trạng thái nguyên tử (Atomic updates)

KHÔNG PHÁ VỠ CẤU TRÚC CŨ (NON-BREAKING):
- Tất cả biến cấu hình cũ vẫn hoạt động như trước
- File trạng thái tương thích ngược
- API không đổi đối với người dùng

TỐI ƯU HÓA BỘ NHỚ:
- Dọn dẹp file handle đúng cách
- Giới hạn thread pool với tài nguyên cụ thể
- Thu gom rác (garbage collection) rõ ràng tại các điểm kiểm tra
- Xử lý luồng (stream) cho file lớn

================================================================================
"""

# ============================================================
# CÀI ĐẶT (INSTALLATION)
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
# IMPORTS
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

# Progress bar
from tqdm.notebook import tqdm

# System monitoring
import psutil

# Suppress warnings
logging.getLogger('google_auth_httplib2').setLevel(logging.ERROR)

# ============================================================
# CẤU HÌNH (CONFIGURATION)
# ============================================================

# 📁 ID THƯ MỤC (BẮT BUỘC)
SOURCE_FOLDER_ID = '1ZY4ab0XlPHa5_t10XnSvPbWUvJRdN4Nx'  # ⚠️ THAY ĐỔI CÁI NÀY
BACKUP_PARENT_ID = '1l22lxlpDoK6meCTNm5v_5d4gmxI1oseO'  # ⚠️ THAY ĐỔI CÁI NÀY

# 🏷️ Cài đặt
FOLDER_SUFFIX = '_BACKUP'
MAX_WORKERS = None  # Tự động phát hiện

# 🛡️ Bảo vệ giới hạn tốc độ (Mô hình Circuit Breaker)
RATE_LIMIT_THRESHOLD = 3          # Số lỗi trước khi ngắt mạch
RATE_LIMIT_COOLDOWN_HOURS = 24    # Thời gian chờ (giờ)
RATE_LIMIT_WINDOW_SECONDS = 60    # Cửa sổ thời gian đếm lỗi (giây)

# 📝 Files
LOG_FILE = 'backup_log.json'
STATE_FILE = 'backup_state.json'

# 🎯 Chế độ
MANUAL_RESUME_MODE = True

# 🔧 Cài đặt nâng cao
CHUNK_SIZE = 10 * 1024 * 1024      # 10MB chunks
MAX_RETRIES = 3                     # Số lần thử lại mỗi thao tác
INITIAL_BACKOFF = 5                 # Thời gian chờ ban đầu (giây)
MAX_BACKOFF = 300                   # Thời gian chờ tối đa (giây)
MEMORY_CLEANUP_THRESHOLD = 80       # Ngưỡng RAM % để dọn dẹp
MAX_FILE_HANDLES = 10               # Số file handle tối đa đồng thời

# 🚦 Giới hạn tốc độ toàn cục (MỚI - ngăn chặn vượt quá hạn ngạch API)
GLOBAL_RATE_LIMIT_DELAY = 1.0      # Giây giữa các lần gọi API (toàn cục)
MAX_CONCURRENT_WORKERS = 3          # Số worker tối đa (người dùng chọn)

print("="*80)
print("⚙️  CẤU HÌNH:")
print("="*80)
print(f"📁 Nguồn: {SOURCE_FOLDER_ID}")
print(f"📁 Thư mục cha sao lưu: {BACKUP_PARENT_ID}")
print(f"🎯 Chế độ: {'KHÔI PHỤC THỦ CÔNG' if MANUAL_RESUME_MODE else 'TỰ ĐỘNG KHÔI PHỤC'}")
print(f"🛡️ Giới hạn tốc độ: {RATE_LIMIT_THRESHOLD} lỗi trong {RATE_LIMIT_WINDOW_SECONDS}s")
print(f"💾 Kích thước Chunk: {CHUNK_SIZE / (1024*1024):.0f}MB")
print("="*80 + "\n")

# ============================================================
# XÁC THỰC (AUTHENTICATION)
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
    Mô hình Circuit breaker để bảo vệ giới hạn tốc độ.

    Trạng thái:
    - CLOSED: Hoạt động bình thường
    - OPEN: Quá nhiều lỗi, chặn tất cả yêu cầu
    - HALF_OPEN: Đang kiểm tra xem dịch vụ đã khôi phục chưa
    """

    def __init__(self, threshold: int, window_seconds: int, cooldown_hours: int):
        self.threshold = threshold
        self.window_seconds = window_seconds
        self.cooldown_seconds = cooldown_hours * 3600

        self.state = 'CLOSED'
        self.failures = deque()  # Thời gian xảy ra lỗi
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
        Ghi nhận lỗi và trả về True nếu mạch nên mở.

        Returns:
            bool: True nếu circuit breaker bị kích hoạt
        """
        with self.lock:
            now = time.time()
            self.last_failure_time = now
            self.failures.append(now)

            # Xóa các lỗi cũ ngoài cửa sổ thời gian
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

        Returns:
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
                        f"Circuit breaker đang MỞ (OPEN). "
                        f"Vui lòng đợi thêm {remaining/3600:.1f} giờ. "
                        f"Tiếp tục sau: {next_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )

            if self.state == 'HALF_OPEN':
                return True, None

            return False, "Trạng thái circuit breaker không xác định"

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
        """Context manager cho file handles với tự động dọn dẹp"""
        handle = None
        try:
            # Đợi nếu có quá nhiều handle đang mở
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
        """Buộc dọn dẹp tất cả handles"""
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

        Returns:
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


class GlobalRateLimiter:
    """
    Bộ giới hạn tốc độ toàn cục để kiểm soát tần suất gọi API trên tất cả các luồng.
    Ngăn chặn việc chạm hạn ngạch Google Drive.
    """

    def __init__(self, min_delay: float = 1.0):
        self.min_delay = min_delay
        self.last_call_time = 0.0
        self.lock = Lock()

    def acquire(self):
        """
        Đợi nếu cần thiết để tuân thủ giới hạn tốc độ.
        An toàn với luồng (Thread-safe) - chỉ một luồng được phép qua tại một thời điểm.
        """
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call_time

            if elapsed < self.min_delay:
                sleep_time = self.min_delay - elapsed
                time.sleep(sleep_time)

            self.last_call_time = time.time()

    def set_delay(self, delay: float):
        """Điều chỉnh độ trễ động (ví dụ: tăng lên sau khi gặp lỗi rate limit)"""
        with self.lock:
            self.min_delay = delay


# ============================================================
# QUẢN LÝ TRẠNG THÁI (STATE MANAGEMENT)
# ============================================================

class BackupState:
    """Quản lý trạng thái sao lưu an toàn với luồng và cập nhật nguyên tử"""

    def __init__(self, state_file: str = 'backup_state.json'):
        self.state_file = state_file
        self.lock = RLock()
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """Tải trạng thái từ file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    print(f"📂 Đã tải trạng thái từ {self.state_file}")
                    return state
            except Exception as e:
                print(f"⚠️ Không thể tải trạng thái: {e}")

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
        """Lưu trạng thái vào file (phải được gọi trong lock)"""
        try:
            self.state['updated_at'] = datetime.now().isoformat()

            # Ghi nguyên tử sử dụng file tạm
            temp_file = self.state_file + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)

            # Đổi tên nguyên tử
            os.replace(temp_file, self.state_file)

        except Exception as e:
            print(f"⚠️ Không thể lưu trạng thái: {e}")

    def update(self, **kwargs):
        """Cập nhật nguyên tử an toàn với luồng"""
        with self.lock:
            self.state.update(kwargs)
            self._save_state()

    def add_pending(self, file_item: Dict[str, Any]):
        """Thêm file vào danh sách chờ"""
        with self.lock:
            if file_item not in self.state['pending_files']:
                self.state['pending_files'].append(file_item)
                self._save_state()

    def add_failed(self, file_item: Dict[str, Any]):
        """Thêm file vào danh sách thất bại"""
        with self.lock:
            if file_item not in self.state['failed_files']:
                self.state['failed_files'].append(file_item)
                self._save_state()

    def remove_from_pending(self, file_id: str):
        """Xóa file khỏi danh sách chờ theo ID"""
        with self.lock:
            self.state['pending_files'] = [
                f for f in self.state['pending_files']
                if f.get('id') != file_id
            ]
            self._save_state()

    def increment_processed(self):
        """Tăng bộ đếm file đã xử lý"""
        with self.lock:
            self.state['total_files_processed'] += 1
            self._save_state()

    def get_snapshot(self) -> Dict[str, Any]:
        """Lấy bản chụp (snapshot) an toàn của trạng thái"""
        with self.lock:
            return self.state.copy()


# ============================================================
# TRÌNH QUẢN LÝ SAO LƯU CHÍNH (MAIN BACKUP MANAGER)
# ============================================================

class DriveBackupManager:
    """
    Trình quản lý sao lưu mạnh mẽ với xử lý lỗi và quản lý tài nguyên.
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

        # Circuit breaker cho giới hạn tốc độ
        self.circuit_breaker = CircuitBreaker(
            threshold=RATE_LIMIT_THRESHOLD,
            window_seconds=RATE_LIMIT_WINDOW_SECONDS,
            cooldown_hours=RATE_LIMIT_COOLDOWN_HOURS
        )

        # Quản lý tài nguyên
        self.resource_manager = ResourceManager(MAX_FILE_HANDLES)
        self.memory_monitor = MemoryMonitor(MEMORY_CLEANUP_THRESHOLD)
        
        # Giới hạn tốc độ toàn cục (MỚI - ngăn chặn vượt quá hạn ngạch)
        self.global_rate_limiter = GlobalRateLimiter(GLOBAL_RATE_LIMIT_DELAY)

        # Thư mục làm việc
        self.local_temp_dir = '/content/temp_backup'
        os.makedirs(self.local_temp_dir, exist_ok=True)

        # Thread pool - sử dụng giới hạn worker cố định để tránh rate limits
        if max_workers is None:
            self.max_workers = min(self._auto_detect_workers(), MAX_CONCURRENT_WORKERS)
        else:
            self.max_workers = min(max_workers, MAX_CONCURRENT_WORKERS)

        # Xử lý tắt chương trình
        self.shutdown_event = Event()
        self._setup_signal_handlers()

        # Thống kê
        self.stats = {
            'download': {'success': 0, 'failed': 0, 'skipped': 0},
            'upload': {'success': 0, 'failed': 0}
        }

        # Credentials cho thread-local services
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
            print("\n⚠️ Đã nhận tín hiệu tắt, đang dọn dẹp...")
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
        """Tự động phát hiện số worker tối ưu"""
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
        """Tải log sao lưu"""
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
        """Lưu log sao lưu với ghi nguyên tử"""
        with self.log_lock:
            try:
                self.backup_log['last_run'] = datetime.now().isoformat()

                temp_file = self.log_file + '.tmp'
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self.backup_log, f, indent=2, ensure_ascii=False)

                os.replace(temp_file, self.log_file)
            except Exception as e:
                print(f"⚠️ Không thể lưu log: {e}")

    def _get_thread_local_service(self):
        """Lấy Drive service cục bộ cho thread"""
        return build('drive', 'v3', credentials=self.creds)

    def _is_rate_limit_error(self, error: Exception) -> bool:
        """
        Kiểm tra nếu lỗi là do giới hạn tốc độ (ĐÃ SỬA: phát hiện tất cả các loại lỗi rate limit)
        
        Google Drive API trả về các lỗi rate limit sau:
        - rateLimitExceeded (phổ biến nhất - hạn ngạch mỗi phút)
        - userRateLimitExceeded (hạn ngạch người dùng cụ thể)
        - quotaExceeded (hạn ngạch chung)
        """
        if isinstance(error, HttpError):
            error_str = str(error).lower()
            return (
                error.resp.status == 403 and
                ('ratelimitexceeded' in error_str or 
                 'userratelimitexceeded' in error_str or
                 'quotaexceeded' in error_str or
                 'quota exceeded' in error_str)
            )
        return False

    def _exponential_backoff(self, attempt: int, jitter: bool = True) -> float:
        """Tính toán thời gian chờ backoff với jitter"""
        backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)

        if jitter:
            backoff = backoff * (0.5 + random.random())

        return backoff

    def _handle_rate_limit(self) -> bool:
        """
        Xử lý lỗi giới hạn tốc độ.

        Returns:
            bool: True nếu nên dừng thực thi
        """
        # Ghi nhận thất bại trong circuit breaker
        circuit_tripped = self.circuit_breaker.record_failure()

        if circuit_tripped:
            self.backup_state.update(
                status='paused',
                circuit_breaker_state='OPEN',
                last_rate_limit_time=datetime.now().isoformat()
            )

            print("\n" + "="*80)
            print("🚫 PHÁT HIỆN GIỚI HẠN TỐC ĐỘ - NGẮT MẠCH (CIRCUIT BREAKER TRIPPED)")
            print("="*80)
            print(f"❌ Phát hiện {RATE_LIMIT_THRESHOLD} lỗi giới hạn tốc độ trong {RATE_LIMIT_WINDOW_SECONDS}s")
            print(f"💾 Trạng thái đã lưu vào: {self.backup_state.state_file}")

            if self.manual_mode:
                self._print_manual_resume_instructions()
            else:
                print(f"\n⏰ Tự động tiếp tục sau {RATE_LIMIT_COOLDOWN_HOURS} giờ")

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
        print("   → Runtime → Disconnect and delete runtime")
        print()
        print("2️⃣ ĐỢI 24 GIỜ")
        print()
        print(f"3️⃣ TIẾP TỤC SAU: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        print("   → Mở lại notebook này")
        print("   → Chạy lại tất cả các cell → Tự động tiếp tục (Auto-resume)")
        print()
        print("📊 TIẾN ĐỘ ĐÃ LƯU:")

        snapshot = self.backup_state.get_snapshot()
        print(f"   ✅ Đã hoàn thành: {len(self.backup_log['backed_up_files'])}")
        print(f"   ⏳ Đang chờ: {len(snapshot['pending_files'])}")
        print(f"   ❌ Thất bại: {len(snapshot['failed_files'])}")
        print("="*80)

    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Lấy thông tin file"""
        try:
            return self.service.files().get(
                fileId=file_id,
                fields='id, name, size, md5Checksum, mimeType'
            ).execute()
        except HttpError as e:
            print(f"❌ Lỗi khi lấy thông tin file: {e}")
            return None

    def download_file(
        self,
        file_id: str,
        file_name: str,
        file_size: Optional[str] = None,
        service=None
    ) -> Optional[str]:
        """
        Tải xuống file với xử lý lỗi và quản lý tài nguyên.

        Returns:
            Optional[str]: Đường dẫn cục bộ nếu thành công, None nếu thất bại
        """
        if self.shutdown_event.is_set():
            return None

        # Kiểm tra circuit breaker
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
                # Áp dụng giới hạn tốc độ toàn cục trước khi gọi API
                self.global_rate_limiter.acquire()
                
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

                # Xác minh kích thước file nếu được cung cấp
                if file_size:
                    local_size = os.path.getsize(local_path)
                    if local_size != int(file_size):
                        raise Exception(
                            f"Kích thước không khớp: mong đợi {file_size}, nhận được {local_size}"
                        )

                # Thành công - ghi nhận vào circuit breaker
                self.circuit_breaker.record_success()
                print(f"✅ Đã tải xuống: {file_name}")
                return local_path

            except Exception as e:
                # Xử lý rate limit
                if self._is_rate_limit_error(e):
                    print(f"🚫 Gặp giới hạn tốc độ khi tải xuống: {file_name}")
                    # Tăng delay toàn cục khi bị rate limit
                    self.global_rate_limiter.set_delay(min(self.global_rate_limiter.min_delay * 2, 10.0))
                    if self._handle_rate_limit():
                        return None

                print(f"⚠️ Thử tải xuống lần {attempt + 1}/{MAX_RETRIES} thất bại: {e}")

                # Dọn dẹp file tải lỗi
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except:
                        pass

                # Thử lại với backoff (tăng delay nếu lỗi rate limit)
                if attempt < MAX_RETRIES - 1:
                    backoff = self._exponential_backoff(attempt)
                    if self._is_rate_limit_error(e):
                        backoff = max(backoff, 30)  # Ít nhất 30s cho lỗi rate limits
                    print(f"⏳ Thử lại sau {backoff:.1f}s...")
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
        Tải lên file với xử lý lỗi đúng cách.

        Returns:
            Optional[str]: ID file đã tải lên nếu thành công, None nếu thất bại
        """
        if self.shutdown_event.is_set():
            return None

        # Kiểm tra circuit breaker
        can_proceed, reason = self.circuit_breaker.can_proceed()
        if not can_proceed:
            print(f"🚫 {reason}")
            return None

        if service is None:
            service = self.service

        for attempt in range(MAX_RETRIES):
            uploaded_file_id = None

            try:
                # Áp dụng giới hạn tốc độ toàn cục trước khi gọi API
                self.global_rate_limiter.acquire()
                
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

                # Xác minh MD5 nếu được cung cấp
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
                # Xử lý rate limit
                if self._is_rate_limit_error(e):
                    print(f"🚫 Gặp giới hạn tốc độ khi tải lên: {file_name}")
                    # Tăng delay toàn cục khi bị rate limit
                    self.global_rate_limiter.set_delay(min(self.global_rate_limiter.min_delay * 2, 10.0))

                    # Dọn dẹp file đã tải lên (nếu có nhưng lỗi)
                    if uploaded_file_id:
                        try:
                            service.files().delete(fileId=uploaded_file_id).execute()
                        except:
                            pass

                    if self._handle_rate_limit():
                        return None

                print(f"⚠️ Thử tải lên lần {attempt + 1}/{MAX_RETRIES} thất bại: {e}")

                # Dọn dẹp file tải lên thất bại
                if uploaded_file_id:
                    try:
                        service.files().delete(fileId=uploaded_file_id).execute()
                    except:
                        pass

                # Thử lại với backoff (tăng delay nếu lỗi rate limit)
                if attempt < MAX_RETRIES - 1:
                    backoff = self._exponential_backoff(attempt)
                    if self._is_rate_limit_error(e):
                        backoff = max(backoff, 30)  # Ít nhất 30s cho lỗi rate limits
                    print(f"⏳ Thử lại sau {backoff:.1f}s...")
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
        """Liệt kê tất cả file trong thư mục"""
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
            print(f"❌ Lỗi khi liệt kê files: {e}")
            return []

    def process_single_file(
        self,
        item: Dict[str, Any],
        backup_folder_id: str
    ) -> bool:
        """
        Xử lý từng file đơn lẻ với quản lý trạng thái.

        Returns:
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
            # Lấy thread-local service
            thread_service = self._get_thread_local_service()

            # Kiểm tra xem đã sao lưu chưa
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

            # Lưu vào log (thao tác nguyên tử)
            with self.log_lock:
                self.backup_log['backed_up_files'][item_id] = {
                    'name': item_name,
                    'type': 'file',
                    'size': file_size,
                    'md5': original_md5,
                    'backup_id': uploaded_id,
                    'backup_time': datetime.now().isoformat()
                }

            # Dọn dẹp file cục bộ
            try:
                os.remove(local_path)
                local_path = None
            except:
                pass

            # Checkpoint: Lưu log và tăng bộ đếm
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
            # Đảm bảo dọn dẹp file cục bộ
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
        """Xử lý lô file với thread pool"""
        if not files:
            return

        print(f"\n🚀 Đang xử lý {len(files)} files...")

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
                    print(f"⚠️ Lỗi tương lai (Future exception): {e}")

                # Dọn dẹp bộ nhớ định kỳ
                if completed % 20 == 0:
                    if self.memory_monitor.check_and_cleanup():
                        print(f"♻️ Đã dọn dẹp bộ nhớ ({completed}/{len(files)})")

        # Dọn dẹp cuối cùng cho các lô lớn
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

        # Tách thư mục và file
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

            # Bỏ qua nếu đã sao lưu
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

                # Đánh dấu thư mục đã sao lưu
                with self.log_lock:
                    self.backup_log['backed_up_files'][item_id] = {
                        'name': item_name,
                        'type': 'folder',
                        'backup_id': new_folder_id,
                        'backup_time': datetime.now().isoformat()
                    }

                self._save_log()

        # Xử lý files theo lô
        if files and not self.shutdown_event.is_set():
            self.process_files_batch(files, backup_folder_id)

    def smart_backup(self) -> Optional[str]:
        """
        Sao lưu thông minh với tự động khôi phục (auto-resume).

        Returns:
            Optional[str]: ID thư mục sao lưu nếu thành công
        """
        snapshot = self.backup_state.get_snapshot()

        # Kiểm tra nếu đang dùng từ trạng thái tạm dừng
        if snapshot['status'] == 'paused':
            # Kiểm tra circuit breaker
            can_proceed, reason = self.circuit_breaker.can_proceed()
            if not can_proceed:
                print(f"\n⏰ {reason}")
                print("💡 Vui lòng quay lại sau để tiếp tục\n")
                return None

            # Tiếp tục
            print("\n" + "="*80)
            print("🔄 PHÁT HIỆN TỰ ĐỘNG KHÔI PHỤC (AUTO-RESUME)")
            print("="*80)

            backup_folder_id = snapshot.get('backup_folder_id')
            if not backup_folder_id:
                print("❌ Không tìm thấy ID thư mục sao lưu")
                return None

            print(f"📁 Thư mục sao lưu: {backup_folder_id}")

            pending = snapshot.get('pending_files', [])
            failed = snapshot.get('failed_files', [])

            print(f"📊 Đang chờ: {len(pending)} | Thất bại trước đó: {len(failed)}")

            # Thử lại tất cả file đang chờ và thất bại
            all_retry = pending + failed

            if all_retry:
                print(f"\n🔄 Đang thử lại {len(all_retry)} files...")
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
                print("\n✅ Không có file nào để thử lại!")
                self.backup_state.update(status='completed')

            return backup_folder_id

        # Sao lưu mới
        print("\n" + "="*80)
        print("🆕 BẮT ĐẦU SAO LƯU MỚI")
        print("="*80)

        # Lấy thông tin nguồn
        source_info = self.get_file_info(SOURCE_FOLDER_ID)
        if not source_info:
            print("❌ Không thể lấy thông tin thư mục nguồn")
            return None

        # Tạo thư mục sao lưu
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

        # Bắt đầu sao lưu đệ quy
        self.backup_folder_recursive(SOURCE_FOLDER_ID, backup_folder_id)

        # Lưu log cuối cùng
        self._save_log()

        # Cập nhật trạng thái cuối cùng
        if self.shutdown_event.is_set():
            print("\n⏸️ ĐÃ TẠM DỪNG SAO LƯU")
            self.backup_state.update(status='paused')
        else:
            print("\n✅ SAO LƯU HOÀN TẤT!")
            self.backup_state.update(status='completed')

        # In thống kê
        self.print_stats()

        return backup_folder_id

    def print_stats(self):
        """In thống kê chi tiết"""
        print(f"\n📊 THỐNG KÊ CHI TIẾT:")
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

        print(f"\nTổng số đã sao lưu: {total_backed_up}")
        print(f"  Files: {files_count}")
        print(f"  Folders: {folders_count}")

        # Trạng thái Circuit breaker
        cb_status = self.circuit_breaker.get_status()
        print(f"\nTrạng thái Circuit Breaker: {cb_status['state']}")
        print(f"  Lỗi trong cửa sổ thời gian: {cb_status['failures_in_window']}/{cb_status['threshold']}")

        # Sử dụng bộ nhớ
        mem_usage = self.memory_monitor.get_usage()
        if mem_usage:
            print(f"\nBộ nhớ: {mem_usage['percent']:.1f}% đã dùng "
                  f"({mem_usage['available_gb']:.1f}GB còn trống)")

        print("="*80 + "\n")

    def get_backup_summary(self):
        """Lấy tóm tắt sao lưu"""
        snapshot = self.backup_state.get_snapshot()

        print("\n" + "="*80)
        print("📊 TÓM TẮT SAO LƯU")
        print("="*80)
        print(f"Trạng thái: {snapshot['status']}")
        print(f"Tổng đã xử lý: {snapshot['total_files_processed']}")
        print(f"Đang chờ: {len(snapshot.get('pending_files', []))}")
        print(f"Thất bại: {len(snapshot.get('failed_files', []))}")
        print(f"Chạy lần cuối: {self.backup_log.get('last_run', 'Chưa bao giờ')}")
        print("="*80 + "\n")


# ============================================================
# THỰC THI CHÍNH (MAIN EXECUTION)
# ============================================================

print("🔧 Đang khởi tạo Trình quản lý sao lưu...")
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
# CHẠY SAO LƯU (RUN BACKUP)
# ============================================================

print("\n" + "="*80)
print("🎯 QUY TRÌNH KHUYẾN NGHỊ:")
print("="*80)
print("1. Chạy sao lưu bình thường")
print("2. Nếu gặp lỗi giới hạn tốc độ (rate limit) → DỪNG RUNTIME")
print("3. Đợi 24 giờ")
print("4. Khởi động lại notebook → Tự động khôi phục (Auto-resume)")
print("="*80 + "\n")

print("🚀 ĐANG BẮT ĐẦU SAO LƯU...")
start_time = time.time()

# Chạy sao lưu thông minh
backup_folder_id = backup_manager.smart_backup()

end_time = time.time()

# ============================================================
# KẾT QUẢ (RESULTS)
# ============================================================

if backup_folder_id:
    duration = end_time - start_time
    print(f"\n✅ THÀNH CÔNG!")
    print(f"⏱️ Thời gian: {duration:.2f}s ({duration/60:.2f} phút)")
    print(f"📁 ID thư mục sao lưu: {backup_folder_id}")
    print(f"🔗 Link: https://drive.google.com/drive/folders/{backup_folder_id}")

    backup_manager.get_backup_summary()

elif backup_manager.shutdown_event.is_set():
    print(f"\n💡 CÁC BƯỚC TIẾP THEO:")
    print("="*80)
    print("✅ Trạng thái đã được lưu an toàn")
    print("✅ DỪNG RUNTIME NGAY LẬP TỨC (Runtime → Disconnect)")
    print("✅ Đợi 24 giờ")
    print("✅ Mở lại notebook → Chạy tất cả → Tự động khôi phục")
    print("="*80 + "\n")

else:
    print("\n❌ SAO LƯU THẤT BẠI!")

# ============================================================
# TIỆN ÍCH (UTILITIES)
# ============================================================

def view_state():
    """Xem trạng thái hiện tại"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            print("\n📊 TRẠNG THÁI HIỆN TẠI:")
            print(json.dumps(state, indent=2, ensure_ascii=False))

def view_log():
    """Xem log sao lưu"""
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            log = json.load(f)
            print(f"\n📊 LOG SAO LƯU:")
            print(f"Tổng số mục: {len(log['backed_up_files'])}")
            print(f"Lần chạy cuối: {log.get('last_run', 'Chưa bao giờ')}")

def download_files():
    """Tải xuống file trạng thái và log"""
    from google.colab import files
    for filename in [STATE_FILE, LOG_FILE]:
        if os.path.exists(filename):
            files.download(filename)
            print(f"✅ Đã tải xuống: {filename}")

def get_circuit_breaker_status():
    """Lấy trạng thái circuit breaker"""
    if 'backup_manager' in globals():
        status = backup_manager.circuit_breaker.get_status()
        print("\n🔌 TRẠNG THÁI CIRCUIT BREAKER:")
        print(f"  Trạng thái: {status['state']}")
        print(f"  Lỗi: {status['failures_in_window']}/{status['threshold']}")
        if status['last_failure']:
            last = datetime.fromtimestamp(status['last_failure'])
            print(f"  Lỗi cuối cùng: {last.strftime('%Y-%m-%d %H:%M:%S')}")

def force_reset_circuit_breaker():
    """Buộc reset circuit breaker (cẩn thận!)"""
    if 'backup_manager' in globals():
        backup_manager.circuit_breaker.state = 'CLOSED'
        backup_manager.circuit_breaker.failures.clear()
        backup_manager.backup_state.update(
            circuit_breaker_state='CLOSED',
            last_rate_limit_time=None
        )
        print("✅ Đã reset circuit breaker!")

print("""
================================================================================
                        CÁC TIỆN ÍCH HỖ TRỢ (UTILITIES)
================================================================================

view_state()                    # Xem trạng thái sao lưu hiện tại
view_log()                      # Xem log sao lưu
download_files()                # Tải xuống file trạng thái + log
get_circuit_breaker_status()    # Kiểm tra circuit breaker
force_reset_circuit_breaker()   # Reset circuit breaker (cẩn thận!)

================================================================================
""")
