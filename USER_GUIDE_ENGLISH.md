# Google Drive Folder Backup Tool - User Guide (English)

## 📖 Overview

This tool helps you backup entire folders from Google Drive with advanced features including multi-threaded downloads, automatic validation, and intelligent retry mechanisms. It's optimized for speed and reliability.

---

## 🌟 Key Features

- ✅ **Full Folder Backup**: Backup entire shared folders from Google Drive
- ✅ **Automatic Naming**: Creates backup folder with "_BACKUP" suffix
- ✅ **File Validation**: Checks file size and MD5 checksum before deletion
- ✅ **Smart Logging**: JSON-based logging prevents duplicate backups
- ✅ **Auto Cleanup**: Automatically removes local files after successful upload
- ✅ **Retry Mechanism**: Handles network errors with intelligent retry
- ✅ **Progress Tracking**: Real-time progress monitoring
- 🚀 **Multi-threaded Downloads**: 3-5 files downloaded simultaneously
- 🚀 **Auto-optimization**: Automatically adjusts workers based on available RAM/CPU
- 🚀 **No Timeout Warnings**: Clean execution without unnecessary warnings

---

## 🚀 Quick Start Guide

### Step 1: Open in Google Colab

1. Upload the `.ipynb` file to Google Drive
2. Open with Google Colab
3. The script is ready to use!

### Step 2: Configure Settings

Locate the **MAIN CONFIGURATION** section at the top of the script:

```python
# ⚙️  MAIN CONFIGURATION - EDIT HERE

# 📁 Source folder ID (from Google Drive URL)
SOURCE_FOLDER_ID = 'your-source-folder-id-here'

# 📁 Backup destination folder ID (optional)
BACKUP_PARENT_ID = 'your-backup-parent-id-here'  # or None for root

# 🏷️  Backup folder suffix
FOLDER_SUFFIX = '_BACKUP'

# 🚀 Number of concurrent download threads
MAX_WORKERS = None  # None = auto-detect, or set 4, 6, 8...
```

### Step 3: Get Folder IDs

**How to get Google Drive Folder ID:**

1. Open Google Drive in your browser
2. Navigate to the folder you want to backup
3. Look at the URL in your browser:
   ```
   https://drive.google.com/drive/folders/1ZY4ab0XlPHa5_t10XnSvPbWUvJRdN4Nx
                                            ↑ This is the Folder ID
   ```
4. Copy everything after `/folders/`

**Example:**
- Source folder URL: `https://drive.google.com/drive/folders/1ABC123xyz`
- Source folder ID: `1ABC123xyz`

### Step 4: Run the Script

1. Click **Runtime** → **Run all** in Google Colab menu
2. When prompted, authenticate with your Google account
3. The backup will start automatically
4. Monitor progress in the output

---

## ⚙️ Configuration Options

### SOURCE_FOLDER_ID
- **Required**: Yes
- **Description**: The ID of the folder you want to backup
- **How to find**: See Step 3 above
- **Example**: `'1ZY4ab0XlPHa5_t10XnSvPbWUvJRdN4Nx'`

### BACKUP_PARENT_ID
- **Required**: No
- **Description**: The ID of the folder where backup will be saved
- **Default**: `None` (saves to root "My Drive")
- **Example**: `'1XYZ789abc'` or `None`

### FOLDER_SUFFIX
- **Required**: No
- **Description**: Suffix added to backup folder name
- **Default**: `'_BACKUP'`
- **Example**: If source is "Photos", backup will be "Photos_BACKUP"

### MAX_WORKERS
- **Required**: No
- **Description**: Number of simultaneous download threads
- **Default**: `None` (auto-detect based on system resources)
- **Recommended values**: 
  - `None` - Let system auto-detect (recommended)
  - `3-4` - For systems with limited RAM (< 4GB)
  - `5-8` - For systems with good RAM (8GB+)

---

## 📊 Understanding the Output

### During Backup

```
🚀 Number of workers in use: 6
💾 Available RAM: 12.5 GB
🖥️  CPU cores: 2
⚙️  Optimal workers: 6

📊 Found 45 items in folder

🚀 Starting download of 40 files with 6 concurrent threads...
📥 Downloading example_file.pdf...
✅ Downloaded: example_file.pdf
✅ Uploaded: example_file.pdf (ID: 1ABC...)
🗑️  Cleaned up local file: example_file.pdf
```

### Statistics

```
📊 Download Stats: ✅ 38 success | ❌ 2 failed | ⏭️  5 skipped
📊 Upload Stats: ✅ 38 success | ❌ 0 failed
```

### Final Report

```
📋 DETAILED BACKUP REPORT
========================================
📁 Total folders: 5
📄 Total files: 38
💾 Total size: 2.45 GB (2,631,456,789 bytes)
✅ Files with MD5 validation: 38/38
🕐 Last backup time: 2026-02-01T14:30:25
```

---

## 🔧 Advanced Features

### 1. Incremental Backup

The tool automatically tracks backed-up files in `backup_log.json`:
- Already backed-up files are **skipped**
- Only new or modified files are backed up
- Saves time and bandwidth

### 2. Automatic Retry

If files fail to download/upload:
- Automatic retry up to 3 times per file
- After initial backup, failed files are retried 2 more times
- Final report shows any files that couldn't be backed up

### 3. Validation

Every file is validated:
- **Size check**: Ensures downloaded file matches original size
- **MD5 checksum**: Verifies file integrity after upload
- **Verification**: Counts files in source vs backup folders

### 4. Memory Management

- Smart garbage collection prevents memory overflow
- Automatic cleanup of temporary files
- Optimized chunk sizes for fast transfers

---

## 📝 Common Use Cases

### Case 1: First-time Full Backup

```python
SOURCE_FOLDER_ID = '1ABC123xyz'
BACKUP_PARENT_ID = None  # Save to My Drive root
FOLDER_SUFFIX = '_BACKUP'
MAX_WORKERS = None  # Auto-detect
```

### Case 2: Backup to Specific Location

```python
SOURCE_FOLDER_ID = '1ABC123xyz'
BACKUP_PARENT_ID = '1XYZ789abc'  # Your "Backups" folder
FOLDER_SUFFIX = '_2026_Feb'
MAX_WORKERS = 6
```

### Case 3: Limited System Resources

```python
SOURCE_FOLDER_ID = '1ABC123xyz'
BACKUP_PARENT_ID = None
FOLDER_SUFFIX = '_BACKUP'
MAX_WORKERS = 3  # Use fewer threads
```

---

## 🛠️ Troubleshooting

### Problem: "Authentication Failed"

**Solution:**
1. In Colab, go to Runtime → Restart runtime
2. Run the authentication cell again
3. Make sure you're using the correct Google account

### Problem: "Folder ID not found"

**Solution:**
1. Check that the folder ID is correct
2. Make sure the folder is shared with you
3. Verify you have permission to access the folder

### Problem: "Too many files failed"

**Solution:**
1. Check your internet connection
2. Try reducing MAX_WORKERS to 3-4
3. Run the backup again (it will skip successful files)

### Problem: "Out of memory error"

**Solution:**
1. Set MAX_WORKERS to a lower value (3 or 4)
2. The script will automatically manage memory better
3. Consider backing up in smaller batches

### Problem: "Backup is very slow"

**Solution:**
1. Increase MAX_WORKERS to 6-8 (if you have good RAM)
2. Check your internet speed
3. Large files naturally take longer

---

## 📚 Additional Utilities

### View Backup Log

Run this cell to see all backed-up files:

```python
if os.path.exists('backup_log.json'):
    with open('backup_log.json', 'r', encoding='utf-8') as f:
        log_data = json.load(f)
        print(json.dumps(log_data, indent=2, ensure_ascii=False))
```

### Download Backup Log

Save the log file to your computer:

```python
from google.colab import files
files.download('backup_log.json')
```

### Reset Backup Log

⚠️ **WARNING**: This will erase all backup history and backup everything again!

```python
reset_log = {'backed_up_files': {}, 'last_run': None}
with open('backup_log.json', 'w', encoding='utf-8') as f:
    json.dump(reset_log, f, indent=2, ensure_ascii=False)
print("🔄 Backup log has been reset!")
```

---

## ⚡ Performance Tips

1. **Optimal Workers**: Leave MAX_WORKERS as `None` for best auto-detection
2. **Large Files**: For folders with many large files (>100MB), consider MAX_WORKERS = 3-4
3. **Many Small Files**: For folders with many small files, MAX_WORKERS = 6-8 works well
4. **Resume Capability**: If backup stops, just run again - it will skip completed files
5. **Internet Speed**: Faster internet = more workers beneficial

---

## 🔒 Privacy & Security

- **Local Processing**: Files are temporarily stored only in Colab's memory
- **Automatic Cleanup**: Local files are deleted immediately after upload
- **No External Sharing**: Your data never leaves Google's infrastructure
- **Authentication**: Uses official Google OAuth2 authentication
- **Permissions**: Only requires Drive API access

---

## 📊 Backup Strategy Recommendations

### For Small Folders (< 1GB, < 100 files)
- MAX_WORKERS: Auto or 4-6
- Expected time: 5-15 minutes
- Run frequency: Weekly or as needed

### For Medium Folders (1-10GB, 100-1000 files)
- MAX_WORKERS: Auto or 6-8
- Expected time: 30-90 minutes
- Run frequency: Weekly

### For Large Folders (> 10GB, > 1000 files)
- MAX_WORKERS: Auto or 4-6 (for stability)
- Expected time: 2+ hours
- Run frequency: Monthly
- Consider: Breaking into smaller sub-folders

---

## ❓ Frequently Asked Questions

**Q: Can I backup multiple folders at once?**
A: Change SOURCE_FOLDER_ID and run the script again for each folder.

**Q: Will this delete my original files?**
A: No! It only creates copies. Your original files remain untouched.

**Q: What happens if backup stops midway?**
A: Just run the script again. It will skip already backed-up files.

**Q: Can I schedule automatic backups?**
A: Not directly in Colab, but you can set reminders to run it periodically.

**Q: How much storage do I need?**
A: At least the same size as your source folder, plus some buffer.

**Q: Does it work with shared folders?**
A: Yes! As long as you have view/download permissions.

---

## 🆘 Support

If you encounter issues:

1. **Check the troubleshooting section** above
2. **Review the output messages** - they often indicate the problem
3. **Verify your configuration** settings are correct
4. **Try reducing MAX_WORKERS** if experiencing errors
5. **Check Google Drive storage quota** - you need available space

---

## 📝 Changelog

**Version 2.0 (Optimized)**
- Added multi-threaded downloads
- Auto-optimization based on system resources
- Improved memory management
- Enhanced error handling and retry logic
- Better progress tracking
- Removed timeout warnings

**Version 1.0**
- Initial release
- Basic backup functionality
- Single-threaded downloads

---

## ✅ Best Practices

1. **Test First**: Try backing up a small folder first
2. **Monitor Progress**: Watch the output for any errors
3. **Stable Connection**: Use stable internet connection
4. **Adequate Storage**: Ensure enough Google Drive space
5. **Keep Log File**: Download backup_log.json for records
6. **Regular Backups**: Run periodically for important folders
7. **Verify Results**: Check the final verification report

---

## 🎯 Success Indicators

Your backup is successful when you see:

```
✅ BACKUP COMPLETED SUCCESSFULLY!
✅ VERIFICATION PASSED: All files have been backed up!
📁 Backup folder: [Your folder name]_BACKUP
🔗 Link: https://drive.google.com/drive/folders/[ID]
```

---

**Happy Backing Up! 🎉**

*This tool is designed to make Google Drive backups simple, fast, and reliable. If you have suggestions or feedback, please let us know!*
