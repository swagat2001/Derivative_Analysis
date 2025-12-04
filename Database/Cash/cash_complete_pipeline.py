"""
NSE FULL BHAVCOPY + SECURITY DELIVERABLE DATA DOWNLOADER
=========================================================
Downloads: sec_bhavdata_full_*.csv files only
WITH TIMEOUT, RETRY LOGIC, AND LIVE PROGRESS
"""

import urllib.request
import urllib.error
import socket
import os
from datetime import datetime, timedelta
import pandas as pd
import time

# ===========================================
# Configuration
# ===========================================
save_folder = "D:/NSE_EOD_CASH"

# Timeout settings
DOWNLOAD_TIMEOUT = 30  # seconds
MAX_RETRIES = 2
RETRY_DELAY = 2  # seconds

# ===========================================
# Download with Timeout
# ===========================================
def download_with_timeout(url, filepath, timeout=DOWNLOAD_TIMEOUT):
    """
    Download file with timeout and progress
    """
    # Set socket timeout
    socket.setdefaulttimeout(timeout)
    
    try:
        # Add headers to avoid blocking
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
        
        with urllib.request.urlopen(req, timeout=timeout) as response:
            with open(filepath, 'wb') as out_file:
                out_file.write(response.read())
        
        return True
    
    except socket.timeout:
        raise TimeoutError(f"Download timeout after {timeout}s")
    
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise FileNotFoundError("File not available (404)")
        else:
            raise Exception(f"HTTP Error {e.code}")
    
    except Exception as e:
        raise Exception(f"Download failed: {str(e)[:50]}")


# ===========================================
# Download Full Bhavcopy Files
# ===========================================
def download_full_bhavcopy():
    """
    Download NSE Full Bhavcopy + Security Deliverable data
    File format: sec_bhavdata_full_DDMMYYYY.csv
    """
    print("\n" + "="*80)
    print("NSE FULL BHAVCOPY + SECURITY DELIVERABLE DATA DOWNLOADER")
    print("="*80 + "\n")
    
    # Create save folder
    os.makedirs(save_folder, exist_ok=True)
    
    # Get date range
    print("Enter date range to download:")
    start_date_str = input("Start date (DD-MM-YYYY) [e.g., 22-03-2026]: ").strip()
    
    if not start_date_str:
        start_date = datetime.now().date()
    else:
        try:
            start_date = datetime.strptime(start_date_str, "%d-%m-%Y").date()
        except:
            print("⚠️ Invalid date format. Using today.")
            start_date = datetime.now().date()
    
    end_date_str = input("End date (DD-MM-YYYY) [default: same as start]: ").strip()
    
    if not end_date_str:
        end_date = start_date
    else:
        try:
            end_date = datetime.strptime(end_date_str, "%d-%m-%Y").date()
        except:
            print("⚠️ Invalid date format. Using start date.")
            end_date = start_date
    
    # Generate weekday dates
    date_range = []
    delta_days = (end_date - start_date).days
    
    for i in range(delta_days + 1):
        d = start_date + timedelta(days=i)
        if d.weekday() < 5:  # Monday=0, Friday=4
            date_range.append(d)
    
    if not date_range:
        print("⚠️ No weekday dates in range")
        return False
    
    print(f"\n📅 Downloading {len(date_range)} file(s)")
    print(f"⏱️  Timeout: {DOWNLOAD_TIMEOUT}s per file | Retries: {MAX_RETRIES}\n")
    print("="*80)
    
    # Track downloaded files
    downloaded_files = []
    failed_dates = []
    skipped_dates = []
    
    start_time = time.time()
    
    for idx, date_obj in enumerate(date_range, 1):
        date_display = date_obj.strftime("%d-%m-%Y")
        date_str_ddmmyyyy = date_obj.strftime("%d%m%Y")  # 03222026
        
        # Progress indicator
        progress = f"[{idx}/{len(date_range)}]"
        print(f"{progress:12} {date_display}...", end=" ", flush=True)
        
        # Build URL
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str_ddmmyyyy}.csv"
        
        # Build local filename
        filename = f"sec_bhavdata_full_{date_str_ddmmyyyy}.csv"
        filepath = os.path.join(save_folder, filename)
        
        # Check if already downloaded
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                row_count = len(df)
                file_size = os.path.getsize(filepath)
                
                if file_size < 1024*1024:
                    size_str = f"{file_size/1024:.1f} KB"
                else:
                    size_str = f"{file_size/(1024*1024):.2f} MB"
                
                print(f"⏭️  Already exists ({row_count} stocks, {size_str})")
                
                downloaded_files.append({
                    'date': date_display,
                    'filename': filename,
                    'filepath': filepath,
                    'rows': row_count,
                    'columns': len(df.columns),
                    'size': size_str,
                    'size_bytes': file_size,
                    'status': 'existing'
                })
                continue
            
            except:
                # If file corrupt, delete and redownload
                os.remove(filepath)
        
        # Try download with retries
        success = False
        last_error = None
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                if attempt > 0:
                    print(f"\n{'':12}          Retry {attempt}/{MAX_RETRIES}...", end=" ", flush=True)
                    time.sleep(RETRY_DELAY)
                
                # Download with timeout
                download_with_timeout(url, filepath, timeout=DOWNLOAD_TIMEOUT)
                
                # Verify file
                df = pd.read_csv(filepath)
                row_count = len(df)
                col_count = len(df.columns)
                
                # Get file size
                file_size = os.path.getsize(filepath)
                if file_size < 1024*1024:
                    size_str = f"{file_size/1024:.1f} KB"
                else:
                    size_str = f"{file_size/(1024*1024):.2f} MB"
                
                print(f"✅ ({row_count} stocks, {col_count} cols, {size_str})")
                
                # Store file info
                downloaded_files.append({
                    'date': date_display,
                    'filename': filename,
                    'filepath': filepath,
                    'rows': row_count,
                    'columns': col_count,
                    'size': size_str,
                    'size_bytes': file_size,
                    'status': 'downloaded'
                })
                
                success = True
                break
            
            except FileNotFoundError:
                print(f"⚠️  Not available (holiday)")
                skipped_dates.append(date_display)
                success = True  # Don't retry for 404
                break
            
            except TimeoutError as e:
                last_error = "Timeout"
                if attempt == MAX_RETRIES:
                    print(f"❌ Timeout after {MAX_RETRIES} retries")
            
            except Exception as e:
                last_error = str(e)[:30]
                if attempt == MAX_RETRIES:
                    print(f"❌ {last_error}")
        
        if not success:
            failed_dates.append((date_display, last_error))
        
        # Show progress every 10 files
        if idx % 10 == 0:
            elapsed = time.time() - start_time
            avg_time = elapsed / idx
            remaining = (len(date_range) - idx) * avg_time
            print(f"{'':12}          Progress: {idx}/{len(date_range)} | "
                  f"Elapsed: {int(elapsed)}s | ETA: {int(remaining)}s")
    
    total_time = time.time() - start_time
    
    # ====================================================================
    # DISPLAY DOWNLOADED FILES IN TERMINAL
    # ====================================================================
    print("\n" + "="*80)
    print("DOWNLOAD SUMMARY")
    print("="*80)
    
    print(f"\n⏱️  Total time: {int(total_time)}s ({total_time/60:.1f} minutes)")
    print(f"✅ Successfully downloaded: {len([f for f in downloaded_files if f['status']=='downloaded'])} new file(s)")
    print(f"⏭️  Already existed: {len([f for f in downloaded_files if f['status']=='existing'])} file(s)")
    print(f"⚠️  Skipped (holidays): {len(skipped_dates)} date(s)")
    print(f"❌ Failed: {len(failed_dates)} date(s)")
    
    if downloaded_files:
        print("\n" + "="*80)
        print("DOWNLOADED FILES")
        print("="*80 + "\n")
        
        # Show last 10 files (or all if less than 10)
        display_files = downloaded_files[-10:] if len(downloaded_files) > 10 else downloaded_files
        
        if len(downloaded_files) > 10:
            print(f"Showing last 10 of {len(downloaded_files)} files:\n")
        
        # Table header
        print(f"{'#':<4} {'DATE':<12} {'FILENAME':<35} {'STOCKS':<8} {'SIZE':<10} {'STATUS':<10}")
        print("-" * 85)
        
        # Table rows
        total_size = 0
        total_stocks = 0
        
        for i, file_info in enumerate(display_files, 1):
            status_icon = "📥" if file_info['status'] == 'downloaded' else "✓"
            print(f"{i:<4} {file_info['date']:<12} {file_info['filename']:<35} "
                  f"{file_info['rows']:<8} {file_info['size']:<10} {status_icon}")
            total_size += file_info['size_bytes']
            total_stocks += file_info['rows']
        
        # Summary row
        print("-" * 85)
        
        # Calculate total size for ALL files
        total_size_all = sum(f['size_bytes'] for f in downloaded_files)
        total_stocks_all = sum(f['rows'] for f in downloaded_files)
        
        if total_size_all < 1024*1024:
            total_size_str = f"{total_size_all/1024:.1f} KB"
        else:
            total_size_str = f"{total_size_all/(1024*1024):.2f} MB"
        
        print(f"{'ALL':<4} {'':<12} {len(downloaded_files)} files{'':<26} "
              f"{total_stocks_all:<8} {total_size_str:<10}")
        
        print(f"\n📁 Location: {save_folder}")
    
    # Show failed dates
    if failed_dates:
        print("\n" + "="*80)
        print("FAILED DOWNLOADS")
        print("="*80 + "\n")
        for date_str, error in failed_dates[:10]:  # Show first 10
            print(f"  ❌ {date_str}: {error}")
        if len(failed_dates) > 10:
            print(f"  ... and {len(failed_dates)-10} more")
    
    print("\n" + "="*80 + "\n")
    
    return len(downloaded_files) > 0


# ===========================================
# MAIN EXECUTION
# ===========================================
if __name__ == "__main__":
    print("\n" + "="*80)
    print("NSE FULL BHAVCOPY + SECURITY DELIVERABLE DATA")
    print("="*80)
    print("\nDownloads: sec_bhavdata_full_DDMMYYYY.csv")
    print("Contains: Full equity data + deliverable quantity/percentage")
    print("\n⚠️  NO DATABASE UPLOAD - Files saved to folder only")
    print("="*80 + "\n")
    
    input("Press Enter to start download...")
    
    try:
        result = download_full_bhavcopy()
        
        if result:
            print("✅ Download complete! Check summary above.")
        else:
            print("⚠️ No files downloaded - check dates or NSE website availability")
    
    except KeyboardInterrupt:
        print("\n\n⚠️ Download cancelled by user")
    
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    
    input("\nPress Enter to exit...")
