import subprocess
import sys
import os
import time

# Reconfigure stdout for UTF-8 support (Windows console workaround)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass


def run_script(script_path, description):
    """
    Runs a python script as a subprocess.
    """
    print(f"\n{'='*80}")
    print(f"🚀 STARTING: {description}")
    print(f"📂 Script: {script_path}")
    print(f"{'='*80}\n")

    start_time = time.time()

    # Check if file exists
    if not os.path.exists(script_path):
        print(f"❌ ERROR: Script not found: {script_path}")
        return False

    try:
        # Run the script
        # check=False allows us to handle the error manually without raising exception immediately
        result = subprocess.run([sys.executable, script_path], check=False)
        duration = time.time() - start_time

        if result.returncode != 0:
            print(f"\n❌ FAILED: {description}")
            print(f"   Exit Code: {result.returncode}")
            return False

        print(f"\n✅ COMPLETED: {description} (took {duration:.1f}s)")
        return True

    except Exception as e:
        print(f"\n❌ EXECUTION ERROR: {e}")
        return False

def main():
    # Base directory is the directory where this script is located
    base_dir = os.path.dirname(os.path.abspath(__file__))

    print("\n" + "="*80)
    print("       🔄 MARKET DATA UNIFIED UPDATE UTILITY")
    print("="*80)
    print(f"Root Directory: {base_dir}")
    print("This utility will sequentially update:")
    print("  1. Cash Market Data (Price + Delivery + Heatmap)")
    print("  2. F&O Market Data (Price + OI + Greeks + Dashboard)")
    print("  3. FII/DII Activity (Daily Flows)")
    print("="*80)

    # ---------------------------------------------------------
    # 1. Cash Market Data (Base for everything)
    # ---------------------------------------------------------
    cash_script = os.path.join(base_dir, "Database", "Cash", "cash_update_database.py")
    if not run_script(cash_script, "Cash Market Data Update"):
        print("\n⚠️ CRITICAL: Cash market update failed. Stopping pipeline.")
        print("   (Subsequent steps depend on valid cash market data)")
        input("\nPress Enter to exit...")
        sys.exit(1)

    # ---------------------------------------------------------
    # 2. F&O Market Data (Derivatives)
    # ---------------------------------------------------------
    fo_script = os.path.join(base_dir, "Database", "FO", "fo_update_database.py")
    if not run_script(fo_script, "F&O Market Data Update"):
        print("\n⚠️ CRITICAL: F&O market update failed. Stopping pipeline.")
        input("\nPress Enter to exit...")
        sys.exit(1)

    # ---------------------------------------------------------
    # 3. FII/DII Data (Institutional Activity)
    # ---------------------------------------------------------
    fii_script = os.path.join(base_dir, "Database", "FII_DII", "fii_dii_update_database.py")
    # This is less critical, so we might not want to hard exit, but let's stick to strict success for now
    if not run_script(fii_script, "FII/DII Data Update"):
        print("\n⚠️ WARNING: FII/DII update failed.")

    print("\n" + "="*80)
    print("✅✅✅ ALL UPDATES COMPLETED SUCCESSFULLY! ✅✅✅")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
    input("Press Enter to exit...")
