import subprocess
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

def run_script(name, script_path, args=None):
    """
    Runs a python script as a subprocess and captures output.
    """
    if args is None:
        args = []

    start_time = time.time()

    if not os.path.exists(script_path):
        return {"name": name, "success": False, "error": f"Script not found: {script_path}", "duration": 0}

    command = [sys.executable, script_path] + args
    cwd = os.path.dirname(script_path)

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        print(f"[{name}] ⏳ Starting...")
        process = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            input='\n',
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env
        )

        duration = time.time() - start_time
        output_lines = process.stdout.splitlines()

        if process.returncode == 0:
            out_str = "\n".join(output_lines).lower()

            already_up_to_date_markers = [
                "database is already up to date",
                "no new dates found",
                "no new bizdt dates",
                "up to date"
            ]

            new_data_markers = [
                "records saved",
                "downloaded",
                "inserted",
                "successfully added"
            ]

            new_data_added = False

            if any(marker in out_str for marker in already_up_to_date_markers):
                new_data_added = False
            elif any(marker in out_str for marker in new_data_markers) or "records saved: 0" not in out_str:

                new_data_added = True

            if "records saved: 0" in out_str:
                 new_data_added = False

            print(f"[{name}] ✅ COMPLETED in {duration:.1f}s")
            return {"name": name, "success": True, "output": output_lines, "duration": duration, "new_data_added": new_data_added}
        else:
            print(f"[{name}] ❌ FAILED with exit code {process.returncode} in {duration:.1f}s")
            return {"name": name, "success": False, "output": output_lines, "duration": duration, "error_code": process.returncode}

    except Exception as e:
        duration = time.time() - start_time
        print(f"[{name}] 🔴 EXCEPTION: {str(e)}")
        return {"name": name, "success": False, "output": [str(e)], "duration": duration}

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    tasks = [
        (
            "F&O Database",
            os.path.join(base_dir, "Database", "FO", "fo_update_database.py"),
            []
        ),
        (
            "Cash Database",
            os.path.join(base_dir, "Database", "Cash", "cash_update_database.py"),
            []
        ),
        (
            "FII/DII Historical",
            os.path.join(base_dir, "Database", "FII_DII", "fii_dii_update_database.py"),
            ["historical"]
        ),
        (
            "FII/DII Historical Cash",
            os.path.join(base_dir, "Database", "FII_DII", "fii_dii_update_database.py"),
            ["historical_cash"]
        )
    ]

    print("\n" + "="*80)
    print("       � MARKET DATA PARALLEL UPDATE UTILITY")
    print("="*80)
    print(f"Root Directory: {base_dir}")
    print("This utility will concurrently update:")
    for task_name, _, _ in tasks:
        print(f"  - {task_name}")
    print("="*80 + "\n")

    overall_start = time.time()
    results = []

    # Run all tasks concurrently
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        future_to_task = {
            executor.submit(run_script, name, path, args): name
            for name, path, args in tasks
        }

        for future in as_completed(future_to_task):
            task_name = future_to_task[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                print(f"[{task_name}] generated an exception: {exc}")
                results.append({"name": task_name, "success": False, "duration": 0})

    overall_end = time.time()

    # Print Summary
    print("\n" + "="*80)
    print(f"📊 TASK SUMMARY (Total Time: {overall_end - overall_start:.1f}s)")
    print("="*80)

    all_success = True
    for res in sorted(results, key=lambda x: x["name"]):
        if res.get("success"):
            status_msg = "🆕 NEW DATA ADDED" if res.get("new_data_added") else "✅ ALREADY UP TO DATE"
            print(f"{status_msg} - {res['name']} ({res.get('duration', 0):.1f}s)")
        else:
            all_success = False
            print(f"❌ FAILED - {res['name']} ({res.get('duration', 0):.1f}s)")
            if "error_code" in res:
                print(f"   Exit Code: {res['error_code']}")
            elif "error" in res:
                print(f"   Error: {res['error']}")

            # Print last 5 lines of output for debugging
            if "output" in res and res["output"]:
                print("   Last output:")
                for line in res["output"][-5:]:
                    print(f"     {line}")

    print("="*80)
    if all_success:
        print("🎉 ALL UPDATES COMPLETED SUCCESSFULLY!")
    else:
        print("⚠️ SOME UPDATES FAILED. Check the logs above.")

if __name__ == "__main__":
    main()
    print("\n")
    input("Press Enter to exit...")
