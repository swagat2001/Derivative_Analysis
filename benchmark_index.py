
import sys
import os
import time

# Add project root to path
sys.path.append(os.getcwd())

from Analysis_Tools.app.models.index_model import get_index_stocks_with_data

def benchmark_full():
    print("Benchmarking get_index_stocks_with_data('nifty50')...")

    start_time = time.time()
    data = get_index_stocks_with_data("nifty50")
    end_time = time.time()

    print(f"Total time: {end_time - start_time:.4f} seconds")
    print(f"Items returned: {len(data)}")

if __name__ == "__main__":
    benchmark_full()
