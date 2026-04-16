"""
main.py
-------
End-to-end orchestrator for the Power Consumption Analytics Pipeline.

Run order:
  1. simulate_data.py  → power_readings.csv
  2. load_to_db.py     → energy.db
  3. queries.py        → prints analytics results to stdout
  4. visualize.py      → dashboard.png

Usage:
    python main.py              # run full pipeline
    python main.py --skip-sim   # skip data generation (reuse existing CSV)
"""

import argparse
import os
import sys
import time

# ── Make sure we can import sibling modules regardless of CWD ────────────────
sys.path.insert(0, os.path.dirname(__file__))

import simulate_data
import load_to_db
import queries
import visualize


DIVIDER = "\n" + "─" * 64 + "\n"


def banner(step: str) -> None:
    print(f"\n{'━'*64}")
    print(f"  STEP: {step}")
    print(f"{'━'*64}")


def main():
    parser = argparse.ArgumentParser(description="Power Analytics Pipeline")
    parser.add_argument("--skip-sim", action="store_true",
                        help="Skip data simulation (reuse existing power_readings.csv)")
    args = parser.parse_args()

    t0 = time.time()

    # ── Change working directory so relative paths resolve correctly ─────────
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # ── Step 1: Generate simulation data ────────────────────────────────────
    if args.skip_sim and os.path.exists("power_readings.csv"):
        print("⏭   Skipping simulation — using existing power_readings.csv")
    else:
        banner("1 / 4  —  Simulating smart-meter data")
        simulate_data.main()

    # ── Step 2: Load into SQLite ─────────────────────────────────────────────
    banner("2 / 4  —  Loading data into SQLite (energy.db)")
    load_to_db.main()

    # ── Step 3: Run SQL analytics queries ────────────────────────────────────
    banner("3 / 4  —  Running analytics queries")
    query_results = queries.main()

    # ── Step 4: Generate visualisation dashboard ─────────────────────────────
    banner("4 / 4  —  Generating visualisation dashboard")
    visualize.main()

    # ── Summary ──────────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    print(DIVIDER)
    print(f"  Pipeline complete in {elapsed:.1f}s")
    print(f"  Outputs:")
    for filename in ("power_readings.csv", "energy.db", "dashboard.png"):
        path = os.path.join(script_dir, filename)
        if os.path.exists(path):
            size = os.path.getsize(path)
            print(f"    ✓  {filename:<25} ({size/1024:.1f} KB)")
        else:
            print(f"    ✗  {filename}  NOT FOUND")
    print(DIVIDER)


if __name__ == "__main__":
    main()
