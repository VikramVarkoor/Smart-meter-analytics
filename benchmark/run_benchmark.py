"""
Smart Meter Analytics — Benchmark Harness
==========================================
Runs both the pandas/SQLite and PySpark pipelines at three data sizes,
records wall-clock time for each query, saves results to CSV, and
produces a grouped bar chart saved to results/benchmark_chart.png.

Usage:
    python benchmark/run_benchmark.py --data data/raw

The benchmark is deliberately honest:
  - At small scale, Spark's JVM startup and shuffle overhead mean it
    will likely be SLOWER than pandas. This is expected and shown.
  - Spark's advantage only kicks in at larger data sizes where pandas
    can no longer fit the working set in RAM or single-core sort dominates.
"""

import os
import sys
import time
import argparse
import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Make sure sibling packages resolve correctly when run from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.pandas_analytics import run_all_queries as run_pandas
from pipeline.spark_analytics   import create_spark_session, load_data
from pipeline.spark_analytics   import (
    q1_peak_consumption_window,
    q2_consecutive_overload_intervals,
    q3_anomaly_rate_by_week,
    q4_tou_tariff_cost,
)

QUERY_NAMES = [
    "q1_peak_window",
    "q2_overload_intervals",
    "q3_anomaly_rate_by_week",
    "q4_tou_tariff_cost",
]

SCALE_LABELS = {
    100_000:    "100k",
    500_000:    "500k",
    1_000_000:  "1M",
    3_000_000:  "3M",
    5_000_000:  "5M",
    10_000_000: "10M",
    10_512_000: "10.5M (full)",
}


# ---------------------------------------------------------------------------
# Spark sub-runner (reuses a single SparkSession across scales for fairness)
# ---------------------------------------------------------------------------

def run_spark_queries(spark, df_full, n_rows: int) -> dict:
    """
    Sample n_rows from the full Spark DF (using limit for repeatability),
    run all 4 queries, return timings dict.
    """
    timings = {}

    if n_rows is not None and n_rows < df_full.count():
        df = df_full.limit(n_rows).cache()
        df.count()  # materialise
    else:
        df = df_full

    df.createOrReplaceTempView("meter_readings")

    queries = [
        ("q1_peak_window",         lambda: q1_peak_consumption_window(spark, df)),
        ("q2_overload_intervals",   lambda: q2_consecutive_overload_intervals(spark, df)),
        ("q3_anomaly_rate_by_week", lambda: q3_anomaly_rate_by_week(spark, df)),
        ("q4_tou_tariff_cost",      lambda: q4_tou_tariff_cost(spark, df)),
    ]

    for name, fn in queries:
        t0 = time.time()
        result = fn()
        result.count()   # trigger execution
        timings[name] = round(time.time() - t0, 3)
        print(f"    spark {name}: {timings[name]:.3f}s")

    return timings


# ---------------------------------------------------------------------------
# pandas sub-runner
# ---------------------------------------------------------------------------

def run_pandas_at_scale(parquet_path: str, n_rows: int) -> dict:
    from pipeline.pandas_analytics import load_into_sqlite
    from pipeline.pandas_analytics import (
        q1_peak_consumption_window,
        q2_consecutive_overload_intervals,
        q3_anomaly_rate_by_week,
        q4_tou_tariff_cost,
    )

    conn, df, _ = load_into_sqlite(parquet_path, n_rows=n_rows)
    timings = {}

    pairs = [
        ("q1_peak_window",         lambda: q1_peak_consumption_window(conn)),
        ("q2_overload_intervals",   lambda: q2_consecutive_overload_intervals(conn, df)),
        ("q3_anomaly_rate_by_week", lambda: q3_anomaly_rate_by_week(conn)),
        ("q4_tou_tariff_cost",      lambda: q4_tou_tariff_cost(conn, df)),
    ]

    for name, fn in pairs:
        t0 = time.time()
        fn()
        timings[name] = round(time.time() - t0, 3)
        print(f"    pandas {name}: {timings[name]:.3f}s")

    conn.close()
    return timings


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------

def plot_benchmark(results: dict, out_path: str):
    """
    Grouped bar chart: x-axis = scale, groups = engines, bars = per-query time.
    One subplot per query for clarity.
    """
    scales = sorted(results.keys())
    scale_labels = [SCALE_LABELS.get(s, str(s)) for s in scales]
    n_queries = len(QUERY_NAMES)

    fig, axes = plt.subplots(1, n_queries, figsize=(16, 5), sharey=False)
    fig.suptitle("Smart Meter Analytics — pandas/SQLite vs PySpark\n(local mode, single machine)", fontsize=13)

    palette = {"pandas": "#4C8BBF", "spark": "#E8614C"}
    x = np.arange(len(scales))
    width = 0.35

    for ax_idx, qname in enumerate(QUERY_NAMES):
        ax = axes[ax_idx]
        pandas_times = [results[s]["pandas"].get(qname, 0) for s in scales]
        spark_times  = [results[s]["spark"].get(qname,  0) for s in scales]

        bars_p = ax.bar(x - width/2, pandas_times, width, label="pandas/SQLite", color=palette["pandas"], alpha=0.88)
        bars_s = ax.bar(x + width/2, spark_times,  width, label="PySpark",       color=palette["spark"],  alpha=0.88)

        # Value labels on bars
        for bar in bars_p + bars_s:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width()/2, h + 0.02, f"{h:.2f}s",
                        ha="center", va="bottom", fontsize=7.5)

        ax.set_title(qname.replace("_", " "), fontsize=9)
        ax.set_xticks(x)
        ax.set_xticklabels(scale_labels)
        ax.set_xlabel("Dataset size")
        ax.set_ylabel("Wall-clock time (s)")
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1fs"))
        ax.legend(fontsize=8)
        ax.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"\nChart saved → {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data",    default="data/raw",                      help="Parquet directory")
    parser.add_argument("--scales",  default="100000,1000000,5000000",        help="Comma-separated row counts to test")
    parser.add_argument("--out",     default="results/benchmark_results.json",help="JSON output path")
    parser.add_argument("--chart",   default="results/benchmark_chart.png",   help="Chart output path")
    args = parser.parse_args()

    scales = [int(s) for s in args.scales.split(",") if s.strip()]
    os.makedirs("results", exist_ok=True)

    # ------------------------------------------------------------------
    # Count actual rows available
    # ------------------------------------------------------------------
    print("Counting available rows in dataset…")
    sample = pd.read_parquet(args.data)
    actual_rows = len(sample)
    print(f"  Dataset contains {actual_rows:,} rows")
    del sample

    # Cap scales to what actually exists
    scales = [s for s in scales if s <= actual_rows] or [actual_rows]
    if actual_rows not in scales:
        scales.append(actual_rows)
    scales = sorted(set(scales))
    print(f"  Running benchmarks at scales: {[SCALE_LABELS.get(s, str(s)) for s in scales]}")

    results = {}

    # ------------------------------------------------------------------
    # Spark — create session once (JVM startup included only in first run)
    # ------------------------------------------------------------------
    print("\nInitialising Spark session…")
    t_spark_init = time.time()
    spark = create_spark_session("SmartMeterBenchmark")
    df_full = load_data(spark, args.data)
    df_full.cache()
    df_full.count()   # warm up
    spark_init_time = time.time() - t_spark_init
    print(f"  Spark session + data load: {spark_init_time:.1f}s")

    for scale in scales:
        label = SCALE_LABELS.get(scale, str(scale))
        print(f"\n{'='*60}")
        print(f"Scale: {label} rows")
        print(f"{'='*60}")
        results[scale] = {}

        print("  [pandas/SQLite]")
        results[scale]["pandas"] = run_pandas_at_scale(args.data, n_rows=scale)

        print("  [PySpark]")
        results[scale]["spark"]  = run_spark_queries(spark, df_full, n_rows=scale)

    spark.stop()

    # ------------------------------------------------------------------
    # Persist results
    # ------------------------------------------------------------------
    serialisable = {str(k): v for k, v in results.items()}
    with open(args.out, "w") as f:
        json.dump(serialisable, f, indent=2)
    print(f"\nResults saved → {args.out}")

    # ------------------------------------------------------------------
    # Print summary table
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print(f"{'Query':<30} {'Scale':<8} {'pandas(s)':<12} {'Spark(s)':<12} {'Winner'}")
    print("=" * 72)
    for scale in scales:
        label = SCALE_LABELS.get(scale, str(scale))
        for qname in QUERY_NAMES:
            p = results[scale]["pandas"].get(qname, 0)
            s = results[scale]["spark"].get(qname, 0)
            winner = "pandas" if p < s else "Spark "
            print(f"  {qname:<28} {label:<8} {p:<12.3f} {s:<12.3f} {winner}")

    # ------------------------------------------------------------------
    # Chart
    # ------------------------------------------------------------------
    plot_benchmark(results, args.chart)


if __name__ == "__main__":
    main()
