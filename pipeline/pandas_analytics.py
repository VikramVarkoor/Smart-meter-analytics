"""
Smart Meter Analytics — Original pandas/SQLite Approach
========================================================
Reimplements the same 4 analytics queries using the original stack:
pandas for data manipulation and sqlite3 for SQL queries.

This is intentionally kept as close to the original approach as possible
so the benchmark comparison is fair: same logic, different execution engine.
"""

import sqlite3
import time
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Data loader — reads Parquet into a pandas DataFrame, loads into SQLite
# ---------------------------------------------------------------------------

def load_into_sqlite(parquet_path: str, n_rows: int = None, conn=None) -> tuple:
    """
    Read a Parquet file (or directory) into pandas, then load into an
    in-memory SQLite database. Returns (conn, df, load_time).

    For the directory case (partitioned Parquet), we use pandas.read_parquet
    which handles the directory transparently.
    """
    t0 = time.time()
    df = pd.read_parquet(parquet_path)

    if n_rows is not None:
        df = df.sample(n=min(n_rows, len(df)), random_state=42).reset_index(drop=True)

    # Parse timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    if conn is None:
        conn = sqlite3.connect(":memory:")

    df.to_sql("meter_readings", conn, if_exists="replace", index=False)

    # Indexes to speed up GROUP BY queries (mirrors what an RDBMS would auto-create)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts      ON meter_readings(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_device  ON meter_readings(device_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anomaly ON meter_readings(is_anomaly)")
    conn.commit()

    load_time = time.time() - t0
    return conn, df, load_time


# ---------------------------------------------------------------------------
# Query 1 — Peak Consumption Window Detection
# ---------------------------------------------------------------------------

def q1_peak_consumption_window(conn, top_n: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT
            CAST(strftime('%H', timestamp) AS INTEGER)  AS hour_of_day,
            ROUND(AVG(consumption_kw), 4)               AS avg_consumption_kw,
            ROUND(MAX(consumption_kw), 4)               AS max_consumption_kw,
            COUNT(*)                                    AS n_readings
        FROM meter_readings
        GROUP BY hour_of_day
        ORDER BY avg_consumption_kw DESC
        LIMIT {top_n}
    """
    return pd.read_sql_query(sql, conn)


# ---------------------------------------------------------------------------
# Query 2 — Consecutive Overload Interval Identification
# ---------------------------------------------------------------------------
# SQLite doesn't have LAG/window functions built-in until 3.25 (2018),
# so we use pandas for the island-detection step, matching the original
# project's approach.

def q2_consecutive_overload_intervals(conn, df_cached: pd.DataFrame) -> pd.DataFrame:
    overloads = df_cached[df_cached["is_overload"]].copy().sort_values(["device_id", "timestamp"])

    if overloads.empty:
        return pd.DataFrame()

    # Island detection: flag where a new island starts (gap > 15 min or new device)
    overloads["prev_ts"]     = overloads.groupby("device_id")["timestamp"].shift(1)
    overloads["prev_device"] = overloads["device_id"].shift(1)
    overloads["new_island"]  = (
        (overloads["device_id"] != overloads["prev_device"]) |
        ((overloads["timestamp"] - overloads["prev_ts"]).dt.total_seconds() > 900)
    )
    overloads["island_id"] = overloads["new_island"].cumsum()

    result = (
        overloads.groupby(["device_id", "island_id"])
        .agg(
            overload_start=("timestamp", "min"),
            overload_end=("timestamp", "max"),
            n_consecutive_readings=("timestamp", "count"),
            avg_overload_kw=("consumption_kw", "mean"),
        )
        .reset_index()
        .drop(columns=["island_id"])
    )
    result["duration_minutes"] = (
        (result["overload_end"] - result["overload_start"]).dt.total_seconds() / 60 + 15
    )
    result["avg_overload_kw"] = result["avg_overload_kw"].round(4)
    return result.sort_values("n_consecutive_readings", ascending=False).head(20)


# ---------------------------------------------------------------------------
# Query 3 — Anomaly Rate Analysis by Week
# ---------------------------------------------------------------------------

def q3_anomaly_rate_by_week(conn) -> pd.DataFrame:
    sql = """
        SELECT
            CAST(strftime('%W', timestamp) AS INTEGER) + 1 AS week_of_year,
            CAST(strftime('%Y', timestamp) AS INTEGER)     AS year,
            COUNT(*)                                       AS total_readings,
            SUM(CAST(is_anomaly AS INTEGER))               AS anomaly_count,
            SUM(CASE WHEN anomaly_type = 'theft'   THEN 1 ELSE 0 END) AS theft_count,
            SUM(CASE WHEN anomaly_type = 'dropout' THEN 1 ELSE 0 END) AS dropout_count,
            ROUND(100.0 * SUM(CAST(is_anomaly AS INTEGER)) / COUNT(*), 4) AS anomaly_rate_pct
        FROM meter_readings
        GROUP BY year, week_of_year
        ORDER BY year, week_of_year
    """
    return pd.read_sql_query(sql, conn)


# ---------------------------------------------------------------------------
# Query 4 — Time-of-Use Tariff Cost Estimation
# ---------------------------------------------------------------------------

def q4_tou_tariff_cost(conn, df_cached: pd.DataFrame) -> pd.DataFrame:
    """
    pandas-based ToU cost calculation (SQLite lacks HOUR() function).
    """
    df = df_cached.copy()
    df["hour"]       = df["timestamp"].dt.hour
    df["dow"]        = df["timestamp"].dt.dayofweek   # 0=Mon, 6=Sun
    df["is_weekend"] = df["dow"] >= 5

    df["tariff"] = np.where(
        df["is_weekend"], 0.20,
        np.where(df["hour"].between(8, 21), 0.38, 0.23)
    )
    df["energy_kwh"] = df["consumption_kw"] / 4.0
    df["cost_aed"]   = df["energy_kwh"] * df["tariff"]
    df["year"]       = df["timestamp"].dt.year
    df["month"]      = df["timestamp"].dt.month

    result = (
        df.groupby(["year", "month", "device_type"])
        .agg(
            total_energy_kwh=("energy_kwh", "sum"),
            total_cost_aed  =("cost_aed",   "sum"),
            n_readings      =("cost_aed",   "count"),
        )
        .reset_index()
    )
    result["total_energy_kwh"] = result["total_energy_kwh"].round(4)
    result["total_cost_aed"]   = result["total_cost_aed"].round(2)
    return result.sort_values(["year", "month", "device_type"])


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_all_queries(parquet_path: str, n_rows: int = None, show_results: bool = True) -> dict:
    timings = {}

    print(f"\nLoading into pandas + SQLite (n_rows={n_rows or 'all'})…")
    conn, df, load_time = load_into_sqlite(parquet_path, n_rows=n_rows)
    timings["load"] = load_time
    print(f"  {len(df):,} rows loaded in {load_time:.2f}s")

    queries = [
        ("q1_peak_window",          lambda: q1_peak_consumption_window(conn)),
        ("q2_overload_intervals",    lambda: q2_consecutive_overload_intervals(conn, df)),
        ("q3_anomaly_rate_by_week",  lambda: q3_anomaly_rate_by_week(conn)),
        ("q4_tou_tariff_cost",       lambda: q4_tou_tariff_cost(conn, df)),
    ]

    for name, fn in queries:
        print(f"=== {name} ===")
        t0 = time.time()
        result = fn()
        elapsed = time.time() - t0
        timings[name] = elapsed
        print(f"  Completed in {elapsed:.3f}s")
        if show_results and result is not None:
            print(result.head(10).to_string(index=False))
        print()

    conn.close()
    return timings


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data",   default="data/raw", help="Parquet path")
    parser.add_argument("--rows",   type=int, default=None, help="Subsample N rows")
    parser.add_argument("--quiet",  action="store_true")
    args = parser.parse_args()

    timings = run_all_queries(args.data, n_rows=args.rows, show_results=not args.quiet)
    print("\nQuery timings (seconds):")
    for k, v in timings.items():
        print(f"  {k:<30s} {v:.3f}s")
