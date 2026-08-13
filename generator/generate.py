"""
Smart Meter Analytics at Scale — Synthetic Data Generator
==========================================================
Generates realistic power consumption readings for 200–500 simulated IoT devices
(same domain as an SCT-013 current-transformer + ESP32 setup) using:
  - Double-peak sine wave daily load curves (morning + evening peaks)
  - Weekend uplift multipliers (residential devices stay home)
  - Per-device consumption profiles (e.g. HVAC draws more than a lamp)
  - ~1.5% injected anomalies: theft spikes and sudden dropouts

Output: Parquet files partitioned by year/month for realistic big-data layout.
This generator runs in plain Python/NumPy (no Spark needed) to separate
concerns: generation is a CPU-bound task; Spark handles distributed processing.
"""

import os
import argparse
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Device profiles — mirrors real IoT sensor domains (HVAC, lighting, EV, etc.)
# ---------------------------------------------------------------------------
DEVICE_PROFILES = {
    "HVAC":        {"base_kw": 3.5,  "variance": 0.8,  "weekend_mult": 1.25},
    "WaterHeater": {"base_kw": 2.0,  "variance": 0.4,  "weekend_mult": 1.10},
    "Lighting":    {"base_kw": 0.6,  "variance": 0.15, "weekend_mult": 1.15},
    "EV_Charger":  {"base_kw": 7.2,  "variance": 1.2,  "weekend_mult": 1.40},
    "Refrigerator":{"base_kw": 0.15, "variance": 0.03, "weekend_mult": 1.00},
    "Washer":      {"base_kw": 1.8,  "variance": 0.3,  "weekend_mult": 1.30},
    "Dryer":       {"base_kw": 2.5,  "variance": 0.4,  "weekend_mult": 1.30},
    "Oven":        {"base_kw": 2.2,  "variance": 0.5,  "weekend_mult": 1.20},
    "Dishwasher":  {"base_kw": 1.2,  "variance": 0.25, "weekend_mult": 1.10},
    "Generic":     {"base_kw": 0.8,  "variance": 0.2,  "weekend_mult": 1.05},
}

PROFILE_NAMES = list(DEVICE_PROFILES.keys())


def double_peak_load_curve(hours: np.ndarray) -> np.ndarray:
    """
    Return a normalised load multiplier (0–1) for each hour of the day.
    Two peaks: morning (08:00) and evening (19:00), with a trough at midday.
    This mirrors real residential demand curves measured by SCT-013 sensors.
    """
    morning_peak = np.exp(-0.5 * ((hours - 8.0) / 2.0) ** 2)
    evening_peak = np.exp(-0.5 * ((hours - 19.0) / 2.5) ** 2)
    base_load    = 0.15  # always-on baseline (refrigerators, standby, etc.)
    curve = base_load + 0.55 * morning_peak + 0.80 * evening_peak
    return curve / curve.max()


def generate_device_readings(
    device_id: str,
    profile_name: str,
    start_date: datetime,
    end_date: datetime,
    interval_minutes: int = 15,
    anomaly_rate: float = 0.015,
    rng: np.random.Generator = None,
) -> pd.DataFrame:
    """Generate all readings for a single device over the date range."""
    if rng is None:
        rng = np.random.default_rng()

    profile = DEVICE_PROFILES[profile_name]
    base_kw = profile["base_kw"]
    variance = profile["variance"]
    weekend_mult = profile["weekend_mult"]

    # Build timestamp index at given interval
    timestamps = pd.date_range(start=start_date, end=end_date, freq=f"{interval_minutes}min", inclusive="left")
    n = len(timestamps)

    hours = timestamps.hour + timestamps.minute / 60.0
    load_curve = double_peak_load_curve(hours.values)

    # Weekend uplift
    is_weekend = np.array(timestamps.dayofweek >= 5, dtype=float)
    day_mult = 1.0 + (weekend_mult - 1.0) * is_weekend

    # Per-reading consumption with Gaussian noise
    consumption_kw = (
        base_kw
        * load_curve
        * day_mult
        * (1.0 + rng.normal(0, variance / base_kw, size=n))
    )
    consumption_kw = np.clip(consumption_kw, 0.01, None)

    # ------------------------------------------------------------------
    # Inject anomalies (~1.5% of readings)
    # Two types:
    #   - Theft spike: reading suddenly 3–6× normal (energy being diverted)
    #   - Dropout:     reading collapses to near-zero (sensor loss / outage)
    # ------------------------------------------------------------------
    anomaly_flags = np.zeros(n, dtype="U8")
    n_anomalies = int(n * anomaly_rate)

    # Theft spikes
    spike_indices = rng.choice(n, size=n_anomalies // 2, replace=False)
    spike_mults   = rng.uniform(3.0, 6.0, size=len(spike_indices))
    consumption_kw[spike_indices] *= spike_mults
    anomaly_flags[spike_indices]   = "theft"

    # Sudden dropouts (must not overlap spikes)
    remaining = np.setdiff1d(np.arange(n), spike_indices)
    dropout_indices = rng.choice(remaining, size=n_anomalies // 2, replace=False)
    consumption_kw[dropout_indices] = rng.uniform(0.001, 0.01, size=len(dropout_indices))
    anomaly_flags[dropout_indices]   = "dropout"

    # Overload flag (>threshold for this device profile)
    overload_threshold = base_kw * 2.5
    is_overload = consumption_kw > overload_threshold

    df = pd.DataFrame({
        "timestamp":       timestamps,
        "device_id":       device_id,
        "device_type":     profile_name,
        "consumption_kw":  np.round(consumption_kw, 4),
        "is_anomaly":      anomaly_flags != "",
        "anomaly_type":    anomaly_flags,
        "is_overload":     is_overload,
    })
    return df


def generate_dataset(
    n_devices: int,
    start_date: str = "2024-01-01",
    end_date: str   = "2024-12-31",
    interval_minutes: int = 15,
    anomaly_rate: float = 0.015,
    output_dir: str = "data/raw",
    seed: int = 42,
) -> dict:
    """
    Generate the full dataset and write partitioned Parquet files.
    Partitioned by year= / month= so Spark can exploit partition pruning.
    Returns metadata dict with row counts and file paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    rng = np.random.default_rng(seed)

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")

    # Assign each device a profile deterministically
    profile_assignments = [PROFILE_NAMES[i % len(PROFILE_NAMES)] for i in range(n_devices)]

    print(f"\nGenerating {n_devices} devices  |  {start_date} → {end_date}  |  {interval_minutes}-min intervals")
    expected_rows_per_device = int((end - start).days * 24 * 60 / interval_minutes)
    print(f"Expected rows: ~{n_devices * expected_rows_per_device:,}")

    total_rows = 0
    t0 = time.time()

    for i, profile_name in enumerate(profile_assignments):
        device_id = f"DEV_{i+1:04d}"
        df = generate_device_readings(
            device_id=device_id,
            profile_name=profile_name,
            start_date=start,
            end_date=end,
            interval_minutes=interval_minutes,
            anomaly_rate=anomaly_rate,
            rng=rng,
        )

        # Partition by year + month
        df["year"]  = df["timestamp"].dt.year
        df["month"] = df["timestamp"].dt.month

        for (year, month), grp in df.groupby(["year", "month"]):
            part_dir = os.path.join(output_dir, f"year={year}", f"month={month:02d}")
            os.makedirs(part_dir, exist_ok=True)
            out_path = os.path.join(part_dir, f"{device_id}.parquet")
            grp.drop(columns=["year", "month"]).to_parquet(out_path, index=False)

        total_rows += len(df)

        if (i + 1) % 50 == 0 or (i + 1) == n_devices:
            elapsed = time.time() - t0
            rate = total_rows / elapsed
            print(f"  [{i+1:>4d}/{n_devices}]  total rows: {total_rows:>10,}  |  {rate:,.0f} rows/s")

    elapsed = time.time() - t0
    print(f"\nDone — {total_rows:,} rows written in {elapsed:.1f}s  →  {output_dir}")

    return {
        "n_devices":    n_devices,
        "total_rows":   total_rows,
        "output_dir":   output_dir,
        "start_date":   start_date,
        "end_date":     end_date,
        "interval_min": interval_minutes,
        "elapsed_s":    elapsed,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Meter synthetic data generator")
    parser.add_argument("--devices",   type=int,   default=300,          help="Number of simulated devices (default: 300)")
    parser.add_argument("--start",     type=str,   default="2024-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",       type=str,   default="2024-12-31", help="End date YYYY-MM-DD")
    parser.add_argument("--interval",  type=int,   default=15,           help="Interval in minutes (default: 15)")
    parser.add_argument("--output",    type=str,   default="data/raw",   help="Output directory")
    parser.add_argument("--seed",      type=int,   default=42,           help="Random seed")
    args = parser.parse_args()

    meta = generate_dataset(
        n_devices=args.devices,
        start_date=args.start,
        end_date=args.end,
        interval_minutes=args.interval,
        anomaly_rate=0.015,
        output_dir=args.output,
        seed=args.seed,
    )
    print(f"\nMetadata: {meta}")
