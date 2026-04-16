"""
simulate_data.py
----------------
Generates 6 months of realistic hourly smart-meter readings for 5 devices.

Patterns baked in:
  • Sinusoidal daily load curve  (low ~2-5am, peak ~7-9pm)
  • Weekend uplift               (+15-25 % higher average consumption)
  • Gaussian noise per reading
  • Random theft/fault anomalies (~1.5 % of rows), flagged with is_anomaly=1

Output: power_readings.csv  (~4 300 rows per device × 5 devices = ~21 900 rows total)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random

# ── Reproducibility ──────────────────────────────────────────────────────────
np.random.seed(42)
random.seed(42)

# ── Config ───────────────────────────────────────────────────────────────────
START_DATE   = datetime(2024, 1, 1, 0, 0, 0)
END_DATE     = datetime(2024, 7, 1, 0, 0, 0)           # 6 months
DEVICE_IDS   = [f"METER_{i:03d}" for i in range(1, 6)]  # 5 smart meters
ANOMALY_RATE = 0.015                                     # 1.5 %
VOLTAGE_BASE = 230.0                                     # Volts (UK/EU mains)

# Per-device base-load multiplier (simulates different property sizes)
DEVICE_PROFILES = {
    "METER_001": 1.00,   # average household
    "METER_002": 1.35,   # large house / home office
    "METER_003": 0.70,   # small flat
    "METER_004": 1.55,   # small business
    "METER_005": 0.85,   # energy-efficient home
}


def daily_load_curve(hour: int) -> float:
    """
    Return a [0, 1] load factor for a given hour using a double-peak sine curve.
    Trough  ~4 am  (factor ≈ 0.15)
    Morning peak  ~8 am  (factor ≈ 0.65)
    Evening peak  ~20:00  (factor ≈ 1.00)
    """
    # Primary evening peak
    evening = 0.5 * (1 + np.sin(2 * np.pi * (hour - 14) / 24))
    # Secondary morning shoulder
    morning = 0.25 * max(0, np.sin(2 * np.pi * (hour - 4) / 14))
    raw = evening + morning
    # Normalise to [0.15, 1.0]
    normalised = 0.15 + 0.85 * (raw / 1.25)
    return float(normalised)


def generate_device_readings(device_id: str, timestamps: pd.DatetimeIndex) -> pd.DataFrame:
    """Generate one row per timestamp for a single device."""
    multiplier = DEVICE_PROFILES[device_id]
    rows = []

    for ts in timestamps:
        hour       = ts.hour
        is_weekend = ts.dayofweek >= 5          # Sat=5, Sun=6
        is_anomaly = 0

        # ── Base wattage ─────────────────────────────────────────────────
        load_factor   = daily_load_curve(hour)
        weekend_boost = np.random.uniform(1.15, 1.25) if is_weekend else 1.0
        base_watts    = 500.0 * multiplier * load_factor * weekend_boost

        # Gaussian noise (±10 % std)
        noise  = np.random.normal(0, base_watts * 0.10)
        wattage = max(50.0, base_watts + noise)

        # ── Anomaly injection ────────────────────────────────────────────
        if np.random.random() < ANOMALY_RATE:
            anomaly_type = np.random.choice(["spike", "dropout"])
            if anomaly_type == "spike":
                wattage = wattage * np.random.uniform(3.5, 6.0)   # theft / fault
            else:
                wattage = wattage * np.random.uniform(0.0, 0.05)  # near-zero (dropout)
            is_anomaly = 1

        # ── Derive electrical quantities ─────────────────────────────────
        voltage = VOLTAGE_BASE + np.random.normal(0, 2.5)          # ±2.5 V noise
        current = wattage / voltage                                 # P = V × I

        rows.append({
            "timestamp" : ts.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id" : device_id,
            "voltage"   : round(voltage, 3),
            "current"   : round(current, 4),
            "wattage"   : round(wattage, 2),
            "is_anomaly": is_anomaly,
        })

    return pd.DataFrame(rows)


def main():
    print("Generating timestamps …")
    timestamps = pd.date_range(start=START_DATE, end=END_DATE, freq="h", inclusive="left")
    print(f"  {len(timestamps):,} hourly slots  ×  {len(DEVICE_IDS)} devices")

    all_frames = []
    for device_id in DEVICE_IDS:
        print(f"  Simulating {device_id} …")
        df = generate_device_readings(device_id, timestamps)
        all_frames.append(df)

    combined = pd.concat(all_frames, ignore_index=True)
    combined.sort_values(["timestamp", "device_id"], inplace=True)
    combined.reset_index(drop=True, inplace=True)

    output_path = "power_readings.csv"
    combined.to_csv(output_path, index=False)

    # ── Summary ──────────────────────────────────────────────────────────
    total_rows  = len(combined)
    anomaly_cnt = combined["is_anomaly"].sum()
    print(f"\n✓  Saved {output_path}")
    print(f"   Total rows    : {total_rows:,}")
    print(f"   Date range    : {combined['timestamp'].iloc[0]}  →  {combined['timestamp'].iloc[-1]}")
    print(f"   Anomalies     : {anomaly_cnt:,}  ({anomaly_cnt/total_rows*100:.2f} %)")
    print(f"   Avg wattage   : {combined['wattage'].mean():.1f} W")
    print(f"   Peak wattage  : {combined['wattage'].max():.1f} W")


if __name__ == "__main__":
    main()
