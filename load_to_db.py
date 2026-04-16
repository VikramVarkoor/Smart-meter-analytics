"""
load_to_db.py
-------------
Loads power_readings.csv into an SQLite database (energy.db) and creates
supporting tables used by the analytics queries.

Tables created:
  readings       — raw meter data from the CSV
  tariff_rates   — time-of-use electricity pricing tiers
  devices        — device metadata / location lookup
"""

import sqlite3
import pandas as pd
import os

DB_PATH  = "energy.db"
CSV_PATH = "power_readings.csv"


# ── Tariff schedule (UK-style Economy 7 + peak pricing) ─────────────────────
TARIFF_DATA = [
    # tier_name,       start_hour, end_hour, rate_pence_per_kwh
    ("off_peak_night",  0,  7,  8.5),
    ("standard",        7, 16, 24.0),
    ("peak_evening",   16, 20, 35.5),
    ("standard_late",  20, 24, 24.0),
]

DEVICE_META = [
    # device_id, location,          property_type,  occupants
    ("METER_001", "14 Oak Street",    "Semi-detached", 3),
    ("METER_002", "7 Elm Avenue",     "Detached",      4),
    ("METER_003", "Flat 2B, Maple Rd","Flat",          1),
    ("METER_004", "22 High Street",   "Small Business",0),
    ("METER_005", "9 Birch Close",    "Semi-detached", 2),
]


def create_schema(conn: sqlite3.Connection) -> None:
    """Drop & recreate all tables."""
    cur = conn.cursor()

    cur.executescript("""
        DROP TABLE IF EXISTS readings;
        DROP TABLE IF EXISTS tariff_rates;
        DROP TABLE IF EXISTS devices;

        CREATE TABLE readings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT    NOT NULL,
            device_id   TEXT    NOT NULL,
            voltage     REAL    NOT NULL,
            current     REAL    NOT NULL,
            wattage     REAL    NOT NULL,
            is_anomaly  INTEGER NOT NULL DEFAULT 0,
            -- Derived columns added on load
            hour_of_day INTEGER,
            day_of_week INTEGER,   -- 0=Mon … 6=Sun
            iso_week    INTEGER,
            kwh         REAL       -- wattage * 1h / 1000
        );

        CREATE INDEX idx_readings_device    ON readings(device_id);
        CREATE INDEX idx_readings_timestamp ON readings(timestamp);
        CREATE INDEX idx_readings_hour      ON readings(hour_of_day);

        CREATE TABLE tariff_rates (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            tier_name         TEXT NOT NULL,
            start_hour        INTEGER NOT NULL,
            end_hour          INTEGER NOT NULL,
            rate_pence_per_kwh REAL  NOT NULL
        );

        CREATE TABLE devices (
            device_id     TEXT PRIMARY KEY,
            location      TEXT,
            property_type TEXT,
            occupants     INTEGER
        );
    """)
    conn.commit()
    print("✓  Schema created")


def load_readings(conn: sqlite3.Connection) -> None:
    """Read CSV, enrich columns, bulk-insert into readings table."""
    print(f"   Reading {CSV_PATH} …")
    df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])

    df["hour_of_day"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["iso_week"]    = df["timestamp"].dt.isocalendar().week.astype(int)
    df["kwh"]         = (df["wattage"] / 1000).round(6)     # watts → kWh (1-hour slot)

    # Convert timestamp back to string for SQLite TEXT column
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df.to_sql("readings", conn, if_exists="append", index=False)
    print(f"✓  Loaded {len(df):,} rows into readings")


def load_tariffs(conn: sqlite3.Connection) -> None:
    df = pd.DataFrame(TARIFF_DATA, columns=["tier_name","start_hour","end_hour","rate_pence_per_kwh"])
    df.to_sql("tariff_rates", conn, if_exists="append", index=False)
    print(f"✓  Loaded {len(df)} tariff tiers")


def load_device_meta(conn: sqlite3.Connection) -> None:
    df = pd.DataFrame(DEVICE_META, columns=["device_id","location","property_type","occupants"])
    df.to_sql("devices", conn, if_exists="append", index=False)
    print(f"✓  Loaded {len(df)} device records")


def print_summary(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    print("\n── Database summary ─────────────────────────────────────────────")
    for table in ("readings", "tariff_rates", "devices"):
        n = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"   {table:<20} {n:>8,} rows")

    sample = cur.execute("""
        SELECT timestamp, device_id, wattage, is_anomaly
        FROM   readings
        ORDER  BY RANDOM()
        LIMIT  3
    """).fetchall()
    print("\n   Sample readings:")
    for row in sample:
        print(f"     {row}")


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"   Removed existing {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        create_schema(conn)
        load_readings(conn)
        load_tariffs(conn)
        load_device_meta(conn)
        print_summary(conn)
        print(f"\n✓  Database ready: {DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
