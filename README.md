# Smart Energy Meter — Power Consumption Analytics Pipeline

> **Portfolio project.** Built on top of a hardware smart-meter prototype,
> this pipeline simulates, stores, queries, and visualises 6 months of
> real-world-pattern power readings from 5 devices.

---

## Motivation

I built a Smart Energy Meter prototype in hardware (ESP32 micro-controller +
SCT-013 current-transformer clamp) that logs voltage, current, and derived
wattage over MQTT.  This project adds the **full data layer** on top of that
device: a simulation engine that reproduces the exact patterns the real hardware
would generate, a structured SQLite store, analytics queries for operational
insights, and a four-panel visual dashboard.

The end-to-end story: *hardware generates the data → pipeline surfaces the
insights from it.*

---

## Project Structure

```
power_analytics/
├── simulate_data.py   # Step 1 — generate power_readings.csv
├── load_to_db.py      # Step 2 — load CSV into SQLite + seed lookup tables
├── queries.py         # Step 3 — four SQL analytics queries
├── visualize.py       # Step 4 — four-panel matplotlib dashboard
├── main.py            # Orchestrator — runs all four steps in order
│
├── power_readings.csv # Generated: 21,840 rows (5 devices × 4,368 hours)
├── energy.db          # SQLite database
└── dashboard.png      # Output visualisation
```

---

## Quick Start

```bash
# Install dependencies
pip install pandas numpy matplotlib seaborn

# Run the full pipeline (sim → db → queries → chart)
python main.py

# Re-run analytics only (skip re-simulation)
python main.py --skip-sim
```

---

## Data Model

### `power_readings.csv` / `readings` table

| Column       | Type    | Description                                   |
|--------------|---------|-----------------------------------------------|
| timestamp    | TEXT    | ISO-8601 datetime, hourly resolution          |
| device_id    | TEXT    | METER_001 … METER_005                         |
| voltage      | REAL    | Mains voltage (V), ~230 V ± noise             |
| current      | REAL    | Derived from P = V × I (A)                    |
| wattage      | REAL    | Instantaneous power draw (W)                  |
| is_anomaly   | INTEGER | 1 = theft spike or dropout fault, else 0      |
| hour_of_day  | INTEGER | 0–23 (enriched on load)                       |
| day_of_week  | INTEGER | 0=Mon … 6=Sun (enriched on load)              |
| iso_week     | INTEGER | ISO calendar week (enriched on load)          |
| kwh          | REAL    | wattage / 1000 (1-hour slot = 1 kWh reading)  |

### `tariff_rates` table

Time-of-use pricing tiers (UK Economy 7 + peak model):

| Tier              | Hours    | Rate (p/kWh) |
|-------------------|----------|--------------|
| off_peak_night    | 00–07    | 8.5          |
| standard          | 07–16    | 24.0         |
| peak_evening      | 16–20    | 35.5         |
| standard_late     | 20–24    | 24.0         |

### `devices` table

Device metadata: location, property type, occupant count.

---

## Simulation Design

The simulation is intentionally realistic rather than random:

**Daily load curve** (`simulate_data.py → daily_load_curve()`):
- A double-peak sine wave: a morning shoulder (~08:00) and a main evening
  peak (~21:00), with a trough around 04:00.
- Formula: `0.5 × (1 + sin(2π(h-14)/24)) + 0.25 × max(0, sin(2π(h-4)/14))`

**Weekend uplift**: +15–25 % average load (people home during the day).

**Per-device multipliers**: 0.70× (small flat) to 1.55× (small business).

**Anomaly injection** (~1.5 % of rows):
- *Spike*: wattage × 3.5–6.0 → simulates energy theft or a faulty appliance.
- *Dropout*: wattage × 0–0.05 → simulates a metering fault or power cut.

---

## SQL Analytics Queries

### Q1 — Peak consumption windows
```sql
SELECT hour_of_day, ROUND(AVG(wattage), 1) AS avg_wattage_w, ...
FROM   readings
WHERE  is_anomaly = 0
GROUP  BY hour_of_day
ORDER  BY hour_of_day;
```
Reveals the classic dual-peak shape: morning shoulder (~7–8 am, ~189 W avg)
and evening peak (~21:00, ~564 W avg for all meters combined).

### Q2 — Devices exceeding threshold for ≥3 consecutive hours
Uses a **window function** (`SUM OVER`) to group contiguous high-wattage
readings into "runs", then filters for runs ≥ 3 hours.  METER_004 (small
business) accounts for the majority, with several 5–6 hour peak windows on
weekend evenings.

### Q3 — Anomaly rate by ISO week
Simple `CASE WHEN` aggregation.  Rate stays close to the target 1.5 %,
confirming the simulation is well-calibrated.

### Q4 — Total cost estimate (JOIN)
```sql
JOIN tariff_rates t
  ON r.hour_of_day >= t.start_hour AND r.hour_of_day < t.end_hour
```
6-month cost across 5 meters: METER_004 (business) £447, METER_003 (flat) £201.

---

## Dashboard Panels

| Panel | Title | Key insight |
|-------|-------|-------------|
| Top-left  | Daily Load Curve | Clear weekday vs weekend split; evening peak ~21:00 |
| Top-right | Weekly Anomaly Heatmap | No systematic drift — anomalies are evenly distributed |
| Bottom-left | Cumulative kWh Over Time | Linear growth confirms stable simulation; device ordering consistent |
| Bottom-right | Device Comparison | METER_004 consumes 2.2× more than METER_003; cost scales proportionally |

---

## Stack

| Layer | Technology |
|-------|-----------|
| Simulation | Python 3, NumPy (sine curves + Gaussian noise) |
| Storage | SQLite 3 (via Python `sqlite3` stdlib) |
| Analytics | pandas + raw SQL (window functions, JOINs, CASE WHEN) |
| Visualisation | matplotlib + seaborn |
| Hardware (prototype) | ESP32, SCT-013 current transformer, 230V split-core clamp |

---

## Interview Talking Points

**"Walk me through this project."**

> I started with the hardware — an ESP32 with a current-transformer clamp
> measuring real mains current.  Once I had the device working and streaming
> data over MQTT, I wanted to build the analytics layer on top.  I wrote a
> simulation engine in Python that replicates the exact patterns my hardware
> produces: a sinusoidal daily load curve, weekend uplift, per-device load
> profiles, and injected anomalies for theft/fault detection.  That gave me
> ~22,000 rows of realistic hourly readings which I loaded into SQLite.  I then
> wrote four SQL queries covering peak-consumption windows using GROUP BY,
> consecutive-hour thresholds using a LAG/SUM window function, weekly anomaly
> rates, and 6-month cost estimation via a tariff JOIN.  Finally I pulled it all
> into a four-panel matplotlib dashboard.

**"Why simulate the data instead of using a real dataset?"**

> Because I understand the data model completely — I designed it.  I can
> explain every column, every pattern, and every anomaly because I built them.
> That's actually harder to fake in an interview than pulling a Kaggle dataset.

**"What would you do differently at scale?"**

> SQLite works fine here but at real-time IoT scale I'd move to TimescaleDB
> (Postgres extension for time-series) or InfluxDB.  The SQL queries are
> already written in standard SQL so migration would be mostly a config change.
> For the dashboard, I'd replace the static PNG with a Grafana panel or a
> Streamlit app with live refresh.
