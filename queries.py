"""
queries.py
----------
Runs the four core analytics SQL queries against energy.db and prints
formatted results.  Each query is also returned as a pandas DataFrame
for downstream use (visualisations, exports, etc.).

Queries
-------
  Q1 – Peak consumption windows  (GROUP BY hour, AVG wattage)
  Q2 – Devices over threshold for ≥3 consecutive hours  (LAG window function)
  Q3 – Anomaly rate by ISO week   (CASE WHEN + window)
  Q4 – Total cost estimate        (JOIN tariff_rates, SUM kWh × rate)
"""

import sqlite3
import pandas as pd
import textwrap

DB_PATH           = "energy.db"
THRESHOLD_WATTS   = 800          # Q2: wattage that triggers a "high consumption" flag
MIN_CONSECUTIVE_H = 3            # Q2: minimum consecutive hours over threshold


def run_query(conn: sqlite3.Connection, sql: str, label: str) -> pd.DataFrame:
    df = pd.read_sql_query(textwrap.dedent(sql), conn)
    print(f"\n{'═'*60}")
    print(f"  {label}")
    print('═'*60)
    print(df.to_string(index=False))
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q1 — Peak consumption windows by hour of day
# ────────────────────────────────────────────────────────────────────────────
SQL_Q1 = """
    SELECT
        hour_of_day                          AS hour,
        ROUND(AVG(wattage), 1)               AS avg_wattage_w,
        ROUND(MAX(wattage), 1)               AS peak_wattage_w,
        ROUND(AVG(kwh) * 1000, 4)            AS avg_kwh,
        COUNT(*)                             AS reading_count
    FROM   readings
    WHERE  is_anomaly = 0
    GROUP  BY hour_of_day
    ORDER  BY hour_of_day;
"""

# ────────────────────────────────────────────────────────────────────────────
# Q2 — Devices exceeding threshold for ≥3 consecutive hours
#       Uses LAG to detect the start of a "high" run, then groups runs.
# ────────────────────────────────────────────────────────────────────────────
SQL_Q2 = f"""
    WITH flagged AS (
        SELECT
            device_id,
            timestamp,
            wattage,
            CASE WHEN wattage > {THRESHOLD_WATTS} THEN 1 ELSE 0 END AS over_thresh
        FROM readings
        WHERE is_anomaly = 0
    ),
    runs AS (
        SELECT
            device_id,
            timestamp,
            wattage,
            over_thresh,
            -- Each time over_thresh flips, start a new run group
            SUM(CASE WHEN over_thresh = 0 THEN 1 ELSE 0 END)
                OVER (PARTITION BY device_id ORDER BY timestamp)  AS run_grp
        FROM flagged
    ),
    run_lengths AS (
        SELECT
            device_id,
            run_grp,
            MIN(timestamp)                  AS window_start,
            MAX(timestamp)                  AS window_end,
            COUNT(*)                        AS consecutive_hours,
            ROUND(AVG(wattage), 1)          AS avg_wattage_w,
            ROUND(MAX(wattage), 1)          AS max_wattage_w
        FROM  runs
        WHERE over_thresh = 1
        GROUP BY device_id, run_grp
        HAVING COUNT(*) >= {MIN_CONSECUTIVE_H}
    )
    SELECT
        device_id,
        window_start,
        window_end,
        consecutive_hours,
        avg_wattage_w,
        max_wattage_w
    FROM   run_lengths
    ORDER  BY consecutive_hours DESC, device_id
    LIMIT  20;
"""

# ────────────────────────────────────────────────────────────────────────────
# Q3 — Anomaly rate by week
# ────────────────────────────────────────────────────────────────────────────
SQL_Q3 = """
    SELECT
        iso_week                                        AS week_number,
        COUNT(*)                                        AS total_readings,
        SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
        ROUND(
            100.0 * SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*),
            2
        )                                               AS anomaly_pct
    FROM   readings
    GROUP  BY iso_week
    ORDER  BY iso_week;
"""

# ────────────────────────────────────────────────────────────────────────────
# Q4 — Total electricity cost estimate (JOIN tariff_rates)
#       Maps each reading's hour to the correct tariff tier, then sums cost.
# ────────────────────────────────────────────────────────────────────────────
SQL_Q4 = """
    SELECT
        r.device_id,
        d.property_type,
        ROUND(SUM(r.kwh), 2)                                    AS total_kwh,
        ROUND(SUM(r.kwh * t.rate_pence_per_kwh) / 100.0, 2)    AS total_cost_gbp,
        ROUND(AVG(t.rate_pence_per_kwh), 2)                     AS avg_rate_p_per_kwh
    FROM   readings          r
    JOIN   tariff_rates      t
        ON r.hour_of_day >= t.start_hour
       AND r.hour_of_day  < t.end_hour
    JOIN   devices           d
        ON r.device_id = d.device_id
    WHERE  r.is_anomaly = 0
    GROUP  BY r.device_id, d.property_type
    ORDER  BY total_cost_gbp DESC;
"""


def main() -> dict:
    conn = sqlite3.connect(DB_PATH)
    results = {}

    try:
        results["peak_by_hour"]       = run_query(conn, SQL_Q1, "Q1 — Average wattage by hour of day")
        results["high_consumption"]   = run_query(conn, SQL_Q2, f"Q2 — Devices over {THRESHOLD_WATTS} W for ≥{MIN_CONSECUTIVE_H} consecutive hours (top 20)")
        results["anomaly_by_week"]    = run_query(conn, SQL_Q3, "Q3 — Anomaly rate by ISO week")
        results["cost_by_device"]     = run_query(conn, SQL_Q4, "Q4 — Estimated electricity cost per device (6-month period)")
    finally:
        conn.close()

    return results


if __name__ == "__main__":
    main()
