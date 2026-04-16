"""
visualize.py
------------
Produces a single 2×2 figure (dashboard.png) with four analytics panels:

  Panel 1 (top-left)  — Daily load curve     : avg wattage by hour of day,
                                                split weekday vs. weekend
  Panel 2 (top-right) — Weekly anomaly heatmap: anomaly count per device per week
  Panel 3 (bottom-left) — Cumulative kWh over time (one line per device)
  Panel 4 (bottom-right) — Device comparison bar chart: total kWh & estimated cost
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")                              # headless (no display needed)
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from matplotlib.lines import Line2D

DB_PATH    = "energy.db"
OUTPUT_PNG = "dashboard.png"

# ── Visual style ─────────────────────────────────────────────────────────────
PALETTE    = sns.color_palette("tab10")
BG_COLOR   = "#F7F9FC"
PANEL_BG   = "#FFFFFF"
ACCENT     = "#2563EB"          # blue
ACCENT2    = "#DC2626"          # red / anomaly
GRID_COLOR = "#E5E7EB"


def load_data(conn: sqlite3.Connection) -> dict:
    """Pull the four datasets needed for the panels."""
    # P1 — hourly averages, weekday vs weekend
    p1 = pd.read_sql_query("""
        SELECT
            hour_of_day,
            CASE WHEN day_of_week >= 5 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            AVG(wattage) AS avg_wattage
        FROM   readings
        WHERE  is_anomaly = 0
        GROUP  BY hour_of_day, day_type
        ORDER  BY hour_of_day
    """, conn)

    # P2 — anomaly count per device per ISO week
    p2 = pd.read_sql_query("""
        SELECT device_id, iso_week,
               SUM(is_anomaly) AS anomaly_count
        FROM   readings
        GROUP  BY device_id, iso_week
        ORDER  BY iso_week
    """, conn)

    # P3 — daily cumulative kWh per device
    p3 = pd.read_sql_query("""
        SELECT
            SUBSTR(timestamp, 1, 10) AS date,
            device_id,
            SUM(kwh)                 AS daily_kwh
        FROM   readings
        WHERE  is_anomaly = 0
        GROUP  BY date, device_id
        ORDER  BY date
    """, conn)

    # P4 — total kWh & cost per device
    p4 = pd.read_sql_query("""
        SELECT
            r.device_id,
            d.property_type,
            ROUND(SUM(r.kwh), 1)                                     AS total_kwh,
            ROUND(SUM(r.kwh * t.rate_pence_per_kwh) / 100.0, 2)     AS total_cost_gbp
        FROM   readings     r
        JOIN   tariff_rates t
            ON r.hour_of_day >= t.start_hour AND r.hour_of_day < t.end_hour
        JOIN   devices      d USING (device_id)
        WHERE  r.is_anomaly = 0
        GROUP  BY r.device_id
        ORDER  BY total_kwh DESC
    """, conn)

    return {"p1": p1, "p2": p2, "p3": p3, "p4": p4}


# ── Panel 1: Daily load curve ────────────────────────────────────────────────
def plot_load_curve(ax: plt.Axes, df: pd.DataFrame) -> None:
    colors = {"Weekday": ACCENT, "Weekend": "#F59E0B"}
    for day_type, grp in df.groupby("day_type"):
        ax.plot(grp["hour_of_day"], grp["avg_wattage"],
                label=day_type, color=colors[day_type],
                linewidth=2.2, marker="o", markersize=4)
        ax.fill_between(grp["hour_of_day"], grp["avg_wattage"],
                        alpha=0.10, color=colors[day_type])

    ax.set_title("Daily Load Curve", fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("Hour of Day", fontsize=10)
    ax.set_ylabel("Avg Wattage (W)", fontsize=10)
    ax.set_xticks(range(0, 24, 2))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 2)], rotation=30, ha="right")
    ax.legend(frameon=False, fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    _style_axis(ax)


# ── Panel 2: Weekly anomaly heatmap ─────────────────────────────────────────
def plot_anomaly_heatmap(ax: plt.Axes, df: pd.DataFrame) -> None:
    pivot = df.pivot(index="device_id", columns="iso_week", values="anomaly_count").fillna(0)

    sns.heatmap(
        pivot,
        ax=ax,
        cmap="YlOrRd",
        linewidths=0.4,
        linecolor=GRID_COLOR,
        cbar_kws={"label": "Anomaly Count", "shrink": 0.85},
        annot=False,
    )
    ax.set_title("Weekly Anomaly Heatmap", fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("ISO Week Number", fontsize=10)
    ax.set_ylabel("Device", fontsize=10)
    # Show every 4th week label to avoid clutter
    ticks = [i for i, col in enumerate(pivot.columns) if col % 4 == 1]
    ax.set_xticks([t + 0.5 for t in ticks])
    ax.set_xticklabels([pivot.columns[t] for t in ticks], rotation=0, fontsize=8)
    ax.tick_params(axis="y", rotation=0)


# ── Panel 3: Cumulative consumption over time ────────────────────────────────
def plot_cumulative(ax: plt.Axes, df: pd.DataFrame) -> None:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    for i, (device, grp) in enumerate(df.groupby("device_id")):
        grp = grp.sort_values("date")
        grp["cum_kwh"] = grp["daily_kwh"].cumsum()
        ax.plot(grp["date"], grp["cum_kwh"],
                label=device, color=PALETTE[i], linewidth=1.8)

    ax.set_title("Cumulative Consumption Over Time", fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("Date", fontsize=10)
    ax.set_ylabel("Cumulative kWh", fontsize=10)
    ax.legend(fontsize=8, frameon=False, ncol=2)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b"))
    ax.xaxis.set_major_locator(matplotlib.dates.MonthLocator())
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    _style_axis(ax)


# ── Panel 4: Device comparison bar chart ────────────────────────────────────
def plot_device_comparison(ax: plt.Axes, df: pd.DataFrame) -> None:
    x       = np.arange(len(df))
    width   = 0.40
    ax2     = ax.twinx()

    bars1 = ax.bar(x - width/2, df["total_kwh"],   width, color=ACCENT,  alpha=0.85, label="Total kWh")
    bars2 = ax2.bar(x + width/2, df["total_cost_gbp"], width, color="#16A34A", alpha=0.85, label="Cost (£)")

    ax.set_title("Device Comparison — 6-month Total", fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("Device / Property", fontsize=10)
    ax.set_ylabel("Total kWh", fontsize=10, color=ACCENT)
    ax2.set_ylabel("Cost (£)", fontsize=10, color="#16A34A")
    ax.set_xticks(x)
    ax.set_xticklabels(
        [f"{row.device_id}\n({row.property_type})" for row in df.itertuples()],
        fontsize=8
    )
    ax.tick_params(axis="y", labelcolor=ACCENT)
    ax2.tick_params(axis="y", labelcolor="#16A34A")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}"))

    # Combined legend
    legend_elements = [
        Line2D([0], [0], color=ACCENT,    lw=6, label="Total kWh"),
        Line2D([0], [0], color="#16A34A", lw=6, label="Est. Cost (£)"),
    ]
    ax.legend(handles=legend_elements, frameon=False, fontsize=9, loc="upper right")
    _style_axis(ax)


# ── Shared axis styling ───────────────────────────────────────────────────────
def _style_axis(ax: plt.Axes) -> None:
    ax.set_facecolor(PANEL_BG)
    ax.grid(axis="y", color=GRID_COLOR, linewidth=0.8, linestyle="--")
    ax.spines[["top", "right"]].set_visible(False)


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    data = load_data(conn)
    conn.close()

    fig, axes = plt.subplots(2, 2, figsize=(16, 11))
    fig.patch.set_facecolor(BG_COLOR)
    fig.suptitle(
        "Smart Energy Meter — Power Consumption Analytics Dashboard",
        fontsize=16, fontweight="bold", y=0.98, color="#111827"
    )
    fig.text(0.5, 0.955, "6-month simulation (Jan–Jun 2024)  |  5 devices  |  hourly readings",
             ha="center", fontsize=9, color="#6B7280")

    plot_load_curve(        axes[0, 0], data["p1"])
    plot_anomaly_heatmap(   axes[0, 1], data["p2"])
    plot_cumulative(        axes[1, 0], data["p3"])
    plot_device_comparison( axes[1, 1], data["p4"])

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close()
    print(f"✓  Dashboard saved: {OUTPUT_PNG}")


if __name__ == "__main__":
    main()
