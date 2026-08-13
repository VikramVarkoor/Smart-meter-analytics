"""
Smart Meter Analytics at Scale — PySpark Pipeline
==================================================
Loads partitioned Parquet data and runs 4 analytics queries using PySpark
DataFrame API and Spark SQL window functions.

The core conceptual shift from the original pandas/SQLite approach:
  - Lazy evaluation: nothing executes until an action (.show(), .collect(), etc.)
    is called. Spark builds a DAG (Directed Acyclic Graph) of transformations
    first, then optimises the physical plan via the Catalyst query optimiser.
  - Partitioned reads: Spark exploits Parquet partition directories (year=/month=)
    to skip reading irrelevant data entirely — equivalent to an index scan in SQL.
  - Window functions: Spark SQL's OVER (PARTITION BY ... ORDER BY ...) is the
    distributed analogue of the LAG/window patterns used in the Retail Operations
    Analytics project. The same logical query compiles to a shuffle-based
    distributed sort rather than a single-process sort in memory.
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def create_spark_session(app_name: str = "SmartMeterAnalytics") -> SparkSession:
    """
    Create a local Spark session optimised for a laptop.
    local[*] uses all available CPU cores; 4g driver memory keeps headroom.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")   # reduce from default 200 for local mode
        .config("spark.sql.adaptive.enabled", "true")  # AQE: auto-coalesce small partitions
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

def load_data(spark: SparkSession, data_path: str):
    """
    Load partitioned Parquet. Spark reads the directory tree and infers
    year/month partition columns from the folder names automatically.
    """
    df = (
        spark.read
        .option("mergeSchema", "false")
        .parquet(data_path)
    )
    # Cast timestamp column (stored as int64 micros from pandas) to TimestampType
    df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))
    df.createOrReplaceTempView("meter_readings")
    return df


# ---------------------------------------------------------------------------
# Query 1 — Peak Consumption Window Detection
# ---------------------------------------------------------------------------
# Goal: find the top-N hourly windows (across all devices) by average
# consumption, so grid operators can identify when to pre-position capacity.

def q1_peak_consumption_window(spark: SparkSession, df, top_n: int = 10):
    """
    Group readings by hour-of-day and compute mean + max consumption.
    Returns the top-N peak hours ranked by average load.

    In the original SQLite project this was a GROUP BY + ORDER BY query.
    Here we use Spark SQL to express the same logic at distributed scale.
    """
    result = spark.sql("""
        SELECT
            HOUR(timestamp)                         AS hour_of_day,
            ROUND(AVG(consumption_kw), 4)           AS avg_consumption_kw,
            ROUND(MAX(consumption_kw), 4)           AS max_consumption_kw,
            COUNT(*)                                AS n_readings
        FROM meter_readings
        GROUP BY HOUR(timestamp)
        ORDER BY avg_consumption_kw DESC
        LIMIT {top_n}
    """.format(top_n=top_n))
    return result


# ---------------------------------------------------------------------------
# Query 2 — Consecutive Overload Interval Identification
# ---------------------------------------------------------------------------
# Goal: find runs of consecutive 15-min readings where a device is in overload.
# This is the "islands and gaps" problem — classic window function territory.
#
# NOTE: This is the direct distributed analogue of the LAG()-based consecutive
# interval detection used in the Retail Operations Analytics project. The same
# pattern (assign an island ID via ROW_NUMBER - group_rank, then aggregate)
# works in both contexts; Spark compiles it to a parallel sort + window scan
# instead of a single-process sort.

def q2_consecutive_overload_intervals(spark: SparkSession, df):
    """
    Identify contiguous runs of overload readings per device.
    Uses the classic island-detection trick:
      island_id = row_number() - rank_within_overload_group
    so consecutive rows share the same island_id.

    Returns the 20 longest overload runs with device, start, end, and duration.
    """
    # Step 1: assign a global row number per device ordered by time
    w_device = Window.partitionBy("device_id").orderBy("timestamp")

    overloads = (
        df.filter(F.col("is_overload") == True)
        .withColumn("rn",     F.row_number().over(w_device))
    )

    # Step 2: assign rank within the overload-only sequence
    w_overload = Window.partitionBy("device_id").orderBy("timestamp")
    overloads = overloads.withColumn("rn_over", F.row_number().over(w_overload))

    # Step 3: island key = rn - rn_over (constant within a contiguous run)
    overloads = overloads.withColumn("island_key", F.col("rn") - F.col("rn_over"))

    # Step 4: aggregate each island
    result = (
        overloads
        .groupBy("device_id", "island_key")
        .agg(
            F.min("timestamp").alias("overload_start"),
            F.max("timestamp").alias("overload_end"),
            F.count("*").alias("n_consecutive_readings"),
            F.round(F.avg("consumption_kw"), 4).alias("avg_overload_kw"),
        )
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("overload_end") - F.unix_timestamp("overload_start")) / 60 + 15
        )
        .drop("island_key")
        .orderBy(F.col("n_consecutive_readings").desc())
        .limit(20)
    )
    return result


# ---------------------------------------------------------------------------
# Query 3 — Anomaly Rate Analysis by Week
# ---------------------------------------------------------------------------
# Goal: track injection-rate trends week by week — useful for detecting
# systematic tampering campaigns or seasonal sensor failures.

def q3_anomaly_rate_by_week(spark: SparkSession, df):
    """
    For each ISO week, compute:
      - total readings
      - anomaly count (theft + dropout combined)
      - per-type breakdown
      - anomaly rate %
    """
    result = spark.sql("""
        SELECT
            WEEKOFYEAR(timestamp)                               AS week_of_year,
            YEAR(timestamp)                                     AS year,
            COUNT(*)                                            AS total_readings,
            SUM(CAST(is_anomaly AS INT))                        AS anomaly_count,
            SUM(CASE WHEN anomaly_type = 'theft'   THEN 1 ELSE 0 END) AS theft_count,
            SUM(CASE WHEN anomaly_type = 'dropout' THEN 1 ELSE 0 END) AS dropout_count,
            ROUND(100.0 * SUM(CAST(is_anomaly AS INT)) / COUNT(*), 4) AS anomaly_rate_pct
        FROM meter_readings
        GROUP BY YEAR(timestamp), WEEKOFYEAR(timestamp)
        ORDER BY year, week_of_year
    """)
    return result


# ---------------------------------------------------------------------------
# Query 4 — Time-of-Use Tariff Cost Estimation
# ---------------------------------------------------------------------------
# Goal: calculate electricity cost for each device using a 3-tier ToU tariff,
# then summarise monthly spend per device type.
#
# ToU tiers (replicates a typical UAE DEWA-style tariff):
#   Peak     08:00–22:00  weekdays   →  0.38 AED/kWh
#   Off-peak 22:00–08:00  weekdays   →  0.23 AED/kWh
#   Weekend  all hours               →  0.20 AED/kWh
# Readings are 15 min → energy = kW × (15/60) kWh

def q4_tou_tariff_cost(spark: SparkSession, df):
    """
    Apply Time-of-Use tariff rates and aggregate monthly cost per device type.
    """
    tariff_df = (
        df.withColumn("hour", F.hour("timestamp"))
          .withColumn("dow",  F.dayofweek("timestamp"))   # 1=Sun, 7=Sat
          .withColumn("is_weekend", (F.col("dow").isin(1, 7)).cast("int"))
          .withColumn(
              "tariff_aed_per_kwh",
              F.when(F.col("is_weekend") == 1, 0.20)
               .when((F.col("hour") >= 8) & (F.col("hour") < 22), 0.38)
               .otherwise(0.23)
          )
          # 15-minute interval → divide by 4 to get kWh
          .withColumn("energy_kwh",  F.col("consumption_kw") / 4.0)
          .withColumn("cost_aed",    F.col("energy_kwh") * F.col("tariff_aed_per_kwh"))
    )

    result = (
        tariff_df
        .groupBy(
            F.year("timestamp").alias("year"),
            F.month("timestamp").alias("month"),
            "device_type",
        )
        .agg(
            F.round(F.sum("energy_kwh"), 4).alias("total_energy_kwh"),
            F.round(F.sum("cost_aed"),   2).alias("total_cost_aed"),
            F.count("*").alias("n_readings"),
        )
        .orderBy("year", "month", "device_type")
    )
    return result


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_all_queries(data_path: str, show_results: bool = True) -> dict:
    """
    Run all 4 analytics queries and return their execution times.
    """
    spark = create_spark_session()
    timings = {}

    print(f"\nLoading data from: {data_path}")
    t0 = time.time()
    df = load_data(spark, data_path)
    # Materialise count to measure load time (forces a scan)
    n_rows = df.count()
    timings["load"] = time.time() - t0
    print(f"  Loaded {n_rows:,} rows in {timings['load']:.2f}s\n")

    queries = [
        ("q1_peak_window",           lambda: q1_peak_consumption_window(spark, df)),
        ("q2_overload_intervals",     lambda: q2_consecutive_overload_intervals(spark, df)),
        ("q3_anomaly_rate_by_week",   lambda: q3_anomaly_rate_by_week(spark, df)),
        ("q4_tou_tariff_cost",        lambda: q4_tou_tariff_cost(spark, df)),
    ]

    for name, fn in queries:
        print(f"=== {name} ===")
        t0 = time.time()
        result = fn()
        result.cache()          # cache so .count() and .show() share the same plan
        result.count()          # trigger execution
        elapsed = time.time() - t0
        timings[name] = elapsed
        print(f"  Completed in {elapsed:.3f}s")
        if show_results:
            result.show(truncate=False)
        print()

    spark.stop()
    return timings


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data",  default="data/raw", help="Path to partitioned Parquet directory")
    parser.add_argument("--quiet", action="store_true", help="Suppress result tables")
    args = parser.parse_args()

    timings = run_all_queries(args.data, show_results=not args.quiet)
    print("\nQuery timings (seconds):")
    for k, v in timings.items():
        print(f"  {k:<30s} {v:.3f}s")
