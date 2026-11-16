#!/usr/bin/env python3
"""Clickstream sessionization in PySpark.

Input : clickstream events (user_id, ts, url).
Rule  : a new session starts after 30 minutes of inactivity for a user.
Output: (1) sessions per day, (2) average session length.

Sessionization is done purely with window functions:
  - lag(ts) over (partition by user_id order by ts)  -> previous event time
  - a "new session" flag when there is no previous event or the gap > 30 min
  - a running sum of that flag -> a per-user session index
  - session_id = user_id + '-' + session_index

Usage:
    spark-submit sessionize.py [path/to/events.csv]
    python sessionize.py data/events.csv
"""

import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SESSION_GAP_SECONDS = 30 * 60  # 30 minutes of inactivity => new session


def build_sessions(events):
    """Add session_id to each event using window functions."""
    w = Window.partitionBy("user_id").orderBy("ts")

    prev_ts = F.lag("ts").over(w)
    gap_seconds = F.unix_timestamp("ts") - F.unix_timestamp(prev_ts)

    # First event for a user, or a gap longer than the threshold, starts a session.
    is_new_session = (prev_ts.isNull() | (gap_seconds > SESSION_GAP_SECONDS)).cast("int")

    w_running = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    return (
        events
        .withColumn("is_new_session", is_new_session)
        .withColumn("session_index", F.sum("is_new_session").over(w_running))
        .withColumn("session_id", F.concat_ws("-", F.col("user_id"), F.col("session_index")))
    )


def main(path: str) -> None:
    spark = SparkSession.builder.appName("clickstream-sessions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = (
        spark.read.option("header", True).csv(path)
        .withColumn("ts", F.to_timestamp("ts"))
    )

    sessionized = build_sessions(events)

    # One row per session, with its span.
    sessions = (
        sessionized
        .groupBy("user_id", "session_id")
        .agg(
            F.min("ts").alias("session_start"),
            F.max("ts").alias("session_end"),
            F.count("*").alias("event_count"),
        )
        .withColumn(
            "session_length_seconds",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
        )
        .withColumn("session_day", F.to_date("session_start"))
    )

    # (1) sessions per day + average session length per day
    sessions_per_day = (
        sessions
        .groupBy("session_day")
        .agg(
            F.count("*").alias("sessions"),
            F.round(F.avg("session_length_seconds"), 2).alias("avg_session_length_seconds"),
        )
        .orderBy("session_day")
    )

    # (2) overall average session length
    overall = sessions.agg(
        F.round(F.avg("session_length_seconds"), 2).alias("overall_avg_session_length_seconds"),
        (F.round(F.avg("session_length_seconds") / 60, 2)).alias("overall_avg_session_length_minutes"),
    )

    print("\n=== Sessions (one row per session) ===")
    sessions.orderBy("user_id", "session_start").show(truncate=False)

    print("=== Sessions per day & average length per day ===")
    sessions_per_day.show(truncate=False)

    print("=== Overall average session length ===")
    overall.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/events.csv"
    main(input_path)
