#!/usr/bin/env python3
"""Pure-pandas reference implementation of the same sessionization logic.

Useful to sanity-check the PySpark job's numbers without a Spark install.
Produces identical results to sessionize.py.

Usage: python reference_check.py [data/events.csv]
"""

import sys
import pandas as pd

SESSION_GAP_SECONDS = 30 * 60


def main(path: str) -> None:
    df = pd.read_csv(path, parse_dates=["ts"]).sort_values(["user_id", "ts"])

    prev = df.groupby("user_id")["ts"].shift()
    gap = (df["ts"] - prev).dt.total_seconds()
    df["is_new_session"] = (prev.isna() | (gap > SESSION_GAP_SECONDS)).astype(int)
    df["session_index"] = df.groupby("user_id")["is_new_session"].cumsum()
    df["session_id"] = df["user_id"] + "-" + df["session_index"].astype(str)

    g = df.groupby(["user_id", "session_id"])["ts"]
    sessions = pd.DataFrame({"session_start": g.min(), "session_end": g.max()})
    sessions["session_length_seconds"] = (
        sessions["session_end"] - sessions["session_start"]
    ).dt.total_seconds()
    sessions["session_day"] = sessions["session_start"].dt.date

    per_day = sessions.groupby("session_day").agg(
        sessions=("session_length_seconds", "size"),
        avg_session_length_seconds=("session_length_seconds", "mean"),
    )

    print("Sessions per day & average length per day:")
    print(per_day)
    print("\nOverall average session length (s):",
          round(sessions["session_length_seconds"].mean(), 2))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/events.csv")
