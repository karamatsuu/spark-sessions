# Clickstream Sessionization (PySpark)

Assign session IDs to clickstream events and compute **sessions per day** and
**average session length**. A new session starts after **30 minutes of
inactivity** for a user. Sessionization uses **window functions only**.

## Input

`data/events.csv` — clickstream events:

| column   | meaning                        |
|----------|--------------------------------|
| user_id  | user identifier                |
| ts       | event timestamp (`YYYY-MM-DD HH:MM:SS`) |
| url      | page visited                   |

## How it works (`sessionize.py`)

All with window functions over `partition by user_id order by ts`:

1. `lag(ts)` → previous event time for the user.
2. **new-session flag** = `1` when there is no previous event *or* the gap > 30 min.
3. running `sum()` of that flag → a per-user **session index**.
4. `session_id = user_id + '-' + session_index`.

Then each session is reduced to `min(ts)`/`max(ts)`, and:

- **sessions per day** = count of sessions grouped by the session's start date;
- **average session length** = mean of `session_end - session_start`, per day and overall.

## Run

```bash
pip install -r requirements.txt

# with Spark:
spark-submit sessionize.py data/events.csv
# or:
python sessionize.py data/events.csv
```

No Spark handy? A pandas reference with identical logic:

```bash
python reference_check.py data/events.csv
```

## Expected output for the sample data

Sessions per day:

| session_day | sessions | avg_session_length_seconds |
|-------------|----------|----------------------------|
| 2026-03-01  | 4        | 450.0                      |
| 2026-03-02  | 2        | 1200.0                     |

Overall average session length: **700.0 s** (≈ 11.67 min).

The sample (`data/events.csv`) is built so the boundaries are easy to check by
eye: user `u1` has a 65-minute gap that splits one visit into two sessions,
`u2` has a 50-minute gap plus a next-day visit, and `u3` is a single 40-minute
session.

## Layout

```
.
├── sessionize.py        # PySpark job (window-function sessionization)
├── reference_check.py   # pandas cross-check (same numbers)
├── data/events.csv      # sample clickstream
├── requirements.txt
└── README.md
```
