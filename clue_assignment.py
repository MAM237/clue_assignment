import json
import uuid
from datetime import datetime, timedelta
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ------------------------------------------
# 1. Generate synthetic event data
# ------------------------------------------

EVENT_TYPES = ["log_period", "log_mood", "log_pain"]
MOOD_VALUES = ["happy", "sad", "neutral"]
PAIN_VALUES = ["cramps", "headache", "backache"]
PERIOD_VALUES = ["light_flow", "heavy_flow", "spotting"]

def generate_event():
    event_type = random.choice(EVENT_TYPES)
    if event_type == "log_mood":
        value = random.choice(MOOD_VALUES)
    elif event_type == "log_pain":
        value = random.choice(PAIN_VALUES)
    else:
        value = random.choice(PERIOD_VALUES)
    return {
        "user_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": (datetime.utcnow() - timedelta(minutes=random.randint(0, 10000))).isoformat(),
        "value": value
    }

synthetic_data = [generate_event() for _ in range(100)]
with open("raw_events.json", "w") as f:
    json.dump(synthetic_data, f, indent=2)

print("✅ Synthetic health events written to raw_events.json")

# ------------------------------------------
# 2. Load and validate events
# ------------------------------------------

def validate_event(event: dict) -> dict:
    """
    Validate required fields, UUID format, timestamp, and event_type membership.
    """
    required_fields = {"user_id", "event_type", "timestamp", "value"}
    missing = required_fields - event.keys()
    if missing:
        raise ValueError(f"Missing fields: {missing}")

    try:
        uuid.UUID(event["user_id"])
    except Exception:
        raise ValueError(f"Invalid user_id UUID: {event['user_id']}")

    try:
        datetime.fromisoformat(event["timestamp"])
    except Exception:
        raise ValueError(f"Invalid timestamp format: {event['timestamp']}")

    if event["event_type"] not in EVENT_TYPES:
        raise ValueError(f"Unexpected event_type: {event['event_type']}")

    return event

# Load raw JSON
with open("raw_events.json") as f:
    raw_events = json.load(f)

validated = []
for ev in raw_events:
    try:
        validated.append(validate_event(ev))
    except ValueError as e:
        # In prod you might push this to Sentry or CloudWatch
        print(f"[Validation error] {e}")

# ------------------------------------------
# 3. Transform into analytics-ready schema
# ------------------------------------------

df = pd.DataFrame(validated)

# Normalize fields
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["event_type"] = df["event_type"].str.lower()
df["value"] = df["value"].str.lower()

# Enforce final schema
expected_columns = ["user_id", "event_type", "timestamp", "value"]
if not all(col in df.columns for col in expected_columns):
    raise Exception("Schema validation failed after transform")

# Save to Parquet
arrow_table = pa.Table.from_pandas(df)
pq.write_table(arrow_table, "clean_events.parquet")

print("✅ Clean events saved to clean_events.parquet")

