import random
from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from faker import Faker
# pyrefly: ignore [missing-import]
from sqlalchemy import text
import pandas as pd
from tqdm import tqdm
from config.db_config import get_db_engine

fake = Faker()
engine = get_db_engine()

# -----------------------------
# CLEAN EXISTING DATA
# -----------------------------
def clear_tables():
    tables = [
        "session_attendance", "calls", "lead_status_history",
        "activities", "opportunities", "refunds", "payments",
        "sessions", "counselors", "campaigns", "users"
    ]

    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        for table in tables:
            try:
                print(f"Clearing {table}...")
                conn.execute(text(f"TRUNCATE TABLE {table}"))
            except:
                print(f"Skipping {table}")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

# -----------------------------
# HELPERS
# -----------------------------
def random_date(days=365):
    return datetime.now() - timedelta(days=random.randint(0, days))

def maybe_null(value, prob=0.1):
    return value if random.random() > prob else None

# -----------------------------
# DATA GENERATION
# -----------------------------

users = [{
    "user_id": i,
    "name": maybe_null(fake.name(), 0.1),
    "email": maybe_null(fake.email(), 0.1),
    "city": maybe_null(fake.city(), 0.15),
    "created_at": random_date(),
    "updated_at": random_date()
} for i in tqdm(range(1, 10001), desc="Users")]

counselors = [{
    "counselor_id": i,
    "counselor_name": maybe_null(fake.name(), 0.1),
    "team_name": random.choice(["Team A", "Team B", None]),
    "manager_name": maybe_null(fake.name(), 0.2),
    "updated_at": random_date()
} for i in range(1, 101)]

campaigns = [{
    "campaign_id": i,
    "platform": random.choice(["facebook", "google", None]),
    "source": random.choice(["ads", "organic", None]),
    "daily_spend": maybe_null(random.randint(1000, 10000), 0.1),
    "clicks": maybe_null(random.randint(100, 1000), 0.1),
    "campaign_date": random_date().date(),
    "updated_at": random_date()
} for i in range(1, 501)]

opportunities = [{
    "opportunity_id": i,
    "user_id": random.randint(1, 10000),
    "assigned_counselor_id": maybe_null(random.randint(1, 100), 0.1),
    "campaign_id": maybe_null(random.randint(1, 500), 0.3),
    "stage": random.choice(["NEW", "CONTACTED", "QUALIFIED", "WON", None]),
    "created_at": random_date(),
    "updated_at": random_date()
} for i in tqdm(range(1, 10001), desc="Opportunities")]

lead_history = [{
    "id": i,
    "opportunity_id": random.randint(1, 10000),
    "status": random.choice(["NEW", "CONTACTED", "QUALIFIED", "WON", None]),
    "updated_at": random_date()
} for i in tqdm(range(1, 100001), desc="Lead History")]

# -----------------------------
# ACTIVITIES (after opportunities)
# -----------------------------
activities = []
for i in tqdm(range(1, 150001), desc="Activities"):
    opp = random.choice(opportunities)
    activities.append({
        "activity_id": i,
        "opportunity_id": opp["opportunity_id"],
        "user_id": opp["user_id"],
        "campaign_id": opp["campaign_id"],
        "activity_type": random.choice([
            "EMAIL_SENT", "EMAIL_OPENED",
            "MEETING_SCHEDULED", "MEETING_HELD"
        ]),
        "activity_date": random_date(),
        "notes": fake.sentence()
    })

calls = [{
    "call_id": i,
    "user_id": random.randint(1, 10000),
    "counselor_id": maybe_null(random.randint(1, 100), 0.1),
    "opportunity_id": random.randint(1, 10000),
    "call_datetime": random_date(),
    "duration_seconds": maybe_null(random.randint(30, 600), 0.1),
    "call_outcome": random.choice(["CONNECTED", "NO_ANSWER", None]),
    "call_type": random.choice(["INBOUND", "OUTBOUND", None])
} for i in tqdm(range(1, 50001), desc="Calls")]

sessions = [{
    "session_id": i,
    "session_type": random.choice(["INTERVIEW", "CLOSURE", None]),
    "session_datetime": random_date(),
    "mentor_id": maybe_null(random.randint(1, 100), 0.1)
} for i in range(1, 501)]

attendance = [{
    "attendance_id": i,
    "session_id": random.randint(1, 500),
    "user_id": random.randint(1, 10000),
    "joined_at": random_date(),
    "attended_flag": random.choice([True, False, None])
} for i in tqdm(range(1, 30001), desc="Attendance")]

payments = [{
    "payment_id": i,
    "user_id": random.randint(1, 10000),
    "amount": maybe_null(random.randint(10000, 100000), 0.1),
    "payment_status": random.choice(["SUCCESS", "FAILED", None]),
    "payment_datetime": random_date()
} for i in tqdm(range(1, 10001), desc="Payments")]

refunds = [{
    "refund_id": i,
    "payment_id": random.randint(1, 10000),
    "refund_amount": maybe_null(random.randint(1000, 5000), 0.2),
    "reason": random.choice(["User Request", None]),
    "refund_datetime": random_date()
} for i in range(1, 3001)]

# -----------------------------
# INSERT FUNCTION
# -----------------------------
def insert(table, data):
    try:
        print(f"Inserting {table}...")
        df = pd.DataFrame(data)
        df.to_sql(table, engine, if_exists="append", index=False, chunksize=1000)
        print(f"✅ {table} done")
    except Exception as e:
        print(f"❌ Error inserting {table}")
        print(e)

# -----------------------------
# EXECUTION
# -----------------------------
if __name__ == "__main__":
    print("🧹 Clearing old data...")
    clear_tables()

    print("🚀 Loading Data...")

    insert("users", users)
    insert("counselors", counselors)
    insert("campaigns", campaigns)   # ✅ FIXED ORDER
    insert("opportunities", opportunities)
    insert("lead_status_history", lead_history)
    insert("activities", activities)
    insert("calls", calls)
    insert("sessions", sessions)
    insert("session_attendance", attendance)
    insert("payments", payments)
    insert("refunds", refunds)

    print("🎉 Data loaded successfully!")