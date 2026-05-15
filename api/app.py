# pyrefly: ignore [missing-import]
from fastapi import FastAPI
import random
from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from faker import Faker

app = FastAPI()
fake = Faker()

def random_date():
    return datetime.now() - timedelta(days=random.randint(0, 365))


@app.get("/campaigns")
def get_campaigns(limit: int = 100):
    data = []

    for i in range(limit):
        record = {
            "campaign_id": i,
            "platform": random.choice(["facebook", "google"]),
            "source": "ads",
            "daily_spend": random.randint(1000, 10000),
            "clicks": random.randint(100, 1000),
            "campaign_date": str(random_date().date()),
            "updated_at": str(random_date())
        }

        # 🔥 Simulate schema evolution
        if random.random() > 0.7:
            record["ad_format"] = random.choice(["video", "image"])

        if random.random() > 0.8:
            record["conversion_rate"] = round(random.uniform(0.1, 5.0), 2)

        if random.random() > 0.85:
            record["region"] = fake.country()

        data.append(record)

    return data

@app.get("/payments")
def get_payments(limit: int = 100):
    data = []

    for i in range(limit):
        record = {
            "payment_id": i,
            "user_id": random.randint(1, 10000),
            "amount": random.randint(10000, 100000),
            "payment_status": random.choice(["SUCCESS", "FAILED"]),
            "payment_datetime": str(random_date()),
            "updated_at": str(random_date())
        }

        # 🔥 Schema evolution
        if random.random() > 0.7:
            record["payment_mode"] = random.choice(["UPI", "CARD", "NETBANKING"])

        if random.random() > 0.8:
            record["failure_reason"] = random.choice([
                "INSUFFICIENT_FUNDS",
                "NETWORK_ERROR",
                None
            ])

        if random.random() > 0.85:
            record["currency"] = "INR"

        data.append(record)

    return data