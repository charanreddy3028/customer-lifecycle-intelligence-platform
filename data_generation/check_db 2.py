import os
import time
# pyrefly: ignore [missing-import]
from sqlalchemy import create_engine, text
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# DB Connection Details
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "edtech")

# Auto-detect if we are running on Host or inside Docker
if DB_HOST == "db":
    try:
        import socket
        socket.gethostbyname("db")
    except socket.gaierror:
        DB_HOST = "127.0.0.1"
        DB_PORT = "3333" # Targeting mysql-db container

# Construct Connection String
connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_engine(retries=5, delay=5):
    for i in range(retries):
        try:
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                return engine
        except Exception as e:
            print(f"Connection attempt {i+1} failed. Retrying in {delay}s... (Error: {e})")
            time.sleep(delay)
    raise Exception("Could not connect to database")

def check_counts():
    try:
        engine = get_engine()
        tables = [
            "users", "counselors", "opportunities", "lead_status_history",
            "activities", "calls", "sessions", "session_attendance", 
            "payments", "refunds", "campaigns"
        ]
        
        print(f"\n{'Table Name':<25} | {'Row Count':<10}")
        print("-" * 40)
        
        with engine.connect() as conn:
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    print(f"{table:<25} | {count:<10}")
                except Exception as e:
                    print(f"{table:<25} | Error: {table} likely doesn't exist.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_counts()