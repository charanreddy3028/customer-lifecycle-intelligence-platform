# -----------------------------
# TABLE NAMES
# -----------------------------
USERS = "users"
COUNSELORS = "counselors"
OPPORTUNITIES = "opportunities"
LEAD_STATUS_HISTORY = "lead_status_history"
CALLS = "calls"
SESSIONS = "sessions"
SESSION_ATTENDANCE = "session_attendance"
PAYMENTS = "payments"
REFUNDS = "refunds"
CAMPAIGNS = "campaigns"
ACTIVITIES = "activities"

# -----------------------------
# FILE PATHS
# -----------------------------
BRONZE_PATH = "storage/bronze/"
SILVER_PATH = "storage/silver/"
GOLD_PATH = "storage/gold/"

# -----------------------------
# BUSINESS CONSTANTS
# -----------------------------
VALID_STAGES = ["NEW", "CONTACTED", "QUALIFIED", "WON"]

CALL_OUTCOMES = ["CONNECTED", "NO_ANSWER"]

SESSION_TYPES = ["INTERVIEW", "CLOSURE"]

PLATFORMS = ["facebook", "google"]

# -----------------------------
# DATA QUALITY DEFAULTS
# -----------------------------
DEFAULT_NULL_PROBABILITY = 0.1
MAX_NULL_PROBABILITY = 0.3

# -----------------------------
# AWS Kinesis Configs
# -----------------------------
AWS_REGION = "ap-south-1"



API_ENDPOINTS = {
    "payments": "http://127.0.0.1:8000/payments",
    "campaigns": "http://127.0.0.1:8000/campaigns"
}

S3_BUCKET = "customer-lifecycle-intelligence-platform-bronze-layer"
S3_PREFIX = "bronze"