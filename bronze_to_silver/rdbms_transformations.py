# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, trim, lower

def transform_users(df):
    """Cleanse the users table."""
    # Example: Drop null IDs, trim whitespace, and standardize emails
    df = df.filter(col("user_id").isNotNull())
    if "email" in df.columns:
        df = df.withColumn("email", lower(trim(col("email"))))
    return df

def transform_activities(df):
    """Cleanse the activities table."""
    df = df.filter(col("activity_id").isNotNull())
    return df

def transform_opportunities(df):
    """Cleanse the opportunities table."""
    df = df.filter(col("opportunity_id").isNotNull())
    return df

def transform_refunds(df):
    """Cleanse the refunds table."""
    df = df.filter(col("refund_id").isNotNull())
    return df

def transform_sessions(df):
    """Cleanse the sessions table."""
    df = df.filter(col("session_id").isNotNull())
    return df

def transform_counselors(df):
    """Cleanse the counselors table."""
    df = df.filter(col("counselor_id").isNotNull())
    return df

# Dictionary mapping table names to their specific transformation functions
TRANSFORMATION_MAP = {
    "users": transform_users,
    "activities": transform_activities,
    "opportunities": transform_opportunities,
    "refunds": transform_refunds,
    "sessions": transform_sessions,
    "counselors": transform_counselors
}
