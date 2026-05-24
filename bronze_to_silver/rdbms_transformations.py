# pyrefly: ignore [missing-import]
from pandas.core import window
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, trim, lower, initcap, current_timestamp, from_utc_timestamp,row_number, lit   
# pyrefly: ignore [missing-import]
from pyspark.sql.window import Window 


def transform_users(df):
    """Cleanse the users table."""
    # Example: Drop null IDs, trim whitespace, and standardize emails
    
    df = df.filter(col("user_id").isNotNull())
    if "updated_at" in df.columns:
        df = df.filter(col("updated_at").isNotNull())
    if "email" in df.columns:
        df = df.withColumn("email", lower(trim(col("email"))))
    df = df.withColumn("proper_name",initcap(trim(col("name")))) \
            .withColumn("created_at",from_utc_timestamp(col("created_at"),"Asia/Kolkata")) \
            .withColumn("updated_at",from_utc_timestamp(col("updated_at"),"Asia/Kolkata")) \
            .withColumn("user_id", col("user_id").cast("integer"))

    window = Window.partitionBy("user_id").orderBy(from_utc_timestamp(col("updated_at"),"Asia/Kolkata").desc())

    df = df.withColumn("rn", row_number().over(window)).filter(col("rn") ==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime",from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
           .withColumn("source", lit("mysql_users")) \
           .withColumn("batch_id", col("run_id"))

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
