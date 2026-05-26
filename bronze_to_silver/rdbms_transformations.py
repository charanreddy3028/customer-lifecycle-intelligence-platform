# pyrefly: ignore [missing-import]
from pandas.core import window
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, trim, lower, initcap, current_timestamp, from_utc_timestamp,row_number, lit, expr   
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
    if "activity_date" in df.columns:
        df = df.withColumn("activity_date",from_utc_timestamp(col("activity_date"),"Asia/Kolkata"))
    
    df = df.withColumn("activity_type",expr('CASE WHEN activity_type IN ("EMAIL_SENT", "EMAIL_OPENED","MEETING_SCHEDULED", "MEETING_HELD") THEN activity_type ELSE "OTHERS" END')) 

    window = Window.partitionBy("activity_id").orderBy(col("activity_date").desc())

    df = df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime", from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
            .withColumn("source",lit("mysql_activities")) \
            .withColumn("batch_id", col("run_id"))

    return df

def transform_opportunities(df):
    """Cleanse the opportunities table."""
    df = df.filter(col("opportunity_id").isNotNull())
    if "updated_at" in df.columns:
        df = df.withColumn("updated_at",from_utc_timestamp(col("updated_at"),"Asia/Kolkata"))
    if "created_at" in df.columns:
        df = df.withColumn("created_at",from_utc_timestamp(col("created_at"),"Asia/Kolkata"))

    window = Window.partitionBy("opportunity_id").orderBy(col("updated_at").desc())

    df = df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime", from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
            .withColumn("source",lit("mysql_opportunities")) \
            .withColumn("batch_id", col("run_id"))    
    return df

def transform_refunds(df):
    """Cleanse the refunds table."""
    df = df.filter(col("refund_id").isNotNull())
    if "refund_datetime" in df.columns:
        df = df.withColumn("refund_datetime",from_utc_timestamp(col("refund_datetime"),"Asia/Kolkata"))

    window = Window.partitionBy("refund_id").orderBy(col("refund_datetime").desc())

    df = df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime", from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
            .withColumn("source",lit("mysql_refunds")) \
            .withColumn("batch_id", col("run_id"))  
    
    # discrepancy flagging
    discrepancy_window = Window.partitionBy("payment_id").orderBy("refund_datetime")
    df = df.withColumn("payment_id_discrepancy",expr("CASE WHEN row_number() over(discrepancy_window) >1 THEN 'YES' ELSE 'NO' END"))

    return df

def transform_sessions(df):
    """Cleanse the sessions table."""
    df = df.filter(col("session_id").isNotNull())
    if "session_id" in df.columns:
        df = df.withColumn("session_datetime",from_utc_timestamp(col("session_datetime"),"Asia/Kolkata"))

    window = Window.partitionBy("session_id").orderBy(col("session_datetime").desc())

    df = df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime", from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
            .withColumn("source",lit("mysql_sessions")) \
            .withColumn("batch_id", col("run_id"))  
    
    return df

def transform_counselors(df):
    """Cleanse the counselors table."""
    df = df.filter(col("counselor_id").isNotNull())
    if "counselor_id" in df.columns:
        df = df.withColumn("updated_at",from_utc_timestamp(col("updated_at"),"Asia/Kolkata"))

    window = Window.partitionBy("counselor_id").orderBy(col("updated_at").desc())

    df = df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime", from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
            .withColumn("source",lit("mysql_counselors")) \
            .withColumn("batch_id", col("run_id")) 
    return df

def transform_calls(df):
    """Cleanse the calls table."""
    df = df.filter(col("call_id").isNotNull())
    if "call_datetime" in df.columns:
        df = df.withColumn("call_datetime",from_utc_timestamp(col("call_datetime"),"Asia/Kolkata"))

    window = Window.partitionBy("call_id").orderBy(col("call_datetime").desc())

    df = df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime", from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
            .withColumn("source",lit("mysql_calls")) \
            .withColumn("batch_id", col("run_id"))  
    
    return df

def transform_lead_status_history(df):
    """Cleanse the lead_status_history table."""
    df = df.filter(col("id").isNotNull())
    if "updated_at" in df.columns:
        df = df.withColumn("updated_at",from_utc_timestamp(col("updated_at"),"Asia/Kolkata"))

    window = Window.partitionBy("id").orderBy(col("updated_at").desc())

    df = df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime", from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
            .withColumn("source",lit("mysql_lead_status_history")) \
            .withColumn("batch_id", col("run_id"))  
    
    return df

def transform_sessionattendance(df):
    """Cleanse the sessionattendance table."""
    df = df.filter(col("attendance_id").isNotNull())
    if "joined_at" in df.columns:
        df = df.withColumn("joined_at",from_utc_timestamp(col("joined_at"),"Asia/Kolkata"))

    window = Window.partitionBy("attendance_id").orderBy(col("joined_at").desc())

    df = df.withColumn("rn",row_number().over(window)).filter(col("rn")==1).drop("rn")

    # Adding Metadata Columns
    df = df.withColumn("ingested_datetime", from_utc_timestamp(current_timestamp(),"Asia/Kolkata")) \
            .withColumn("source",lit("mysql_sessionattendance")) \
            .withColumn("batch_id", col("run_id"))  
    
    return df

# Dictionary mapping table names to their specific transformation functions
TRANSFORMATION_MAP = {
    "users": transform_users,
    "activities": transform_activities,
    "opportunities": transform_opportunities,
    "refunds": transform_refunds,
    "sessions": transform_sessions,
    "calls": transform_calls,
    "sessionattendance": transform_sessionattendance,
    "lead_status_history": transform_lead_status_history,
    "counselors": transform_counselors
}
