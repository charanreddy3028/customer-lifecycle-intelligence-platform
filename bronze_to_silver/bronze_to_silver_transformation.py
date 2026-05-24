import sys
# pyrefly: ignore [missing-import]
from awsglue.transforms import *
# pyrefly: ignore [missing-import]
from awsglue.utils import getResolvedOptions
# pyrefly: ignore [missing-import]
from pyspark.context import SparkContext
# pyrefly: ignore [missing-import]
from awsglue.context import GlueContext
# pyrefly: ignore [missing-import]
from awsglue.job import Job
# pyrefly: ignore [missing-import]
from awsglue.dynamicframe import DynamicFrame
from rdbms_transformations import TRANSFORMATION_MAP

# 1. Initialization and Parameter parsing
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bronze_bucket = args['BRONZE_BUCKET']
silver_bucket = args['SILVER_BUCKET']

# List of all entities we expect in the Bronze layer
entities = [
    "users",
    "activities",
    "opportunities",
    "refunds",
    "sessions",
    "counselors"
]

for entity in entities:
    print(f"Processing entity: {entity}")
    
    bronze_path = f"s3://{bronze_bucket}/bronze/{entity}/"
    silver_path = f"s3://{silver_bucket}/silver/{entity}/"
    
    try:
        # 2. Read from Bronze (Raw CSV/JSON)
        # Using dynamic frame to infer schema automatically
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={"paths": [bronze_path], "recurse": True},
            transformation_ctx=f"read_{entity}"
        )
        
        # If the dynamic frame is empty, skip to the next entity
        if dynamic_frame.count() == 0:
            print(f"No data found for {entity}, skipping.")
            continue
            
        # 3. Merge with Existing Silver Data (for Incremental Upserts)
        df = dynamic_frame.toDF()
        spark = glueContext.spark_session
        try:
            existing_silver_df = spark.read.parquet(silver_path)
            print(f"Found existing Silver data for {entity}. Unioning for deduplication.")
            df = existing_silver_df.unionByName(df, allowMissingColumns=True)
        except Exception:
            print(f"No existing Silver data found for {entity}. Processing as first run.")
            
        # 4. Apply Modular Transformations & Deduplication
        if entity in TRANSFORMATION_MAP:
            # The transformation function (e.g. transform_users) contains the Window logic 
            # that will automatically deduplicate the unioned DataFrame!
            transform_func = TRANSFORMATION_MAP[entity]
            final_df = transform_func(df)
        else:
            print(f"Warning: No transformation logic defined for {entity}. Proceeding without deduplication.")
            final_df = df
        
        # 5. Write to Silver (Optimized Parquet Format)
        # OVERWRITE mode replaces the Silver table with the newly merged and deduplicated dataset.
        final_df.write.mode("overwrite").parquet(silver_path)
        print(f"Successfully processed and wrote {entity} to Silver layer.")
        
    except Exception as e:
        print(f"Error processing {entity}: {str(e)}")

# Commit job bookmark to keep track of processed data
job.commit()
