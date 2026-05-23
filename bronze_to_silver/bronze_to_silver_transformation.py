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
            
        # 3. Apply Modular Transformations (Data Cleansing)
        if entity in TRANSFORMATION_MAP:
            # Convert DynamicFrame to Spark DataFrame
            df = dynamic_frame.toDF()
            
            # Apply the specific cleaning function
            transform_func = TRANSFORMATION_MAP[entity]
            cleaned_df = transform_func(df)
            
            # Convert back to DynamicFrame
            dynamic_frame = DynamicFrame.fromDF(cleaned_df, glueContext, f"cleaned_{entity}")
        else:
            print(f"Warning: No transformation logic defined for {entity}. Proceeding with raw data.")
        
        # 4. Write to Silver (Optimized Parquet Format)
        # We write out in Parquet format which is highly compressed and optimized for Athena/Redshift
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            format="parquet",
            connection_options={"path": silver_path},
            transformation_ctx=f"write_{entity}"
        )
        print(f"Successfully processed and wrote {entity} to Silver layer.")
        
    except Exception as e:
        print(f"Error processing {entity}: {str(e)}")

# Commit job bookmark to keep track of processed data
job.commit()
