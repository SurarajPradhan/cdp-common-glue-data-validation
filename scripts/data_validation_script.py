from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

def read_and_count_from_s3(glue_context, s3_path):
    # Read data from S3 into a DynamicFrame
    dynamic_frame = glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path]},
        format="csv", # Change format based on your data
        format_options={"withHeader": True} # Change options based on your data format
    )

    # Count records
    record_count = dynamic_frame.count()
    return record_count

def write_count_to_s3(glue_context, record_count, output_bucket, output_key):
    # Convert count to Spark DataFrame
    sc = glue_context.spark_session
    counts_df = sc.createDataFrame([(record_count,)], ["count"])

    # Convert to Glue DynamicFrame
    counts_dynamic_frame = DynamicFrame.fromDF(counts_df, glue_context, "countsDF")

    # Write data out to S3 as Parquet
    glue_context.write_dynamic_frame.from_options(
        frame=counts_dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"s3://{output_bucket}/{output_key}"},
        format="parquet"
    )

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)

# Name to S3 path mapping
name_to_s3_path = {
    "name1": "s3://bucket1/path/to/data/",
    "name2": "s3://bucket2/path/to/data/",
    # Add more mappings as needed
}

# Output bucket and key
output_bucket = "your-output-bucket"
output_key_base = "output/path/"

# Process each dataset
for name, s3_path in name_to_s3_path.items():
    count = read_and_count_from_s3(glue_context, s3_path)
    output_key = f"{output_key_base}{name}/count.parquet"
    write_count_to_s3(glue_context, count, output_bucket, output_key)
