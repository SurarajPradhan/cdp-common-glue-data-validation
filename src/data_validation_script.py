import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from helpers.data_frame_helpers import *
from validators.data_validation_checks import *

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

job = Job(glueContext)
# Assume 'args' will contain the S3 bucket and the prefix for tables
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'TABLE_PREFIX'])

s3_bucket = args['S3_BUCKET']
print(f"s3_bucket: {s3_bucket}")

table_prefix = args['TABLE_PREFIX']
print(f"table_prefix: {table_prefix}")

# Create the empty DataFrame
validation_table_statistics_df = create_empty_validation_table_statistics_dataframe(spark)
validation_table_statistics_df.printSchema()
validation_columm_statistics_df = create_empty_validation_column_statistics_dataframe(spark)
validation_columm_statistics_df.printSchema()
validation_result_df = create_empty_validation_result_dataframe(spark)
validation_result_df.printSchema()

# Define your table names
table_names = ["ACTION_CODE_TYPE"]  # Replace with your actual table names


def getExpectedRowCount(table_name):
    return 100


def getExpectedColumnName(table_name):
    return 6


for table_name in table_names:
    # Construct S3 path for each table
    s3_path = f"s3://{s3_bucket}/{table_prefix}/{table_name}/"

    # Read data from S3 for each table
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path], "recurse": True},
        format="parquet"
    )

    # dynamic_frame.printSchema()
    # dynamic_frame.show()

    expected_count = getExpectedRowCount(table_name)
    expected_column_names = getExpectedColumnName(table_name)
    daff = dynamic_frame.toDF()
    daff.printSchema()
    daff.columns().foreach(print())

    # Perform data validation on table wide validations
    # validation_table_statistics_df = row_count_validation(dynamic_frame, expected_count, validation_table_statistics_df)
    # validation_table_statistics_df = validate_column_count(dynamic_frame, expected_count,

    # Writing table wide validated result data back to S3
    glueContext.write_dynamic_frame.from_options(
        frame=validation_table_statistics_df,
        connection_type="s3",
        connection_options={"path": f"s3://{s3_bucket}/validation-result/validation_table_statistics/"},
        format="parquet"
    )

# Writing column wide validated result data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=validation_columm_statistics_df,
    connection_type="s3",
    connection_options={"path": f"s3://{s3_bucket}/validation-result/validation_columm_statistics/"},
    format="parquet"
)

# Writing final table validation result data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=validation_result_df,
    connection_type="s3",
    connection_options={"path": f"s3://{s3_bucket}/validation-result/validation_result/"},
    format="parquet"
)
