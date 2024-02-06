from pyspark.sql.types import StructType, StructField, StringType

from enum import Enum




def create_empty_validation_table_statistics_dataframe(spark):
    schema = StructType([
        StructField("tablename", StringType(), True),
        StructField("date_processed", StringType(), True),
        StructField("Validation", StringType(), True),
        StructField("expected", StringType(), True),
        StructField("actual", StringType(), True),
        StructField("validation_output", StringType(), True)
    ])

    return spark.createDataFrame([], schema)


def create_empty_validation_column_statistics_dataframe(spark):
    schema = StructType([
        StructField("tablename", StringType(), True),
        StructField("date_processed", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("Validation ", StringType(), True),
        StructField("validation_output", StringType(), True)
    ])

    return spark.createDataFrame([], schema)


def create_empty_validation_result_dataframe(spark):
    schema = StructType([
        StructField("tablename", StringType(), True),
        StructField("date_processed", StringType(), True),
        StructField("validation_output", StringType(), True)
    ])

    return spark.createDataFrame([], schema)

