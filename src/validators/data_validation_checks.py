import logging
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import Row

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Define a function to perform row count validation and append results to the validation DataFrame
def row_count_validation(table_data_frame, expected_count, validation_df):
    """
        Validate if the DataFrame contains the expected no or rows.
    Parameters:
    dataframe (DataFrame): The DataFrame to validate.
    expected_count (int): expected count of records .

    Returns:
    dataframe (DataFrame): The DataFrame to with new row of validation result .
    """
    actual_count = table_data_frame.count()
    validation_status = "passed" if actual_count == expected_count else "failed"
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create a new row with validation results
    new_row = Row(
        tablename=table_data_frame.rdd.name(),  # Assuming the DataFrame has a name
        date_processed=current_time,
        Validation="Row Count",
        expected=expected_count,
        actual=actual_count,
        validation_output=validation_status
    )

    # Append the new row to the empty validation DataFrame
    return validation_df.union(spark.createDataFrame([new_row]))


def validate_column_count(table_data_frame, expected_columns, validation_df):
    """
    Validate if the DataFrame contains the expected columns.

    Parameters:
    dataframe (DataFrame): The DataFrame to validate.
    expected_columns (List[str]): A list of expected column names.

    Returns:
    dataframe (DataFrame): The DataFrame to with new row of validation result .
    """
    # Get the list of actual column names from the DataFrame
    actual_columns = table_data_frame.toDF.columns
    validation_status = "passed" if set(actual_columns) == set(
        expected_columns) else "failed"
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create a new row with validation results
    new_row = Row(
        tablename=table_data_frame.toDF.rdd.name(),  # Assuming the DataFrame has a name
        date_processed=current_time,
        Validation="Row Count",
        expected=actual_columns.count(),
        actual=actual_columns.count(),
        validation_output=validation_status
    )

    # Append the new row to the empty validation DataFrame
    return validation_df.union(spark.createDataFrame([new_row]))
