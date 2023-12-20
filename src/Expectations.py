import pandas as pd

import sys
import json
import platform
import os,requests
from pathlib import Path
import glob

from configs.config import snowflake_conn_prop_local as snowflake_conn_prop
from src.DataValidationContext import GEDataValidationContext
from src.BatchRequest import getBatchRequest 
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


def createExpectationSuite(context,suitename):
    context.create_expectation_suite(
    expectation_suite_name=suitename, overwrite_existing=True)
    
    
def createExpectations(session, context, suitename, local_batch_request, pandasdataframe, table):
    from snowflake.snowpark.functions import col
    
    # Creating the validator
    validator = context.get_validator(
        batch_request=local_batch_request, expectation_suite_name=suitename
    )

    # Retrieve the expectations from the table
    df_sql = session.table("CITIBIKE_2.VALIDATION.EXPECTATIONS").filter(col("TABLE_NAME") == table.upper())
    data = df_sql.collect()
    expectations_df = pd.DataFrame(data)

    if not expectations_df.empty:
        for _, row in expectations_df.iterrows():
            column_name = row['COLUMN_NAME']
            expectation_type = row['EXPECTATION']
            parameters = json.loads(row['PARAMETERS'])

            if column_name == "NONE":
                # Handle table-level expectations
                if expectation_type == 'expect_table_row_count_to_be_between':
                    min_value = parameters.get("min")
                    max_value = parameters.get("max")
                    # Assuming your validation framework supports a method like this
                    validator.expect_table_row_count_to_be_between(min_value, max_value)
                # Add more table-level expectation types as needed
            else:
                # Apply column-level expectations
                if expectation_type == 'expect_column_mean_to_be_between':
                    min_value = parameters.get("min")
                    max_value = parameters.get("max")
                    validator.expect_column_mean_to_be_between(column_name, min_value, max_value)
                elif expectation_type == 'expect_column_values_to_be_in_set':
                    expected_values = parameters.get("expectedValues", [])
                    validator.expect_column_values_to_be_in_set(column_name, expected_values)
                # Add more column-level expectation types as needed

    else:
        raise ValueError(f"No expectations found for the table '{table}'.")

    # Saving the expectation suite
    validator.save_expectation_suite(discard_failed_expectations=False)


