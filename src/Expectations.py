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
    
    
def createExpectations(context,suitename,local_batch_request,pandasdataframe, table):
    
    # Creating the validator which takes the batch request and expectation suite name
    validator = context.get_validator(
        batch_request=local_batch_request, expectation_suite_name=suitename
    )
    
    if table == "CONTACTS":
        # Expectations for table1
        validator.expect_column_mean_to_be_between("AGE", 20, 25)
        # Add more expectations specific to table1 as needed

    elif table == "TRIPS":
        # Expectations for table2
        validator.expect_column_mean_to_be_between("TRIPDURATION", 5, 500)
        # Add more expectations specific to table2 as needed
    
    else:
        # Error message if table name doesn't match any of the predefined tables
        raise ValueError(f"The table '{table}' does not exist.")

    #Saving the expectation 
    validator.save_expectation_suite(discard_failed_expectations=False)

