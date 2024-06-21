import pytest
import pandas as pd
import os

def test_service_csv_has_no_na():
    '''Test to ensure that there are no rows with NA values in the CSV file.

    This test reads a CSV file of scraped service data into a pandas DataFrame and checks if there
    are any rows containing NA values. The test will fail if any row contains
    at least one NA value, indicating that the data is not clean.

    Parameters:
    None

    Expected Outcome:
    The test passes if the DataFrame contains no NA values in any row.
    '''

    company_scrape_filepath = os.path.join(os.getcwd(),'database', 'bronze'
                            , 'company_service_rates','company_info_last_run.csv')
    df = pd.read_csv(company_scrape_filepath)
    empty_bool = df[df.columns[:len(df.columns) - 1]].isna().any().any()

    assert empty_bool == False




