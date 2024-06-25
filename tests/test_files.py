import pytest
import pandas as pd
import os

from utils import dataprocessing

def test_service_csv_has_no_na():
    '''Test to ensure that there are no rows with NA values in the CSV file.

    This test reads a CSV file of scraped service data into a pandas DataFrame and checks if there
    are any rows containing NA values. The test will fail if any row contains
    at least one NA value, indicating that the data is not clean.

    :Params:
        None

    :Expected Outcome:
        The test passes if the DataFrame contains no NA values in any row.
    '''

    company_scrape_filepath = os.path.join(os.getcwd(),'database', 'bronze'
                            , 'company_service_rates','company_info_last_run.csv')
    df = pd.read_csv(company_scrape_filepath)
    empty_bool = df[df.columns[:len(df.columns) - 1]].isna().any().any()

    assert empty_bool == False

# ------------------------------------------------------ RateCardCleaner Pipeline Tests ----------------------------------------------------------------------------------------------
# Using scope='module' means the RateCardProcessor is instantiated once per pytest run 
@pytest.fixture(scope='module')
def data_processor():
    '''Fixture to instantiate RateCardProcessor class and run transformation pipeline.
    This saves instantiating and runnin gpipeline multiple times
    
    :Params:
        None
    
    :Returns:
        process_rate_cards (dataprocessing.RateCardProcessor): Instance of class, now 
        other tests can use attribute.
    
    '''
    # Setup fixture to instantiate DataProcessor with sample data
    process_rate_cards = dataprocessing.RateCardProcessor()
    process_rate_cards.proccess_rate_cards()
    # df_concat = process_rate_cards.concat_all_dfs(clean_rate_cards_dict=process_rate_cards.cleaned_rate_card_dict)
    # process_rate_cards.separate_and_clean_unique_rate_cards(df_concat=df_concat)
    return process_rate_cards


@pytest.mark.rate_cards
def test_for_new_column_names(data_processor):
    '''Test to ensure different sets of column names (for one Dataframe)
    are known sets
    
    :Params:
    
    :Expected Outcome:
        length of dict containing any new column names == 0 
        (no new column names detected)
    '''
    new_column_names_dict = detect_new_column_names(data_processor.cleaned_rate_card_dict)

    # IfTrue then all cols are as expected
    assert len(new_column_names_dict) == 0




@pytest.mark.rate_cards
def test_str_types_in_df(data_processor):
    ''''''
    # Columns with prices per consultant
    cols_with_prices = [
                        'strategy_and_architecture', 'change_and_transformation',
                        'development_and_implementation', 'delivery_and_operation',
                        'people_and_skills', 'relationships_and_engagement'
                       ]
    
    # Fill cells without prices with str 'N/A'.
    # df_final1 contains rate cards with no price range in cells
    df = data_processor.df_final1[cols_with_prices].fillna('N/A')

    # Produce list of types of str in concatenated df_rate_dards
    types_of_str_in_df = list(df[cols_with_prices].fillna('N/A').map(lambda x: 'Number' if x.isdigit() else x).value_counts().to_dict().keys())
    str_type_dict = {}
    
    # Unravel list of lists
    for inner_list in types_of_str_in_df:
        for str_type in inner_list:
            # Method to get unique vals only
            str_type_dict[str_type] =  str_type 
    # Convert to list for assertion test
    list_of_unique_strs_prices = list(str_type_dict.keys())

    assert list_of_unique_strs_prices == ['Number', 'N/A']





def detect_new_column_names(clean_rate_cards_dict : dict) -> dict:
    '''Produces output for "test_for_new_column_names" test. Output is 
    a dict containing column names from rate card dfs that havent been detected.
    
    :Params:
        None: RateCardProcessor instantiated and necessary methods run to output df of cleaned rate cards
    
    '''
    df_array = list(clean_rate_cards_dict.values())

    # set of col names we expect a df to have
    expected_cols = {
        'most_common' : ['level', 'strategy_and_architecture', 'change_and_transformation', 'development_and_implementation'
                        , 'delivery_and_operation', 'people_and_skills', 'relationships_and_engagement', 'level_name', 'rate_card_id']
        , 'next_common' : ['business_change','solution_development_and_implementation' ,'service_management','procurement_and_management_support','client_interface']
                    }

    # collect any that are unique
    irregular_cols_dict = {}

    # Iterate through each df. Check if there are any cols which arent the above. Append the df and col to a dict containing all irregular col names {irregular_df_num : [df, cols_names_list]}
    cnt = 0
    for df_list in df_array:

        for df in df_list:
            # Initialise empty list for new df
            irregular_cols = []
            col_names = list(df.columns)

            for col_name in col_names:
                # Check if col_name is expected
                if col_name not in expected_cols['most_common']:
                    # Add it to the list
                    irregular_cols.append(col_name)

            # Have fixed cols so all cols should be the same and dict should be empty
            if irregular_cols or irregular_cols == expected_cols['next_common']:
                irregular_cols_dict[cnt] = [df, irregular_cols]
                cnt += 1

    return irregular_cols_dict





