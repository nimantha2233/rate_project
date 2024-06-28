'''Module containing functions to read in and transform data into a useable form'''

import pandas as pd
import re
import numpy as np
from collections import defaultdict
import hashlib
import tabula
from . import supportfunctions as sf
import os
import multiprocessing
from time import time


class DataProcessor:
    '''Process data from scraping via reading csv file
    '''

    def __init__(self, filepath : str):
        self.filepath = filepath
        self.df = pd.read_csv(filepath_or_buffer = filepath)
        self.dict_of_rates = None

    def extract_numbers(self,lst : list) -> list:
        '''Extract numbers from an input list
        
        :Params:
            lst (list): list of strings containing numbers and words
        
        :Returns:
            (list): List of number recast as floats

        '''
        number_pattern = re.compile(r'^\d+\.?\d*$')
    
        return [float(x) for x in lst if number_pattern.match(x)]
    
    def clean_cost_desc(self):
        '''Transform cost column: 
                1. Remove unnecessary words and characters
                2. split remaining words/numbers (both currently strings)   

        :Returns:
            N/A as the dataframe is modified so no output.
        '''

        # Remove non-useful words
        self.df['cost_split'] = self.df['Cost'].str.replace(' to ', ' ')\
            .str.replace(' a ', ' ')\
            .str.replace(',','')\
            .str.replace(' an ', ' ')\
            .apply(lambda x: x.split(' '))


    def create_metadata_and_derived_cols(self):
        ''' Create new columns used for later processing of the data as well
            as for end-user useage (base and max)
        
        :Params:
            None: Cleaning of main DataFrame

        :Returns:
            None: Class attribute DataFrame is transformed after the method calll
        '''

        # Determine number of words in the cost desc. (len(x))
        self.df['cost_len'] = self.df['cost_split'].apply(lambda x: len(x))
        # Determine how many numbers in the list of cost words/numbers
        self.df['num_of_nums_in_cost'] = self.df['cost_split'].apply(self.extract_numbers).apply(lambda x: len(x))
        # Extract numbers and cast them as floats (were initially strings)
        self.df['cost_numbers'] = self.df['cost_split'].apply(self.extract_numbers)

        self.df['base_price'] = self.df['cost_numbers'].apply(lambda x: x[0])
        self.df['max_price'] = self.df['cost_numbers'].apply(lambda x: 'N/A' if len(x) == 1 else x[1])


    def create_array_of_dfs(self) -> dict:
        '''Output an dict with two entries based on whether the row is contains a cost that is a price range or not:  
        1. Not a price range just 1 price.
        2. Two prices and so indicative of a price range with a base and max.

        Each dict entry has an array as a value, where the array has 2 cols:  

        col_1: df name (cost_split_["price type"]
        col_2: df containing projects of a certain pricing scheme e.g. per unit/licence

        :Returns:
            df_dict (dict): A nested dict to store dataframe for easy accessibility

                            {  
                                'cost_type_1' : {'dataframe desc' : pd.DataFrame}  
                                , 'cost_type_2' : {'dataframe desc' : pd.DataFrame}  
                            }

        NOTE: Improve the data structure to be a nested dict.                                      
        '''

        df_dict = defaultdict(dict)
        # The number of numbers in price description
        nums_in_cost_cnt_dict = dict(self.df['num_of_nums_in_cost'].value_counts())

        # Filter For each type of pricing (1 number in desc, 2 nums, 3 nums etc) 
        for num_cnt in list(nums_in_cost_cnt_dict.keys()):
            # Filter for only number of numbers in price desc == num_cnt
            df_num_cnt_filter = self.df.loc[self.df['num_of_nums_in_cost'] == num_cnt]
            # Reset idx and keep old idx vals incase of comparison
            df_num_cnt_filter = df_num_cnt_filter.reset_index()
            df_num_cnt_filter = df_num_cnt_filter.rename( columns = {'index' : 'original_idx'})

            num_words_in_desc_dict = df_num_cnt_filter['cost_split'].apply(lambda x: len(x[num_cnt:])).value_counts()
            
            # For the current num_cnt rows in original df iterate through the different desc (by num words after the numbers)
            for word_cnt in list(num_words_in_desc_dict.keys()):
                # produce Filter to keep rows with a specific num of words in price desc.
                word_cnt_mask = df_num_cnt_filter['cost_split'].apply(lambda x: len(x[num_cnt:]) == word_cnt)
                # Put this into a dict then iterate through each pricing type ()
                price_type_dict = dict(df_num_cnt_filter[word_cnt_mask]['cost_split'].apply(lambda x: x[num_cnt]).value_counts())

                for price_type in list(price_type_dict.keys()):
                    mask_single_price_type = df_num_cnt_filter[word_cnt_mask]['cost_split'].apply(lambda x: x[num_cnt]) == price_type
                    filtered_df = df_num_cnt_filter[word_cnt_mask][mask_single_price_type]

                    df_dict['cost_type_' + str(num_cnt)]['price_type_' + price_type] = filtered_df
                    # df_dict['cost_type_' + str(num_cnt)].append(['price_type_' + price_type, df_num_cnt_filter[word_cnt_mask][mask_single_price_type]])        

        return df_dict

    def clean_dfs(self, dict_of_dfs):
        '''Remove unncessary rows etc to produce final DFs for Output to xlsx files
        
        NOTE: Unfinished -> Need to reassign the clean dfs into the dict.
        '''
        # Values are dicts themselves
        for price_type, df_dict in list(dict_of_dfs.items()):
            # Iterate through dfs and reassign transformed df to same k-v pair
            for df_name, df in df_dict.items():
                # Cols were only used during formation of dict of dfs
                df_clean = df.drop(columns=['original_idx', 'cost_split', 'cost_len', 'cost_numbers', 'num_of_nums_in_cost'])
                dict_of_dfs[price_type][df_name] = df_clean
        
        # Assign final dict to class attribute
        self.dict_of_rates = dict_of_dfs

        return dict_of_dfs
    



class RateCardProcessor:
    '''Extract rate tables from rate card pdfs located in database/bronze/company_rate_cards'''

    def __init__(self):
        self.rate_card_filenames = os.listdir(sf.get_filepath(
            os.getcwd(),'database', 'bronze', 'company_rate_cards')
                                             )
        self.df_final1 = None
        self.df_final2_price_range = None
        self.cleaned_rate_card_dict = None


    def proccess_rate_cards(self) -> pd.DataFrame:
        '''Process all rate cards and output csv file containing all ratecards.
        
        :Params:
            None: This function executes a chain of commands in the pipeline 
                  to extract and transform the rate card data in pdfs to useable Pandas DataFrames

        :Returns:
            df_final1: DataFrame of rate cards from pdfs for rate cards showing 
                       a single price not a range
        '''
        print('Extracting all tables from rate card pdfs...')
        rate_card_raw_tables_dict = self.extract_all_tables_from_pdfs()
        print('Filtering out non-rate card tables...')
        rate_cards_dict = self.filter_for_rate_card_tables(rate_card_raw_tables_dict)
        print('Cleaning rate card dfs...')
        cleaned_rate_card_dict = self.clean_rate_card_dfs(rate_cards_dict)
        self.cleaned_rate_card_dict = cleaned_rate_card_dict 
        df_concat = self.concat_all_dfs(cleaned_rate_card_dict=cleaned_rate_card_dict)
        self.separate_and_clean_unique_rate_cards(df_concat=df_concat)


        return self.df_final1


    def extract_all_tables_from_pdfs(self, path_to_rate_cards : list[str] = 
                                    ['database','bronze', 'company_rate_cards']):
        
        '''Extracts tables from PDF files and organizes them into a dictionary
        based on rate_card_id. {rate_card_id (str): tables_extracted_from_pdf (list[pd.DataFrame]) }

        Returns:
            rate_card_raw_tables_dict (defaultdict): A dictionary where keys are 
            company names and values are lists of tables extracted from 
            corresponding PDF files.
        '''
        rate_card_raw_tables_dict = defaultdict(list)
            
        # Iterate through the directory of pdfs
        for rate_card_filename in self.rate_card_filenames:
            # Get ratecard pdf filepath
            rate_card_filepath = sf.get_filepath(*path_to_rate_cards, rate_card_filename)    
            # Extract tables in pdf to list of tables 
            tables_list = tabula.read_pdf(rate_card_filepath, pages='all', lattice = True, multiple_tables=True, stream = True)
            # Get company name from pdf
            rate_card_id = rate_card_filename.split('.')[0]
            # Append tables_list to dict, key is rate card id so we can link final dfs to a rate card pdf
            rate_card_raw_tables_dict[rate_card_id] = tables_list



        return rate_card_raw_tables_dict


    def filter_for_rate_card_tables(self, rate_card_tables_dict : dict) -> defaultdict:

        ''' Filter for rate card tables in a dictionary of tables associated with rate card IDs.

        Args:
        rate_card_tables_dict (dict): A dictionary where keys are rate card IDs and values are lists of DataFrames
                                    representing tables extracted from rate card PDFs.

        Returns:
        rate_cards_dict (defaultdict): A filtered dictionary where keys are rate card IDs and values are lists of DataFrames
                    containing rate card tables that meet specific criteria.

        ----------- { rate_card_id (str) : list_of_actual_rate_card_dfs ( list[pd.DataFrame] ) } -----------
        '''
        rate_cards_dict = defaultdict(list)

        # Iterate through dict. Key is a list of list. Second list contains tables from 1 pdf
        for rate_card_id, table_list in rate_card_tables_dict.items():
            # Iterate through tables
            for df_table in table_list:
                rate_card_table_cols = ['Unnamed: 0', 'Strategy and\rarchitecture']
                column_exists = rate_card_table_cols[0] in df_table.columns \
                                and rate_card_table_cols[1] in df_table.columns
                
                if column_exists:
                    # Check if 'Unnamed: 0' column has no missing values
                    no_missing_values = not df_table['Unnamed: 0'].isna().any()

                    if no_missing_values:
                        contains_keyword = df_table['Unnamed: 0'].str.contains('.follow', case=False).any()

                        if contains_keyword:
                                # These are the rate card tables
                                rate_cards_dict[rate_card_id].append(df_table)

        return rate_cards_dict


    def clean_rate_card_dfs(self,rate_cards_dict):
        '''Transform rate card DataFrames: column renaming, add new cols, and transform vals in cols (remove £ or commas in prices)

        :Params:
            rate_cards_dict (dict): Dict where key = rate_card_id & value = list of rate card dfs to transform

        :Returns:
            transformed_rate_cards_dict (defaultdict): same as input dict structure but transformed dfs    
        '''

        # Initialise dict to place cleaned rate cards with rate_card_id as key
        transformed_rate_cards_dict = defaultdict(list)

        # Iterate through each pdf
        for rate_card_id, rate_cards_list in rate_cards_dict.items():
            # iterate through df rate cards from same pdf 
            for df_rate_card in rate_cards_list:
                # rename and clean columns (remove spaces and '\r' chars in names) 
                df_rate_card, new_cols = self.rename_columns(df_rate_card=df_rate_card)
                # Create a new columns and transform vals in another
                df_rate_card_final = self.create_and_transform_column_values(
                                    df_rate_card=df_rate_card, new_cols=new_cols
                                    , rate_card_id=rate_card_id)

                transformed_rate_cards_dict[rate_card_id].append(df_rate_card_final)
                
        return transformed_rate_cards_dict


    def rename_columns(self, df_rate_card):
        '''For rate card dfs from a single pdf clean and standardise column names.
        
        :Params:
            rate_cards_list (list[pd.DataFrame])
        
        :Returns:
            df_rate_card (pd.DataFrame): column names have been cleaned and standardise
        '''
        # Some rate cards dont use the correct SFIA categorys 
        # (for some reason they use categories) and The below mapping will correct this
        # by renaming the cols later in the func
        cols_mapping = {'level' : 'level', 'strategy_and_architecture' : 'strategy_and_architecture'
            , 'business_change' : 'change_and_transformation'
            ,'solution_development_and_implementation' : 'development_and_implementation'
            ,'service_management' : 'delivery_and_operation'
            ,'procurement_and_management_support' : 'people_and_skills'
            ,'client_interface' : 'relationships_and_engagement'
                        }

        old_cols = df_rate_card.columns
        new_cols = ['level']

        # First col name is cleaned manually above (has to be renamed not transformed like others)
        for col_name in old_cols[1:]:
            # clean col names
            new_col_name = col_name.replace('\r', ' ').replace(' ','_').lower()
            new_cols.append(new_col_name)
            df_rate_card[col_name] = df_rate_card[col_name].apply(lambda x: str(x).replace('£', '').replace(',',''))

        # Map old to new to use in rename cols method
        cols_dict = {old : new for old, new in zip(old_cols,new_cols)}
        
        # Rename columns to cleaned names
        df_rate_card = df_rate_card.rename(columns=cols_dict)
        # Some columns have been incorrectly named so rename these to the common standard
        if new_cols == list(cols_mapping.keys()):
            df_rate_card = df_rate_card.rename(columns=cols_mapping)          


        return df_rate_card, new_cols
                    

    def create_and_transform_column_values(self, rate_card_id, df_rate_card, new_cols):
        '''Create new cols to increase readability of data and transform vals in columns
        (remove currency labels and commas)

        :Params:
            rate_card_id (str): unique rate card identifier (company_name + pdf_filename)
            new_cols (list[str]): List of new column names
        
        :Returns:
            df_rate_card (pd.DataFrame): cleaned dataframe
        '''
        # Create separate first col in two with one have level name and other has level id
        df_rate_card['level_name'] = df_rate_card[new_cols[0]].apply(lambda x: x[2:].replace('\r', ' '))
        df_rate_card[new_cols[0]] = df_rate_card[new_cols[0]].apply(lambda x: x[0])
        # Add new col where val is the id of the rate card
        df_rate_card['rate_card_id'] = rate_card_id                

        return df_rate_card
    

    def concat_all_dfs(self, cleaned_rate_card_dict = None) -> pd.DataFrame:
        '''Intake a dict where the values are a list of dfs. Unravel these and concatenate them together
        
        :Params:
            clean_rate_cards_dict (dict): dict where key = rate_card_id & value = list of rate card dfs.

        :Returns:
            pd.DataFrame: concatenated rate cards
        '''
        
        list_of_final_rate_card_dfs = [] 
        for df_list in list(cleaned_rate_card_dict.values()):
            for df_rate_card in df_list:
                list_of_final_rate_card_dfs.append(df_rate_card)

        return pd.concat(list_of_final_rate_card_dfs). reset_index(drop = True)
    

    def separate_and_clean_unique_rate_cards(self, df_concat) -> pd.DataFrame:
        ''''''
        # cleaner func to clean cells in df
        def non_digit_cleaner(s):
            return s.replace(['nan','NA', '-'], 'N/A')
        # Quantitative columns
        cols_with_prices = df_concat.columns[1:len(df_concat.columns)-2]
        
        # Filter for cells which aren't pure digits (e.g. has a hypen)
        mask_not_digit_string = df_concat[cols_with_prices]['strategy_and_architecture'].apply(lambda x: not x.isdigit())

        # Remove decimal point and digits after it (2 digits) & replace cells without digits with 'N/A'
        df = df_concat[mask_not_digit_string].map(lambda x: str(x[:len(x)-3]) if '.' in str(x) else x).apply(non_digit_cleaner)

        # Clean and assign df_final (1 price)
        df_concat[cols_with_prices] = df_concat[~mask_not_digit_string][cols_with_prices].map(lambda x: x.replace('-', 'N/A'))
        self.df_final1 = df_concat
        self.df_final1[cols_with_prices] = self.df_final1[cols_with_prices].fillna('N/A')
        
        # Filter to obtain cells with a hyphen (price range)
        mask_price_range = df['strategy_and_architecture'].map(lambda x: '-' in x)
        df_price_range_rate_card = df[mask_price_range].reset_index(drop = True)
        # Split the price range into a 2 element list [min, max]
        # df_final = df_price_range_rate_card[cols_with_prices].map(lambda x: x.split('-') if '-' in x else x)

        # df_price_range_rate_card[cols_with_prices].map(lambda x: x.split('-') if '-' in x else x)
        for col in cols_with_prices:
            # Iterate thorugh list elements
            for i in range(1,3):
                # min here
                if i == 1:
                    df_price_range_rate_card[col + '_min'] = df_price_range_rate_card[col].apply((lambda x: x.split('-')[0] if '-' in x else x))
                # Max here
                else:
                    df_price_range_rate_card[col + '_max'] = df_price_range_rate_card[col].apply((lambda x: x.split('-')[1] if '-' in x else x))

        df_final = df_price_range_rate_card.drop(columns=cols_with_prices)
        self.df_final2_price_range = df_final

        return df_final
