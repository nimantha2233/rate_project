'''Module to extract clean and transform unstructured data from the 
database/bronze/company_service_rates/company_info_service_rates.csv 
file into a dataframe.Initially this data is used to extract minimum
 and maximum consultant day rates for companies who do not display a rate card.
 '''

import pandas as pd
import re
from collections import defaultdict
from . import supportfunctions as sf


class DataProcessor:
    '''Process data from scraping.

    Methods:
        extract_numbers: -
        clean_cost_desc: -
        create_metadata_and_derived_cols: -
        create_array_of_dfs:- 
        clean_dfs: -

    '''

    def __init__(self, filepath : str):
        self.filepath = filepath
        self.df = pd.read_csv(filepath_or_buffer = filepath)
        self.dict_of_rates = None

    def extract_numbers(self,lst : list) -> list:
        '''Extract numbers from an input list
        
        Params:
            lst (list): list of strings containing numbers and words
        
        Returns:
            (list): List of number recast as floats

        '''
        number_pattern = re.compile(r'^\d+\.?\d*$')
    
        return [float(x) for x in lst if number_pattern.match(x)]
    
    def clean_cost_desc(self):
        '''Transform cost column: 

                1. Remove unnecessary words and characters
                2. split remaining words/numbers (both currently strings)   
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

            - col_1: df name (cost_split_["price type"]
            - col_2: df containing projects of a certain pricing scheme e.g. per unit/licence

        Returns:
            df_dict (dict): A nested dict to store dataframe for easy accessibility.

                        -  {  
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

    def clean_dfs(self, dict_of_dfs : dict) -> dict:
        '''Remove unncessary rows etc to produce final DFs.

        NOTE: The dicts in the params and returns are both nested dicts (same dimension).

        Params:
            dict_of_dfs (dict): Dict containg unclean DataFrames.

        Returns:
            dict_of_dfs (dict): Dict containing clean DataFrames.

        
        
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
    


