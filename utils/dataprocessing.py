'''Module containing functions to read in and transform data into a useable form'''

import pandas as pd
import re
import numpy as np
from collections import defaultdict



class DataProcessor:
    '''Process data from scraping via reading csv file'''

    def __init__(self, filepath = r'C:\Users\NimanthaFernando\Innovation_Team_Projects\Market_Intelligence\govt_contracts.csv'):
        self.filepath = filepath
        self.df = pd.read_csv(filepath_or_buffer = filepath)

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
        self.df['cost_split'] = self.df['Cost'].str.replace(' to ', ' ').str.replace(' a ', ' ')\
            .str.replace(',','')\
            .str.replace(' an ', ' ')\
            .apply(lambda x: x.split(' '))



    def create_metadata_and_derived_cols(self):
        ''' Create new columns used for later processing of the data as well as for end-user useage (base and max)
        
        
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
            df_dict (dict): dict where entries based on whether the row is contains a cost that is a price range or a single price.                                         
        '''

        df_dict = defaultdict(list)
        # The number of numbers in price description
        nums_in_cost_cnt_dict = dict(self.df['num_of_nums_in_cost'].value_counts())

        # Filter For each type of pricing (1 number in desc, 2 nums, 3 nums etc) 
        for num_cnt in list(nums_in_cost_cnt_dict.keys()):
            # Filter for only number of numbers in price desc == num_cnt
            df_num_cnt_filter = self.df[self.df['num_of_nums_in_cost'] == num_cnt]
            # Reset idx and keep old idx vals incase of comparison
            df_num_cnt_filter.reset_index(inplace=True)
            df_num_cnt_filter.rename(inplace = True, columns = {'index' : 'original_idx'})

            num_words_in_desc_dict = df_num_cnt_filter['cost_split'].apply(lambda x: len(x[num_cnt:])).value_counts()
            
            # For the current num_cnt rows in original df iterate through the different desc (by num words after the numbers)
            for word_cnt in list(num_words_in_desc_dict.keys()):
                # produce Filter to keep rows with a specific num of words in price desc.
                word_cnt_mask = df_num_cnt_filter['cost_split'].apply(lambda x: len(x[num_cnt:]) == word_cnt)
                # Put this into a dict then iterate through each pricing type ()
                price_type_dict = dict(df_num_cnt_filter[word_cnt_mask]['cost_split'].apply(lambda x: x[num_cnt]).value_counts())

                for price_type in list(price_type_dict.keys()):
                    mask_single_price_type = df_num_cnt_filter[word_cnt_mask]['cost_split'].apply(lambda x: x[num_cnt]) == price_type
                    df_dict['cost_type_' + str(num_cnt)].append(['price_type_' + price_type, df_num_cnt_filter[word_cnt_mask][mask_single_price_type]])        

        return df_dict

    def clean_dfs(self, dict_array_of_dfs):
        '''Remove unncessary rows etc to produce final DFs for Output to xlsx files
        
        NOTE: Unfinished -> Need to reassign the clean dfs into the dict.
        '''
        for df_array in list(dict_array_of_dfs.values()):
            for list_element in df_array:
                df = list_element[1]
                df_clean = df.drop(columns=['original_idx', 'cost_split', 'cost_len', 'cost_numbers', 'num_of_nums_in_cost'])
                # display(df_clean.iloc[0:5])
        return 0
    





    