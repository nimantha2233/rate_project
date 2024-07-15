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

    def clean_dfs(self, dict_of_dfs : dict) -> dict:
        '''Remove unncessary rows etc to produce final DFs.

        NOTE: The dicts in the params and returns are both nested dicts (same dimension).

        :Params:
            dict_of_dfs (dict): Dict containg unclean DataFrames.

        :Returns:
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
    



class RateCardProcessor:
    '''Extract rate tables from rate card pdfs located in database/bronze/company_rate_cards'''

    def __init__(self):
        self.rate_card_filenames = os.listdir(sf.get_filepath(
            os.getcwd(),'database', 'bronze', 'company_rate_cards')
                                             )
        self.df_final1 = None
        self.df_final2_price_range = None
        self.cleaned_rate_card_dict = None
        self.cleaned_and_enriched_rate_card_dict = None


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
        # Add new attribute (column) to data
        cleaned_and_enriched_rate_card_dict = self.onshore_offshore_enrichment(
                                                            cleaned_rate_card_dict=cleaned_rate_card_dict
                                                                                   )
        self.cleaned_and_enriched_rate_card_dict = cleaned_and_enriched_rate_card_dict
        # concatenate all dfs together
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
            new_cols (list[str]): new_cols to use when referencing cols in other methods
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
        df_rate_card['level_name'] = df_rate_card[new_cols[0]].apply(lambda x: x[2:]\
                                                              .replace('\r', ' ').replace('.','').strip())
        
        df_rate_card[new_cols[0]] = df_rate_card[new_cols[0]].apply(lambda x: x[0])
        # Add new col where val is the id of the rate card
        df_rate_card['rate_card_id'] = rate_card_id                

        return df_rate_card
    
    def onshore_offshore_enrichment(self, cleaned_rate_card_dict : dict[list[pd.DataFrame]]) -> dict[list[pd.DataFrame]]:
        """Enriches rate card dfs by assigning onshore/offshore to each df via a new column. 

        Arguments:
            cleaned_rate_card_dict (Dict): Dict of the form {rate card ID : list of rate card dfs}

        Returns:
            cleaned_and_enriched_rate_card_dict (Dict): same dict as before but each df has new column.
        """
        cleaned_and_enriched_rate_card_dict = {}
        # levels = ['Apply', 'Assist', 'Enable', 'Ensure or advise', 'Follow', 'Initiate or influence']
        for rate_card_id, l_rate_card_dfs in cleaned_rate_card_dict.items():
            if len(l_rate_card_dfs) > 1:
                df1 = l_rate_card_dfs[0]
                df2 = l_rate_card_dfs[1]

                df1_price_for_comparison, df2_price_for_comparison = self.onshore_offshore_helper(df1=df1, df2=df2)

                # first rate card has higher day rate for same sfia level
                if df1_price_for_comparison > df2_price_for_comparison:
                    df1['on_off_shore'] = 'onshore'
                    df2['on_off_shore'] = 'offshore'
                # Second rate card has higher day rate for same sfia level
                else:
                    df1['on_off_shore'] = 'offshore'
                    df2['on_off_shore'] = 'onshore'

                l_rate_card_dfs = [df1, df2]
                
            # Only one rate card therefore must be onshore
            else:
                df = l_rate_card_dfs[0]
                df['on_off_shore'] = 'onshore'
                l_rate_card_dfs = [df]

            # Place newly enriched dfs into dict
            cleaned_and_enriched_rate_card_dict[rate_card_id] = l_rate_card_dfs
            
        return cleaned_and_enriched_rate_card_dict

    def onshore_offshore_helper(self, df1 : pd.DataFrame, df2 : pd.DataFrame) -> tuple[int]:
        """ Takes two rate cards from a single pdf and outputs prices from each  
        for the same SFIA category and level to determine which rate card is offshore.  

        Intakes two DataFrames:  
        
        -> limits columns to show ['level_name', 'development_and_implementation']  
        
        -> converts string digits to integers  
        
        -> filters for only digit (integer) rows   
        
        -> merges both dfs to obtain for which levels both rate cards have numbers present  
        
        -> gets first common level for price comparison  
        
        -> makes price comparison and assigns onshore/offshore labels accordingly  

            
        Arguments:
            df1 (pd.DataFrame) -- Input df of rate card data  

            df2 (pd.DataFrame) -- Input df of rate card data

        Returns:
            df1_price_for_comparison (int): Day rate for comparison 

            df2_price_for_comparison (int): Day rate for comparison
        """
        # Remove unused cols
        df1 = df1.loc[:, ['level_name', 'development_and_implementation']]
        df2 = df2.loc[:, ['level_name', 'development_and_implementation']]

        # Masks/Filters to have only rows with a number (day-rate)
        mask_digit_strings1 = df1['development_and_implementation'].apply(lambda x: x.isdigit() if x.isdigit() else x.isdigit())
        mask_digit_strings2 = df2['development_and_implementation'].apply(lambda x: x.isdigit() if x.isdigit() else x.isdigit())

        # find shared sfia levels to compare two day-rates against
        day_rate_comparison_level = pd.merge(df1[mask_digit_strings1]['level_name'], df2[mask_digit_strings2]['level_name'], on='level_name').loc[0]['level_name']

        # Get same category and level values to enable accurate comparison
        df1_price_for_comparison = int(df1[df1['level_name'] == day_rate_comparison_level].iloc[0]['development_and_implementation'])
        df2_price_for_comparison = int(df2[df2['level_name'] == day_rate_comparison_level].iloc[0]['development_and_implementation'])

        return (df1_price_for_comparison, df2_price_for_comparison)


    

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
            # only put 'N/A' if cell doesnt have price range
            if len(s) > 3:
                return s.replace(['nan','NA', '-'], 'N/A')
            else:
                return s
            
        # Quantitative columns
        cols_with_prices = [
                            'strategy_and_architecture', 'change_and_transformation', 'development_and_implementation'
                            , 'delivery_and_operation', 'people_and_skills', 'relationships_and_engagement'
                           ]
        
        
        # Filter for cells which aren't pure digits (e.g. has a hypen)
        mask_not_digit_string = df_concat[cols_with_prices]['strategy_and_architecture'].apply(lambda x: not x.isdigit())

        # Remove decimal point and digits after it (2 digits) & replace cells without digits with 'N/A'
        df_concat.loc[mask_not_digit_string] = df_concat[mask_not_digit_string].map(lambda x: str(x[:len(x)-3]) if '.' in str(x) else x).apply(non_digit_cleaner)
        
        # Removed decimals above so these cells won't be included in below mask anymore
        mask_not_digit_string = df_concat[cols_with_prices]['strategy_and_architecture'].apply(lambda x: not x.isdigit())
        
        # Separate dataframe for rate cards showing price range for each rate card
        mask_price_range = df_concat['strategy_and_architecture'].map(lambda x: '-' in x and x[0].isdigit() and x[-1].isdigit())
        df_price_range_rate_card = df_concat[mask_price_range].reset_index(drop = True)

        self.df_final1 = df_concat.loc[~mask_price_range].reset_index(drop=True)
        # Clean and assign df_final (1 price)
        # df_concat.loc[:, cols_with_prices] = df_concat[~mask_not_digit_string][cols_with_prices].map(lambda x: x.replace('-', 'N/A'))
        self.df_final1.loc[:, cols_with_prices] = self.df_final1[cols_with_prices].map(lambda x: x.replace('-', 'N/A'))
        self.df_final1.loc[:, cols_with_prices] = self.df_final1[cols_with_prices].fillna('N/A')

        # For rate cards showing a price range in each cell separate these into two new cols min and max
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



class CombineRates:
    """Combine rates data output by the DataProcessor and RateCardProcessor classes"""

    def __init__(self):
        """_summary_
        """            
        self.df_page_rates = None
        self.df_rates_final_minmax = None

    def get_service_page_rates_data(self):
        """ Through use of DataProcessor class 
        """        
        # Process service prices from service page data
        latest_scrape_filepath = sf.get_filepath('database', 'bronze', 'company_service_rates', 'company_info_last_run.csv')
        process_data = DataProcessor(filepath=latest_scrape_filepath)
        process_data.clean_cost_desc()
        process_data.create_metadata_and_derived_cols()
        df_dict = process_data.create_array_of_dfs()
        dict_of_dfs = process_data.clean_dfs(dict_of_dfs=df_dict)

        # Cleaned Services data with pricing
        df = process_data.df
        
        # Filter for only rates of the form "£x per unit per day"
        mask_unit_day = df['cost_split'].apply(lambda x: 'unit' in x and 'day' in x)
        df_unit_day = df.loc[mask_unit_day]

        # Groupby company and get max day rate - this represents day rate of most senior consultant
        df_unit_day = df_unit_day[['Company', 'Project', 'Cost', 'cost_split', 'base_price', 'max_price']]

        # Remove 'N/A'
        mask_na = df_unit_day['max_price'] != 'N/A'
        df_unit_day = df_unit_day.loc[mask_na]
        # Get max price for max and base, so there are no offshore rates
        df_max = df_unit_day.groupby(by='Company')[['max_price']].max().reset_index()
        df_min = df_unit_day.groupby(by='Company')[['base_price']].max().reset_index()

        # Merge max and min dfs
        df_page_rates = df_max.merge( df_min, on='Company', how='outer')
        df_page_rates = df_page_rates.rename(columns={'base_price' : 'min_price', 'Company' : 'company'})

        self.df_page_rates = df_page_rates

        return df_page_rates
    

    def get_rate_card_data(self):
        """ Using RateCardProcessor class extract RateCard day rates and filter for Junior and Senior rates.
        Final DataFrame has company name, min and max day rates, and rate_card_id.

        :Params:
            None: RateCardProcessor class is instantiated here

        :Returns: 
            df_rates_final_minmax (pd.DataFrame): cols = [company name, min day rates, max day rates, rate_card_id]
        """

        extract_ratecard_data = RateCardProcessor()
        df_ratecard_rates = extract_ratecard_data.proccess_rate_cards()

        # Masks for the most junior and most senior consultant day rates and not 'N/A' values
        mask_levels = df_ratecard_rates['level_name'].apply(lambda x: x in ['Follow', 'Set strategy or inspire'])
        mask_na =  df_ratecard_rates['development_and_implementation'] != 'N/A'

        # Filter together
        df_ratecard_rates_final = df_ratecard_rates.loc[mask_levels & mask_na][['level_name', 'development_and_implementation', 'rate_card_id']]
        df_ratecard_rates_final['development_and_implementation'] = df_ratecard_rates_final['development_and_implementation'].apply(lambda x: float(x))

        # Separate junior and senior for processing
        mask_level_follow = df_ratecard_rates_final['level_name'] == 'Follow'
        df_follow = df_ratecard_rates_final.loc[mask_level_follow].reset_index(drop=True)
        df_set_strat = df_ratecard_rates_final.loc[~mask_level_follow].reset_index(drop=True)

        # Rename columns
        df_follow = df_follow.rename(columns= {'development_and_implementation' : 'min_price'})
        df_set_strat = df_set_strat.rename(columns= {'development_and_implementation' : 'max_price'})

        # Get final prices for "Follow" (remove offshore rates data) and obtain min for each companys' offshore rates
        # Group by and obtain max for each rate card to ensure we dont capture offshore rates
        df_follow['company'] = df_follow['rate_card_id'].apply(lambda x: x.split('_')[0])
        df_follow = df_follow[['company', 'min_price', 'rate_card_id', 'level_name']]

        # Get max() from each rate card so we get onshore rates
        df_temp = df_follow.groupby(by=['company', 'rate_card_id'])[['min_price']].max().reset_index()
        # Groupby and obtain mean min rate (infosys has multiple rate cards)
        df_follow_final = df_temp.groupby(by='company')[['min_price']].mean().sort_values(by='min_price').reset_index()

        # Same as above but for Senior rates
        df_set_strat['company'] = df_set_strat['rate_card_id'].apply(lambda x: x.split('_')[0])
        df_set_strat = df_set_strat[['company', 'max_price', 'rate_card_id', 'level_name']]
        df_temp = df_set_strat.groupby(by=['company', 'rate_card_id'])[['max_price']].max().reset_index()

        # Temperary mask as infosys pdf has table spread across pages part of table isnt read in causing issues
        mask_temp = df_temp['max_price'] > 324
        df_temp = df_temp.loc[mask_temp]
        # Groupby and obtain mean for each company
        df_set_strat_final = df_temp.groupby(by='company')[['max_price']].mean().sort_values(by='max_price', ascending=False).reset_index()

        # Merge Junior and Senior rates to form one df
        df_rates_final_minmax = df_set_strat_final.merge(df_follow_final, on='company', how='outer')

        self.df_rates_final_minmax = df_rates_final_minmax

        return df_rates_final_minmax
    

    def combine_rates_data(self):
        """Combine rates data from the two sources. Primary source is the rate cards, however in the
        event that the tabular rate data in a pdf cannot be read by tabula OR a company doesn't make
        their rates publicly available then use the rates listed on their services page.

        :Params:
            None

        :Returns:
            df_combined (pd.DataFrame): Combined rates data to be verified.
        """
        # For companies with no rate card use pricing data displayed on page of a service
        mask_companies_no_ratecard_data = ~self.df_page_rates['company'].apply(lambda x: x.replace(' ','')).isin(self.df_rates_final_minmax['company'])
        df_page_rates_no_rate_card = self.df_page_rates.loc[mask_companies_no_ratecard_data]
        # Concatenate df of rate cards and page rates to fill in companies whose rate card isnt used. 
        df_combined = pd.concat([df_page_rates_no_rate_card,self.df_rates_final_minmax], ignore_index=True)

        return df_combined