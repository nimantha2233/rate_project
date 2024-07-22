'''Rate card operations: 

RateCardProccesor:
-> Extract rate card tables from pdf files

-> Clean and transform rate card dataframes



'''

import pandas as pd
from collections import defaultdict
import tabula
from . import supportfunctions as sf
import os


class RateCardFileHandler:
    def __init__(self, path_to_rate_cards):
        self.path_to_rate_cards = path_to_rate_cards
        self.rate_card_filenames = os.listdir(sf.get_filepath(os.getcwd(), *self.path_to_rate_cards))

    def extract_all_tables_from_pdfs(self):
        rate_card_raw_tables_dict = defaultdict(list)

        for rate_card_filename in self.rate_card_filenames:
            rate_card_filepath = sf.get_filepath(*self.path_to_rate_cards, rate_card_filename)
            tables_list = tabula.read_pdf(rate_card_filepath, pages='all', lattice=True, multiple_tables=True, stream=True)
            rate_card_file = rate_card_filename.split('.')[0]
            rate_card_raw_tables_dict[rate_card_file] = tables_list

        return rate_card_raw_tables_dict

class RateCardFilter:
    @staticmethod
    def filter_for_rate_card_tables(rate_card_tables_dict):
        rate_cards_dict = defaultdict(list)
        for rate_card_file, table_list in rate_card_tables_dict.items():
            for df_table in table_list:
                if 'Unnamed: 0' in df_table.columns and 'Strategy and\rarchitecture' in df_table.columns:
                    if not df_table['Unnamed: 0'].isna().any() and df_table['Unnamed: 0'].str.contains('.follow', case=False).any():
                        rate_cards_dict[rate_card_file].append(df_table)
        return rate_cards_dict

class RateCardCleaner:
    @staticmethod
    def rename_columns(df_rate_card):
        cols_mapping = {
            'level': 'level', 
            'strategy_and_architecture': 'strategy_and_architecture',
            'business_change': 'change_and_transformation',
            'solution_development_and_implementation': 'development_and_implementation',
            'service_management': 'delivery_and_operation',
            'procurement_and_management_support': 'people_and_skills',
            'client_interface': 'relationships_and_engagement'
        }
        old_cols = df_rate_card.columns
        new_cols = ['level']
        for col_name in old_cols[1:]:
            new_col_name = col_name.replace('\r', ' ').replace(' ', '_').lower()
            new_cols.append(new_col_name)
            df_rate_card[col_name] = df_rate_card[col_name].apply(lambda x: str(x).replace('£', '').replace(',', ''))
        cols_dict = {old: new for old, new in zip(old_cols, new_cols)}
        df_rate_card = df_rate_card.rename(columns=cols_dict)
        if new_cols == list(cols_mapping.keys()):
            df_rate_card = df_rate_card.rename(columns=cols_mapping)
        return df_rate_card, new_cols

    @staticmethod
    def create_and_transform_column_values(rate_card_file, df_rate_card, new_cols):
        df_rate_card['level_name'] = df_rate_card[new_cols[0]].apply(lambda x: x[2:].replace('\r', ' ').replace('.', '').strip())
        df_rate_card[new_cols[0]] = df_rate_card[new_cols[0]].apply(lambda x: x[0])
        df_rate_card['rate_card_file'] = rate_card_file
        return df_rate_card

class RateCardEnricher:
    @staticmethod
    def onshore_offshore_enrichment(cleaned_rate_card_dict):
        cleaned_and_enriched_rate_card_dict = {}
        for rate_card_file, l_rate_card_dfs in cleaned_rate_card_dict.items():
            if len(l_rate_card_dfs) > 1:
                df1, df2 = l_rate_card_dfs[0], l_rate_card_dfs[1]
                df1_price_for_comparison, df2_price_for_comparison = RateCardEnricher.onshore_offshore_helper(df1, df2)
                if df1_price_for_comparison > df2_price_for_comparison:
                    df1['location_type'] = 'onshore'
                    df2['location_type'] = 'offshore'
                else:
                    df1['location_type'] = 'offshore'
                    df2['location_type'] = 'onshore'
                l_rate_card_dfs = [df1, df2]
            else:
                df = l_rate_card_dfs[0]
                df['location_type'] = 'onshore'
                l_rate_card_dfs = [df]
            cleaned_and_enriched_rate_card_dict[rate_card_file] = l_rate_card_dfs
        return cleaned_and_enriched_rate_card_dict

    @staticmethod
    def onshore_offshore_helper(df1, df2):
        df1 = df1.loc[:, ['level_name', 'development_and_implementation']]
        df2 = df2.loc[:, ['level_name', 'development_and_implementation']]
        mask_digit_strings1 = df1['development_and_implementation'].apply(lambda x: x.isdigit())
        mask_digit_strings2 = df2['development_and_implementation'].apply(lambda x: x.isdigit())
        day_rate_comparison_level = pd.merge(df1[mask_digit_strings1]['level_name'], df2[mask_digit_strings2]['level_name'], on='level_name').iloc[0]['level_name']
        df1_price_for_comparison = int(df1[df1['level_name'] == day_rate_comparison_level].iloc[0]['development_and_implementation'])
        df2_price_for_comparison = int(df2[df2['level_name'] == day_rate_comparison_level].iloc[0]['development_and_implementation'])
        return df1_price_for_comparison, df2_price_for_comparison

class RateCardAggregator:
    @staticmethod
    def concat_all_dfs(cleaned_rate_card_dict):
        list_of_final_rate_card_dfs = []
        for df_list in cleaned_rate_card_dict.values():
            list_of_final_rate_card_dfs.extend(df_list)
        return pd.concat(list_of_final_rate_card_dfs).reset_index(drop=True)

    @staticmethod
    def separate_and_clean_unique_rate_cards(df_concat):
        def non_digit_cleaner(s):
            if len(s) > 3:
                return s.replace(['nan', 'NA', '-'], 'N/A')
            else:
                return s
        
        cols_with_prices = [
            'strategy_and_architecture', 'change_and_transformation', 'development_and_implementation',
            'delivery_and_operation', 'people_and_skills', 'relationships_and_engagement'
        ]
        
        mask_not_digit_string = df_concat[cols_with_prices]['strategy_and_architecture'].apply(lambda x: not x.isdigit())
        df_concat.loc[mask_not_digit_string] = df_concat[mask_not_digit_string].map(lambda x: str(x[:len(x)-3]) if '.' in str(x) else x).apply(non_digit_cleaner)
        mask_not_digit_string = df_concat[cols_with_prices]['strategy_and_architecture'].apply(lambda x: not x.isdigit())
        mask_price_range = df_concat['strategy_and_architecture'].map(lambda x: '-' in x and x[0].isdigit() and x[-1].isdigit())
        df_price_range_rate_card = df_concat[mask_price_range].reset_index(drop=True)

        df_final1 = df_concat.loc[~mask_price_range].reset_index(drop=True)
        df_final1.loc[:, cols_with_prices] = df_final1[cols_with_prices].map(lambda x: x.replace('-', 'N/A'))
        df_final1.loc[:, cols_with_prices] = df_final1[cols_with_prices].fillna('N/A')

        for col in cols_with_prices:
            for i in range(1, 3):
                if i == 1:
                    df_price_range_rate_card[col + '_min'] = df_price_range_rate_card[col].apply((lambda x: x.split('-')[0] if '-' in x else x))
                else:
                    df_price_range_rate_card[col + '_max'] = df_price_range_rate_card[col].apply((lambda x: x.split('-')[1] if '-' in x else x))

        df_final2 = df_price_range_rate_card.drop(columns=cols_with_prices)

        return df_final1, df_final2




class RateCardProcessor:
    def __init__(self):
        self.file_handler = RateCardFileHandler(['database', 'bronze', 'company_rate_cards'])
        self.filter = RateCardFilter()
        self.cleaner = RateCardCleaner()
        self.enricher = RateCardEnricher()
        self.aggregator = RateCardAggregator()
        # self.transformer = RateCardTransformer()
        self.df_final1 = None
        self.df_final2_price_range = None
        self.cleaned_rate_card_dict = None
        self.cleaned_and_enriched_rate_card_dict = None
        self.final_df1_output_location = sf.get_filepath('database', 'silver', 'rate_cards', 'rate_cards_silver.csv')
        self.df_gold_filepath = sf.get_filepath('database', 'gold', 'gold_rate_card.csv')

    def process_rate_cards(self) -> pd.DataFrame:
        """Instantiate and run methods from various classes of RateCardProccesor pipeline   
        to ultimately write silver and gold level date card csv files.

        Returns:
            df_rate_card_gold (pd.DataFrame): gold level rate card data with columns: company, rate_card_id, location_type
                                              , sfia_levels (price for each exp. level)
        """
        print('Extracting all tables from rate card pdfs...')
        rate_card_raw_tables_dict = self.file_handler.extract_all_tables_from_pdfs()

        print('Filtering for rate card tables...')
        rate_cards_dict = self.filter.filter_for_rate_card_tables(rate_card_raw_tables_dict)

        print('Cleaning rate card tables...')
        cleaned_rate_card_dict = defaultdict(list)
        for rate_card_file, rate_card_dfs_list in rate_cards_dict.items():
            for df_rate_card in rate_card_dfs_list:
                df_rate_card, new_cols = self.cleaner.rename_columns(df_rate_card)
                df_rate_card = self.cleaner.create_and_transform_column_values(rate_card_file, df_rate_card, new_cols)
                cleaned_rate_card_dict[rate_card_file].append(df_rate_card)
        self.cleaned_rate_card_dict = cleaned_rate_card_dict

        print('Enriching rate card tables...')
        self.cleaned_and_enriched_rate_card_dict = self.enricher.onshore_offshore_enrichment(self.cleaned_rate_card_dict)

        print('Concatenating all dataframes...')
        df_concat = self.aggregator.concat_all_dfs(self.cleaned_and_enriched_rate_card_dict)

        print('Separating unique and price range rate cards...')
        self.df_final1, self.df_final2_price_range = self.aggregator.separate_and_clean_unique_rate_cards(df_concat=df_concat)

        # Write to csv file
        self.df_final1.to_csv(self.final_df1_output_location)

        # # add cols: company and rate_card_id then pivot data
        # df_rate_card_gold = self.transformer.transform_rate_card_data()

        # df_rate_card_gold.to_csv(self.df_gold_filepath)

        return self.df_final1, self.df_final2_price_range



class RateCardTransformer:
    """Prepare dataframes to extract statistical measures from them and to produce Monte Carlos"""

    def __init__(self, silver_rate_card_filepath : str = sf.get_filepath(
                                    'database', 'silver', 'rate_cards', 'rate_cards_silver.csv'
                                                                              )) -> None:
        
        self.df_rate_card_silver = pd.read_csv(filepath_or_buffer=silver_rate_card_filepath)
        # sfia table gold layer location
        self.gold_rate_card_path = sf.get_filepath('database', 'gold', 'dim_ratecard.csv')

    def transform_rate_card_data(self):
        
        # Add columns: company and rate_card_id then pivot df
        df_rate_card_gold = self.transform_and_pivot(df_rate_card_silver=self.df_rate_card_silver)
        

        return df_rate_card_gold


    
    def  transform_and_pivot(self, df_rate_card_silver : pd.DataFrame) -> pd.DataFrame:

        # Filter out unncecessary cols - 'development_and_implementation' is only category relevant for Kubrick Group
        cols = ['company', 'location_type', 'level_name', 'development_and_implementation', 'rate_card_id']

        # sfia table gold layer location
        dim_rate_card_path = sf.get_filepath('database', 'gold', 'dim_ratecard.csv')

        # Add new col: rate_card id, using dimension rate card csv
        df_dim_rate_card = pd.read_csv(filepath_or_buffer=dim_rate_card_path, index_col=0)
        rate_card_id_dict = dict(zip(df_dim_rate_card['rate_card_file'], df_dim_rate_card['id']))

        # Replace rate_card_filename with id
        df_rate_card_silver['rate_card_id'] = df_rate_card_silver['rate_card_file'].apply(lambda x: rate_card_id_dict[x])
        # Modify Output from RateCardProcessor to have separate company column
        df_rate_card_silver['company'] = df_rate_card_silver['rate_card_file'].apply(lambda x: x.split('_')[0])


        # Pivot SFIA level names - this is now gold level data ready for reporting
        df_rate_card_gold = df_rate_card_silver.loc[:, cols].\
            pivot(index=['rate_card_id', 'company', 'location_type']
                , columns='level_name', values='development_and_implementation').reset_index()

        return df_rate_card_gold





        


        



# class RateCardProcessor:
#     '''Extract rate tables from rate card pdfs located in database/bronze/company_rate_cards'''

#     def __init__(self):
#         self.rate_card_filenames = os.listdir(sf.get_filepath(
#             os.getcwd(),'database', 'bronze', 'company_rate_cards')
#                                              )
#         self.df_final1 = None
#         self.df_final2_price_range = None
#         self.cleaned_rate_card_dict = None
#         self.cleaned_and_enriched_rate_card_dict = None
#         self.final_df1_output_location = sf.get_filepath('database', 'silver', 'rate_cards','rate_cards_silver.csv')


#     def proccess_rate_cards(self) -> pd.DataFrame:
#         '''Process all rate cards and output csv file containing all ratecards.
        
#         :Params:
#             None: This function executes a chain of commands in the pipeline 
#                   to extract and transform the rate card data in pdfs to useable Pandas DataFrames

#         :Returns:
#             df_final1: DataFrame of rate cards from pdfs for rate cards showing 
#                        a single price not a range
#         '''
#         print('Extracting all tables from rate card pdfs...')
#         rate_card_raw_tables_dict = self.extract_all_tables_from_pdfs()
#         print('Filtering out non-rate card tables...')
#         rate_cards_dict = self.filter_for_rate_card_tables(rate_card_raw_tables_dict)
#         print('Cleaning rate card dfs...')
#         cleaned_rate_card_dict = self.clean_rate_card_dfs(rate_cards_dict)
#         self.cleaned_rate_card_dict = cleaned_rate_card_dict 
#         # Add new attribute (column) to data
#         cleaned_and_enriched_rate_card_dict = self.onshore_offshore_enrichment(
#                                                             cleaned_rate_card_dict=cleaned_rate_card_dict
#                                                                                    )
#         self.cleaned_and_enriched_rate_card_dict = cleaned_and_enriched_rate_card_dict
#         # concatenate all dfs together
#         df_concat = self.concat_all_dfs(cleaned_rate_card_dict=cleaned_rate_card_dict)
#         self.separate_and_clean_unique_rate_cards(df_concat=df_concat)


#         return self.df_final1


#     def extract_all_tables_from_pdfs(self, path_to_rate_cards : list[str] = 
#                                     ['database','bronze', 'company_rate_cards']):
        
#         '''Extracts tables from PDF files and organizes them into a dictionary
#         based on rate_card_file. {rate_card_file (str): tables_extracted_from_pdf (list[pd.DataFrame]) }

#         Returns:
#             rate_card_raw_tables_dict (defaultdict): A dictionary where keys are 
#             company names and values are lists of tables extracted from 
#             corresponding PDF files.
#         '''
#         rate_card_raw_tables_dict = defaultdict(list)
            
#         # Iterate through the directory of pdfs
#         for rate_card_filename in self.rate_card_filenames:
#             # Get ratecard pdf filepath
#             rate_card_filepath = sf.get_filepath(*path_to_rate_cards, rate_card_filename)    
#             # Extract tables in pdf to list of tables 
#             tables_list = tabula.read_pdf(rate_card_filepath, pages='all', lattice = True, multiple_tables=True, stream = True)
#             # Get company name from pdf
#             rate_card_file = rate_card_filename.split('.')[0]
#             # Append tables_list to dict, key is rate card id so we can link final dfs to a rate card pdf
#             rate_card_raw_tables_dict[rate_card_file] = tables_list



#         return rate_card_raw_tables_dict


#     def filter_for_rate_card_tables(self, rate_card_tables_dict : dict) -> defaultdict:

#         ''' Filter for rate card tables in a dictionary of tables associated with rate card IDs.

#         Args:
#         rate_card_tables_dict (dict): A dictionary where keys are rate card IDs and values are lists of DataFrames
#                                     representing tables extracted from rate card PDFs.

#         Returns:
#         rate_cards_dict (defaultdict): A filtered dictionary where keys are rate card IDs and values are lists of DataFrames
#                     containing rate card tables that meet specific criteria.

#         ----------- { rate_card_file (str) : list_of_actual_rate_card_dfs ( list[pd.DataFrame] ) } -----------
#         '''
#         rate_cards_dict = defaultdict(list)

#         # Iterate through dict. Key is a list of list. Second list contains tables from 1 pdf
#         for rate_card_file, table_list in rate_card_tables_dict.items():
#             # Iterate through tables
#             for df_table in table_list:
#                 rate_card_table_cols = ['Unnamed: 0', 'Strategy and\rarchitecture']
#                 column_exists = rate_card_table_cols[0] in df_table.columns \
#                                 and rate_card_table_cols[1] in df_table.columns
                
#                 if column_exists:
#                     # Check if 'Unnamed: 0' column has no missing values
#                     no_missing_values = not df_table['Unnamed: 0'].isna().any()

#                     if no_missing_values:
#                         contains_keyword = df_table['Unnamed: 0'].str.contains('.follow', case=False).any()

#                         if contains_keyword:
#                                 # These are the rate card tables
#                                 rate_cards_dict[rate_card_file].append(df_table)

#         return rate_cards_dict


#     def clean_rate_card_dfs(self,rate_cards_dict):
#         '''Transform rate card DataFrames: column renaming, add new cols, and transform vals in cols (remove £ or commas in prices)

#         :Params:
#             rate_cards_dict (dict): Dict where key = rate_card_file & value = list of rate card dfs to transform

#         :Returns:
#             transformed_rate_cards_dict (defaultdict): same as input dict structure but transformed dfs    
#         '''

#         # Initialise dict to place cleaned rate cards with rate_card_file as key
#         transformed_rate_cards_dict = defaultdict(list)

#         # Iterate through each pdf
#         for rate_card_file, rate_cards_list in rate_cards_dict.items():
#             # iterate through df rate cards from same pdf 
#             for df_rate_card in rate_cards_list:
#                 # rename and clean columns (remove spaces and '\r' chars in names) 
#                 df_rate_card, new_cols = self.rename_columns(df_rate_card=df_rate_card)
#                 # Create a new columns and transform vals in another
#                 df_rate_card_final = self.create_and_transform_column_values(
#                                     df_rate_card=df_rate_card, new_cols=new_cols
#                                     , rate_card_file=rate_card_file)

#                 transformed_rate_cards_dict[rate_card_file].append(df_rate_card_final)
                
#         return transformed_rate_cards_dict
    
#     def rename_columns(self, df_rate_card):
#         '''For rate card dfs from a single pdf clean and standardise column names.
        
#         :Params:
#             rate_cards_list (list[pd.DataFrame])
        
#         :Returns:
#             df_rate_card (pd.DataFrame): column names have been cleaned and standardise
#             new_cols (list[str]): new_cols to use when referencing cols in other methods
#         '''
#         # Some rate cards dont use the correct SFIA categorys 
#         # (for some reason they use categories) and The below mapping will correct this
#         # by renaming the cols later in the func
#         cols_mapping = {'level' : 'level', 'strategy_and_architecture' : 'strategy_and_architecture'
#             , 'business_change' : 'change_and_transformation'
#             ,'solution_development_and_implementation' : 'development_and_implementation'
#             ,'service_management' : 'delivery_and_operation'
#             ,'procurement_and_management_support' : 'people_and_skills'
#             ,'client_interface' : 'relationships_and_engagement'
#                         }
    
#         old_cols = df_rate_card.columns
#         new_cols = ['level']

#         # First col name is cleaned manually above (has to be renamed not transformed like others)
#         for col_name in old_cols[1:]:
#             # clean col names
#             new_col_name = col_name.replace('\r', ' ').replace(' ','_').lower()
#             new_cols.append(new_col_name)
#             df_rate_card[col_name] = df_rate_card[col_name].apply(lambda x: str(x).replace('£', '').replace(',',''))

#         # Map old to new to use in rename cols method
#         cols_dict = {old : new for old, new in zip(old_cols,new_cols)}
        
#         # Rename columns to cleaned names
#         df_rate_card = df_rate_card.rename(columns=cols_dict)
#         # Some columns have been incorrectly named so rename these to the common standard
#         if new_cols == list(cols_mapping.keys()):
#             df_rate_card = df_rate_card.rename(columns=cols_mapping)          


#         return df_rate_card, new_cols
                    

#     def create_and_transform_column_values(self, rate_card_file, df_rate_card, new_cols):
#         '''Create new cols to increase readability of data and transform vals in columns
#         (remove currency labels and commas)

#         :Params:
#             rate_card_file (str): unique rate card identifier (company_name + pdf_filename)
#             new_cols (list[str]): List of new column names
        
#         :Returns:
#             df_rate_card (pd.DataFrame): cleaned dataframe
#         '''
#         # Create separate first col in two with one have level name and other has level id
#         df_rate_card['level_name'] = df_rate_card[new_cols[0]].apply(lambda x: x[2:]\
#                                                               .replace('\r', ' ').replace('.','').strip())
        
#         df_rate_card[new_cols[0]] = df_rate_card[new_cols[0]].apply(lambda x: x[0])
#         # Add new col where val is the id of the rate card
#         df_rate_card['rate_card_file'] = rate_card_file                

#         return df_rate_card
    
#     def onshore_offshore_enrichment(self, cleaned_rate_card_dict : dict[list[pd.DataFrame]]) -> dict[list[pd.DataFrame]]:
#         """Enriches rate card dfs by assigning onshore/offshore to each df via a new column. 

#         Arguments:
#             cleaned_rate_card_dict (Dict): Dict of the form {rate card ID : list of rate card dfs}

#         Returns:
#             cleaned_and_enriched_rate_card_dict (Dict): same dict as before but each df has new column.
#         """
#         cleaned_and_enriched_rate_card_dict = {}
#         # levels = ['Apply', 'Assist', 'Enable', 'Ensure or advise', 'Follow', 'Initiate or influence']
#         for rate_card_file, l_rate_card_dfs in cleaned_rate_card_dict.items():
#             if len(l_rate_card_dfs) > 1:
#                 df1 = l_rate_card_dfs[0]
#                 df2 = l_rate_card_dfs[1]

#                 df1_price_for_comparison, df2_price_for_comparison = self.onshore_offshore_helper(df1=df1, df2=df2)

#                 # first rate card has higher day rate for same sfia level
#                 if df1_price_for_comparison > df2_price_for_comparison:
#                     df1['on_off_shore'] = 'onshore'
#                     df2['on_off_shore'] = 'offshore'
#                 # Second rate card has higher day rate for same sfia level
#                 else:
#                     df1['on_off_shore'] = 'offshore'
#                     df2['on_off_shore'] = 'onshore'

#                 l_rate_card_dfs = [df1, df2]
                
#             # Only one rate card therefore must be onshore
#             else:
#                 df = l_rate_card_dfs[0]
#                 df['on_off_shore'] = 'onshore'
#                 l_rate_card_dfs = [df]

#             # Place newly enriched dfs into dict
#             cleaned_and_enriched_rate_card_dict[rate_card_file] = l_rate_card_dfs
            
#         return cleaned_and_enriched_rate_card_dict

#     def onshore_offshore_helper(self, df1 : pd.DataFrame, df2 : pd.DataFrame) -> tuple[int]:
#         """ Takes two rate cards from a single pdf and outputs prices from each  
#         for the same SFIA category and level to determine which rate card is offshore.  

#         Intakes two DataFrames:  
        
#         -> limits columns to show ['level_name', 'development_and_implementation']  
        
#         -> converts string digits to integers  
        
#         -> filters for only digit (integer) rows   
        
#         -> merges both dfs to obtain for which levels both rate cards have numbers present  
        
#         -> gets first common level for price comparison  
        
#         -> makes price comparison and assigns onshore/offshore labels accordingly  

            
#         Arguments:
#             df1 (pd.DataFrame) -- Input df of rate card data  

#             df2 (pd.DataFrame) -- Input df of rate card data

#         Returns:
#             df1_price_for_comparison (int): Day rate for comparison 

#             df2_price_for_comparison (int): Day rate for comparison
#         """
#         # Remove unused cols
#         df1 = df1.loc[:, ['level_name', 'development_and_implementation']]
#         df2 = df2.loc[:, ['level_name', 'development_and_implementation']]

#         # Masks/Filters to have only rows with a number (day-rate)
#         mask_digit_strings1 = df1['development_and_implementation'].apply(lambda x: x.isdigit() if x.isdigit() else x.isdigit())
#         mask_digit_strings2 = df2['development_and_implementation'].apply(lambda x: x.isdigit() if x.isdigit() else x.isdigit())

#         # find shared sfia levels to compare two day-rates against
#         day_rate_comparison_level = pd.merge(df1[mask_digit_strings1]['level_name'], df2[mask_digit_strings2]['level_name'], on='level_name').loc[0]['level_name']

#         # Get same category and level values to enable accurate comparison
#         df1_price_for_comparison = int(df1[df1['level_name'] == day_rate_comparison_level].iloc[0]['development_and_implementation'])
#         df2_price_for_comparison = int(df2[df2['level_name'] == day_rate_comparison_level].iloc[0]['development_and_implementation'])

#         return (df1_price_for_comparison, df2_price_for_comparison)
 

#     def concat_all_dfs(self, cleaned_rate_card_dict = None) -> pd.DataFrame:
#         '''Intake a dict where the values are a list of dfs. Unravel these and concatenate them together
        
#         :Params:
#             clean_rate_cards_dict (dict): dict where key = rate_card_file & value = list of rate card dfs.

#         :Returns:
#             pd.DataFrame: concatenated rate cards
#         '''
        
#         list_of_final_rate_card_dfs = [] 
#         for df_list in list(cleaned_rate_card_dict.values()):
#             for df_rate_card in df_list:
#                 list_of_final_rate_card_dfs.append(df_rate_card)

#         return pd.concat(list_of_final_rate_card_dfs). reset_index(drop = True)
    
#     def separate_and_clean_unique_rate_cards(self, df_concat) -> pd.DataFrame:
#         """_summary_

#         :Params:
#             df_concat (pd.DataFrame): DataFrame of concatenated rate cards.

#         :Returns:
#             df_final2 (pd.DataFrame): DataFrame of concatenated rate cards which contained a price range for each cell.
#         """
#         # cleaner func to clean cells in df
#         def non_digit_cleaner(s):
#             # only put 'N/A' if cell doesnt have price range
#             if len(s) > 3:
#                 return s.replace(['nan','NA', '-'], 'N/A')
#             else:
#                 return s
            
#         # Quantitative columns
#         cols_with_prices = [
#                             'strategy_and_architecture', 'change_and_transformation', 'development_and_implementation'
#                             , 'delivery_and_operation', 'people_and_skills', 'relationships_and_engagement'
#                            ]
        
        
#         # Filter for cells which aren't pure digits (e.g. has a hypen)
#         mask_not_digit_string = df_concat[cols_with_prices]['strategy_and_architecture'].apply(lambda x: not x.isdigit())

#         # Remove decimal point and digits after it (2 digits) & replace cells without digits with 'N/A'
#         df_concat.loc[mask_not_digit_string] = df_concat[mask_not_digit_string].map(lambda x: str(x[:len(x)-3]) if '.' in str(x) else x).apply(non_digit_cleaner)
        
#         # Removed decimals above so these cells won't be included in below mask anymore
#         mask_not_digit_string = df_concat[cols_with_prices]['strategy_and_architecture'].apply(lambda x: not x.isdigit())
        
#         # Separate dataframe for rate cards showing price range for each rate card
#         mask_price_range = df_concat['strategy_and_architecture'].map(lambda x: '-' in x and x[0].isdigit() and x[-1].isdigit())
#         df_price_range_rate_card = df_concat[mask_price_range].reset_index(drop = True)

#         self.df_final1 = df_concat.loc[~mask_price_range].reset_index(drop=True)
#         # Clean and assign df_final (1 price)
#         self.df_final1.loc[:, cols_with_prices] = self.df_final1[cols_with_prices].map(lambda x: x.replace('-', 'N/A'))
#         self.df_final1.loc[:, cols_with_prices] = self.df_final1[cols_with_prices].fillna('N/A')

#         self.df_final1.to_csv(self.final_df1_output_location)

#         # For rate cards showing a price range in each cell separate these into two new cols min and max
#         for col in cols_with_prices:
#             # Iterate thorugh list elements
#             for i in range(1,3):
#                 # min here
#                 if i == 1:
#                     df_price_range_rate_card[col + '_min'] = df_price_range_rate_card[col].apply((lambda x: x.split('-')[0] if '-' in x else x))
#                 # Max here
#                 else:
#                     df_price_range_rate_card[col + '_max'] = df_price_range_rate_card[col].apply((lambda x: x.split('-')[1] if '-' in x else x))

#         df_final2 = df_price_range_rate_card.drop(columns=cols_with_prices)
#         self.df_final2_price_range = df_final2

#         return df_final2


