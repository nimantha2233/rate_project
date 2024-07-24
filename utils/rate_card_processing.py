'''Rate card operations: 

RateCardProccesor:
 
- Extract rate card tables from pdf files

- Clean and transform rate card dataframes

'''

import pandas as pd
from collections import defaultdict
import tabula
from . import supportfunctions as sf
import os


class RateCardFileHandler:
    """
    Extracts all tables from rate card pdfs in database/bronze/company_rate_cards directory.

    Attributes:
        path_to_rate_cards (str): self-explanatory.
        rate_card_filenames (list[str]): list of all rate card filenames in the database/bronze/company_rate_cards directory.

    Methods:
         extract_all_tables_from_pdfs: -

    """
    def __init__(self, path_to_rate_cards : str):

        self.path_to_rate_cards = path_to_rate_cards
        self.rate_card_filenames = os.listdir(sf.get_filepath(os.getcwd(), *self.path_to_rate_cards))

    def extract_all_tables_from_pdfs(self):
        """ Iterates through each rate card pdf and uses tabula to extract all tables in pdf. 
        
        Returns:
            rate_card_raw_tables_dict (dict): dict of all tables extracted via tabula.
                                            {'rate card filename' : list of tables from file}
        """

        rate_card_raw_tables_dict = defaultdict(list)

        for rate_card_filename in self.rate_card_filenames:
            rate_card_filepath = sf.get_filepath(*self.path_to_rate_cards, rate_card_filename)
            tables_list = tabula.read_pdf(rate_card_filepath, pages='all', lattice=True, multiple_tables=True, stream=True)
            rate_card_file = rate_card_filename.split('.')[0]
            rate_card_raw_tables_dict[rate_card_file] = tables_list

        return rate_card_raw_tables_dict


class RateCardFilter:
    """Iterate through tables and extract only those that are rate cards.

    Methods:
        filter_for_rate_card_tables(rate_card_tables_dict): -
    """
    @staticmethod
    def filter_for_rate_card_tables(rate_card_tables_dict : dict) -> dict:
        """For input of tables extracted from rate card pdfs filter for only 
        tables describing day rates.

        Params:
            rate_card_tables_dict (dict): 
                dict where key = filename, values = list of all tables (dfs) 
                from corresponding pdf.

        Returns:
            rate_cards_dict (dict): 
                Dict containing only tables which outline day rates.
        """
        rate_cards_dict = defaultdict(list)

        for rate_card_file, table_list in rate_card_tables_dict.items():
            for df_table in table_list:
                # Rate card tables contain the following
                if 'Unnamed: 0' in df_table.columns and 'Strategy and\rarchitecture' in df_table.columns:
                    if not df_table['Unnamed: 0'].isna().any() and df_table['Unnamed: 0'].str.contains('.follow', case=False).any():
                        rate_cards_dict[rate_card_file].append(df_table)
        return rate_cards_dict


class RateCardCleaner:
    """For an individual rate card clean/rename column names, clean values 
    in each column, and introduce new columns. 

    Methods:
        rename_columns(df_rate_card): -

        create_and_transform_column_values(rate_card_file, df_rate_card, new_cols): -
    """
    @staticmethod
    def rename_columns(df_rate_card : pd.DataFrame) -> tuple:
        """For each rate card df each column name will be cleaned/renamed to common standard

        Params:
            df_rate_card (pd.DataFrame):
                A single rate card.

        Returns:
            tuple: A tuple containing the updated DataFrame and the list of new column names.

                - pd.DataFrame: The updated DataFrame after processing.
                - list of str: The list of new column names.
                
        """
        # Some rate cards use incorrect sfia category labels, fix them using below dict
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

        # First col name is cleaned manually above (has to be renamed not transformed like others)
        for col_name in old_cols[1:]:
            # clean col names
            new_col_name = col_name.replace('\r', ' ').replace(' ', '_').lower()
            new_cols.append(new_col_name)
            df_rate_card[col_name] = df_rate_card[col_name].apply(lambda x: str(x).replace('£', '').replace(',', ''))
        
        # Map old to new to use in rename cols method
        cols_dict = {old: new for old, new in zip(old_cols, new_cols)}
        df_rate_card = df_rate_card.rename(columns=cols_dict)

        # for dfs with incorrect sfia category labels
        if new_cols == list(cols_mapping.keys()):
            df_rate_card = df_rate_card.rename(columns=cols_mapping)

        return df_rate_card, new_cols

    @staticmethod
    def create_and_transform_column_values(rate_card_file : str, df_rate_card : pd.DataFrame, new_cols : list[str]) -> pd.DataFrame:
        """Add new cols ('level_name' and 'rate_card_file') 

        Params:
            rate_card_file (str): 
                Rate card filename as in database/bronze/company_rate_cards dir.

            df_rate_card (pd.DataFrame):
                Single rate card dataframe.
            new_cols (list[str]):
                List of new col names.

        Returns:
            df_rate_card (pd.DataFrame):
                DataFrame with cleaned and common column names. New cols 
                ('level_name' and 'rate_card_file') added to identify where 
                rate card originates from (filename).
        """

        # Create separate first col in two with one have (sfia) level name and pdf filename 
        df_rate_card['level_name'] = df_rate_card[new_cols[0]].apply(lambda x: x[2:].replace('\r', ' ').replace('.', '').strip())
        df_rate_card[new_cols[0]] = df_rate_card[new_cols[0]].apply(lambda x: x[0])
        df_rate_card['rate_card_file'] = rate_card_file

        return df_rate_card

class RateCardEnricher:
    """Add a new column 'location_type' describing whether rate card is for 
    onshore or offshore consultants.

    Methods:
        onshore_offshore_enrichment(cleaned_rate_card_dict): -
        onshore_offshore_helper(df1, df2): -

    """
    @staticmethod
    def onshore_offshore_enrichment(cleaned_rate_card_dict : dict) -> dict:
        """Adds new column ('location_type') to each dataframe describing consultant location 

        Params:
            cleaned_rate_card_dict (dict):
                Dict where keys are rate card file IDs and values are lists of DataFrames.

        Returns:
            dict: Dictwith the same keys, but DataFrames have new col 'location_type'.
        """
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
    def onshore_offshore_helper(df1 : pd.DataFrame, df2 : pd.DataFrame) -> tuple:
        """Extracts relevant day rate for comparison to determine which rate card (from same file) is offshore. 

        Params:
            df1 (pd.DataFrame): A rate card DataFrame.
            df2 (pd.DataFrame): A rate card DataFrame from same file as df1.

        Returns:
            tuple: Day rate from same category.

        """
        df1 = df1.loc[:, ['level_name', 'development_and_implementation']]
        df2 = df2.loc[:, ['level_name', 'development_and_implementation']]
        mask_digit_strings1 = df1['development_and_implementation'].apply(lambda x: x.isdigit())
        mask_digit_strings2 = df2['development_and_implementation'].apply(lambda x: x.isdigit())
        day_rate_comparison_level = pd.merge(df1[mask_digit_strings1]['level_name'], df2[mask_digit_strings2]['level_name'], on='level_name').iloc[0]['level_name']
        df1_price_for_comparison = int(df1[df1['level_name'] == day_rate_comparison_level].iloc[0]['development_and_implementation'])
        df2_price_for_comparison = int(df2[df2['level_name'] == day_rate_comparison_level].iloc[0]['development_and_implementation'])
        
        return df1_price_for_comparison, df2_price_for_comparison

class RateCardAggregator:
    """Concatenate all separate rate card dataframs into one single dataframe.

    Methods:
        concat_all_dfs: -
        separate_and_clean_unique_rate_cards: -
    """
    @staticmethod
    def concat_all_dfs(cleaned_rate_card_dict : dict) -> pd.DataFrame:
        """Concatenate all dfs from dict to form one large dataframe.

        Params:
            cleaned_rate_card_dict (dict):
                Dict of form {rate card filename (str) : list of rate cards (pd.DataFrame)}

        Returns:
            pd.DataFrame: Concatenated DataFrame contianing all rate cards
        """
        list_of_final_rate_card_dfs = []
        for df_list in cleaned_rate_card_dict.values():
            list_of_final_rate_card_dfs.extend(df_list)
        
        return pd.concat(list_of_final_rate_card_dfs).reset_index(drop=True)

    @staticmethod
    def separate_and_clean_unique_rate_cards(df_concat : pd.DataFrame) -> tuple:
        """ Seprate rate cards containing price range for each day rate.

        Params:
            df_concat (pd.DataFrame): DataFrame containing all rate card data.

        Returns:
            tuple:
                - df_final1 (pd.DataFrame): contains rate cards with single price.
                - df_final2 (pd.DataFrame): contains rate cards with price range.
        """

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
    """Pipeline of rate card processing method calls. Start via extracting raw tables from 
    pdfs and output is one large dataframe of all rate card data written to csv file in silver layer.

    Methods:
        process_rate_cards: -
    """
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
    
    """Prepare dataframes to extract statistical measures from them and to produce Monte Carlos.
    
    Methods:
        transform_rate_card_data: -
        transform_and_pivot: -
    """

    def __init__(self, silver_rate_card_filepath : str = sf.get_filepath(
                                    'database', 'silver', 'rate_cards', 'rate_cards_silver.csv'
                                                                              )) -> None:
        
        self.df_rate_card_silver = pd.read_csv(filepath_or_buffer=silver_rate_card_filepath)
        # sfia table gold layer location
        self.gold_rate_card_path = sf.get_filepath('database', 'gold', 'dim_ratecard.csv')

    def transform_rate_card_data(self) -> pd.DataFrame:
        """Execute transform and pivot method with correct filepath to rate card (silver layer).

        Returns:
            pd.DataFrame: rate card data ready for reporting layer.
            
        """
        # Add columns: company and rate_card_id then pivot df
        df_rate_card_gold = self.transform_and_pivot(df_rate_card_silver=self.df_rate_card_silver)
        

        return df_rate_card_gold


    
    def  transform_and_pivot(self, df_rate_card_silver : pd.DataFrame) -> pd.DataFrame:
        """Filter for only development_and_implementation out of all categories. Add 2 new columns
        ('rate_card_id', 'company'), and pivot on 'level_name' column.

        Params:
            df_rate_card_silver (pd.DataFrame):
                Rate card DataFrame from database/silver/rate_cards directory.

        Returns:
            pd.DataFrame: Filtered, pivoted, and new columns added ('rate_card_id', 'company')
        """

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





        


        

