'''Combine rate card data from pdfs and webpage for minimum and maximum plots'''

import pandas as pd
from . import supportfunctions as sf
from . import dataprocessing
frok . import rate_card_processing


class CombineRates:
    """Combine rates data output by the DataProcessor and RateCardProcessor classes
    
    NOTE: This is only relevant for min and max rate distributions. 
    """

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
        # TODO: Modify so data read in from csv file (file is output from DataProcessor class)
        process_data = dataprocessing.DataProcessor(filepath=latest_scrape_filepath)
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

        # TODO: Should read in rate data (the output from RateCardProcessor)
        extract_ratecard_data = rate_card_processing.RateCardProcessor()
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