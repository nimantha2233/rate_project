'''Main web-scraping carried out in this python file'''

from utils import supportfunctions as sf
from utils import config as conf
from utils import extractor
import logging
from datetime import datetime
import pandas as pd
import os
import concurrent.futures
import requests
import hashlib

# Get the current working directory
current_dir = os.getcwd()
companies_to_scrape = []

# Construct the relative path to the company_info.csv file
company_info_path = os.path.join(current_dir,'database', 'gold', 'company_info.csv')
company_new_pdfs = {}
company_rate_card_dict = {}
incomplete_service_dict = {}


def main():
    '''Run main logic from here'''
    
    global company_new_pdfs
    global company_rate_card_dict

    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main_logger = logging.getLogger(__name__)
    main_logger.info(f'New run starting at {datetime.now()}')

    # Instantiate instance of writer
    writer = sf.WriteToCSV()
    writer.write_headers()
    # Read in company names
    df_full_companies_metadata = pd.read_csv(filepath_or_buffer = company_info_path, index_col = 0)

    # Filter dataframe so only companies in company list are scraped
    if companies_to_scrape:
        df_companies = sf.filter_company_df(company_list=companies_to_scrape
                             , df_all_companies=df_full_companies_metadata)
        companies = df_companies['govt_url_name'].to_list()
    else:
        companies = df_full_companies_metadata['govt_url_name'].to_list()

    max_workers = 8

    # Create a ThreadPoolExecutor with the specified max_workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks (scrape_company function) to the thread pool and run them
        futures = [executor.submit(scrape_company, company, writer) for company in companies]
        
        # All tasks in pool finished now loop through the results
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                print(f"Error fetching data for {company}: {e}")

    # Scraping now completed below are post-scraping operations
    company_names = list(company_rate_card_dict.keys())
    num_rate_cards = list(company_rate_card_dict.values())
    ratecard_metadata_path = os.path.join(current_dir,'database', 'bronze','rate_card_metadata.csv')
    pd.DataFrame({'company_name' : company_names, 'num_rate_cards' : num_rate_cards})\
        .to_csv(ratecard_metadata_path)
    
    # Create a list to store the service-company pairs
    service_company_list = []

    # Iterate through the dictionary of 
    for company, services in incomplete_service_dict.items():
        for service in services:
            service_company_list.append((service, company))

    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(service_company_list, columns=["service", "company_name"])
    incomplete_rows_filepath = os.path.join(current_dir,'database', 'bronze','incomplete_rows.csv')
    df.to_csv(incomplete_rows_filepath)

    pdf_path = os.path.join(
            current_dir,'database', 'bronze', 'company_rate_cards')
    
    # Remove duplicated pdf files
    folder_path = sf.get_filepath('database', 'bronze', 'company_rate_cards')
    duplicate_dict, duplicates = find_and_remove_duplicates(folder_path=folder_path)
    rename_duplicate_rate_card_ids(duplicate_dict=duplicate_dict)
    
    return 0


def scrape_company(company_name : str, writer : sf.WriteToCSV):
        '''Scraper logic for individual company.

        Params:
            company_name (str): Name of company to scrape/search digital marketplace
            writer (sf.WriteToCSV): Instance of Custom CSV writer found in supportfunctions.py
        
        '''
        global company_new_pdfs, company_rate_card_dict
        
        # Instantiate Extractor object for a unique company
        extract = extractor.Extractor(company_name = company_name, writer = writer)

        URL = extract.check_page_num_produce_url(page = 1)
        webpage_soup = extract.soup_from_url(URL)

        # Start at page 1 then page incremented in function
        extract.loop_through_all_pages_for_company_search(webpage_soup = webpage_soup, page = 1)     
        new_pdfs = extract.new_pdfs
        company_new_pdfs[company_name] = len(new_pdfs) 
        num_rate_cards = extract.has_ratecard()  
        company_rate_card_dict[company_name] = num_rate_cards
        incomplete_service_dict[company_name] = extract.incomplete_service_data



def hash_file(file_path):
    """Generate SHA-256 hash of the file.

    Params:
        file_path (str): Path to directory containing pdfs. 
    """
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()




def find_and_remove_duplicates(folder_path : str) -> tuple:
    """Find and remove duplicate files in the given folder. Delete duplicate files and 
    return a dict mapping the rate card filenames of the removed files to the dupe file kept.
    
    Params:
        folder_path (str): Path to directory containing pdf rate cards (duplicate files)

    Returns:
        tuple:
            - duplicate_dict (dict): {duplicate file to delete : duplicate file to keep}
            - l_duplicates (list): list of duplictae files to delete
    """

    file_hashes = {}
    l_duplicates = []
    duplicate_dict = {}
    
    for pdf_directory, _, files in os.walk(folder_path):
        for filename in files:
            if filename.lower().endswith('.pdf'):
                file_path = os.path.join(pdf_directory, filename)
                file_hash = hash_file(file_path)

                # Get filename of first instance of this hash
                if file_hash in file_hashes:
                    dupe_file_to_keep = file_hashes[file_hash].split('\\')[-1]
                    # Map the duplicate filename to the filename (rate_card_id we're keeping)
                    duplicate_dict[filename] = dupe_file_to_keep
                    l_duplicates.append(file_path)
                else:
                    file_hashes[file_hash] = file_path
                    duplicate_dict[filename] = filename

    # Removing duplicates
    for duplicate in l_duplicates:
        os.remove(duplicate)

    return (duplicate_dict, l_duplicates)


def rename_duplicate_rate_card_ids(duplicate_dict : dict):
    ''' Rename rate_card_filenames for each service where the rate card was a duplicate. 
    The new name is the name of the duplicate file that was kept in the company_rate_cards directory.

    Params:
        duplicate_dict (dict): {duplicate rate card pdf that was deleted : duplicated rate card not deleted}
    
    
    '''
    transformed_df_filepath = sf.get_filepath('database', 'silver', 'company_services_transfomed_rates.csv')
    
    price_scrape_data_filepath = sf.get_filepath('database', 'bronze', 'company_service_rates','company_info_last_run.csv')
    df_raw = pd.read_csv(price_scrape_data_filepath)

    def get_new_rate_card_id(rate_card_id, duplicate_dict = dict) -> str:
        if isinstance(rate_card_id,str):
            return duplicate_dict[rate_card_id]
        else:
            return rate_card_id
        
    df_raw['rate_card_id'] = df_raw['rate_card_id'].apply(get_new_rate_card_id, duplicate_dict=duplicate_dict)
    
    df_raw.to_csv(transformed_df_filepath)



if __name__ == '__main__':
    main()










