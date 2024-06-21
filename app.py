'''Main application runs from here'''

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

    global company_new_pdfs
    '''Run main logic from here'''
    global company_rate_card_dict

    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main_logger = logging.getLogger(__name__)
    # main_logger.info(f'New run starting at {datetime.now()} to find government company names')
    main_logger.info(f'New run starting at {datetime.now()}')
    company_new_pdfs = {}

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


    # If new pdfs were added log this.
    for company, count in company_new_pdfs.items():
        if count != 0:
            logging.info(f'New PDFs added for {company}: {count}')

    company_names = list(company_rate_card_dict.keys())
    num_rate_cards = list(company_rate_card_dict.values())
    ratecard_metadata_path = os.path.join(current_dir,'database', 'bronze','rate_card_metadata.csv')
    pd.DataFrame(
        {'company_name' : company_names
         , 'num_rate_cards' : num_rate_cards
        }
                ).to_csv(ratecard_metadata_path)
    
    # Create a list to store the service-company pairs
    service_company_list = []

    # Iterate through the dictionary
    for company, services in incomplete_service_dict.items():
        for service in services:
            service_company_list.append((service, company))

    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(service_company_list, columns=["service", "company_name"])
    incomplete_rows_filepath = os.path.join(current_dir,'database', 'bronze','incomplete_rows.csv')
    df.to_csv(incomplete_rows_filepath)

    pdf_path = os.path.join(
            current_dir,'database', 'bronze', 'company_rate_cards')
    find_and_remove_duplicates(folder_path=pdf_path)
    return 0

def scrape_company(company_name : str, writer : sf.WriteToCSV):
        '''Scraper logic for individual company'''
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
    """Generate SHA-256 hash of the file."""
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def find_and_remove_duplicates(folder_path) -> None:
    """Find and remove duplicate files in the given folder.
    
    :Params:
        folder_path (str): Path to directory containing pdf rate cards (duplicate files)

    :Returns:
        N/A: Removes duplicate rate cards from directory
    """
    file_hashes = {}
    duplicates = []

    for pdf_directory, _, files in os.walk(folder_path):
        for filename in files:
            if filename.lower().endswith('.pdf'):
                file_path = os.path.join(pdf_directory, filename)
                file_hash = hash_file(file_path)

                if file_hash in file_hashes:
                    duplicates.append(file_path)
                else:
                    file_hashes[file_hash] = file_path
    # Removing duplicates
    for duplicate in duplicates:
        os.remove(duplicate)





if __name__ == '__main__':
    main()










