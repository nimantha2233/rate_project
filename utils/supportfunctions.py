'''Support functions and classes for rate project'''

import requests
from bs4 import BeautifulSoup
import csv
from . import config as cf
import os
import hashlib
import pandas as pd
import threading
from collections import defaultdict
# Create a lock object
write_lock = threading.Lock()

current_dir = os.getcwd()

# Construct the relative path to the company_info.csv file
company_service_rates_path = os.path.join(current_dir,'database', 'bronze', 'company_Service_rates', 'company_info_last_run.csv')


class Service:
    '''Project data structure'''
    def __init__(self, company : str):
        self.name = 'No name'
        self.cost = 'No cost'
        self.company = company
        self.url = 'No URL'
        self.page_soup = None
        self.rate_card_id = 'n/a'


    def output_attrs_to_list(self):
        '''Output data to list for writing to csv file'''
        return [self.company, self.name, self.cost, self.url, self.rate_card_id]
    


class WriteToCSV:
    def __init__(self, column_names = ['Company', 'Project', 'Cost', 'URL', 'rate_card_id'], 
                filepath = company_service_rates_path):
        
        self.filepath = filepath
        self.column_names = column_names

    def write_headers(self):
        if self.column_names:
            with open(self.filepath, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(self.column_names)

    def write_row(self, row):
        '''Write row to csv file'''
        with write_lock:
            with open(self.filepath, "a",encoding='utf-8', newline="") as f:
                writer = csv.writer(f)
                writer.writerow(row)




    


class RateCardDirectoryCleaner: 

    def __init__(self,filepath : str = os.path.join(
            current_dir,'database', 'bronze', 'company_rate_cards')):
        
        self.filepath = filepath

    def hash_file(self):
        '''Generate SHA-256 hash of the file.

        :Params:
            file_path (str): Path to directory containing pdfs.        
        '''

        hasher = hashlib.sha256()
        with open(self.filepath, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()

    def find_and_remove_duplicates(self) -> None:
        """Find and remove duplicate files in the given folder.
        
        :Params:
            folder_path (str): Path to directory containing pdf rate cards (duplicate files)

        :Returns:
            N/A: Removes duplicate rate cards from directory
        """
        file_hashes = {}
        duplicates = []
        pdf_duplicate_dict = {}
        filename_to_hash_dict_dict = {}

        for pdf_directory, _, files in os.walk(self.filepath):
            for filename in files:
                if filename.lower().endswith('.pdf'):
                    file_path_to_pdf = os.path.join(pdf_directory, filename)
                    print(file_path_to_pdf)
                    print(type(file_path_to_pdf))
                    file_hash = self.hash_file()
                    # Map the file hash to the filename
                    filename_to_hash_dict_dict[filename] = file_hash

                    if file_hash in file_hashes:
                        duplicates.append(file_path_to_pdf)
                        # Get filename of first instance of 
                        original_pdf_filename = filename_to_hash_dict_dict[file_hash]
                        pdf_duplicate_dict[original_pdf_filename].append(filename)
                    else:
                        # this filename will be kept in the dir
                        file_hashes[file_hash] = file_path_to_pdf
                        pdf_duplicate_dict[filename] = []

        # Removing duplicates
        for duplicate in duplicates:
            print(f"Removing duplicate file: {duplicate}")
            os.remove(duplicate)


def filter_company_df(company_list : list[str], df_all_companies : pd.DataFrame) -> pd.DataFrame:
    '''Output company_df for comapnies in company list to   
    scrape for these only. Useful for testing purposes

    :Params:
        company_list (list[str]): List of companies to scrape service
        information for. Names should be the govt company name

        df_all_companies (pd.DataFrame): DataFrame of all company metadata 

    :Returns:
        filtered_company_df (pd.DataFrame): company_df 
        containing only companies in company_list.    
    '''
    # For a certian set of companies this is how we get the dataframe to do a run
    mask = df_all_companies['company_name'].isin(company_list)
    filtered_company_df = df_all_companies[mask]#['company_name'].to_list()

    return filtered_company_df



def get_filepath(*args) -> str:
    '''Generate a file path in the current working directory
    using additional path components provided as arguments.

    :Params:
        *args: Additional path components to be joined.

    :Returns:
        str: Resultant file path.
    '''
    current_dir = os.getcwd()
     
    
    resultant_path = os.path.join(current_dir, *args)
    

    return resultant_path



def hash_file(file_path):
    """Generate SHA-256 hash of the file."""
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def find_and_remove_duplicates(folder_path : str) -> dict:
    """Find and remove duplicate files in the given folder. Delete duplicate files and 
    return a dict mapping the rate card ids of the removed files to the dupe file we keep.
    
    :Params:
        folder_path (str): Path to directory containing pdf rate cards (duplicate files)

    :Returns:
        N/A: Removes duplicate rate cards from directory
    """

    file_hashes = {}
    duplicates = []
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
                    duplicates.append(file_path)
                else:
                    file_hashes[file_hash] = file_path
                    duplicate_dict[filename] = filename

    # Removing duplicates
    for duplicate in duplicates:
        os.remove(duplicate)

    return (duplicate_dict, duplicates)
