'''Config file for constants'''

import os


# Get the current working directory
current_dir = os.getcwd()
# Construct the relative path to the company_info.csv file
pdfs_file_path = os.path.join(current_dir,'database', 'bronze', 'company_rate_cards')

class Config:
        
        BASE_URL = r'https://www.applytosupply.digitalmarketplace.service.gov.uk'
        TEST_COMPANY = 'Infosys%20Limited'
        PDF_FILES_LIST = os.listdir(pdfs_file_path)

