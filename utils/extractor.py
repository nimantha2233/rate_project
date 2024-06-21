'''Extractor class'''

import requests
from bs4 import BeautifulSoup
from . import supportfunctions as sf
import logging
from . import config as cf
import os
from collections import defaultdict


# Get the current working directory
current_dir = os.getcwd()
# Construct the relative path to the company_info.csv file
pdfs_file_path = os.path.join(current_dir,'database', 'bronze', 'company_rate_cards')


class Extractor:
    '''For a given company will search for projects on government site and write project data to csv'''

    def __init__(self, company_name : str, writer : sf.WriteToCSV = None, mode : int = 0):
        # Either Legal name or govt name (sometimes they are different)
        self.company_name = company_name
        self.writer = writer
        self.logger = logging.getLogger(__name__) # name of logger will be module name
        self.logger.info(f'Scraping for Company: {self.company_name}')
        # Mode is either extract company names or scrape companies project data
        self.mode = mode
        # Define the mode-to-function map
        self.mode_function_map = {
            0: self.loop_through_projects,
            1: self.govt_name_extractor
                                 }
        self.service = None
        self.__ratecard_element = None
        self.ratecard_handler = RateCardHandler()
        self.incomplete_service_data = []


    def check_page_num_produce_url(self, page : int) -> str:
        '''Check if page 1 or not as URL format is slighly different
        
        :Params:
            page (int): page number to insert into URL

        :Returns: 
            URL (Str): URL to be used for GET request
        '''

        # Page 1 has slightly different url (no page key-val pair)
        if page > 1:
            URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?page={page}&q="{"%20".join(self.company_name.split())}"'
        else:
            URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?q="{"%20".join(self.company_name.split())}"'

        return URL
    
        
    def soup_from_url(self, URL : str) -> BeautifulSoup:
        '''For URL input Do GET request and produce soup object
        
        :Params:
            URL(str): the URL of page to fetch

        :Returns:
            BeatifulSoup: Soup object from page    
        '''
        r = requests.get(URL)
        soup = BeautifulSoup(r.content, 'html5lib')
        return soup


    def loop_through_all_pages_for_company_search(self, webpage_soup : BeautifulSoup, page : int):
        '''For a given given initial webpage soup for a given company search 
        loop through pages containing search results until reaching page limit

        :Params:
            webpage (BeautifulSoup): soup from page 1 of search results
            page (int): starting page_number
        '''
        # Check existing PDFs before scraping
        existing_pdfs = set(os.listdir(pdfs_file_path))

        # Get the appropriate function based on the mode
        process_function = self.mode_function_map.get(self.mode)
        
        # webpage_soup is from page 1 here
        if webpage_soup.select('span.govuk-pagination__link-label'):
            num_pages_to_search = webpage_soup.select(
                'span.govuk-pagination__link-label'
                )[0].text.strip().split(' ')[-1]
            
            # self.logger.info(f'Current Page: {page}/{num_pages_to_search}')
        else:
            num_pages_to_search = 1

        # While there is an option to go back a page (doesn't exist if past page limit)
        while webpage_soup.select('div.govuk-pagination__prev') or page == 1:
            self.logger.info(f'{self.company_name} ------ Current Page: {page}/{num_pages_to_search}')
            # Cycle through each project on search page
            process_function(webpage_soup=webpage_soup)

            # Increment page and get new soup from next page
            page += 1
            URL = self.check_page_num_produce_url(page)
            webpage_soup = self.soup_from_url(URL)
        
        # Get new pdfs to log
        all_pdfs = set(os.listdir(pdfs_file_path))
        self.new_pdfs = list(all_pdfs - existing_pdfs)


    def loop_through_projects(self, webpage_soup : BeautifulSoup):
        '''Loop through each project in list of project soups
        
        :Params:
            webpage_soup (BeautifulSoup): soup object for whole page.
        '''
        # service_soup is HTML content of a service on the service search page
        services_soup_list = webpage_soup.select('li.app-search-result')
        for service_soup in services_soup_list:

            company_offering_service = service_soup\
                                        .select('p')[0].text.strip().lower()
            # Check it is actually the company targetted
            if self.company_name.lower() in company_offering_service:
                self.extract_project_data(webpage_soup, service_soup)


    def extract_project_data(self, webpage_soup : BeautifulSoup, service_soup : BeautifulSoup):
        '''Parse HTML, store project data, and then write row to CSV file.

        :Params:
            webpage_soup (BeautifulSoup): soup object for whole page.

            service_soup (BeautifulSoup): Soup object for service in search
                                          page containing multiple services.

        :Returns: 
            Nothing, but parses soup and assigns values to object attrs.
        
        '''
        # Instantiate project object for new project with company name
        self.service = sf.Service(company = service_soup.select('p')[0].text.strip())
        self.parse_and_extract(service_soup)
        if len(self.writer.column_names) == len(self.service.output_attrs_to_list()):
            # Write row to csv file
            self.writer.write_row(self.service.output_attrs_to_list())
        else:
            # Number of data points not what is required
            self.incomplete_service_data.append(self.service.name)


    def parse_and_extract(self, service_soup : BeautifulSoup):
        '''Assign values to attributes of class instance
        
        :Params:
            page_soup (BeautifulSoup): soup from page containing services (service search results page)

        :Returns: Nothing but assigns value to object attrs.
        '''
        # Assign attributes values (service details)
        self.service.name = service_soup.select('a')[0].text.strip()
        self.service.url = cf.Config.BASE_URL + service_soup.select('a')[0]['href'].strip()

        # Access service page to obtain rates
        self.service.page_soup = BeautifulSoup(requests.get(self.service.url).content, 'html5lib')
        # Find rate card if exists and if unique add to the database
        # Service cost
        self.service.cost = self.service.page_soup.select(
            'div[id="meta"] > p[class = "govuk-!-font-weight-bold govuk-!-margin-bottom-1"]'
                                        )[0].text.strip().replace('£','')

        # Pass service object to rate card handler
        self.ratecard_handler.service = self.service
        # Check if a ratecard exists and the service cost describes a cost per person/unit
        if self.ratecard_handler.check_rate_card_exists() \
            and contains_unit_or_person(self.service.cost):
            
            # unique per unit cost 
            if self.service.cost not in self.ratecard_handler.cost_to_ratecard_dict.keys():            

                # Get ratecard details
                ratecard_filename, ratecard_url = self.ratecard_handler.get_ratecard_details()
                # Check detail against local directory and download if unique
                self.ratecard_handler.download_ratecard_if_unique(
                    ratecard_filename=ratecard_filename, ratecard_url=ratecard_url
                                                                 )
            
            else:
                # service unit cost already exists
                # assign ratecard filename to attrs id
                self.service.rate_card_id = self.ratecard_handler\
                                            .cost_to_ratecard_dict[self.service.cost]  
    

    def govt_name_extractor(self, webpage_soup : BeautifulSoup):
        '''Loop through each project in list of project soups
        
        :Params:
            webpage_soup (BeautifulSoup): soup object for whole page.
        '''
        for project_soup in webpage_soup.select('li.app-search-result'):
            
            # Check it is actually the company targetted
            if self.company_name in project_soup.select('p')[0].text.strip():
                self.writer.write_row([self.company_name, project_soup.select('p')[0].text.strip()])  

    def has_ratecard(self):
        '''Outputs Number of ratecards downloaded (unique URL and filename) 
        along with company name
        
        :Params:
            None
        
        :Returns:
            num_rate_cards: Number of unique rate cards downloaded                            
        '''

        if self.ratecard_handler.cost_to_ratecard_dict:
            num_rate_cards = len(self.ratecard_handler.cost_to_ratecard_dict)
            return num_rate_cards



            
def contains_unit_or_person(text : str) -> bool:
    '''Checks if unit or paerson str in input 
    str and returns corresponding bool
    '''
    return 'unit' in text or 'person' in text



class RateCardHandler:
    def __init__(self):
        self.cost_to_ratecard_dict = {}
        self.__ratecard_element = None
        self.service = None

            
    def check_rate_card_exists(self):
        '''Check if a rate card exists for service
        
        :Returns:
            None: This function operates by side-effect, downloading a PDF file.
        '''
        service_doc_elements = self.service.page_soup.select(
                                    'div[id="meta"] > ul > li[class*="gouk-!-margin-bottom-2"]'
                                                            )
        
        for service_doc in service_doc_elements:
            # Cleaner code otherwise go beyond line limit
            service_doc_name = service_doc.select_one(
                                           'p[class="dm-attachment__title govuk-!-font-size-16"] > a'
                                                     ).text.strip()

            # Check if service doc is a rate card
            if service_doc_name =='Skills Framework for the Information Age rate card':
                # Assign ratecard element to service attribute
                self.__ratecard_element = service_doc.select_one(
                                           'p[class="dm-attachment__title govuk-!-font-size-16"] > a'
                                                     )
                return True
            
        
        return False


    def get_ratecard_details(self) -> tuple:
        '''Output ratecard filename and url
        
        :Returns:
            ratecard_filename (str): filename of ratecard in local dir.
            ratecard_url (str): URL to download ratecard
        '''

        ratecard_url = self.__ratecard_element['href']
        
        # Get filename from url and concat with company name for comparison with our pdf lib
        ratecard_filename = self.service.company.replace(' ','') + '_' + ratecard_url.split('/')[-1]

        return (ratecard_filename, ratecard_url)


    def download_ratecard_if_unique(self, ratecard_filename, ratecard_url):
        '''If rate card exists download to bronze layer
        
        :Returns:
            None: This function operates by side-effect, downloading a PDF file.
        '''
        if ratecard_filename not in cf.Config.PDF_FILES_LIST:

            # Download rate card
            r = requests.get(ratecard_url)
            r.raise_for_status()  # Raise an error if the request was unsuccessful
            pdf_filepath = os.path.join(pdfs_file_path, ratecard_filename) 
            # Save the PDF to a file
            with open(f'{pdf_filepath}', 'wb') as f:
                f.write(r.content)
            
            # New k-v pair mapping cost to ratecard
            self.cost_to_ratecard_dict[self.service.cost] = ratecard_filename
            # Assign the ratecard_filename to this services' attribute
            self.service.rate_card_id = ratecard_filename
        
        else:
            # ratecard_filename exists so assign it to rate_card id
            self.service.rate_card_id = ratecard_filename
            
    

  