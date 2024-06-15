'''Extractor class'''

import requests
from bs4 import BeautifulSoup
from . import supportfunctions as sf
import logging
from . import config as cf
import os


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
        self.new_pdfs = []



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

        # Write row to csv file
        self.writer.write_row(self.service.output_attrs_to_list())


    def loop_through_projects(self, webpage_soup : BeautifulSoup):
        '''Loop through each project in list of project soups
        
        :Params:
            webpage_soup (BeautifulSoup): soup object for whole page.
        '''
        # service_soup is HTML content of a service on the service search page
        for service_soup in webpage_soup.select('li.app-search-result'):

            # Check it is actually the company targetted
            if self.company_name in service_soup.select('p')[0].text.strip():
                self.extract_project_data(webpage_soup, service_soup)


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
            num_pages_to_search = webpage_soup.select('span.govuk-pagination__link-label')[0].text.strip().split(' ')[-1]
            # self.logger.info(f'Current Page: {page}/{num_pages_to_search}')
        else:
            num_pages_to_search = 1

        # While there is an option to go back a page (doesn't exist if past page limit)
        while webpage_soup.select('div.govuk-pagination__prev') or page == 1:
            self.logger.info(f'Current Page: {page}/{num_pages_to_search}')
            # Cycle through each project on search page
            # self.loop_through_projects(webpage_soup=webpage_soup)
            process_function(webpage_soup=webpage_soup)

            # Increment page and get new soup from next page
            
            page += 1
            URL = self.check_page_num_produce_url(page)
            webpage_soup = self.soup_from_url(URL)
        
        # Get new pdfs to log
        all_pdfs = set(os.listdir(self.database_dir))
        self.new_pdfs = list(all_pdfs - existing_pdfs)




    def govt_name_extractor(self, webpage_soup : BeautifulSoup):
        '''Loop through each project in list of project soups
        
        :Params:
            webpage_soup (BeautifulSoup): soup object for whole page.
        '''
        for project_soup in webpage_soup.select('li.app-search-result'):
            
            # Check it is actually the company targetted
            if self.company_name in project_soup.select('p')[0].text.strip():
                self.writer.write_row([self.company_name, project_soup.select('p')[0].text.strip()])

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
        self.find_and_download_rate_card()
        # Service cost
        self.service.cost = self.service.page_soup.select(
            'div[id="meta"] > p[class = "govuk-!-font-weight-bold govuk-!-margin-bottom-1"]'
                                        )[0].text.strip().replace('£','')
        
        
        
        
    def find_and_download_rate_card(self):
        '''If rate card exists download to bronze layer
        
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
            
            if service_doc_name =='Skills Framework for the Information Age rate card':
                #
                pdf_url = service_doc.select_one(
                    'p[class="dm-attachment__title govuk-!-font-size-16"] > a')['href']
                
                # Get filename from url and concat with company name for comparison with our pdf lib
                pdf_filename = self.service.company.replace(' ','') + '_' + pdf_url.split('/')[-1]
                
                # If a rate card already exists dont add (its a duplicate)
                if pdf_filename not in cf.Config.PDF_FILES_LIST:

                    # Download rate card
                    r = requests.get(pdf_url)
                    r.raise_for_status()  # Raise an error if the request was unsuccessful
                    pdf_filepath = os.path.join(pdfs_file_path, pdf_filename) 
                    # Save the PDF to a file
                    with open(f'{pdf_filepath}', 'wb') as f:
                        f.write(r.content)
    
    def get_new_pdfs(self):
        '''Return new pdfs'''
        return self.new_pdfs
                    




            



