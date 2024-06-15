'''Extractor class'''

import requests
from bs4 import BeautifulSoup
from . import supportfunctions as sf
import logging


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



    def extract_project_data(self, webpage_soup : BeautifulSoup, project_soup : BeautifulSoup):
        '''Parse HTML, store project data, and then write row to CSV file.

        :Params:
            webpage_soup (BeautifulSoup): soup object for whole page.

            project_soup (BeautifulSoup): Soup object for project webpage.

        :Returns: 
            Nothing, but parses soup and assigns values to object attrs.
        
        '''
        # Instantiate project object for new project with company name
        service_obj = sf.Service(project_soup.select('p')[0].text.strip())
        service_obj.parse_and_extract(webpage_soup, project_soup)

        # Write row to csv file
        self.writer.write_row(service_obj.output_attrs_to_list())


    def loop_through_projects(self, webpage_soup : BeautifulSoup):
        '''Loop through each project in list of project soups
        
        :Params:
            webpage_soup (BeautifulSoup): soup object for whole page.
        '''
        for project_soup in webpage_soup.select('li.app-search-result'):

            # Check it is actually the company targetted
            if self.company_name in project_soup.select('p')[0].text.strip():
                self.extract_project_data(webpage_soup, project_soup)


    def loop_through_all_pages_for_company_search(self, webpage_soup : BeautifulSoup, page : int):
        '''For a given given initial webpage soup for a given company search 
        loop through pages containing search results until reaching page limit

        :Params:
            webpage (BeautifulSoup): soup from page 1 of search results
            page (int): starting page_number
        '''
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




    def govt_name_extractor(self, webpage_soup : BeautifulSoup):
        '''Loop through each project in list of project soups
        
        :Params:
            webpage_soup (BeautifulSoup): soup object for whole page.
        '''
        for project_soup in webpage_soup.select('li.app-search-result'):
            
            # Check it is actually the company targetted
            if self.company_name in project_soup.select('p')[0].text.strip():
                self.writer.write_row([self.company_name, project_soup.select('p')[0].text.strip()])
            



