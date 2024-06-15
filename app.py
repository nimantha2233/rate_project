'''Main application runs from here'''

from utils import supportfunctions as sf
from utils import config as conf
from utils import extractor
import logging
from datetime import datetime
import pandas as pd
import os

# Get the current working directory
current_dir = os.getcwd()

# Construct the relative path to the company_info.csv file
company_info_path = os.path.join(current_dir,'database', 'gold', 'company_info.csv')


def main():
    ''''''
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main_logger = logging.getLogger(__name__)
    # main_logger.info(f'New run starting at {datetime.now()} to find government company names')
    main_logger.info(f'New run starting at {datetime.now()}')

    # writer = sf.WriteToCSV(filepath= r'govt_company_names_comparison.csv'
    #                        , column_names= ['company_name','govt_name']
    #                       )
    writer = sf.WriteToCSV()
    writer.write_headers()
    # Read in company names
    df = pd.read_csv(filepath_or_buffer = company_info_path, index_col = 0)
    companies = df['govt_url_name'].to_list()

    for company_name in companies:
        # Instantiate Extractor object for a unique company
        extract = extractor.Extractor(company_name = company_name, writer = writer)

        URL = extract.check_page_num_produce_url(page = 1)
        webpage_soup = extract.soup_from_url(URL)

        # Start at page 1 then page incremented in function
        extract.loop_through_all_pages_for_company_search(webpage_soup = webpage_soup, page = 1)        


    return 0

if __name__ == '__main__':
    main()












# def main():
#     '''Main application logic'''
#     logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     main_logger = logging.getLogger(__name__)
#     main_logger.info(f'New run starting at {datetime.now()}')

#     writer = sf.WriteToCSV()
#     writer.write_headers()
#     df = pd.read_csv(r'C:\Users\NimanthaFernando\Innovation_Team_Projects\Market_Intelligence\rate_project\company_info.csv', index_col= 0)
#     new_companies = df[df['govt_company_name'] == '0']['company_name'].to_list()

#     for company_name in conf.Config.company_dict['govt_company_name'][0:1]:
#         # Instantiate Extractor object for a unique company
#         extract = extractor.Extractor(company_name = company_name, writer = writer)

#         URL = extract.check_page_num_produce_url(page = 1)
#         webpage_soup = extract.soup_from_url(URL)

#         # Start at page 1 then page incremented in function
#         extract.loop_through_all_pages_for_company_search(webpage_soup = webpage_soup, page = 1)

#     return 0




















# def check_page_num_produce_url(page : int, company_name : str) -> str:
#     '''Check if page 1 or not as URL format is slighly different'''
#     # Page 1 has slightly different url (no page key-val pair)
#     if page > 1:
#         URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?page={page}&q={"%20".join(company_name.split())}'
#     else:
#         URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?q={"%20".join(company_name.split())}'

#     return URL
        
# def soup_from_url(URL : str) -> BeautifulSoup:
#     '''For URL input Do get req. and produce soup object
    
#     :Params:
#         URL(str): the URL of page to fetch

#     :Returns:
#         BeatifulSoup: Soup object from page    
#     '''
#     r = requests.get(URL)
#     soup = BeautifulSoup(r.content, 'html5lib')
#     return soup



# def extract_project_data(webpage_soup : BeautifulSoup, project_soup : BeautifulSoup, writer : sf.WriteToCSV ):
#     '''Parse HTML, store project data, and then write row to CSV file.
     
#     '''
#     # Instantiate project object for new project
#     project_obj = sf.Project(project_soup.select('p')[0].text.strip())
#     project_obj.parse_and_extract(webpage_soup)

#     # Write row to csv file
#     writer.write_row(project_obj.output_attrs_to_list())


# def loop_through_projects(webpage_soup : BeautifulSoup,  writer : sf.WriteToCSV):
#     '''Loop through each project in list of project soups'''
#     for project_soup in webpage_soup.select('li.app-search-result'):

#         # Check it is actually the company targetted
#         if company_name in project_soup.select('p')[0].text.strip():
#             extract_project_data(webpage_soup, project_soup, writer=writer)


# def loop_through_all_pages_for_company_search(webpage_soup : BeautifulSoup, page : int,  writer : sf.WriteToCSV):
#     '''For a given given initial webpage soup for a given company search 
#     loop through pages containing search results until reaching page limit
#     '''
#     # While there is an option to go back a page (doesn't exist if past page limit)
#     while webpage_soup.select('div.govuk-pagination__prev') or page == 1:
#         print(f'On page: {page}')

#         # Cycle through each project on search page
#         loop_through_projects(webpage_soup=webpage_soup,  writer=writer)

#         # Increment page and get new soup from next page
#         page += 1
#         URL = check_page_num_produce_url(page, company_name=company_name)
#         webpage_soup = soup_from_url(URL)