'''Support functions and classes for rate project'''

import requests
from bs4 import BeautifulSoup
import csv
from . import config as cf


class Service:
    '''Project data structure'''
    def __init__(self, company : str):
        self.name = 'No name'
        self.cost = 'No cost'
        self.company = company
        self.url = 'No URL'


    def output_attrs_to_list(self):
        '''Output data to list for writing to csv file'''
        return [self.company, self.name, self.cost, self.url]
    

    def parse_and_extract(self, page_soup : BeautifulSoup, project : BeautifulSoup):
        '''Assign values to attributes of class instance
        
        :Params:
            page_soup (BeautifulSoup): soup from page containing projects

        :Returns: Nothing but assigns value to object attrs.
        '''
        # Assign attributes values (project details)
        self.name = project.select('a')[0].text.strip()
        self.url = cf.Config.BASE_URL + project.select('a')[0]['href'].strip()

        # Access Project page to obtain rates
        project_soup = BeautifulSoup(requests.get(self.url).content, 'html5lib')
        # Service cost
        self.cost = project_soup.select(
            'div[id="meta"] > p[class = "govuk-!-font-weight-bold govuk-!-margin-bottom-1"]'
                                        )[0].text.strip().replace('£','')


class WriteToCSV:
    def __init__(self, column_names = ['Company', 'Project', 'Cost', 'URL'], 
                filepath = r'C:\Users\NimanthaFernando\Innovation_Team_Projects\Market_Intelligence\govt_contracts.csv'):
        
        self.filepath = filepath
        self.column_names = column_names

    def write_headers(self):
        if self.column_names:
            with open(self.filepath, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(self.column_names)

    def write_row(self, row):
        with open(self.filepath, "a",encoding='utf-8', newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
    




















    
# # print(f'Exit on page {page}')

# BASE_URL = r'https://www.applytosupply.digitalmarketplace.service.gov.uk'
# page = 1
# company_name = 'Infosys%20Limited'
# URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?q={company_name}'

# r = requests.get(URL)
# soup = BeautifulSoup(r.content, 'html5lib')

# # project_dict = defaultdict(list)
# writer = WriteToCSV()
# writer.write_headers()




# print(f'On page: {page}')
# # While a 'Next page exists keep iterating through them'
# while soup.select('div.govuk-pagination__next'):
#     for project in soup.select('li.app-search-result'):
#         project_obj = Project(company = company_name.replace('%20',' '))

#         project_obj.name = project.select('a')[0].text.strip()
#         project_obj.url = BASE_URL + project.select('a')[0]['href'].strip()

#         project_soup = BeautifulSoup(requests.get(project_obj.url).content, 'html5lib')
#         project_obj.cost = project_soup.select(
#             'div[id="meta"] > p[class = "govuk-!-font-weight-bold govuk-!-margin-bottom-1"]'
#                                         )[0].text.strip().replace('£','')
        
#         writer.write_row(project_obj.output_attrs_to_list())

#     # Do all parsing before this point
#     page += 1
#     URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?page={page}&q={company_name}'
#     r = requests.get(URL)
#     soup = BeautifulSoup(r.content, 'html5lib')
#     print(f'On page: {page}')


# BASE_URL = r'https://www.applytosupply.digitalmarketplace.service.gov.uk'

# company_name = 'Infosys%20Limited'
# change_name_or_not_working = []
# writer = WriteToCSV()
# writer.write_headers()


# for company_name in cf.Config.company_dict['govt_company_name']:
#     print(company_name)
#     page = 1

#     # Page 1 has slightly different url (no page key-val pair)
#     if page > 1:
#         URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?page={page}&q={company_name}'
#     else:
#         URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?q={company_name}'

#     r = requests.get(URL)
#     soup = BeautifulSoup(r.content, 'html5lib')
#     #print(soup.select('div.govuk-pagination__next')[0].select('span.govuk-pagination__link-label'))
#     while soup.select('div.govuk-pagination__prev') or page == 1:
#         print(page)
#         cnt = 0
#         #print(soup.select('span.govuk-pagination__link-label')[0].text.strip())
#         # print(f"num_projects: ----- {len(soup.select('li.app-search-result'))}")
#         for project in soup.select('li.app-search-result'):
#             cnt += 1
#             #print(f'counter ---- {cnt}')
#             # print(project.select('p')[0].text.strip())
#             if company_name in project.select('p')[0].text.strip():
#                 project_obj = Project(project.select('p')[0].text.strip())

#                 project_obj.name = project.select('a')[0].text.strip()
#                 project_obj.url = BASE_URL + project.select('a')[0]['href'].strip()

#                 project_soup = BeautifulSoup(requests.get(project_obj.url).content, 'html5lib')
#                 project_obj.cost = project_soup.select(
#                     'div[id="meta"] > p[class = "govuk-!-font-weight-bold govuk-!-margin-bottom-1"]'
#                                                 )[0].text.strip().replace('£','')
#                 #print(project_obj.output_attrs_to_list())
#                 writer.write_row(project_obj.output_attrs_to_list())

#         # Do all parsing before this point
#         page += 1
#         URL = fr'https://www.applytosupply.digitalmarketplace.service.gov.uk/g-cloud/search?page={page}&q={"%20".join(company_name.split())}'
#         r = requests.get(URL)
#         soup = BeautifulSoup(r.content, 'html5lib')

    # if int(soup.select('span.app-search-summary__count')[0].text.strip()) == 0:
    # print(URL)
    # print(soup.select('span.app-search-summary__count')[0].text.strip())
    # print('\n')
    