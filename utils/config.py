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





















        # company_dict = {'company_name':['Capgemini SE', 'AND Digital', 'Dufrain', 'FDM Group (Holdings) Ltd', 'Slalom', 'Tata Consultancy Services Limited', 'Wipro Limited', 'Infosys Limited', 'Credera', 'Infinite Lambda', 'Mesh AI', 'Sparta Global', 'Ten10', 'Fjord Consulting Group', 'BetterGov', 'Cambridge Consultants Limited', 'Capco Limited', 'Cognizant Technology Solutions Corporation', 'IQVIA Holdings Inc', 'Kubrick Group Limited'],
        #         'company_url':['https://www.capgemini.com/','https://www.and.digital/','https://www.dufrain.co.uk/','https://www.fdmgroup.com/','https://www.slalom.com/','https://www.tcs.com','https://www.wipro.com/', 'https://www.infosys.com/', 'https://www.credera.com/en-gb', 'https://infinitelambda.com', 'https://www.mesh-ai.com/', 'https://www.spartaglobal.com', 'https://ten10.com', 'https://fjordconsultinggroup.com', 'https://www.bettergov.co.uk/', 'https://www.cambridgeconsultants.com/', 'https://www.capco.com', 'https://www.cognizant.com', 'https://jobs.iqvia.com/en', 'https://www.kubrickgroup.com'],
        #         'status':['Public','Private','Private','Public','Private','Public','Public', 'Public', 'Private', 'Private', 'Private', 'Private', 'Private', 'Private', 'Private', 'Private', 'Private', 'Public', 'Public', 'Private'],
        #         'ticker':['CAP.PA', '', '', 'FDM.L', '','TCS.BO','WIT', 'INFY', '', '', '', '', '', '', '', '', '', 'CTSH', 'IQV', ''],
        #         'scraper':['scraper_capgemini','scraper_digital','scraper_dufrain','scraper_fdmgroup','scraper_slalom','scraper_tcs','scraper_wipro', 'scraper_infosys', 'scraper_credera', 'scraper_infinitelambda', 'scraper_meshai', 'scraper_spartaglobal', 'scraper_ten10', 'scraper_fjord', 'scraper_bettergov', 'scraper_cambridge', 'scraper_capco', 'scraper_cognizant', 'scraper_iqvia', 'scraper_kubrick'],
        #         'govt_company_name':['Capgemini UK plc', 'AND Digital', 'Dufrain', 'FDM Group', 'Slalom', 'Tata Consultancy Services Limited', 'Wipro Limited', 'Infosys Limited', 'Credera', 'Infinite Lambda', 'Mesh AI', 'Sparta Global', 'Ten10', 'Fjord Consulting Group', 'BetterGov', 'Cambridge Consultants Limited', 'Capco Limited', 'Cognizant Worldwide Limited', 'IQVIA LTD.', 'Kubrick Group']}


        # full_company_list = {
        # 'Capgemini SE' : 'CAP.PA',
        # 'AND Digital' : '',
        # 'Dufrain' : '',
        # 'FDM Group (Holdings) Ltd' : 'FDM.L',
        # 'Slalom' : '',
        # 'Tata Consultancy Services Limited' : 'TCS.BO',
        # 'Wipro Limited' : 'WIT',
        # 'Fjord Consulting Group' : '',
        # 'Mesh AI' : '',
        # 'Sparta Global' : '',
        # 'Ten10' : '',
        # 'Credera' : '',
        # 'Infinite Lambda' : '',
        # 'BetterGov' : '',
        # 'Cambridge Consultants Limited' : '',
        # 'Capco Limited' : '',
        # 'Cognizant Technology Solutions Corporation' : 'CTSH',
        # 'Infosys Limited' : 'INFY',
        # 'IQVIA Holdings Inc' : 'IQV',
        # 'Kubrick Group Limited' : '',
        # '11:FS' : '',
        # 'a1qa' : '',
        # 'Accenture PLC' : 'ACN',
        # 'Adatis' : '',
        # 'Afiniti' : '',
        # 'Airwalk Reply' : '',
        # 'Alchemmy' : '',
        # 'Alix Partners' : '',
        # 'Apexon' : '',
        # 'Arthur D Little' : '',
        # 'Atos Group' : '', # Unsure on what the ticker is? Mentioned it is public in excel by Simon
        # 'Atos SE' : 'ATO.PA',
        # 'Avanade Inc.' : '',
        # 'Bain & Company' : '',
        # 'Baringa' : '',
        # 'Billigence' : '',
        # 'BJSS' : '',
        # 'Blueberry Consultants Ltd.' : '',
        # 'Booz Allen Hamilton Holding Corp' : 'BAH',
        # 'Boston Consulting Group' : '',
        # 'Broadstones Tech' : '',
        # 'Cambridge Design Partnership' : '',
        # 'Canon Inc.' : '7751.T',
        # 'Capita' : 'CPI.L',
        # 'Centric Consulting' : '',
        # 'CGI' : '',
        # 'CIGNITI Technologies Ltd' : 'CIGNITITEC.BO',
        # 'Clarasys' : '',
        # 'Cognifide' : '',
        # 'Contino Ltd.' : '',
        # 'Credo Consulting' : '',
        # 'Deloitte' : '',
        # 'Designit' : '',
        # 'Digital Workplace Group' : '',
        # 'DXC Technology' : 'DXC',
        # 'Eclature Technologies' : '',
        # 'Eden McCallum' : '',
        # 'Edge Testing Solutions' : '',
        # 'Elixirr International Plc' : 'ELIX.L',
        # 'EPAM Systems Inc' : 'EPAM',
        # 'Equal Experts' : '',
        # 'EY' : '',
        # 'Faculty AI' : '',
        # 'Frog Design' : '',
        # 'Frontier Economics' : '',
        # 'GeekTek' : '',
        # 'Genpact Ltd' : 'G',
        # 'HCL Technologies Ltd' : 'HCLTECH.BO',
        # 'Hexaware Technologies Ltd' : '',
        # 'HP Inc' : 'HPQ',
        # 'Icon PLC' : 'ICLR',
        # 'IDEO' : '',
        # 'Infostretch Corp' : '',
        # 'International Business Machines Corp' : 'IBM',
        # 'JMAN Group' : '',
        # 'Kainos' : 'KNOS.L',
        # 'Kearney' : '',
        # 'KONICA MINOLTA INC' : 'KNCAF',
        # 'KPMG' : '',
        # 'Laboratory Corp Of America Holdings' : 'LAB.F',
        # 'LockPath, Inc.' : '',
        # 'LogicManager' : '',
        # 'LogicSource, Inc.' : '',
        # 'Lovelytics' : '',
        # 'Lunar' : '',
        # 'Made Tech Group Plc ' : 'MTEC.L',
        # 'Mason Advisory' : '',
        # 'McKinsey' : '',
        # 'METRICSTREAM INC' : '',
        # 'MindTree Ltd' : '',
        # 'Mosaic Island' : '',
        # 'Mphasis Ltd' : 'MPHASIS.BO',
        # 'mthree' : '',
        # 'Neoris' : '',
        # 'NexInfo' : '',
        # 'North Highland' : '',
        # 'OC&C Strategy Consultants' : '',
        # 'Oliver Wyman' : '',
        # 'OpenCredo' : '',
        # 'Oracle Consulting' : '',
        # 'PA Consulting Group' : '',
        # 'Parexel International Corp' : '',
        # 'Peru Consulting' : '',
        # 'Planit Testing' : '',
        # 'PPD Inc' : '',
        # 'PRA Health Sciences Inc' : '',
        # 'Project One' : '',
        # 'Projective' : '',
        # 'Prolifics' : '',
        # 'PwC' : '',
        # 'RICOH CO LTD' : 'RICO.L',
        # 'Roland Berger' : '',
        # 'SEIKO EPSON CORP' : 'SEKEY',
        # 'Simon Kutcher & Partners' : '',
        # 'SkillStorm' : '',
        # 'Softwire' : '',
        # 'Spring Studios' : '',
        # 'Strategy & London' : '',
        # 'Sutherland' : '',
        # 'Switchfast Technologies' : '',
        # 'Syneos Health Inc' : '',
        # 'TAG Solutions' : '',
        # 'Tech Mahindra Ltd' : 'TECHM.BO',
        # 'Terillium' : '',
        # 'Testhouse Ltd' : '',
        # 'The Berkeley Partnership' : '',
        # 'The PSC' : '',
        # 'Thoughtworks' : '7W8.F',
        # 'Ultimus Fund Solutions' : '',
        # 'Verisk Analytics Inc' : 'VRSK',
        # 'Wavestone' : 'WAVE.PA',
        # 'Xerox Corp' : '',
        # 'Zoonou' : '',
        # 'ZS' : '',
        # }