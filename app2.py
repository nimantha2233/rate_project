'''After scraping occurs in app.py, this python file will use utils modules to 
extract rate card data from pdfs and output to a csv file to be used in app3.py
'''

from utils import rate_card_processing

def main():
    # Extract, clean, and transform rate card tables 
    processor = rate_card_processing.RateCardProcessor()
    df_rate_card_silver = processor.process_rate_cards()

    # Further transformations and pivot data
    transformer = rate_card_processing.RateCardTransformer()
    df_rate_card_gold = transformer.transform_rate_card_data()



if __name__ == '__main__':
    main()
    