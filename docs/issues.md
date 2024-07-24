# Issues Log

This section contains issues encountered in the project and how they were fixed as well as any outstanding issues.

## Unresolved

1. Tabula module unable to read tables from some rate card pdfs located in *'database/bronze/company_rate_cards'* directory (e.g. Cognizants rate cards).
    - An attempt was made to use other PDF reader modules (PyPDF2 and camelot) however as with tabula other programmes must be installed for these modules to function and this was not possible due to admin rights issues.
    - As a temporary missing rates were added to gold_rate_card.csv to give rate_card_final.csv which contains all rate card data. 

2. A handful companies who list services on the digital marketplace have their rate card located under a different download link. 
    - See extractor for details on this. The digital marketplace does state all rate cards should be under *"Skills Framework for the Information Age rate card"* however a handul of competitors do not seem to adhere to this and instead have rates located in the *"pricing document"* pdf link.


## Resolved

1. Companies listing multiple services on digital marketplace, will have the same rate card sometimes on each service page however the filename changes between pages. This can lead to the same rate card being downloaded under a different name.
    - This is fixed by ensuring if the same price is listed on the service webpage (e.g. £x per unit per day) the rate card will not be downloaded.
    - Or if the rate card filename already exists in the compay_rate_cards directory then the rate card is not downloaded again. 
    - In the scenario where the rate card is downloaded then post-scraping operations will deal with this. Specifically in app.py pdf hashs are computed and any duplicate hashs (same pdf contents) are deleted.  