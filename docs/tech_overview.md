# Technical Overview

This section outlines the sile structure and process flow of the project. An object-oriented programming approach was taken in an attempt to make the application more easily understandable. In addition to this, there was a focus on taking a modular approach in general, making locating and fixing bugs much easier. 

### Quick links to module pages

1. [Module: extractor.py](extractor.md)
2. [Module: rate_card_processing.py](rate_card_processing.md)
3. [Module: stats](stats.md)
4. [Module: supportfunctions.py](support_functions.md)
5. [Module: dataprocessing.py](dataprocessing.md)
6. [Main applications](app.md)

## File structure

<div style="float: left; margin-right: 10px; text-align: center;">
  <img src="/images/project_file_structure.png" alt="Description of image">
  <div style="font-style: italic;">Figure 3.1: Screenshot of project File Structure</div>
</div>
<p>
The file structure followed is a standard structure and can be seen on the left in figure 3.1.  
</p>

<p>
  All modules are contained within the <b>utils folder</b>, and a more detailed description and explanation of modules can be found here.  
</p>
<p>
  The <b>database</b> isn't a traditional database but just a store for csv, pdfs, and png files, however a medallion architecture has been followed as it brings some clarity when navigating the directory.  
</p>
<p>
  Three <b>app.py</b> files exist (app2 and app3 as well). app.py essentially carries out the scraping, app2.py executes data processing tasks, and app3.py carries out the plotting. 
</p>



## Process flow

The main tasks carried out are:

1. Extract services offered on digital marketplace by target companies. If available on service page then download rate card pdf.
2. Extract, clean, and transform rate card tables from rate card pdf into one large dataframe containing all rate card data.
    - For missing rate data use service page prices as rate estimates (prices of the form "£x per unit per day" only) 
    - For companies without rate cards minimum and maximum prices can be extracted from the rates listed on the service webpage.
3. Produce histogram of day rates for **different SFIA levels** and for **minimum** and **maximum** rates
4. Produce Monte Carlos for each SFIA level and minimum and maximum rates.

NOTE: One major issue encountered is that the tabula module which extracts tables in pdfs is unable to succesfully extract tables from all rate card pdfs, and a sustainable solution for this was not found. This meant that for rate cards that could not be extracted via tabula, the rates were manually noted down. This is however not a sustainable long-term solution so further exploration into different pdf readers should be explored. Some exploration had been done however due to requiring external programmes to use pdf readers I was unable to download them due to not having administrator rights on my work laptop. 

