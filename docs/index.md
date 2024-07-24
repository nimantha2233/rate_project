## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)

## Introduction

This project aims to scrape competitor service rates and download available rate card pdfs as listed on [Digital Marketplace](https://www.applytosupply.digitalmarketplace.service.gov.uk/). Following web-scraping, rate card tables are extracted from pdfs then cleaned and transformed to ultimately produce distributions from rate card data. In addition to this monte carlos are carried out for different SFIA levels (see documentation).

## Features

- Web scraping from [Digital Marketplace](https://www.applytosupply.digitalmarketplace.service.gov.uk/)
- Data extraction and processing
- Storing data in CSV format in medallion architecture style
- Producing Monte Carlos and distribution plots

## Requirements

A comprehensive list of the requirements can be found in the ***requirements.txt*** folder. Make sure to use the most up to date python version (version 3.10.6).

For tabula installation, in addition to pip installing tabula, see the [tabula documentation page](https://tabula.technology/) as Java installation is also required to use tabula.

## Installation

Provide step-by-step instructions on how to set up the project locally.

```bash
# Clone the repository
git clone https://github.com/nimantha2233/rate_project.git

# Change into the project directory
cd yourproject

# Install required packages
pip install -r requirements.txt
```

## Usage

There are 3 separate app.py files in the root directory:

1. **app.py** carries out scraping and downloading tasks
2. **app2.py** carries out data processing tasks (cleaning and transformation) 
3. **app3.py** plots distribtion for day rates for each SFIA level, minimum and maximum day rate plots, and monte carlo plots.

