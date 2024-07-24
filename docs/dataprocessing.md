# Overview

Purpose of this module is to clean and transform unstructured data 
from the database/bronze/company_service_rates/company_info_service_rates.csv file
into a dataframe.Initially this data is used to extract minimum and maximum 
consultant day rates for companies who do not display a rate card.

Future work could however use this data to understand the pricing landscape for 
different technology and consultancy offerings, e.g. there are pricing 
categories for product licences, something Kubrick Group does not offer.

It should be noted this module is working however there may be improvements that could be made. 
The main purpose was to produce a module which irrelevant of the different pricing schemes 
(£x per unit per day, £x per licence, etc), which could in the future change, would 
produce dataframes for each pricing scheme. Each dataframe would for:

1. A single pricing type such as a price range (£x - £y)
2. A single product type (e.g. licence or consultant)



::: utils.dataprocessing