import requests
from bs4 import BeautifulSoup
import pandas as pd

from .mappings import Area


# tries to grab all mapping codes from the website and check them against the ones we already know
def check_new_area_codes():
    # grab the docs page
    r = requests.get('https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html')
    soup = BeautifulSoup(r.text, 'lxml')
    # to select the correct table find a known code and go to the parent table of that cell
    rows = soup.find('p', text='10YNL----------L').parent.parent.parent.find_all('tr')
    table_area_codes = [[x.text.strip('\n') for x in y.find_all('td')] for y in rows]
    # remove the header row
    del table_area_codes[0]

    # grab all codes of our mapping
    mapped_codes = set([x.value for x in Area])
    # grab all codes from the docs
    docs_codes = set([x[0] for x in table_area_codes])
    df_docs_codes = pd.DataFrame(table_area_codes)
    df_docs_codes.columns = ['code', 'area']

    # get all codes that are in the docs but not in our mapping
    missing_codes = docs_codes-mapped_codes

    # search the name for the missing codes and return them
    return df_docs_codes[df_docs_codes['code'].isin(docs_codes-mapped_codes)]

