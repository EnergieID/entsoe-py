import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import Deque
from .mappings import Area
import datetime
import time

MAX_NB_REQUEST_PER_PERIOD = 375 # 400 + safety margin
PERIOD_SEC = 59
LAST_REQUESTS: Deque[datetime.datetime] = Deque(
    maxlen=MAX_NB_REQUEST_PER_PERIOD
)
def wait_until_request_valid() -> None:
    """
    This function will sleep until we can make a request.
    If the queue is at its full capacity, we wait until the first request was at least 1 minute ago
    This function allows to avoid making more than MAX_NB_REQUEST_PER_PERIOD every PERIOD_SEC seconds
    """
    if len(LAST_REQUESTS) >= MAX_NB_REQUEST_PER_PERIOD:
        first_request = LAST_REQUESTS[0]
        while (datetime.datetime.now() - first_request).seconds <= PERIOD_SEC:
            time.sleep(
                PERIOD_SEC - (datetime.datetime.now() - first_request).seconds + 1
            )
    LAST_REQUESTS.append(datetime.datetime.now())

# tries to grab all mapping codes from the website and check them against the ones we already know
def check_new_area_codes():
    # grab the docs page
    r = requests.get('https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html')
    soup = BeautifulSoup(r.text, 'lxml')
    # to select the correct table find a known code and go to the parent table of that cell
    rows = soup.find('p', text='10YNL----------L').parent.parent.parent.find_all('tr')
    table_area_codes = [[x.text for x in y.find_all('td')] for y in rows]
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

