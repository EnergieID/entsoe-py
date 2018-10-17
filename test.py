import pandas as pd
from settings import api_key
from entsoe import Entsoe
e = Entsoe(api_key=api_key, retry_count=20, retry_delay=30)

start = pd.Timestamp('20180101', tz='Europe/Brussels')
end = pd.Timestamp('20180301', tz='Europe/Brussels')

#s = e.query_imbalance_prices(country_code='BE', start=start, end=end, as_dataframe=True)

s = e.query_unavailability_of_production_units(
    domain="10YCZ-CEPS-----N", docstatus=None, start=start, end=end)
print(s)
