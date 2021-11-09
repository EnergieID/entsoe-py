import pandas as pd
from settings import api_key
from entsoe import EntsoePandasClient as Entsoe
e = Entsoe(api_key=api_key, retry_count=20, retry_delay=30)

start = pd.Timestamp('20170601', tz='Europe/Brussels')
end = pd.Timestamp('20171201', tz='Europe/Brussels')

#s = e.query_imbalance_prices(country_code='BE', start=start, end=end, as_dataframe=True)

"""domains = [["10YIT-GRTN-----B", "Italy, IT CA / MBA"],
           ["10Y1001A1001A885", "Italy_Saco_AC"],
           ["10Y1001A1001A893", "Italy_Saco_DC"],
           ["10Y1001A1001A699", "IT-Brindisi BZ"],
           ["10Y1001A1001A70O", "IT-Centre-North BZ"],
           ["10Y1001A1001A71M", "IT-Centre-South BZ"],
           ["10Y1001A1001A72K", "IT-Foggia BZ"],
           ["10Y1001A1001A66F", "IT-GR BZ"],
           ["10Y1001A1001A84D", "IT-MACROZONE NORTH MBA"],
           ["10Y1001A1001A85B", "IT-MACROZONE SOUTH MBA"],
           ["10Y1001A1001A877", "IT-Malta BZ"],
           ["10Y1001A1001A73I", "IT-North BZ"],
           ["10Y1001A1001A80L", "IT-North-AT BZ"],
           ["10Y1001A1001A68B", "IT-North-CH BZ"],
           ["10Y1001A1001A81J", "IT-North-FR BZ"],
           ["10Y1001A1001A67D", "IT-North-SI BZ"],
           ["10Y1001A1001A76C", "IT-Priolo BZ"],
           ["10Y1001A1001A77A", "IT-Rossano BZ"],
           ["10Y1001A1001A74G", "IT-Sardinia BZ"],
           ["10Y1001A1001A75E", "IT-Sicily BZ"],
           ["10Y1001A1001A788", "IT-South BZ"]]
"""

domains = [["10YCZ-CEPS-----N","Czech bidding zone"]]

lst = list()
for bzn in domains:
    s = e.query_unavailability_of_production_units(
        country_code=bzn[0], docstatus=None, start=start, end=end)
    if s is not None:
        lst.append(s)

result = pd.concat(lst)
result.to_csv('result.csv')
