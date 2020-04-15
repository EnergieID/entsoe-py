# entsoe-py
Python client for the ENTSO-E API (european network of transmission system operators for electricity)

Documentation of the API found on https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html

## Installation
`python3 -m pip install entsoe-py`

## Usage
The package comes with 2 clients:
- [`EntsoeRawClient`](#EntsoeRawClient): Returns data in its raw format, usually XML or a ZIP-file containing XML's
- [`EntsoePandasClient`](#EntsoePandasClient): Returns data parsed as a Pandas Series or DataFrame
### <a name="EntsoeRawClient"></a>EntsoeRawClient
```python
from entsoe import EntsoeRawClient
import pandas as pd

client = EntsoeRawClient(api_key=<YOUR API KEY>)

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'BE'  # Belgium

# methods that return XML
client.query_day_ahead_prices(country_code, start, end)
client.query_load(country_code, start, end)
client.query_load_forecast(country_code, start, end)
client.query_wind_and_solar_forecast(country_code, start, end, psr_type=None)
client.query_generation_forecast(country_code, start, end)
client.query_generation(country_code, start, end, psr_type=None)
client.query_installed_generation_capacity(country_code, start, end, psr_type=None)
client.query_crossborder_flows(country_code_from, country_code_to, start, end)
client.query_imbalance_prices(country_code, start, end, psr_type=None)

# methods that return ZIP
client.query_unavailability_of_generation_units(country_code, start, end, docstatus=None)
client.query_withdrawn_unavailability_of_generation_units(country_code, start, end)
```
#### Dump result to file
```python
xml_string = client.query_day_ahead_prices(country_code, start, end)
with open('outfile.xml', 'w') as f:
    f.write(xml_string)

zip_bytes = client.query_unavailability_of_generation_units(country_code, start, end)
with open('outfile.zip', 'wb') as f:
    f.write(zip_bytes)
```
#### Making another request
Is the API-call you want not in the list, you can lookup the parameters yourself in the API documentation
```python
params = {
    'documentType': 'A44',
    'in_Domain': '10YBE----------2',
    'out_Domain': '10YBE----------2'
}
response = client.base_request(params=params, start=start, end=end)
print(response.text)
```

### <a name="EntsoePandasClient"></a>EntsoePandasClient
The Pandas Client works similar to the Raw Client, with extras:
- Time periods that span more than 1 year are automatically dealt with
- Requests of large numbers of files are split over multiple API calls
```python
from entsoe import EntsoePandasClient
import pandas as pd

client = EntsoePandasClient(api_key=<YOUR API KEY>)

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'BE'  # Belgium

# methods that return Pandas Series
client.query_day_ahead_prices(country_code, start=start,end=end)
client.query_load(country_code, start=start,end=end)
client.query_load_forecast(country_code, start=start,end=end)
client.query_generation_forecast(country_code, start=start,end=end)

# methods that return Pandas DataFrames
client.query_wind_and_solar_forecast(country_code, start=start,end=end, psr_type=None)
client.query_generation(country_code, start=start,end=end, psr_type=None)
client.query_installed_generation_capacity(country_code, start=start,end=end, psr_type=None)
client.query_crossborder_flows('DE', 'DK', start=start,end=end)
client.query_imbalance_prices(country_code, start=start,end=end, psr_type=None)
client.query_unavailability_of_generation_units(country_code, start=start,end=end, docstatus=None)
client.query_withdrawn_unavailability_of_generation_units('DE', start=start,end=end)
```
#### Dump result to file
See a list of all IO-methods on https://pandas.pydata.org/pandas-docs/stable/io.html
```python
ts = client.query_day_ahead_prices(country_code, start=start, end=end)
ts.to_csv('outfile.csv')
```

### Mappings
These lists are always evolving, so let us know if something's inaccurate!
#### Domains
```python
DOMAIN_MAPPINGS = {
    'AL': '10YAL-KESH-----5',
    'AT': '10YAT-APG------L',
    'BA': '10YBA-JPCC-----D',
    'BE': '10YBE----------2',
    'BG': '10YCA-BULGARIA-R',
    'BY': '10Y1001A1001A51S',
    'CH': '10YCH-SWISSGRIDZ',
    'CZ': '10YCZ-CEPS-----N',
    'DE': '10Y1001A1001A83F',
    'DK': '10Y1001A1001A65H',
    'EE': '10Y1001A1001A39I',
    'ES': '10YES-REE------0',
    'FI': '10YFI-1--------U',
    'FR': '10YFR-RTE------C',
    'GB': '10YGB----------A',
    'GB-NIR': '10Y1001A1001A016',
    'GR': '10YGR-HTSO-----Y',
    'HR': '10YHR-HEP------M',
    'HU': '10YHU-MAVIR----U',
    'IE': '10YIE-1001A00010',
    'IT': '10YIT-GRTN-----B',
    'LT': '10YLT-1001A0008Q',
    'LU': '10YLU-CEGEDEL-NQ',
    'LV': '10YLV-1001A00074',
    # 'MD': 'MD',
    'ME': '10YCS-CG-TSO---S',
    'MK': '10YMK-MEPSO----8',
    'MT': '10Y1001A1001A93C',
    'NL': '10YNL----------L',
    'NO': '10YNO-0--------C',
    'PL': '10YPL-AREA-----S',
    'PT': '10YPT-REN------W',
    'RO': '10YRO-TEL------P',
    'RS': '10YCS-SERBIATSOV',
    'RU': '10Y1001A1001A49F',
    'RU-KGD': '10Y1001A1001A50U',
    'SE': '10YSE-1--------K',
    'SI': '10YSI-ELES-----O',
    'SK': '10YSK-SEPS-----K',
    'TR': '10YTR-TEIAS----W',
    'UA': '10YUA-WEPS-----0',
    'DE-AT-LU': '10Y1001A1001A63L',
}
```
### Bidding Zones
```python
BIDDING_ZONES = DOMAIN_MAPPINGS.copy()
BIDDING_ZONES.update({
    'DE': '10Y1001A1001A63L',  # DE-AT-LU
    'LU': '10Y1001A1001A63L',  # DE-AT-LU
    'IT-NORD': '10Y1001A1001A73I',
    'IT-CNOR': '10Y1001A1001A70O',
    'IT-CSUD': '10Y1001A1001A71M',
    'IT-SUD': '10Y1001A1001A788',
    'IT-FOGN': '10Y1001A1001A72K',
    'IT-ROSN': '10Y1001A1001A77A',
    'IT-BRNN': '10Y1001A1001A699',
    'IT-PRGP': '10Y1001A1001A76C',
    'IT-SARD': '10Y1001A1001A74G',
    'IT-SICI': '10Y1001A1001A75E',
    'NO-1': '10YNO-1--------2',
    'NO-2': '10YNO-2--------T',
    'NO-3': '10YNO-3--------J',
    'NO-4': '10YNO-4--------9',
    'NO-5': '10Y1001A1001A48H',
    'SE-1': '10Y1001A1001A44P',
    'SE-2': '10Y1001A1001A45N',
    'SE-3': '10Y1001A1001A46L',
    'SE-4': '10Y1001A1001A47J',
    'DK-1': '10YDK-1--------W',
    'DK-2': '10YDK-2--------M'
})
```
#### PSR-type
```python
PSRTYPE_MAPPINGS = {
    'A03': 'Mixed',
    'A04': 'Generation',
    'A05': 'Load',
    'B01': 'Biomass',
    'B02': 'Fossil Brown coal/Lignite',
    'B03': 'Fossil Coal-derived gas',
    'B04': 'Fossil Gas',
    'B05': 'Fossil Hard coal',
    'B06': 'Fossil Oil',
    'B07': 'Fossil Oil shale',
    'B08': 'Fossil Peat',
    'B09': 'Geothermal',
    'B10': 'Hydro Pumped Storage',
    'B11': 'Hydro Run-of-river and poundage',
    'B12': 'Hydro Water Reservoir',
    'B13': 'Marine',
    'B14': 'Nuclear',
    'B15': 'Other renewable',
    'B16': 'Solar',
    'B17': 'Waste',
    'B18': 'Wind Offshore',
    'B19': 'Wind Onshore',
    'B20': 'Other',
    'B21': 'AC Link',
    'B22': 'DC Link',
    'B23': 'Substation',
    'B24': 'Transformer'}
```
#### Docstatus
```python
DOCSTATUS = {
    'A05': 'Active',
    'A09': 'Cancelled',
    'A13': 'Withdrawn'
}
```
#### BSN-type
```python
BSNTYPE = {'A29': 'Already allocated capacity (AAC)',
           'A43': 'Requested capacity (without price)',
           'A46': 'System Operator redispatching',
           'A53': 'Planned maintenance',
           'A54': 'Unplanned outage',
           'A85': 'Internal redispatch',
           'A95': 'Frequency containment reserve',
           'A96': 'Automatic frequency restoration reserve',
           'A97': 'Manual frequency restoration reserve',
           'A98': 'Replacement reserve',
           'B01': 'Interconnector network evolution',
           'B02': 'Interconnector network dismantling',
           'B03': 'Counter trade',
           'B04': 'Congestion costs',
           'B05': 'Capacity allocated (including price)',
           'B07': 'Auction revenue',
           'B08': 'Total nominated capacity',
           'B09': 'Net position',
           'B10': 'Congestion income',
           'B11': 'Production unit'}
```
#### DocumentType
```python
DOCUMENTTYPE = {'A09': 'Finalised schedule',
                'A11': 'Aggregated energy data report',
                'A25': 'Allocation result document',
                'A26': 'Capacity document',
                'A31': 'Agreed capacity',
                'A44': 'Price Document',
                'A61': 'Estimated Net Transfer Capacity',
                'A63': 'Redispatch notice',
                'A65': 'System total load',
                'A68': 'Installed generation per type',
                'A69': 'Wind and solar forecast',
                'A70': 'Load forecast margin',
                'A71': 'Generation forecast',
                'A72': 'Reservoir filling information',
                'A73': 'Actual generation',
                'A74': 'Wind and solar generation',
                'A75': 'Actual generation per type',
                'A76': 'Load unavailability',
                'A77': 'Production unavailability',
                'A78': 'Transmission unavailability',
                'A79': 'Offshore grid infrastructure unavailability',
                'A80': 'Generation unavailability',
                'A81': 'Contracted reserves',
                'A82': 'Accepted offers',
                'A83': 'Activated balancing quantities',
                'A84': 'Activated balancing prices',
                'A85': 'Imbalance prices',
                'A86': 'Imbalance volume',
                'A87': 'Financial situation',
                'A88': 'Cross border balancing',
                'A89': 'Contracted reserve prices',
                'A90': 'Interconnection network expansion',
                'A91': 'Counter trade notice',
                'A92': 'Congestion costs',
                'A93': 'DC link capacity',
                'A94': 'Non EU allocations',
                'A95': 'Configuration document',
                'B11': 'Flow-based allocations'}
```

#### ProcessType
```python
PROCESSTYPE = {
    'A01': 'Day ahead',
    'A02': 'Intra day incremental',
    'A16': 'Realised',
    'A18': 'Intraday total',
    'A31': 'Week ahead',
    'A32': 'Month ahead',
    'A33': 'Year ahead',
    'A39': 'Synchronisation process',
    'A40': 'Intraday process',
    'A46': 'Replacement reserve',
    'A47': 'Manual frequency restoration reserve',
    'A51': 'Automatic frequency restoration reserve',
    'A52': 'Frequency containment reserve',
    'A56': 'Frequency restoration reserve'
}
```
