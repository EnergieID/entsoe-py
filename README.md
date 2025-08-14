# entsoe-py
Python client for the ENTSO-E API (european network of transmission system operators for electricity)

Documentation of the API found on https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html

## Installation
`python3 -m pip install entsoe-py`

## Usage
The package comes with 2 clients for the REST API:
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
country_code_from = 'FR'  # France
country_code_to = 'DE_LU' # Germany-Luxembourg
type_marketagreement_type = 'A01'
contract_marketagreement_type = 'A01'
process_type = 'A51'

# methods that return XML
client.query_day_ahead_prices(country_code, start, end)
client.query_aggregated_bids(country_code, start, end, process_type)
client.query_net_position(country_code, start, end, dayahead=True)
client.query_load(country_code, start, end)
client.query_load_forecast(country_code, start, end)
client.query_wind_and_solar_forecast(country_code, start, end, psr_type=None)
client.query_intraday_wind_and_solar_forecast(country_code, start, end, psr_type=None)
client.query_generation_forecast(country_code, start, end)
client.query_generation(country_code, start, end, psr_type=None)
client.query_generation_per_plant(country_code, start, end, psr_type=None)
client.query_installed_generation_capacity(country_code, start, end, psr_type=None)
client.query_installed_generation_capacity_per_unit(country_code, start, end, psr_type=None)
client.query_crossborder_flows(country_code_from, country_code_to, start, end)
client.query_scheduled_exchanges(country_code_from, country_code_to, start, end, dayahead=False)
client.query_net_transfer_capacity_dayahead(country_code_from, country_code_to, start, end)
client.query_net_transfer_capacity_weekahead(country_code_from, country_code_to, start, end)
client.query_net_transfer_capacity_monthahead(country_code_from, country_code_to, start, end)
client.query_net_transfer_capacity_yearahead(country_code_from, country_code_to, start, end)
client.query_intraday_offered_capacity(country_code_from, country_code_to, start, end, implicit=True)
client.query_offered_capacity(country_code_from, country_code_to, start, end, contract_marketagreement_type, implicit=True)
client.query_contracted_reserve_prices(country_code, start, end, type_marketagreement_type, psr_type=None)
client.query_contracted_reserve_prices_procured_capacity(country_code, start, end, process_type type_marketagreement_type, psr_type=None)
client.query_contracted_reserve_amount(country_code, start, end, type_marketagreement_type, psr_type=None)
client.query_procured_balancing_capacity(country_code, start, end, process_type, type_marketagreement_type=None)
client.query_aggregate_water_reservoirs_and_hydro_storage(country_code, start, end)
client.query_activated_balancing_energy_prices(country_code, start, end, process_type='A16', psr_type=None, business_type=None, standard_market_product=None, original_market_product=None)
client.query_activated_balancing_energy(country_code, start, end, business_type, psr_type=None)

# methods that return ZIP (bytes)
client.query_imbalance_prices(country_code, start, end, psr_type=None)
client.query_imbalance_volumes(country_code, start=start, end=end, psr_type=None)
client.query_unavailability_of_generation_units(country_code, start, end, docstatus=None, periodstartupdate=None, periodendupdate=None)
client.query_unavailability_of_production_units(country_code, start, end, docstatus=None, periodstartupdate=None, periodendupdate=None)
client.query_unavailability_transmission(country_code_from, country_code_to, start, end, docstatus=None, periodstartupdate=None, periodendupdate=None)
client.query_unavailability_of_offshore_grid(area_code, start, end)
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
response = client._base_request(params=params, start=start, end=end)
print(response.text)
```

### <a name="EntsoePandasClient"></a>EntsoePandasClient
The Pandas Client works similar to the Raw Client, with extras:
- Time periods that span more than 1 year are automatically dealt with
- Requests of large numbers of files are split over multiple API calls

Please note that this client requires you to specifically set a start= and end= parameter which should be a pandas timestamp with timezone.
If not it will throw an exception
```python
from entsoe import EntsoePandasClient
import pandas as pd

client = EntsoePandasClient(api_key=<YOUR API KEY>)

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'BE'  # Belgium
country_code_from = 'FR'  # France
country_code_to = 'DE_LU' # Germany-Luxembourg
type_marketagreement_type = 'A01'
contract_marketagreement_type = "A01"
process_type = 'A51'

# methods that return Pandas Series
client.query_day_ahead_prices(country_code, start=start, end=end)
client.query_aggregated_bids(country_code, start=start, end=end, process_type)
client.query_net_position(country_code, start=start, end=end, dayahead=True)
client.query_crossborder_flows(country_code_from, country_code_to, start=start, end=end)
client.query_scheduled_exchanges(country_code_from, country_code_to, start=start, end=end, dayahead=False)
client.query_net_transfer_capacity_dayahead(country_code_from, country_code_to, start=start, end=end)
client.query_net_transfer_capacity_weekahead(country_code_from, country_code_to, start=start, end=end)
client.query_net_transfer_capacity_monthahead(country_code_from, country_code_to, start=start, end=end)
client.query_net_transfer_capacity_yearahead(country_code_from, country_code_to, start=start, end=end)
client.query_intraday_offered_capacity(country_code_from, country_code_to, start=start, end=end, implicit=True)
client.query_offered_capacity(country_code_from, country_code_to, contract_marketagreement_type, start=start, end=end, implicit=True)
client.query_aggregate_water_reservoirs_and_hydro_storage(country_code, start=start, end=end)

# methods that return Pandas DataFrames
client.query_load(country_code, start=start, end=end)
client.query_load_forecast(country_code, start=start, end=end)
client.query_load_and_forecast(country_code, start=start, end=end)
client.query_generation_forecast(country_code, start=start, end=end)
client.query_wind_and_solar_forecast(country_code, start=start, end=end, psr_type=None)
client.query_intraday_wind_and_solar_forecast(country_code, start=start, end=end, psr_type=None)
client.query_generation(country_code, start=start, end=end, psr_type=None)
client.query_generation_per_plant(country_code, start=start, end=end, psr_type=None, include_eic=False)
client.query_installed_generation_capacity(country_code, start=start, end=end, psr_type=None)
client.query_installed_generation_capacity_per_unit(country_code, start=start, end=end, psr_type=None)
client.query_activated_balancing_energy_prices(country_code, start=start, end=end, process_type='A16', psr_type=None, business_type=None, standard_market_product=None, original_market_product=None)
client.query_activated_balancing_energy(country_code, start=start, end=end, business_type, psr_type=None)
client.query_imbalance_prices(country_code, start=start, end=end, psr_type=None)
client.query_imbalance_volumes(country_code, start=start, end=end, psr_type=None)
client.query_contracted_reserve_prices(country_code, type_marketagreement_type, start=start, end=end, psr_type=None)
client.query_contracted_reserve_amount(country_code, type_marketagreement_type, start=start, end=end, psr_type=None)
client.query_unavailability_of_generation_units(country_code, start=start, end=end, docstatus=None, periodstartupdate=None, periodendupdate=None)
client.query_unavailability_of_production_units(country_code, start, end, docstatus=None, periodstartupdate=None, periodendupdate=None)
client.query_unavailability_transmission(country_code_from, country_code_to, start=start, end=end, docstatus=None, periodstartupdate=None, periodendupdate=None)
client.query_withdrawn_unavailability_of_generation_units(country_code, start, end)
client.query_unavailability_of_offshore_grid(area_code, start, end)
client.query_physical_crossborder_allborders(country_code, start, end, export=True)
client.query_import(country_code, start, end)
client.query_generation_import(country_code, start, end)
client.query_procured_balancing_capacity(country_code, process_type, start=start, end=end, type_marketagreement_type=None)

```
#### Dump result to file
See a list of all IO-methods on https://pandas.pydata.org/pandas-docs/stable/io.html
```python
ts = client.query_day_ahead_prices(country_code, start=start, end=end)
ts.to_csv('outfile.csv')
```

### Download from ENTSOE File Library
To download from the file libary, which replaced the old SFTP use the ```files``` subpackage with the ```EntsoeFileClient```

```python
from entsoe.files import EntsoeFileClient

client = EntsoeFileClient(username=<YOUR ENTSOE USERNAME>, pwd=<YOUR ENTSOE PASSWORD>)
# this returns a dict of {filename: unique_id}:
file_list = client.list_folder('AcceptedAggregatedOffers_17.1.D')

# either download one file by name:
df = client.download_single_file(folder='AcceptedAggregatedOffers_17.1.D', filename=list(file_list.keys())[0])

# or download multiple by unique_id:
df = client.download_multiple_files(['a1a82b3f-c453-4181-8d20-ad39c948d4b0', '64e47e15-bac6-4212-b2dd-9667bdf33b5d'])
```

### Mappings
These lists are always evolving, so let us know if something's inaccurate!

All mappings can be found in ```mappings.py``` [here](https://github.com/EnergieID/entsoe-py/blob/master/entsoe/mappings.py)

For bidding zone that have changed (splitted/merged) some codes are only valid for certain times. The below table shows these cases.

|  | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 |
| -- | -- | -- | -- | -- | -- | -- | -- |
| DE_AT_LU | yes | yes | yes | yes | No Value | No Value | No Value |
| DE | No Value | No Value | No Value | No Value | No Value | No Value | No Value |
| DE_LU | No Value | No Value | No Value | yes | yes | yes | yes |
| AT | No Value | No Value | No Value | yes | yes | yes | yes |

### Environment variables
| Variables | Description |
| -- | -- |
| ENTSOE_ENDPOINT_URL | Override the default ENTSO-E API endpoint URL. |
