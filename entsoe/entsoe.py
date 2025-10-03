import logging
import os
from typing import Union, Optional, Dict, List, Literal

import pandas as pd
from pandas.tseries.offsets import YearBegin, YearEnd
import pytz
import requests
from bs4 import BeautifulSoup
from bs4.builder import XMLParsedAsHTMLWarning

from entsoe.exceptions import InvalidPSRTypeError, InvalidBusinessParameterError, InvalidParameterError
from .exceptions import NoMatchingDataError, PaginationError
from .mappings import Area, NEIGHBOURS, lookup_area
from .parsers import parse_prices, parse_loads, parse_generation, \
    parse_installed_capacity_per_plant, parse_crossborder_flows, \
    parse_unavailabilities, parse_contracted_reserve, parse_imbalance_prices_zip, \
    parse_imbalance_volumes_zip, parse_netpositions, parse_procured_balancing_capacity, \
    parse_water_hydro,parse_aggregated_bids, parse_activated_balancing_energy_prices, \
    parse_offshore_unavailability, parse_imbalance_volumes
from .decorators import retry, paginated, year_limited, day_limited, documents_limited
import warnings

logger = logging.getLogger(__name__)
warnings.filterwarnings('always')
warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)

__title__ = "entsoe-py"
__version__ = "0.7.5"
__author__ = "EnergieID.be, Frank Boerman"
__license__ = "MIT"

URL = os.getenv("ENTSOE_ENDPOINT_URL") or "https://web-api.tp.entsoe.eu/api"

QUARTER_MTU_SDAC_GOLIVE =  pd.Timestamp('2025-10-01', tz='europe/amsterdam')


class EntsoeRawClient:
    # noinspection LongLine
    """
        Client to perform API calls and return the raw responses API-documentation:
        https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_request_methods

        Attributions: Parts of the code for parsing Entsoe responses were copied
        from https://github.com/tmrowco/electricitymap
        """

    def __init__(
            self, api_key: str = None, session: Optional[requests.Session] = None,
            retry_count: int = 3, retry_delay: int = 10,
            proxies: Optional[Dict] = None, timeout: Optional[int] = None):
        """
        Parameters
        ----------
        api_key : str
        session : requests.Session
        retry_count : int
            number of times to retry the call if the connection fails
        retry_delay: int
            amount of seconds to wait between retries
        proxies : dict
            requests proxies
        timeout : int
        """
        self.api_key = api_key
        if self.api_key is None:
            self.api_key = os.getenv("ENTSOE_API_KEY")
        if self.api_key is None:
            raise TypeError("API key cannot be None")
        if session is None:
            session = requests.Session()
        self.session = session
        self.session.headers.update({
            'user-agent': f'entsoe-py {__version__} (github.com/EnergieID/entsoe-py)'
        })
        self.proxies = proxies
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.timeout = timeout

    @retry
    def _base_request(self, params: Dict, start: pd.Timestamp,
                      end: pd.Timestamp) -> requests.Response:
        """
        Parameters
        ----------
        params : dict
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        requests.Response
        """
        start_str = self._datetime_to_str(start)
        end_str = self._datetime_to_str(end)

        base_params = {
            'securityToken': self.api_key,
            'periodStart': start_str,
            'periodEnd': end_str
        }
        params.update(base_params)

        logger.debug(f'Performing request to {URL} with params {params}')
        response = self.session.get(url=URL, params=params,
                                    proxies=self.proxies, timeout=self.timeout)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.find_all('text')
            if len(text):
                error_text = soup.find('text').text
                if 'No matching data found' in error_text:
                    raise NoMatchingDataError
                elif "check you request against dependency tables" in error_text:
                    raise InvalidBusinessParameterError
                elif "is not valid for this area" in error_text:
                    raise InvalidPSRTypeError
                elif 'amount of requested data exceeds allowed limit' in error_text:
                    requested = error_text.split(' ')[-2]
                    allowed = error_text.split(' ')[-5]
                    raise PaginationError(
                        f"The API is limited to {allowed} elements per "
                        f"request. This query requested for {requested} "
                        f"documents and cannot be fulfilled as is.")
                elif 'requested data to be gathered via the offset parameter exceeds the allowed limit' in error_text:
                    requested = error_text.split(' ')[-9]
                    allowed = error_text.split(' ')[-30][:-2]
                    raise PaginationError(
                        f"The API is limited to {allowed} elements per "
                        f"request. This query requested for {requested} "
                        f"documents and cannot be fulfilled as is.")
            raise e
        else:
            # ENTSO-E has changed their server to also respond with 200 if there is no data but all parameters are valid
            # this means we need to check the contents for this error even when status code 200 is returned
            # to prevent parsing the full response do a text matching instead of full parsing
            # also only do this when response type content is text and not for example a zip file
            if response.headers.get('content-type', '') == 'application/xml':
                if 'No matching data found' in response.text:
                    raise NoMatchingDataError
            return response

    @staticmethod
    def _datetime_to_str(dtm: pd.Timestamp) -> str:
        """
        Convert a datetime object to a string in UTC
        of the form YYYYMMDDhh00

        Parameters
        ----------
        dtm : pd.Timestamp
            Recommended to use a timezone-aware object!
            If timezone-naive, UTC is assumed

        Returns
        -------
        str
        """
        if dtm.tzinfo is not None and dtm.tzinfo != pytz.UTC:
            dtm = dtm.tz_convert("UTC")
        fmt = '%Y%m%d%H00'
        ret_str = dtm.round(freq='h').strftime(fmt)
        return ret_str

    def query_day_ahead_prices(self, country_code: Union[Area, str],
                               start: pd.Timestamp, end: pd.Timestamp,
                               offset: int = 0, sequence: int = None) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A44',
            'in_Domain': area.code,
            'out_Domain': area.code,
            'offset': offset,
            'contract_MarketAgreement.type': 'A01'
        }
        if sequence is not None:
            params['classificationSequence_AttributeInstanceComponent.position'] = sequence
        response = self._base_request(params=params, start=start, end=end)
        return response.text



    def query_intraday_prices(self, country_code: Union[Area, str],
                               start: pd.Timestamp, end: pd.Timestamp,
                               sequence: int, offset: int = 0) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A44',
            'in_Domain': area.code,
            'out_Domain': area.code,
            'offset': offset,
            'contract_MarketAgreement.type': 'A07'
        }
        if sequence is not None:
            params['classificationSequence_AttributeInstanceComponent.position'] = sequence
        response = self._base_request(params=params, start=start, end=end)
        return response.text


    def query_aggregated_bids(self, country_code: Union[Area, str],
                              process_type: str,
                               start: pd.Timestamp, end: pd.Timestamp) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str
            A51 ... aFRR; A47 ... mFRR
            
        Returns
        -------
        str
        """
        if process_type not in ['A51', 'A52', 'A47']:
            raise ValueError('processType allowed values: A51, A52, A47')
        area = lookup_area(country_code)
        params = {
            'documentType': 'A24',
            'area_Domain': area.code,
            'processType': process_type
        }
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_net_position(self, country_code: Union[Area, str],
                           start: pd.Timestamp, end: pd.Timestamp, dayahead: bool = True) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        dayahead : bool

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A25',  # Allocation result document
            'businessType': 'B09',  # net position
            'Contract_MarketAgreement.Type': 'A01',  # daily
            'in_Domain': area.code,
            'out_Domain': area.code
        }
        if not dayahead:
            params.update({'Contract_MarketAgreement.Type': "A07"})

        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_load(self, country_code: Union[Area, str], start: pd.Timestamp,
                   end: pd.Timestamp) -> str:
        """
        Parameters
    
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A65',
            'processType': 'A16',
            'outBiddingZone_Domain': area.code,
            'out_Domain': area.code
        }
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_load_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str = 'A01') -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A65',
            'processType': process_type,
            'outBiddingZone_Domain': area.code,
            # 'out_Domain': domain
        }
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_generation_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str = 'A01') -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A71',
            'processType': process_type,
            'in_Domain': area.code,
        }
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_wind_and_solar_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            process_type: str = 'A01', **kwargs) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        process_type : str

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A69',
            'processType': process_type,
            'in_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_intraday_wind_and_solar_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp, psr_type: Optional[str] = None) -> str:
        return self.query_wind_and_solar_forecast(country_code=country_code,
                                                  start=start,
                                                  end=end,
                                                  psr_type=psr_type,
                                                  process_type='A40')

    def query_generation(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None, **kwargs) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A75',
            'processType': 'A16',
            'in_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_generation_per_plant(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            eic_code: Optional[str] = None, **kwargs) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        eic_code : str
            filter on a single Generation Unit using its EIC Code

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A73',
            'processType': 'A16',
            'in_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        if eic_code:
            params.update({'registeredResource': eic_code})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_installed_generation_capacity(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A68',
            'processType': 'A33',
            'in_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_installed_generation_capacity_per_unit(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A71',
            'processType': 'A33',
            'in_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_aggregate_water_reservoirs_and_hydro_storage(self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        offset : int
            offset for querying more than 100 documents

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A72',
            'processType': 'A16',
            'in_Domain': area.code
        }
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_crossborder_flows(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> str:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        return self._query_crossborder(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end,
            doctype="A11", contract_marketagreement_type=None)

    def query_scheduled_exchanges(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str],
            start: pd.Timestamp,
            end: pd.Timestamp,
            dayahead: bool = False,
            **kwargs) -> str:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        dayahead : bool
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        if dayahead:
            contract_marketagreement_type = "A01"
        else:
            contract_marketagreement_type = "A05"
        return self._query_crossborder(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end,
            doctype="A09", contract_marketagreement_type=contract_marketagreement_type)

    def query_net_transfer_capacity_dayahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> str:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        return self._query_crossborder(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end,
            doctype="A61", contract_marketagreement_type="A01")

    def query_net_transfer_capacity_weekahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> str:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        return self._query_crossborder(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end,
            doctype="A61", contract_marketagreement_type="A02")

    def query_net_transfer_capacity_monthahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> str:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        return self._query_crossborder(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end,
            doctype="A61", contract_marketagreement_type="A03")

    def query_net_transfer_capacity_yearahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> str:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        return self._query_crossborder(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end,
            doctype="A61", contract_marketagreement_type="A04")

    def query_intraday_offered_capacity(
        self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, implicit:bool = True,**kwargs) -> str:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        implicit: bool (True = implicit - default for most borders. False = explicit - for instance BE-GB)

        Returns
        -------
        str
        """
        return self._query_crossborder(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end,
            doctype="A31", contract_marketagreement_type="A07",
            auction_type=("A01" if implicit==True else "A02"))

    def query_offered_capacity(
        self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, contract_marketagreement_type: str,
            implicit:bool = True,**kwargs) -> str:
        """
        Allocated result documents, for OC evolution see query_intraday_offered_capacity

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        contract_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        implicit: bool (True = implicit - default for most borders. False = explicit - for instance BE-GB)

        Returns
        -------
        str
        """
        if implicit:
            business_type = None
        else:
            business_type = "B05"
        return self._query_crossborder(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end,
            doctype=("A31" if implicit else "A25"),
            contract_marketagreement_type=contract_marketagreement_type,
            auction_type=("A01" if implicit else "A02"),
            business_type=business_type)

    def _query_crossborder(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, doctype: str,
            contract_marketagreement_type: Optional[str] = None,
            auction_type: Optional[str] = None, business_type: Optional[str] = None) -> str:
        """
        Generic function called by query_crossborder_flows,
        query_scheduled_exchanges, query_net_transfer_capacity_DA/WA/MA/YA and query_.

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        doctype: str
        contract_marketagreement_type: str
        business_type: str

        Returns
        -------
        str
        """
        area_in = lookup_area(country_code_to)
        area_out = lookup_area(country_code_from)

        params = {
            'documentType': doctype,
            'in_Domain': area_in.code,
            'out_Domain': area_out.code
        }
        if contract_marketagreement_type is not None:
            params[
                'contract_MarketAgreement.Type'] = contract_marketagreement_type
        if auction_type is not None:
            params[
                'Auction.Type'] = auction_type
        if business_type is not None:
            params[
                'businessType'] = business_type

        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_activated_balancing_energy_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, 
            process_type: Optional[str] = 'A16',
            psr_type: Optional[str] = None,
            business_type: Optional[str] = None, 
            standard_market_product: Optional[str] = None,
            original_market_product: Optional[str] = None) -> bytes:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type: str
            A16 used if not provided
        psr_type : str
            filter query for a specific psr type
        business_type: str
            filter query for a specific business type
        standard_market_product: str
        original_market_product: str
            filter query for a specific product
            defaults to standard product
        Returns
        -------
        bytes
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A84',
            'processType': process_type,
            'controlArea_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        if business_type:
            params.update({'businessType': business_type})
        if standard_market_product:
            params.update({'standardMarketProduct': standard_market_product})
        if original_market_product:
            params.update({'originalMarketProduct': original_market_product})
        response = self._base_request(params=params, start=start, end=end)
        
        return response.content
    
    def query_imbalance_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> bytes:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        bytes
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A85',
            'controlArea_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.content

    def query_imbalance_volumes(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> bytes:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        bytes
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A86',
            'controlArea_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.content

    def query_current_balancing_state(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A86',
            'businessType': 'B33',
            'area_Domain': area.code,
        }
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_procured_balancing_capacity(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str,
            type_marketagreement_type: Optional[str] = None) -> bytes:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str
            A51 ... aFRR; A47 ... mFRR
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)

        Returns
        -------
        bytes
        """
        if process_type not in ['A51', 'A52', 'A47']:
            raise ValueError('processType allowed values: A51, A52, A47')

        area = lookup_area(country_code)
        params = {
            'documentType': 'A15',
            'area_Domain': area.code,
            'processType': process_type
        }
        if type_marketagreement_type:
            params.update({'type_MarketAgreement.Type': type_marketagreement_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.content

    def query_activated_balancing_energy(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, business_type: str,
            psr_type: Optional[str] = None) -> bytes:
        """
        Activated Balancing Energy [17.1.E]
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        business_type : str
            type of contract (see mappings.BSNTYPE)
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        bytes
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A83',
            'controlArea_Domain': area.code,
            'businessType': business_type
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.content

    def query_contracted_reserve_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, type_marketagreement_type: str,
            psr_type: Optional[str] = None,
            offset: int = 0) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        psr_type : str
            filter query for a specific psr type
        offset : int
            offset for querying more than 100 documents

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A89',
            'controlArea_Domain': area.code,
            'type_MarketAgreement.Type': type_marketagreement_type,
            'offset': offset
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_contracted_reserve_prices_procured_capacity(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str, 
            type_marketagreement_type: str, psr_type: Optional[str] = None,
            offset: int = 0) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str
            type of process (see mappings.PROCESSTYPE)
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        psr_type : str
            filter query for a specific psr type
        offset : int
            offset for querying more than 100 documents

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A81', # [M] A81 = Contracted reserves
            'businessType': 'B95', # [M] B95 = Procured capacity
            'processType': process_type, # [M*] A51 = Automatic frequency restoration reserve; A52 =  Frequency containment reserve; A47 = Manual frequency restoration reserve; A46 = Replacement reserve
            'controlArea_Domain': area.code,
            'type_MarketAgreement.Type': type_marketagreement_type,
            'offset': offset
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_contracted_reserve_amount(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, type_marketagreement_type: str,
            psr_type: Optional[str] = None,
            offset: int = 0) -> str:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        psr_type : str
            filter query for a specific psr type
        offset : int
            offset for querying more than 100 documents

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A81',
            'controlArea_Domain': area.code,
            'type_MarketAgreement.Type': type_marketagreement_type,
            'offset': offset
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def _query_unavailability(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, doctype: str, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
            mRID=None,
            offset: int = 0) -> bytes:
        """
        Generic unavailibility query method.
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.

        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        doctype : str
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional
        offset : int

        Returns
        -------
        bytes
        """
        area = lookup_area(country_code)
        params = {
            'documentType': doctype,
            'biddingZone_domain': area.code,
            'offset': offset
            # ,'businessType': 'A53 (unplanned) | A54 (planned)'
        }
        if docstatus:
            params['docStatus'] = docstatus
        if periodstartupdate and periodendupdate:
            params['periodStartUpdate'] = self._datetime_to_str(
                periodstartupdate)
            params['periodEndUpdate'] = self._datetime_to_str(periodendupdate)
        if mRID:
            params['mRID'] = mRID
        response = self._base_request(params=params, start=start, end=end)
        return response.content

    def query_unavailability_of_generation_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
            mRID = None,
            offset: int = 0) -> bytes:
        """
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.

        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional
        offset : int


        Returns
        -------
        bytes
        """
        content = self._query_unavailability(
            country_code=country_code, start=start, end=end, doctype="A80",
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate, mRID=mRID, offset=offset)
        return content

    def query_unavailability_of_offshore_grid(self, area_code: Union[Area, str], start: pd.Timestamp, end: pd.Timestamp):
        return self._query_unavailability(
            country_code=area_code, start=start, end=end, doctype='A79'
        )

    def query_unavailability_of_production_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
            mRID: Optional[str] = None) -> bytes:
        """
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.

        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional

        Returns
        -------
        bytes
        """
        content = self._query_unavailability(
            country_code=country_code, start=start, end=end, doctype="A77",
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate,
            mRID = mRID)
        return content

    def query_unavailability_transmission(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
            offset: int = 0,
            **kwargs) -> bytes:
        """
        Generic unavailibility query method.
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional
        offset : int

        Returns
        -------
        bytes
        """
        area_in = lookup_area(country_code_to)
        area_out = lookup_area(country_code_from)
        params = {
            'documentType': "A78",
            'in_Domain': area_in.code,
            'out_Domain': area_out.code,
            'offset': offset
        }
        if docstatus:
            params['docStatus'] = docstatus
        if periodstartupdate and periodendupdate:
            params['periodStartUpdate'] = self._datetime_to_str(
                periodstartupdate)
            params['periodEndUpdate'] = self._datetime_to_str(periodendupdate)
        response = self._base_request(params=params, start=start, end=end)
        return response.content

    def query_withdrawn_unavailability_of_generation_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, mRID: Optional[str] = None) -> bytes:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        bytes
        """
        content = self._query_unavailability(
            country_code=country_code, start=start, end=end,
            doctype="A80", docstatus='A13', mRID=mRID)
        return content

class EntsoePandasClient(EntsoeRawClient):
    @year_limited
    def query_net_position(self, country_code: Union[Area, str],
                            start: pd.Timestamp, end: pd.Timestamp, dayahead: bool = True,
                           resolution = None) -> pd.Series:
        """
        forced to correct resolution for day ahead values
        Parameters
        ----------
        country_code
        start
        end

        Returns
        -------

        """
        if resolution is not None:
            warnings.warn('The resolution parameter is deprecated and will be removed. This function will force the right resolution', DeprecationWarning)
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_net_position(
            country_code=area, start=start, end=end, dayahead=dayahead)
        series = parse_netpositions(text)
        if len(series) == 0:
            raise NoMatchingDataError
        if dayahead:
            # This function should only return SDAC net positions for day ahead, which have a fixed defined resolution
            # before 2025-10-01 its 60min, after 15min
            # this is aligned on businessday in timezone europe/amsterdam
            # some zones already publish in different resolution.
            # for secondary auctions published on entsoe, use the query_day_ahead_prices_local function
            if series.index.max() < QUARTER_MTU_SDAC_GOLIVE:
                series = series.resample('h').first()
            else:
                series_60min = series[series.index < QUARTER_MTU_SDAC_GOLIVE]
                series_15min = series[series.index >= QUARTER_MTU_SDAC_GOLIVE]

                series = pd.concat([
                    series_60min.resample('h').first(),
                    series_15min
                ]).sort_index()

        series = series.tz_convert(area.tz).sort_index()
        series = series.truncate(before=start, after=end)
        # because of the above fix we need to check again if any valid data exists after truncating
        if len(series) == 0:
            raise NoMatchingDataError
        return series

    @year_limited
    def query_aggregated_bids(self, country_code: Union[Area, str],
                              process_type: str,
                              start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """

        Parameters
        ----------
        country_code
        start
        end
        process_type: str,

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_aggregated_bids(
            country_code=area, process_type=process_type, start=start, end=end)
        df = parse_aggregated_bids(text)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    # we need to do offset, but we also want to pad the days so wrap it in an internal call
    def query_day_ahead_prices(
            self, country_code: Union[Area, str],
            start: pd.Timestamp,
            end: pd.Timestamp,
            resolution = None) -> pd.Series:
        """
        Parameters
        ----------
        this will return the SDAC prices, forced to the correct resolution
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        if resolution is not None:
            warnings.warn('The resolution parameter is deprecated and will be removed. This function will force the right SDAC resolution', DeprecationWarning)
        area = lookup_area(country_code)
        # we do here extra days at start and end to fix issue 187
        series = self._query_day_ahead_prices(
            area,
            start=start-pd.Timedelta(days=1),
            end=end+pd.Timedelta(days=1)
        )
        series = series.tz_convert(area.tz).sort_index()
        series = series.truncate(before=start, after=end)
        # because of the above fix we need to check again if any valid data exists after truncating
        if len(series) == 0:
            raise NoMatchingDataError
        return series

    @year_limited
    @documents_limited(100)
    def _query_day_ahead_prices(
            self, area: Area,
            start: pd.Timestamp,
            end: pd.Timestamp,
            offset: int = 0) -> pd.Series:
        text = super(EntsoePandasClient, self).query_day_ahead_prices(
            area,
            start=start,
            end=end,
            offset=offset,
            sequence=1 if area.name in ['DE_LU', 'AT'] else None
        )
        series_all = parse_prices(text)

        # This function should only return SDAC prices, which have a fixed defined resolution
        # before 2025-10-01 its 60min, after 15min
        # this is aligned on businessday in timezone europe/amsterdam
        # some zones already publish in different resolution.
        # for secondary auctions published on entsoe, use the query_day_ahead_prices_local function

        series = pd.concat([x for x in series_all.values() if len(x) > 0]).sort_index().tz_convert('europe/amsterdam')
        if len(series) == 0:
            raise NoMatchingDataError
        if series.index.max() < QUARTER_MTU_SDAC_GOLIVE:
            series = series.resample('h').first()
        else:
            series_60min = series[series.index < QUARTER_MTU_SDAC_GOLIVE]
            series_15min = series[series.index >= QUARTER_MTU_SDAC_GOLIVE]

            series = pd.concat([
                series_60min.resample('h').first(),
                series_15min
            ]).sort_index()

        series = series.tz_convert(area.tz).sort_index()
        series = series.truncate(before=start, after=end)
        # because of the above fix we need to check again if any valid data exists after truncating
        if len(series) == 0:
            raise NoMatchingDataError
        return series

    # we need to do offset, but we also want to pad the days so wrap it in an internal call
    def query_intraday_prices(
            self, country_code: Union[Area, str],
            start: pd.Timestamp,
            end: pd.Timestamp,
            sequence: int) -> pd.Series:
        """
        Parameters
        ----------
        this will return the IDA prices, forced to the correct resolution
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        sequence: int, 1, 2 or 3 corresponding to IDA 1, 2, 3. only some zones publish this on entsoe

        Returns
        -------
        pd.Series
        """

        area = lookup_area(country_code)
        # we do here extra days at start and end to fix issue 187
        series = self._query_intraday_prices(
            area,
            start=start-pd.Timedelta(days=1),
            end=end+pd.Timedelta(days=1),
            sequence=sequence
        )
        series = series.tz_convert(area.tz).sort_index()
        series = series.truncate(before=start, after=end)
        # because of the above fix we need to check again if any valid data exists after truncating
        if len(series) == 0:
            raise NoMatchingDataError
        return series

    @year_limited
    @documents_limited(100)
    def _query_intraday_prices(
            self, area: Area,
            start: pd.Timestamp,
            end: pd.Timestamp,
            sequence: int,
            offset: int = 0) -> pd.Series:
        text = super(EntsoePandasClient, self).query_intraday_prices(
            area,
            start=start,
            end=end,
            offset=offset,
            sequence=sequence
        )
        series = parse_prices(text)['15min']
        series = series.tz_convert(area.tz).sort_index()
        series = series.truncate(before=start, after=end)
        # because of the above fix we need to check again if any valid data exists after truncating
        if len(series) == 0:
            raise NoMatchingDataError
        return series

    # we need to do offset, but we also want to pad the days so wrap it in an internal call
    def query_day_ahead_prices_local(
            self, country_code: Union[Area, str],
            sequence: int,
            start: pd.Timestamp,
            end: pd.Timestamp,
            resolution: Literal['60min', '30min', '15min'] = '60min') -> pd.Series:
        """
        Parameters
        ----------
        this will return local auction prices that are published on entsoe, simply specify the sequence. sequence 1 is always SDAC
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area = lookup_area(country_code)
        # we do here extra days at start and end to fix issue 187
        series = self._query_day_ahead_prices_local(
            area,
            sequence,
            start=start-pd.Timedelta(days=1),
            end=end+pd.Timedelta(days=1),
            resolution=resolution
        )
        series = series.tz_convert(area.tz).sort_index()
        series = series.truncate(before=start, after=end)
        # because of the above fix we need to check again if any valid data exists after truncating
        if len(series) == 0:
            raise NoMatchingDataError
        return series

    @year_limited
    @documents_limited(100)
    def _query_day_ahead_prices_local(
            self, area: Area,
            sequence: int,
            start: pd.Timestamp,
            end: pd.Timestamp,
            offset: int = 0,
            resolution: Literal['60min', '30min', '15min'] = '60min') -> pd.Series:
        text = super(EntsoePandasClient, self).query_day_ahead_prices(
            area,
            start=start,
            end=end,
            offset=offset,
            sequence=sequence
        )
        series_all = parse_prices(text)
        series = series_all[resolution]
        if len(series) == 0:
            raise NoMatchingDataError
        return series

    @year_limited
    def query_load(self, country_code: Union[Area, str], start: pd.Timestamp,
                   end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_load(
            country_code=area, start=start, end=end)

        df = parse_loads(text, process_type='A16')
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_load_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str = 'A01') -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_load_forecast(
            country_code=area, start=start, end=end, process_type=process_type)

        df = parse_loads(text, process_type=process_type)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    def query_load_and_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        """
        utility function to combina query realised load and forecasted day ahead load.
        this mimics the html view on the page Total Load - Day Ahead / Actual

        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.DataFrame
        """
        df_load_forecast_da = self.query_load_forecast(country_code, start=start, end=end)
        df_load = self.query_load(country_code, start=start, end=end)
        return df_load_forecast_da.join(df_load, sort=True, how='outer')


    @year_limited
    def query_generation_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str = 'A01',
            nett: bool = False) -> Union[pd.DataFrame, pd.Series]:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str
        nett : bool
            condense generation and consumption into a nett number

        Returns
        -------
        pd.DataFrame | pd.Series
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_generation_forecast(
            country_code=area, start=start, end=end, process_type=process_type)
        df = parse_generation(text, nett=nett)
        if isinstance(df, pd.DataFrame):
            df = df.rename(columns=lambda c: c.replace('Actual', 'Scheduled'))
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_wind_and_solar_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            process_type: str = 'A01', **kwargs) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        process_type : str

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_wind_and_solar_forecast(
            country_code=area, start=start, end=end, psr_type=psr_type,
            process_type=process_type)
        df = parse_generation(text, nett=True)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    def query_intraday_wind_and_solar_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> pd.DataFrame:
        return self.query_wind_and_solar_forecast(country_code=country_code,
                                                  start=start,
                                                  end=end,
                                                  psr_type=psr_type,
                                                  process_type='A40')


    @year_limited
    def query_generation(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            nett: bool = False, **kwargs) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        nett : bool
            condense generation and consumption into a nett number

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_generation(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_generation(text, nett=nett)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_installed_generation_capacity(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        
        area = lookup_area(country_code)
        text = super(
            EntsoePandasClient, self).query_installed_generation_capacity(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_generation(text)
        df = df.tz_convert(area.tz)
        # Truncate to YearBegin and YearEnd, because answer is always year-based
        df = df.truncate(before=start - YearBegin(), after=end + YearEnd())
        return df

    @year_limited
    def query_installed_generation_capacity_per_unit(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(
            EntsoePandasClient,
            self).query_installed_generation_capacity_per_unit(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_installed_capacity_per_plant(text)
        return df

    @year_limited
    @paginated
    def query_aggregate_water_reservoirs_and_hydro_storage(self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        area = lookup_area(country_code)
        text = super(
            EntsoePandasClient,
            self).query_aggregate_water_reservoirs_and_hydro_storage(
            country_code=area, start=start, end=end)

        df = parse_water_hydro(text)

        return df


    @year_limited
    def query_crossborder_flows(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsoePandasClient, self).query_crossborder_flows(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_scheduled_exchanges(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str],
            start: pd.Timestamp,
            end: pd.Timestamp,
            dayahead: bool = False,
            **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        dayahead : bool
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsoePandasClient, self).query_scheduled_exchanges(
            country_code_from=area_from,
            country_code_to=area_to,
            dayahead=dayahead,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_net_transfer_capacity_dayahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsoePandasClient, self).query_net_transfer_capacity_dayahead(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_net_transfer_capacity_weekahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsoePandasClient, self).query_net_transfer_capacity_weekahead(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_net_transfer_capacity_monthahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsoePandasClient, self).query_net_transfer_capacity_monthahead(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_net_transfer_capacity_yearahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsoePandasClient, self).query_net_transfer_capacity_yearahead(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_intraday_offered_capacity(
        self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, implicit:bool = True, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country  --> to check

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        implicit: bool (True = implicit - default for most borders. False = explicit - for instance BE-GB)
        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsoePandasClient, self).query_intraday_offered_capacity(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end,
            implicit=implicit)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    @paginated
    #@documents_limited(100)
    def query_offered_capacity(
            self,
            country_code_from: Union[Area, str],
            country_code_to: Union[Area, str],
            contract_marketagreement_type: str,
            start: pd.Timestamp,
            end: pd.Timestamp,
            implicit: bool = True, offset: int = 0, **kwargs) -> pd.Series:
        """
        Allocated result documents, for OC evolution see query_intraday_offered_capacity
        Note: Result will be in the timezone of the origin country  --> to check

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        contract_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        start : pd.Timestamp
        end : pd.Timestamp
        implicit: bool (True = implicit - default for most borders. False = explicit - for instance BE-GB)
        offset: int
            offset for querying more than 100 documents
        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsoePandasClient, self).query_offered_capacity(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end,
            contract_marketagreement_type=contract_marketagreement_type,
            implicit=implicit,
            offset=offset)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_activated_balancing_energy_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, 
            process_type: Optional[str] = 'A16',
            psr_type: Optional[str] = None,
            business_type: Optional[str] = None, 
            standard_market_product: Optional[str] = None,
            original_market_product: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type: str
            A16 used if not provided
        psr_type : str
            filter query for a specific psr type
        business_type: str
            filter query for a specific business type
        standard_market_product: str
        original_market_product: str
            filter query for a specific product
            defaults to standard product
        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_activated_balancing_energy_prices(
            country_code=area, start=start, end=end, 
            process_type=process_type,
            psr_type=psr_type,
            business_type=business_type, 
            standard_market_product=standard_market_product,
            original_market_product=original_market_product)
        df = parse_activated_balancing_energy_prices(text)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df
    
    @year_limited
    def query_imbalance_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None, include_resolution: bool = False) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type
        include_resolution: bool
            Add resolution columns to the result
        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        archive = super(EntsoePandasClient, self).query_imbalance_prices(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_imbalance_prices_zip(zip_contents=archive, include_resolution=include_resolution)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        # 
        if include_resolution:
            df = df.rename(columns={'Resolution Long': 'Resolution'})
            df.drop(columns=['Resolution Short'], inplace=True)
        return df

    @year_limited
    def query_imbalance_volumes(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None, include_resolution=False) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type
        include_resolution: bool
            include resolution column in the result
        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        archive = super(EntsoePandasClient, self).query_imbalance_volumes(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_imbalance_volumes_zip(zip_contents=archive, include_resolution=include_resolution)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_current_balancing_state(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_current_balancing_state(
            country_code=area, start=start, end=end)
        df = -1*parse_imbalance_volumes(text)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    def query_procured_balancing_capacity(
            self,
            country_code: Union[Area, str],
            process_type: str,
            start: pd.Timestamp,
            end: pd.Timestamp,
            type_marketagreement_type: Optional[str] = None) -> bytes:
        """
        Parameters
        ----------
        country_code : Area|str
        process_type : str
            A51 ... aFRR; A47 ... mFRR
        start : pd.Timestamp
        end : pd.Timestamp
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_procured_balancing_capacity(
            country_code=area, start=start, end=end,
            process_type=process_type, type_marketagreement_type=type_marketagreement_type)
        df = parse_procured_balancing_capacity(text, area.tz)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_activated_balancing_energy(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, business_type: str,
            psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Activated Balancing Energy [17.1.E]
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        business_type: str
            type of contract (see mappings.BSNTYPE)
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_activated_balancing_energy(
            country_code=area, start=start, end=end,
            business_type=business_type, psr_type=psr_type)
        df = parse_contracted_reserve(text, area.tz, "quantity")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    @documents_limited(100)
    def query_contracted_reserve_prices(
            self,
            country_code: Union[Area, str],
            type_marketagreement_type: str,
            start: pd.Timestamp,
            end: pd.Timestamp,
            psr_type: Optional[str] = None,
            offset: int = 0) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area, str
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type
        offset : int
            offset for querying more than 100 documents

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_contracted_reserve_prices(
            country_code=area, start=start, end=end,
            type_marketagreement_type=type_marketagreement_type,
            psr_type=psr_type, offset=offset)
        df = parse_contracted_reserve(text, area.tz, "procurement_price.amount")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df
    
    @year_limited
    @paginated
    @documents_limited(100)
    def query_contracted_reserve_prices_procured_capacity(
            self,
            country_code: Union[Area, str],
            process_type: str,
            type_marketagreement_type: str,
            start: pd.Timestamp,
            end: pd.Timestamp,
            psr_type: Optional[str] = None,
            offset: int = 0) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area, str
        process_type : str
            type of process (see mappings.PROCESSTYPE)
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type
        offset : int
            offset for querying more than 100 documents

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_contracted_reserve_prices_procured_capacity(
            country_code=area, start=start, end=end,
            process_type=process_type, type_marketagreement_type=type_marketagreement_type,
            psr_type=psr_type, offset=offset)
        df = parse_contracted_reserve(text, area.tz, "procurement_price.amount")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df    

    @year_limited
    @paginated
    @documents_limited(100)
    def query_contracted_reserve_amount(
            self,
            country_code: Union[Area, str],
            type_marketagreement_type: str,
            start: pd.Timestamp,
            end: pd.Timestamp,
            psr_type: Optional[str] = None,
            offset: int = 0) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type
        offset : int
            offset for querying more than 100 documents

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_contracted_reserve_amount(
            country_code=area, start=start, end=end,
            type_marketagreement_type=type_marketagreement_type,
            psr_type=psr_type, offset=offset)
        df = parse_contracted_reserve(text, area.tz, "quantity")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    @documents_limited(200)
    def _query_unavailability(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, doctype: str, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
            mRID: Optional[str] = None,
            offset: int = 0) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        doctype : str
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional
        offset : int

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        content = super(EntsoePandasClient, self)._query_unavailability(
            country_code=area, start=start, end=end, doctype=doctype,
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate, mRID=mRID, offset=offset)
        df = parse_unavailabilities(content, doctype)
        df = df.tz_convert(area.tz)
        df['start'] = df['start'].apply(lambda x: x.tz_convert(area.tz))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(area.tz))
        df = df[(df['start'] < end) | (df['end'] > start)]
        return df

    def query_unavailability_of_offshore_grid(self, area_code: Union[Area, str],
                                              start: pd.Timestamp, end: pd.Timestamp
                                              ) -> pd.DataFrame:
        zipfile = super(EntsoePandasClient, self)._query_unavailability(
            country_code=area_code, start=start, end=end, doctype='A79'
        )
        df = parse_offshore_unavailability(zipfile)
        return df


    def query_unavailability_of_generation_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None, 
            mRID = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        df = self._query_unavailability(
            country_code=country_code, start=start, end=end, doctype="A80",
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate, mRID=mRID)
        return df

    def query_unavailability_of_production_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
            mRID: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        df = self._query_unavailability(
            country_code=country_code, start=start, end=end, doctype="A77",
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate, mRID=mRID)
        return df

    @paginated
    def query_unavailability_transmission(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
            offset: int = 0,
            **kwargs) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional
        offset : int

        Returns
        -------
        pd.DataFrame
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        content = super(EntsoePandasClient,
                        self).query_unavailability_transmission(
            area_from, area_to, start, end, docstatus, periodstartupdate,
            periodendupdate, offset=offset)
        df = parse_unavailabilities(content, "A78")
        df = df.tz_convert(area_from.tz)
        df['start'] = df['start'].apply(lambda x: x.tz_convert(area_from.tz))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(area_from.tz))
        df = df[(df['start'] < end) | (df['end'] > start)]
        return df

    def query_withdrawn_unavailability_of_generation_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.DataFrame
        """
        df = self.query_unavailability_of_generation_units(
            country_code=country_code, start=start, end=end, docstatus='A13')
        df = df[(df['start'] < end) | (df['end'] > start)]
        return df

    @day_limited
    def query_generation_per_plant(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            include_eic: bool = False,
            eic_code: Optional[str] = None,
            **kwargs) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        include_eic: bool
            if True also include the eic code in the output
        eic_code : str
            filter on a single Generation Unit using its EIC Code

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_generation_per_plant(
            country_code=area, start=start, end=end, psr_type=psr_type,
            eic_code=eic_code,
        )
        df = parse_generation(text, per_plant=True, include_eic=include_eic)
        df.columns = df.columns.set_levels(df.columns.levels[0].str.encode('latin-1').str.decode('utf-8'), level=0)
        df = df.tz_convert(area.tz)
        # Truncation will fail if data is not sorted along the index in rare
        # cases. Ensure the dataframe is sorted:
        df = df.sort_index(axis=0)

        if df.columns.nlevels == 2:
            df = df.assign(newlevel='Actual Aggregated').set_index('newlevel', append=True).unstack('newlevel')
        df = df.truncate(before=start, after=end)
        return df

    def query_physical_crossborder_allborders(self, country_code: Union[Area, str], start: pd.Timestamp,
                     end: pd.Timestamp, export: bool, per_hour: bool = False) -> pd.DataFrame:
        """
        Adds together all physical cross-border flows to a country for a given direction
        The neighbours of a country are given by the NEIGHBOURS mapping

        if export is True then all export flows are returned, if False then all import flows are returned
        some borders have more then once data points per hour. Set per_hour=True if you always want hourly data,
        it will then thake the mean
        """
        area = lookup_area(country_code)
        imports = []
        for neighbour in NEIGHBOURS[area.name]:
            try:
                if export:
                    im = self.query_crossborder_flows(country_code_from=country_code,
                                                      country_code_to=neighbour,
                                                      end=end,
                                                      start=start,
                                                      lookup_bzones=True)
                else:
                    im = self.query_crossborder_flows(country_code_from=neighbour,
                                                      country_code_to=country_code,
                                                      end=end,
                                                      start=start,
                                                      lookup_bzones=True)
            except NoMatchingDataError:
                continue
            im.name = neighbour
            imports.append(im)
        df = pd.concat(imports, axis=1, sort=True)
        # drop columns that contain only zero's
        df = df.loc[:, (df != 0).any(axis=0)]
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        df['sum'] = df.sum(axis=1)
        if per_hour:
            df = df.resample('h').first()

        return df

    def query_import(self, country_code: Union[Area, str], start: pd.Timestamp,
                     end: pd.Timestamp) -> pd.DataFrame:
        """
        Utility function wrapper for query_sum_physical_crossborder for backwards compatibility reason
        """
        return self.query_physical_crossborder_allborders(country_code=country_code,
                                                   start=start,
                                                   end=end,
                                                   export=False)

    def query_generation_import(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        """Query the combination of both domestic generation and imports"""
        generation = self.query_generation(country_code=country_code, end=end,
                                           start=start, nett=True)
        generation = generation.loc[:, (generation != 0).any(
            axis=0)]  # drop columns that contain only zero's
        imports = self.query_import(country_code=country_code, start=start,
                                    end=end)

        data = {f'Generation': generation, f'Import': imports}
        df = pd.concat(data.values(), axis=1, keys=data.keys())
        df = df.ffill()
        df = df.truncate(before=start, after=end)
        return df
