import logging
from functools import wraps
from socket import gaierror
from time import sleep
from typing import Union, Optional, Dict

import pandas as pd
from pandas.tseries.offsets import YearBegin, YearEnd
import pytz
import requests
from bs4 import BeautifulSoup

from entsoe.exceptions import InvalidPSRTypeError, InvalidBusinessParameterError
from .exceptions import NoMatchingDataError, PaginationError
from .mappings import Area, NEIGHBOURS, lookup_area
from .misc import year_blocks, day_blocks
from .parsers import parse_prices, parse_loads, parse_generation, \
    parse_installed_capacity_per_plant, parse_crossborder_flows, \
    parse_unavailabilities, parse_contracted_reserve, parse_imbalance_prices_zip, \
    parse_netpositions, parse_procured_balancing_capacity

__title__ = "entsoe-py"
__version__ = "0.3.7"
__author__ = "EnergieID.be"
__license__ = "MIT"

URL = 'https://transparency.entsoe.eu/api'


def retry(func):
    """Catches connection errors, waits and retries"""

    @wraps(func)
    def retry_wrapper(*args, **kwargs):
        self = args[0]
        error = None
        for _ in range(self.retry_count):
            try:
                result = func(*args, **kwargs)
            except (requests.ConnectionError, gaierror) as e:
                error = e
                print("Connection Error, retrying in {} seconds".format(
                    self.retry_delay))
                sleep(self.retry_delay)
                continue
            else:
                return result
        else:
            raise error

    return retry_wrapper


class EntsoeRawClient:
    # noinspection LongLine
    """
        Client to perform API calls and return the raw responses API-documentation:
        https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_request_methods

        Attributions: Parts of the code for parsing Entsoe responses were copied
        from https://github.com/tmrowco/electricitymap
        """

    def __init__(
            self, api_key: str, session: Optional[requests.Session] = None,
            retry_count: int = 1, retry_delay: int = 0,
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
        if api_key is None:
            raise TypeError("API key cannot be None")
        self.api_key = api_key
        if session is None:
            session = requests.Session()
        self.session = session
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

        logging.debug(f'Performing request to {URL} with params {params}')
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
            raise e
        else:
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
        ret_str = dtm.strftime(fmt)
        return ret_str

    def query_day_ahead_prices(self, country_code: Union[Area, str],
                               start: pd.Timestamp, end: pd.Timestamp) -> str:
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
            'out_Domain': area.code
        }
        response = self._base_request(params=params, start=start, end=end)
        return response.text
    
    def query_net_position_dayahead(self, country_code: Union[Area, str],
                            start: pd.Timestamp, end: pd.Timestamp) -> str:
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
            'documentType': 'A25',  # Allocation result document
            'businessType': 'B09',  # net position
            'Contract_MarketAgreement.Type': 'A01',  # daily
            'in_Domain': area.code,
            'out_Domain': area.code
        }
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
            'documentType': 'A73',
            'processType': 'A16',
            'in_Domain': area.code,
        }
        if psr_type:
            params.update({'psrType': psr_type})
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


    def _query_crossborder(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, doctype: str,
            contract_marketagreement_type: Optional[str] = None,
            auction_type: Optional[str] = None) -> str:
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

        response = self._base_request(params=params, start=start, end=end)
        return response.text

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

    def query_procured_balancing_capacity(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str,
            type_marketagreement_type: Optional[str] = None) -> bytes:
        """
        Activated Balancing Energy [17.1.E]
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
        if process_type not in ['A51', 'A47']:
            raise ValueError('processType allowed values: A51, A47')

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
            psr_type: Optional[str] = None) -> str:
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

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A89',
            'controlArea_Domain': area.code,
            'type_MarketAgreement.Type': type_marketagreement_type,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def query_contracted_reserve_amount(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, type_marketagreement_type: str,
            psr_type: Optional[str] = None) -> str:
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

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'documentType': 'A81',
            'controlArea_Domain': area.code,
            'type_MarketAgreement.Type': type_marketagreement_type,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self._base_request(params=params, start=start, end=end)
        return response.text

    def _query_unavailability(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, doctype: str, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> bytes:
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

        Returns
        -------
        bytes
        """
        area = lookup_area(country_code)
        params = {
            'documentType': doctype,
            'biddingZone_domain': area.code
            # ,'businessType': 'A53 (unplanned) | A54 (planned)'
        }
        if docstatus:
            params['docStatus'] = docstatus
        if periodstartupdate and periodendupdate:
            params['periodStartUpdate'] = self._datetime_to_str(
                periodstartupdate)
            params['periodEndUpdate'] = self._datetime_to_str(periodendupdate)
        response = self._base_request(params=params, start=start, end=end)
        return response.content

    def query_unavailability_of_generation_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> bytes:
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
            country_code=country_code, start=start, end=end, doctype="A80",
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate)
        return content

    def query_unavailability_of_production_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> bytes:
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
            periodendupdate=periodendupdate)
        return content

    def query_unavailability_transmission(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None, **kwargs) -> bytes:
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

        Returns
        -------
        bytes
        """
        area_in = lookup_area(country_code_to)
        area_out = lookup_area(country_code_from)
        params = {
            'documentType': "A78",
            'in_Domain': area_in.code,
            'out_Domain': area_out.code
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
            end: pd.Timestamp) -> bytes:
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
            doctype="A80", docstatus='A13')
        return content
    


def paginated(func):
    """Catches a PaginationError, splits the requested period in two and tries
    again. Finally it concatenates the results"""

    @wraps(func)
    def pagination_wrapper(*args, start, end, **kwargs):
        try:
            df = func(*args, start=start, end=end, **kwargs)
        except PaginationError:
            pivot = start + (end - start) / 2
            df1 = pagination_wrapper(*args, start=start, end=pivot, **kwargs)
            df2 = pagination_wrapper(*args, start=pivot, end=end, **kwargs)
            df = pd.concat([df1, df2])
        return df

    return pagination_wrapper


def year_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def year_wrapper(*args, start, end, **kwargs):
        blocks = year_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                logging.debug(f"NoMatchingDataError: between {_start} and {_end}")
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames, sort=True)
        df = df.loc[~df.index.duplicated(keep='first')]
        return df

    return year_wrapper


def day_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def day_wrapper(*args, start, end, **kwargs):
        blocks = day_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                print(f"NoMatchingDataError: between {_start} and {_end}")
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames)
        return df

    return day_wrapper


class EntsoePandasClient(EntsoeRawClient):
    @year_limited
    def query_net_position_dayahead(self, country_code: Union[Area, str],
                            start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        """

        Parameters
        ----------
        country_code
        start
        end

        Returns
        -------

        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_net_position_dayahead(
            country_code=area, start=start, end=end)
        series = parse_netpositions(text)
        series = series.tz_convert(area.tz)
        series = series.truncate(before=start, after=end)
        return series

    @year_limited
    def query_day_ahead_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.Series:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_day_ahead_prices(
            country_code=area, start=start, end=end)
        series = parse_prices(text)
        series = series.tz_convert(area.tz)
        series = series.truncate(before=start, after=end)
        return series

    @year_limited
    def query_load(self, country_code: Union[Area, str], start: pd.Timestamp,
                   end: pd.Timestamp) -> pd.Series:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_load(
            country_code=area, start=start, end=end)
        series = parse_loads(text)
        series = series.tz_convert(area.tz)
        series = series.truncate(before=start, after=end)
        return series

    @year_limited
    def query_load_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str = 'A01') -> pd.Series:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str

        Returns
        -------
        pd.Series
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_load_forecast(
            country_code=area, start=start, end=end, process_type=process_type)
        series = parse_loads(text)
        series = series.tz_convert(area.tz)
        series = series.truncate(before=start, after=end)
        return series

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
    def query_imbalance_prices(
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
        archive = super(EntsoePandasClient, self).query_imbalance_prices(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_imbalance_prices_zip(zip_contents=archive)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    def query_procured_balancing_capacity(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str,
            type_marketagreement_type: Optional[str] = None) -> bytes:
        """
        Activated Balancing Energy [17.1.E]
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
    def query_contracted_reserve_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, type_marketagreement_type: str,
            psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area, str
        start : pd.Timestamp
        end : pd.Timestamp
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_contracted_reserve_prices(
            country_code=area, start=start, end=end,
            type_marketagreement_type=type_marketagreement_type,
            psr_type=psr_type)
        df = parse_contracted_reserve(text, area.tz, "procurement_price.amount")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    def query_contracted_reserve_amount(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, type_marketagreement_type: str,
            psr_type: Optional[str] = None) -> pd.DataFrame:
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

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_contracted_reserve_amount(
            country_code=area, start=start, end=end,
            type_marketagreement_type=type_marketagreement_type,
            psr_type=psr_type)
        df = parse_contracted_reserve(text, area.tz, "quantity")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    def _query_unavailability(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, doctype: str, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> pd.DataFrame:
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

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        content = super(EntsoePandasClient, self)._query_unavailability(
            country_code=area, start=start, end=end, doctype=doctype,
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate)
        df = parse_unavailabilities(content, doctype)
        df = df.tz_convert(area.tz)
        df['start'] = df['start'].apply(lambda x: x.tz_convert(area.tz))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(area.tz))
        df = df.truncate(before=start, after=end)
        return df

    def query_unavailability_of_generation_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> pd.DataFrame:
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
            periodendupdate=periodendupdate)
        return df

    def query_unavailability_of_production_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> pd.DataFrame:
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
            periodendupdate=periodendupdate)
        return df

    @paginated
    def query_unavailability_transmission(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
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

        Returns
        -------
        pd.DataFrame
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        content = super(EntsoePandasClient,
                        self).query_unavailability_transmission(
            area_from, area_to, start, end, docstatus, periodstartupdate,
            periodendupdate)
        df = parse_unavailabilities(content, "A78")
        df = df.tz_convert(area_from.tz)
        df['start'] = df['start'].apply(lambda x: x.tz_convert(area_from.tz))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(area_from.tz))
        df = df.truncate(before=start, after=end)
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
        df = df.truncate(before=start, after=end)
        return df

    @day_limited
    def query_generation_per_plant(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            include_eic: bool = False,
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
        include_eic: bool
            if True also include the eic code in the output

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsoePandasClient, self).query_generation_per_plant(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_generation(text, per_plant=True, include_eic=include_eic)
        df.columns = df.columns.set_levels(df.columns.levels[0].str.encode('latin-1').str.decode('utf-8'), level=0)
        df = df.tz_convert(area.tz)
        # Truncation will fail if data is not sorted along the index in rare
        # cases. Ensure the dataframe is sorted:
        df = df.sort_index(0)
        df = df.truncate(before=start, after=end)
        return df

    def query_import(self, country_code: Union[Area, str], start: pd.Timestamp,
                     end: pd.Timestamp) -> pd.DataFrame:
        """
        Adds together all incoming cross-border flows to a country
        The neighbours of a country are given by the NEIGHBOURS mapping
        """
        area = lookup_area(country_code)
        imports = []
        for neighbour in NEIGHBOURS[area.name]:
            try:
                im = self.query_crossborder_flows(country_code_from=neighbour,
                                                  country_code_to=country_code,
                                                  end=end,
                                                  start=start,
                                                  lookup_bzones=True)
            except NoMatchingDataError:
                continue
            im.name = neighbour
            imports.append(im)
        df = pd.concat(imports, axis=1)
        # drop columns that contain only zero's
        df = df.loc[:, (df != 0).any(axis=0)]
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    def query_generation_import(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        """Query the combination of both domestic generation and imports"""
        generation = self.query_generation(country_code=country_code, end=end,
                                           start=start, lookup_bzones=True)
        generation = generation.loc[:, (generation != 0).any(
            axis=0)]  # drop columns that contain only zero's
        generation = generation.resample('H').sum()
        imports = self.query_import(country_code=country_code, start=start,
                                    end=end)

        data = {f'Generation': generation, f'Import': imports}
        df = pd.concat(data.values(), axis=1, keys=data.keys())
        df = df.truncate(before=start, after=end)
        return df

