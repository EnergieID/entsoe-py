from functools import wraps
from socket import gaierror
from time import sleep

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup

from .exceptions import NoMatchingDataError, PaginationError
from .mappings import DOMAIN_MAPPINGS, BIDDING_ZONES, TIMEZONE_MAPPINGS, NEIGHBOURS
from .misc import year_blocks
from .parsers import parse_prices, parse_loads, parse_generation, parse_generation_per_plant, \
    parse_crossborder_flows, parse_imbalance_prices, parse_unavailabilities

__title__ = "entsoe-py"
__version__ = "0.2.4"
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
                print("Connection Error, retrying in {} seconds".format(self.retry_delay))
                sleep(self.retry_delay)
                continue
            else:
                return result
        else:
            raise error
    return retry_wrapper


class EntsoeRawClient:
    """
    Client to perform API calls and return the raw responses
    API-documentation: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_request_methods

    Attributions: Parts of the code for parsing Entsoe responses were copied
    from https://github.com/tmrowco/electricitymap
    """

    def __init__(self, api_key, session=None, retry_count=1, retry_delay=0,
                 proxies=None):
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

    @retry
    def base_request(self, params, start, end):
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

        response = self.session.get(url=URL, params=params,
                                    proxies=self.proxies)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.find_all('text')
            if len(text):
                error_text = soup.find('text').text
                if 'No matching data found' in error_text:
                    raise NoMatchingDataError
                elif 'amount of requested data exceeds allowed limit' in error_text:
                    requested = error_text.split(' ')[-2]
                    raise PaginationError(
                        f"The API is limited to 200 elements per request. This query requested for {requested} documents and cannot be fulfilled as is.")
            raise e
        else:
            return response


    @staticmethod
    def _datetime_to_str(dtm):
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

    def query_day_ahead_prices(self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': 'A44',
            'in_Domain': domain,
            'out_Domain': domain
        }
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_load(self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': 'A65',
            'processType': 'A16',
            'outBiddingZone_Domain': domain,
            'out_Domain': domain
        }
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_load_forecast(self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        Returns
        -------
        str
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': 'A65',
            'processType': 'A01',
            'outBiddingZone_Domain': domain,
            # 'out_Domain': domain
        }
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_generation_forecast(self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': 'A71',
            'processType': 'A01',
            'in_Domain': domain,
        }
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_wind_and_solar_forecast(self, country_code, start, end, psr_type=None, lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        str
        """
        if not lookup_bzones:
            domain = DOMAIN_MAPPINGS[country_code]
        else:
            domain = BIDDING_ZONES[country_code]

        params = {
            'documentType': 'A69',
            'processType': 'A01',
            'in_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})

        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_generation(self, country_code, start, end, psr_type=None, lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        str
        """
        if not lookup_bzones:
            domain = DOMAIN_MAPPINGS[country_code]
        else:
            domain = BIDDING_ZONES[country_code]

        params = {
            'documentType': 'A75',
            'processType': 'A16',
            'in_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})

        response = self.base_request(params=params, start=start, end=end)
        return response.text
    
    def query_generation_per_plant(self, country_code, start, end, psr_type=None, lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        str
        """
        if not lookup_bzones:
            domain = DOMAIN_MAPPINGS[country_code]
        else:
            domain = BIDDING_ZONES[country_code]

        params = {
            'documentType': 'A73',
            'processType': 'A16',
            'in_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})

        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_installed_generation_capacity(self, country_code, start, end, psr_type=None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        str
        """
        domain = DOMAIN_MAPPINGS[country_code]
        params = {
            'documentType': 'A68',
            'processType': 'A33',
            'in_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})

        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_crossborder_flows(self, country_code_from, country_code_to, start, end, lookup_bzones=False):
        """
        Parameters
        ----------
        country_code_from : str
        country_code_to : str
        start : pd.Timestamp
        end : pd.Timestamp
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        str
        """
        if not lookup_bzones:
            domain_in = DOMAIN_MAPPINGS[country_code_to]
            domain_out = DOMAIN_MAPPINGS[country_code_from]
        else:
            domain_in = BIDDING_ZONES[country_code_to]
            domain_out = BIDDING_ZONES[country_code_from]

        params = {
            'documentType': 'A11',
            'in_Domain': domain_in,
            'out_Domain': domain_out
        }
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_imbalance_prices(self, country_code, start, end, psr_type=None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        str
        """
        domain = DOMAIN_MAPPINGS[country_code]
        params = {
            'documentType': 'A85',
            'controlArea_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_unavailability(self, country_code, start, end,
                            doctype, docstatus=None, periodstartupdate = None,
                            periodendupdate = None) -> bytes:
        """
        Generic unavailibility query method.
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.

        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        doctype : str
        docstatus : str, optional
        periodStartUpdate : pd.Timestamp, optional
        periodEndUpdate : pd.Timestamp, optional

        Returns
        -------
        bytes
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': doctype,
            'biddingZone_domain': domain
            # ,'businessType': 'A53 (unplanned) | A54 (planned)'
        }

        if docstatus:
            params['docStatus'] = docstatus
        if periodstartupdate and periodendupdate:
            params['periodStartUpdate'] = self._datetime_to_str(periodstartupdate)
            params['periodEndUpdate'] = self._datetime_to_str(periodendupdate)

        response = self.base_request(params=params, start=start, end=end)

        return response.content

    def query_unavailability_of_generation_units(self, country_code, start, end,
                                     docstatus=None, periodstartupdate = None,
                                     periodendupdate = None) -> bytes:
        """
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.

        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodStartUpdate : pd.Timestamp, optional
        periodEndUpdate : pd.Timestamp, optional

        Returns
        -------
        bytes
        """
        content = self.query_unavailability(
            country_code=country_code, start=start, end=end,
            doctype="A77", docstatus=docstatus,
            periodstartupdate = periodstartupdate,
            periodendupdate = periodendupdate)
        return content

    def query_unavailability_of_production_units(self, country_code, start, end,
                                     docstatus=None, periodstartupdate = None,
                                     periodendupdate = None) -> bytes:
        """
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.

        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodStartUpdate : pd.Timestamp, optional
        periodEndUpdate : pd.Timestamp, optional

        Returns
        -------
        bytes
        """
        content = self.query_unavailability(
            country_code=country_code, start=start, end=end,
            doctype="A80", docstatus=docstatus,
            periodstartupdate = periodstartupdate,
            periodendupdate = periodendupdate)
        return content

    def query_withdrawn_unavailability_of_generation_units(
            self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        """
        content = self.query_unavailability(
            country_code=country_code, start=start, end=end,
            doctype="A77", docstatus='A13')
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
        frames = [func(*args, start=_start, end=_end, **kwargs) for _start, _end
                  in blocks]
        df = pd.concat(frames)
        return df

    return year_wrapper


class EntsoePandasClient(EntsoeRawClient):
    @year_limited
    def query_day_ahead_prices(self, country_code, start, end) -> pd.Series:
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        text = super(EntsoePandasClient, self).query_day_ahead_prices(
            country_code=country_code, start=start, end=end)
        series = parse_prices(text)
        series = series.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return series

    @year_limited
    def query_load(self, country_code, start, end) -> pd.Series:
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        text = super(EntsoePandasClient, self).query_load(
            country_code=country_code, start=start, end=end)
        series = parse_loads(text)
        series = series.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return series

    @year_limited
    def query_load_forecast(self, country_code, start, end) -> pd.Series:
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        Returns
        -------
        pd.Series
        """
        text = super(EntsoePandasClient, self).query_load_forecast(
            country_code=country_code, start=start, end=end)
        series = parse_loads(text)
        series = series.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return series

    @year_limited
    def query_generation_forecast(self, country_code, start, end) -> pd.Series:
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        Returns
        -------
        pd.Series
        """
        text = super(EntsoePandasClient, self).query_generation_forecast(
            country_code=country_code, start=start, end=end)
        series = parse_loads(text)
        series = series.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return series

    @year_limited
    def query_wind_and_solar_forecast(self, country_code, start, end, psr_type=None,
                                      lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        pd.DataFrame
        """
        text = super(EntsoePandasClient, self).query_wind_and_solar_forecast(
            country_code=country_code, start=start, end=end, psr_type=psr_type,
            lookup_bzones=lookup_bzones)
        df = parse_generation(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    @year_limited
    def query_generation(self, country_code, start, end, psr_type=None,
                         lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        pd.DataFrame
        """
        text = super(EntsoePandasClient, self).query_generation(
            country_code=country_code, start=start, end=end, psr_type=psr_type,
            lookup_bzones=lookup_bzones)
        df = parse_generation(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    @year_limited
    def query_installed_generation_capacity(self, country_code, start, end,
                                            psr_type=None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        text = super(
            EntsoePandasClient, self).query_installed_generation_capacity(
            country_code=country_code, start=start, end=end, psr_type=psr_type)
        df = parse_generation(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    @year_limited
    def query_crossborder_flows(self, country_code_from, country_code_to, start, end, lookup_bzones=False):
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : str
        country_code_to : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        text = super(EntsoePandasClient, self).query_crossborder_flows(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end, lookup_bzones=lookup_bzones)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(TIMEZONE_MAPPINGS[country_code_from])
        return ts

    @year_limited
    def query_imbalance_prices(self, country_code, start, end, psr_type=None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        text = super(EntsoePandasClient, self).query_imbalance_prices(
            country_code=country_code, start=start, end=end, psr_type=psr_type)
        df = parse_imbalance_prices(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    @year_limited
    @paginated
    def query_unavailability(self, country_code, start, end, doctype,
                                     docstatus=None, periodstartupdate = None,
                                     periodendupdate = None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        doctype : str
        docstatus : str, optional
        periodStartUpdate : pd.Timestamp, optional
        periodEndUpdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        content = super(EntsoePandasClient,
                        self).query_unavailability(
            country_code=country_code, start=start, end=end, doctype = doctype,
            docstatus=docstatus,  periodstartupdate = periodstartupdate,
            periodendupdate = periodendupdate)
        df = parse_unavailabilities(content)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        df['start'] = df['start'].apply(lambda x: x.tz_convert(TIMEZONE_MAPPINGS[country_code]))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(TIMEZONE_MAPPINGS[country_code]))
        return df


    @year_limited
    @paginated
    def query_unavailability_of_generation_units(self, country_code, start, end,
                                     docstatus=None, periodstartupdate = None,
                                     periodendupdate = None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodStartUpdate : pd.Timestamp, optional
        periodEndUpdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        content = super(EntsoePandasClient,
                        self).query_unavailability_of_generation_units(
            country_code=country_code, start=start, end=end,
            docstatus=docstatus,  periodstartupdate = periodstartupdate,
            periodendupdate = periodendupdate)
        df = parse_unavailabilities(content)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        df['start'] = df['start'].apply(lambda x: x.tz_convert(TIMEZONE_MAPPINGS[country_code]))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(TIMEZONE_MAPPINGS[country_code]))
        return df

    @year_limited
    @paginated
    def query_unavailability_of_production_units(self, country_code, start, end,
                                     docstatus=None, periodstartupdate = None,
                                     periodendupdate = None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodStartUpdate : pd.Timestamp, optional
        periodEndUpdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        content = super(EntsoePandasClient,
                        self).query_unavailability_of_production_units(
            country_code=country_code, start=start, end=end,
            docstatus=docstatus, periodstartupdate = periodstartupdate,
            periodendupdate = periodendupdate)
        df = parse_unavailabilities(content)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        df['start'] = df['start'].apply(lambda x: x.tz_convert(TIMEZONE_MAPPINGS[country_code]))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(TIMEZONE_MAPPINGS[country_code]))
        return df

    def query_withdrawn_unavailability_of_generation_units(
            self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.DataFrame
        """
        df = self.query_unavailability_of_generation_units(
            country_code=country_code, start=start, end=end, docstatus='A13')
        return df

    @year_limited
    def query_generation_per_plant(self, country_code, start, end, psr_type=None,lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        pd.DataFrame
        """
        text = super(EntsoePandasClient, self).query_generation_per_plant(
            country_code=country_code, start=start, end=end, psr_type=psr_type,
            lookup_bzones=lookup_bzones)
        df = parse_generation_per_plant(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    def query_import(self, country_code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        Adds together all incoming cross-border flows to a country
        The neighbours of a country are given by the NEIGHBOURS mapping
        """
        imports = []
        for neighbour in NEIGHBOURS[country_code]:
            try:
                im = self.query_crossborder_flows(country_code_from=neighbour, country_code_to=country_code, end=end,
                                                  start=start, lookup_bzones=True)
            except NoMatchingDataError:
                continue
            im.name = neighbour
            imports.append(im)
        df = pd.concat(imports, axis=1)
        df = df.loc[:, (df != 0).any(axis=0)]  # drop columns that contain only zero's
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    def query_generation_import(self, country_code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """Query the combination of both domestic generation and imports"""
        generation = self.query_generation(country_code=country_code, end=end, start=start, lookup_bzones=True)
        generation = generation.loc[:, (generation != 0).any(axis=0)]  # drop columns that contain only zero's
        generation = generation.resample('H').sum()
        imports = self.query_import(country_code=country_code, start=start, end=end)

        data = {f'Generation': generation, f'Import': imports}
        df = pd.concat(data.values(), axis=1, keys=data.keys())
        return df
