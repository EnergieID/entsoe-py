import pytz
import requests
from bs4 import BeautifulSoup
from time import sleep
from socket import gaierror
import pandas as pd
from .mappings import DOMAIN_MAPPINGS, BIDDING_ZONES, TIMEZONE_MAPPINGS

__title__ = "entsoe-py"
__version__ = "0.1.18"
__author__ = "EnergieID.be"
__license__ = "MIT"

URL = 'https://transparency.entsoe.eu/api'


class PaginationError(Exception):
    pass


class NoMatchingDataError(Exception):
    pass


class Entsoe:
    """
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

        error = None
        for _ in range(self.retry_count):
            try:
                response = self.session.get(url=URL, params=params,
                                            proxies=self.proxies)
            except (requests.ConnectionError, gaierror) as e:
                error = e
                print("Connection Error, retrying in {} seconds".format(self.retry_delay))
                sleep(self.retry_delay)
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as e:
                error = e
                soup = BeautifulSoup(response.text, 'html.parser')
                text = soup.find_all('text')
                if len(text):
                    error_text = soup.find('text').text
                    if 'No matching data found' in error_text:
                        print(f"No matching data found.")
                        return
                    if 'amount of requested data exceeds allowed limit' in error_text:
                        requested = error_text.split(' ')[-2]
                        print(
                            f"The API is limited to 200 elements per request. This query requested for {requested} documents and cannot be fulfilled as is.")
                        raise PaginationError
                print("HTTP Error, retrying in {} seconds".format(self.retry_delay))
                sleep(self.retry_delay)
            else:
                return response
        else:
            raise error

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

    def query_price(self, country_code, start, end, as_series=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        as_series : bool
            Default False
            If True: Return the response as a Pandas Series
            If False: Return the response as raw XML

        Returns
        -------
        str | pd.Series
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': 'A44',
            'in_Domain': domain,
            'out_Domain': domain
        }
        response = self.base_request(params=params, start=start, end=end)
        if response is None:
            return None
        if not as_series:
            return response.text
        else:
            from . import parsers
            series = parsers.parse_prices(response.text)
            series = series.tz_convert(TIMEZONE_MAPPINGS[country_code])
            return series

    def query_price_series(self, country_code, start, end):
        """
        Query Day Ahead prices as Pandas Series

        This method has the added benefit that you can query over periods larger than one year

        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        from .misc import year_blocks
        import pandas as pd

        series = (self.query_price(country_code=country_code, start=_start, end=_end, as_series=True) for _start, _end in year_blocks(start, end))
        ts = pd.concat(series)
        return ts

    def query_load(self, country_code, start, end, as_series=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        as_series : bool
            Default False
            If True: Return the response as a Pandas Series
            If False: Return the response as raw XML

        Returns
        -------
        str | pd.Series
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': 'A65',
            'processType': 'A16',
            'outBiddingZone_Domain': domain,
            'out_Domain': domain
        }
        response = self.base_request(params=params, start=start, end=end)
        if response is None:
            return None
        if not as_series:
            return response.text
        else:
            from . import parsers
            series = parsers.parse_loads(response.text)
            series = series.tz_convert(TIMEZONE_MAPPINGS[country_code])
            return series

    def query_generation_forecast(self, country_code, start, end, as_dataframe=False, psr_type=None, squeeze=False, lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        as_dataframe : bool
            Default False
            If True: Return the response as a Pandas DataFrame
            If False: Return the response as raw XML
        psr_type : str
            filter on a single psr type
        squeeze : bool
            If a single column is requested, return it as a Series instead of a DataFrame
            If there is just a single value, return it as a float
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        str | pd.DataFrame
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
        if response is None:
            return None
        if not as_dataframe:
            return response.text
        else:
            from . import parsers
            df = parsers.parse_generation(response.text)
            df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
            if squeeze:
                df = df.squeeze()
            return df

    def query_generation(self, country_code, start, end, as_dataframe=False, psr_type=None, squeeze=False, lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        as_dataframe : bool
            Default False
            If True: Return the response as a Pandas DataFrame
            If False: Return the response as raw XML
        psr_type : str
            filter on a single psr type
        squeeze : bool
            If a single column is requested, return it as a Series instead of a DataFrame
            If there is just a single value, return it as a float
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        str | pd.DataFrame
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
        if response is None:
            return None
        if not as_dataframe:
            return response.text
        else:
            from . import parsers
            df = parsers.parse_generation(response.text)
            df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
            if squeeze:
                df = df.squeeze()
            return df

    def query_installed_generation_capacity(self, country_code, start, end, as_dataframe=False, psr_type=None, squeeze=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        as_dataframe : bool
            Default False
            If True: Return the response as a Pandas DataFrame
            If False: Return the response as raw XML
        psr_type : str
            filter query for a specific psr type
        squeeze : bool
            If a single column is requested, return it as a Series instead of a DataFrame
            If there is just a single value, return it as a float

        Returns
        -------
        str | pd.DataFrame
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
        if response is None:
            return None
        if not as_dataframe:
            return response.text
        else:
            from . import parsers
            df = parsers.parse_generation(response.text)
            df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
            if squeeze:
                df = df.squeeze()
            return df
        
    def query_crossborder_flows(self, country_code_from, country_code_to, start, end, as_series=False):
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : str
        country_code_to : str
        start : pd.Timestamp
        end : pd.Timestamp
        as_series : bool
            Default False
            If True: Return the response as a Pandas Series
            If False: Return the response as raw XML

        Returns
        -------
        str | pd.DataFrame
        """
        domain_in = DOMAIN_MAPPINGS[country_code_to]
        domain_out = DOMAIN_MAPPINGS[country_code_from]
        params = {
            'documentType': 'A11',
            'in_Domain': domain_in,
            'out_Domain': domain_out
        }
        response = self.base_request(params=params, start=start, end=end)
        if response is None:
            return None
        if not as_series:
            return response.text
        else:
            from . import parsers
            ts = parsers.parse_crossborder_flows(response.text)
            ts = ts.tz_convert(TIMEZONE_MAPPINGS[country_code_from])
            return ts

    def query_imbalance_prices(self, country_code, start, end, as_dataframe=False, psr_type=None):
        """

        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        as_dataframe : bool

        Returns
        -------
        str \
        """
        domain = DOMAIN_MAPPINGS[country_code]
        params = {
            'documentType': 'A85',
            'controlArea_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self.base_request(params=params, start=start, end=end)
        if response is None:
            return None
        if not as_dataframe:
            return response.text
        else:
            from . import parsers
            df = parsers.parse_imbalance_prices(response.text)
            df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
            return df

    def query_unavailability_of_production_units(self, country_code: str, start: pd.Timestamp, end: pd.Timestamp, docstatus=None) -> pd.DataFrame:
        """
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.
        """
        domain = DOMAIN_MAPPINGS[country_code]
        params = {
            'documentType': 'A77',
            'biddingZone_domain': domain
            #,'businessType': 'A53 (unplanned) | A54 (planned)'
        }

        withdrawn = None
        if docstatus:
            params['docStatus'] = docstatus
        else:
            withdrawn = self.query_unavailability_of_production_units(
                country_code=country_code, docstatus='A13', start=start, end=end)  # withdrawn unavailabilities

        try:
            response = self.base_request(params=params, start=start, end=end)
        except PaginationError:
            print("Too many elements requested, going to split the interval in half.")
            pivot = start + (end - start) / 2
            return pd.concat([self.query_unavailability_of_production_units(country_code=country_code, docstatus=docstatus, start=start, end=pivot), self.query_unavailability_of_production_units(country_code=country_code, docstatus=docstatus, start=pivot, end=end)])
        if response is None:
            return pd.DataFrame()
        else:
            from . import parsers
            df = parsers.parse_unavailabilities(response.content)
            return pd.concat([df, withdrawn])
