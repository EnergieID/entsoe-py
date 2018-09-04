import pytz
import requests
from bs4 import BeautifulSoup
from time import sleep
from socket import gaierror

__title__ = "entsoe-py"
__version__ = "0.1.17"
__author__ = "EnergieID.be"
__license__ = "MIT"

URL = 'https://transparency.entsoe.eu/api'

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
    'IT-SICI': '10Y1001A1001A75E'
})

TIMEZONE_MAPPINGS = {
    'AL': 'Europe/Tirane',
    'AT': 'Europe/Vienna',
    'BA': 'Europe/Sarajevo',
    'BE': 'Europe/Brussels',
    'BG': 'Europe/Sofia',
    'BY': 'Europe/Minsk',
    'CH': 'Europe/Zurich',
    'CZ': 'Europe/Prague',
    'DE': 'Europe/Berlin',
    'DK': 'Europe/Copenhagen',
    'EE': 'Europe/Tallinn',
    'ES': 'Europe/Madrid',
    'FI': 'Europe/Helsinki',
    'FR': 'Europe/Paris',
    'GB': 'Europe/London',
    'GB-NIR': 'Europe/Belfast',
    'GR': 'Europe/Athens',
    'HR': 'Europe/Zagreb',
    'HU': 'Europe/Budapest',
    'IE': 'Europe/Dublin',
    'IT': 'Europe/Rome',
    'LT': 'Europe/Vilnius',
    'LU': 'Europe/Luxembourg',
    'LV': 'Europe/Riga',
    # 'MD': 'MD',
    'ME': 'Europe/Podgorica',
    'MK': 'Europe/Skopje',
    'MT': 'Europe/Malta',
    'NL': 'Europe/Amsterdam',
    'NO': 'Europe/Oslo',
    'PL': 'Europe/Warsaw',
    'PT': 'Europe/Lisbon',
    'RO': 'Europe/Bucharest',
    'RS': 'Europe/Belgrade',
    'RU': 'Europe/Moscow',
    'RU-KGD': 'Europe/Kaliningrad',
    'SE': 'Europe/Stockholm',
    'SI': 'Europe/Ljubljana',
    'SK': 'Europe/Bratislava',
    'TR': 'Europe/Istanbul',
    'UA': 'Europe/Kiev',
    'IT-NORD': 'Europe/Rome',
    'IT-CNOR': 'Europe/Rome',
    'IT-CSUD': 'Europe/Rome',
    'IT-SUD': 'Europe/Rome',
    'IT-FOGN': 'Europe/Rome',
    'IT-ROSN': 'Europe/Rome',
    'IT-BRNN': 'Europe/Rome',
    'IT-PRGP': 'Europe/Rome',
    'IT-SARD': 'Europe/Rome',
    'IT-SICI': 'Europe/Rome',
    'DE-AT-LU': 'Europe/Berlin'
}

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
                        raise error
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
