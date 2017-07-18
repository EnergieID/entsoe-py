import pytz
import requests
from bs4 import BeautifulSoup

__title__ = "entsoe-py"
__version__ = "0.1.0"
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
    'UA': '10YUA-WEPS-----0'
}


class Entsoe:
    """
    Attributions: Parts of the code for parsing Entsoe responses were copied
    from https://github.com/tmrowco/electricitymap
    """
    def __init__(self, api_key, session=None):
        """
        Parameters
        ----------
        api_key : str
        session : requests.Session
        """
        self.api_key = api_key
        if session is None:
            session = requests.Session()
        self.session = session

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

        response = self.session.get(url=URL, params=params)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.find_all('text')
            if len(text):
                error_text = soup.find('text').prettyfy()
                if 'No matching data found' in error_text:
                    return None
                else:
                    raise Exception('Failed to get data. Reason: %s' %
                                    error_text)
            else:
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
        domain = DOMAIN_MAPPINGS[country_code]
        params = {
            'documentType': 'A44',
            'in_Domain': domain,
            'out_Domain': domain
        }
        response = self.base_request(params=params, start=start, end=end)
        if not as_series:
            return response.text
        else:
            from entsoe.parsers import parse_prices
            series = parse_prices(response.text)
            return series
