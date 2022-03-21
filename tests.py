import unittest

import pandas as pd
from bs4 import BeautifulSoup

from entsoe import EntsoeRawClient, EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from settings import *


class EntsoeRawClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = EntsoeRawClient(api_key=api_key)
        cls.start = pd.Timestamp('20180101', tz='Europe/Brussels')
        cls.end = pd.Timestamp('20180107', tz='Europe/Brussels')
        cls.country_code = 'BE'

    def test_datetime_to_str(self):
        start_str = self.client._datetime_to_str(dtm=self.start)
        self.assertIsInstance(start_str, str)
        self.assertEqual(start_str, '201712312300')

    def test_basic_queries(self):
        queries = [
            self.client.query_day_ahead_prices,
            self.client.query_load,
            self.client.query_wind_and_solar_forecast,
            self.client.query_load_forecast,
            self.client.query_generation,
            self.client.query_generation_forecast,
            self.client.query_installed_generation_capacity,
            # these give back a zip so disabled for testing right now
            #self.client.query_imbalance_prices,
            #self.client.query_imbalance_volumes,
            self.client.query_net_position
        ]
        for query in queries:
            text = query(country_code=self.country_code, start=self.start,
                         end=self.end)
            self.assertIsInstance(text, str)
            try:
                BeautifulSoup(text, 'html.parser')
            except Exception as e:
                self.fail(f'Parsing of response failed with exception: {e}')

    def query_crossborder_flows(self):
        text = self.client.query_crossborder_flows(
            country_code_from='BE', country_code_to='NL', start=self.start,
            end=self.end)
        self.assertIsInstance(text, str)
        try:
            BeautifulSoup(text, 'html.parser')
        except Exception as e:
            self.fail(f'Parsing of response failed with exception: {e}')

    def test_query_unavailability_of_generation_units(self):
        text = self.client.query_unavailability_of_generation_units(
            country_code='BE', start=self.start,
            end=self.end)
        self.assertIsInstance(text, bytes)

    def test_query_withdrawn_unavailability_of_generation_units(self):
        with self.assertRaises(NoMatchingDataError):
            self.client.query_withdrawn_unavailability_of_generation_units(
                country_code='BE', start=self.start, end=self.end)

    def test_query_procured_balancing_capacity(self):
        text = self.client.query_procured_balancing_capacity(
            country_code='CZ',
            start=pd.Timestamp('20210101', tz='Europe/Prague'),
            end=pd.Timestamp('20210102', tz='Europe/Prague'),
            process_type='A51'
        )
        self.assertIsInstance(text, bytes)
        try:
            BeautifulSoup(text, 'html.parser')
        except Exception as e:
            self.fail(f'Parsing of response failed with exception: {e}')


class EntsoePandasClientTest(EntsoeRawClientTest):
    @classmethod
    def setUpClass(cls):
        cls.client = EntsoePandasClient(api_key=api_key)
        cls.start = pd.Timestamp('20180101', tz='Europe/Brussels')
        cls.end = pd.Timestamp('20180107', tz='Europe/Brussels')
        cls.country_code = 'BE'

    def test_basic_queries(self):
        pass

    def test_basic_series(self):
        queries = [
            self.client.query_day_ahead_prices,
            self.client.query_generation_forecast,
            self.client.query_net_position
        ]
        for query in queries:
            ts = query(country_code=self.country_code, start=self.start,
                       end=self.end)
            self.assertIsInstance(ts, pd.Series)

    def query_crossborder_flows(self):
        ts = self.client.query_crossborder_flows(
            country_code_from='BE', country_code_to='NL', start=self.start,
            end=self.end)
        self.assertIsInstance(ts, pd.Series)

    def test_basic_dataframes(self):
        queries = [
            self.client.query_load,
            self.client.query_load_forecast,
            self.client.query_wind_and_solar_forecast,
            self.client.query_generation,
            self.client.query_installed_generation_capacity,
            self.client.query_imbalance_prices,
            self.client.query_imbalance_volumes,
            self.client.query_unavailability_of_generation_units
        ]
        for query in queries:
            ts = query(country_code=self.country_code, start=self.start,
                       end=self.end)
            self.assertIsInstance(ts, pd.DataFrame)

    def test_query_unavailability_of_generation_units(self):
        pass

    def test_query_procured_balancing_capacity(self):
        ts = self.client.query_procured_balancing_capacity(
            country_code='CZ',
            start=pd.Timestamp('20210101', tz='Europe/Prague'),
            end=pd.Timestamp('20210102', tz='Europe/Prague'),
            process_type='A51'
        )
        self.assertIsInstance(ts, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
