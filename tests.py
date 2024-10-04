import unittest

import numpy as np
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

    def test_year_limited_truncation(self):
        """
        This is a specific example of polish operator correcting the data
        i.e. there was an additional monthly auction for this period.
        This results in duplicated time indices.

        source: https://www.pse.pl/web/pse-eng/cross-border-electricity-exchange/auction-office/rzeszow-chmielnicka-interconnection/auction-results # noqa
        """
        start = pd.Timestamp('2023-07-17 00:00:00', tz='Europe/Warsaw')
        end = pd.Timestamp('2023-08-01 00:00:00', tz='Europe/Warsaw')
        ts = self.client.query_offered_capacity(
            'UA_IPS', 'PL',
            start=start, end=end,
            contract_marketagreement_type='A03',
            implicit=False
        )
        total_hours = int((end - start).total_seconds()/60/60)
        # Expected behaviour is to keep both initial data and corrections
        # and leave the deduplication to the user.
        self.assertEqual(total_hours*2, ts.shape[0])

    def test_documents_limited_truncation(self):
        ts = pd.DatetimeIndex(
            ["2022-03-01", "2022-03-11", "2022-03-21", "2022-04-01"],
            tz="Europe/Berlin"
        )
        part_dfs = []
        for i in range(len(ts) - 1):
            df = self.client.query_contracted_reserve_prices(
                'DE_LU', start=ts[i], end=ts[i+1],
                type_marketagreement_type='A01'
            )
            part_dfs.append(df)
        df_parts = pd.concat(part_dfs)
        df_full = self.client.query_contracted_reserve_prices(
            'DE_LU', start=ts[0], end=ts[-1],
            type_marketagreement_type='A01'
        )
        self.assertEqual(df_parts.shape, df_full.shape)
        self.assertTrue(all(df_parts.isna().sum() == df_full.isna().sum()))

    def test_query_contracted_reserve_prices(self):
        df = self.client.query_contracted_reserve_prices(
            country_code='NO_2', 
            type_marketagreement_type='A01',
            start=self.start,
            end=self.end)
        self.assertIsInstance(df, pd.DataFrame)

    def test_query_contracted_reserve_prices_no_available_prices(self):
        with self.assertRaises(NoMatchingDataError):
            self.client.query_contracted_reserve_prices(
                country_code='NO_2', 
                type_marketagreement_type='A01',
                start=pd.Timestamp('20240120', tz='Europe/Oslo'),
                end=pd.Timestamp('20240121', tz='Europe/Oslo'))

    def test_query_contracted_reserve_prices_procured_capacity_afrr(self):
        df = self.client.query_contracted_reserve_prices_procured_capacity(
            country_code='NO_2', 
            process_type='A51',
            type_marketagreement_type='A01',
            start=pd.Timestamp('20240120', tz='Europe/Oslo'),
            end=pd.Timestamp('20240121', tz='Europe/Oslo'))
        self.assertIsInstance(df, pd.DataFrame)

    def test_query_contracted_reserve_prices_procured_capacity_afrr_no_available_prices(self):
        with self.assertRaises(NoMatchingDataError):
            self.client.query_contracted_reserve_prices_procured_capacity(
                country_code='NO_2', 
                process_type='A51',
                type_marketagreement_type='A01',
                start=pd.Timestamp('20240101', tz='Europe/Oslo'),
                end=pd.Timestamp('20240118', tz='Europe/Oslo'))

    def test_da_prices(self):
        self.expected_data = [
            74.18, 72.52, 73.36, 74.20, 72.33, 77.10, 103.00, 127.79,
            141.37, 116.57, 100.11, 76.39, 68.61, 59.30, 50.00, 49.10,
            57.88, 77.42, 100.20, 137.24, 123.70, 99.90, 99.90, 90.00
        ]

        start = pd.Timestamp(year=2024, month=10, day=4).tz_localize("Europe/Paris")
        end = start + pd.DateOffset(hours=23)

        self.expected_index = pd.date_range(start=start, end=end, freq="h")
        self.expected_series = pd.Series(data=self.expected_data, index=self.expected_index)


        actual_series = self.client.query_day_ahead_prices(country_code="FR", start=start,
                                           end=end)

        print(actual_series)

        pd.testing.assert_series_equal(actual_series, self.expected_series)


    def test_when_starting_value_missing(self):

        with self.assertRaises(AssertionError):

            # Need to think about how to handle missing data. I'm pretty confident this first 40 is actually missing
            # and has been forward filled. In my opinion, we should replace it with a NaN, rather than forward filling.

            # The reason it hasn't in this case is because there is a buffer in the internal query, so the query isn't
            # really "starting" on the missing value.

            self.expected_data = [
                np.nan, 17.80, 8.00, 32.57, 57.17, 77.63, 86.74, 74.61,
                63.96, 52.65, 40.00, 31.67, 24.47, 15.10, 14.00, 47.88,
                74.61, 125.98, 122.31, 101.33, 91.48, 84.02
            ]

            # The data from 2am Paris time is missing and you can see that it has been forward filled on entsoe. I'm
            # checking that this doesn't fall over.

            start = pd.Timestamp(year=2024, month=10, day=3, hour=2).tz_localize("Europe/Paris")
            end = pd.Timestamp(year=2024, month=10, day=3, hour=23).tz_localize("Europe/Paris")

            self.expected_index = pd.date_range(start=start, end=end, freq="h")
            self.expected_series = pd.Series(data=self.expected_data, index=self.expected_index)

            actual_series = self.client.query_day_ahead_prices(country_code="FR", start=start, end=end)

            pd.testing.assert_series_equal(actual_series, self.expected_series)


if __name__ == '__main__':
    unittest.main()
