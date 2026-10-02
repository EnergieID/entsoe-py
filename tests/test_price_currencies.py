import pandas as pd
import pytest

from entsoe import EntsoePandasClient
from entsoe.entsoe import EntsoeRawClient
from entsoe.exceptions import NoMatchingDataError
from entsoe.parsers import parse_prices_with_currencies


def price_timeseries(start, end, resolution, prices, currency='EUR',
                     curve_type='A01'):
    currency_element = '' if currency is None else (
        f'<currency_Unit.name>{currency}</currency_Unit.name>'
    )
    points = ''.join(
        f'<Point><position>{position}</position>'
        f'<price.amount>{price}</price.amount></Point>'
        for position, price in prices
    )
    return f'''
        <TimeSeries>
          {currency_element}
          <curveType>{curve_type}</curveType>
          <Period>
            <timeInterval><start>{start}</start><end>{end}</end></timeInterval>
            <resolution>{resolution}</resolution>
            {points}
          </Period>
        </TimeSeries>
    '''


def price_document(*timeseries):
    return '<Publication_MarketDocument>' + ''.join(timeseries) + \
        '</Publication_MarketDocument>'


@pytest.mark.parametrize('currency', ['UAH', 'EUR'])
def test_parse_prices_with_currency(currency):
    xml = price_document(price_timeseries(
        '2026-09-25T21:00Z', '2026-09-25T22:00Z', 'PT15M',
        [(1, 4821.50), (2, 4710.00)], currency,
    ))

    frame = parse_prices_with_currencies(xml)['15min']

    assert frame.columns.tolist() == ['Price', 'Currency']
    assert frame['Price'].tolist() == [4821.50, 4710.00]
    assert frame['Currency'].unique().tolist() == [currency]
    assert pd.api.types.is_numeric_dtype(frame['Price'])


def test_parse_multiple_timeseries_retains_matching_currencies():
    xml = price_document(
        price_timeseries(
            '2026-09-25T21:00Z', '2026-09-25T22:00Z', 'PT30M',
            [(1, 10), (2, 11)], 'EUR',
        ),
        price_timeseries(
            '2026-09-25T22:00Z', '2026-09-25T23:00Z', 'PT30M',
            [(1, 20), (2, 21)], 'UAH',
        ),
    )

    frame = parse_prices_with_currencies(xml)['30min']

    assert frame[['Price', 'Currency']].values.tolist() == [
        [10.0, 'EUR'], [11.0, 'EUR'], [20.0, 'UAH'], [21.0, 'UAH']
    ]


def test_parse_multiple_timeseries_with_same_currency():
    xml = price_document(
        price_timeseries(
            '2026-09-25T21:00Z', '2026-09-25T22:00Z', 'PT60M',
            [(1, 10)], 'EUR',
        ),
        price_timeseries(
            '2026-09-25T22:00Z', '2026-09-25T23:00Z', 'PT60M',
            [(1, 20)], 'EUR',
        ),
    )

    frame = parse_prices_with_currencies(xml)['60min']

    assert frame['Currency'].tolist() == ['EUR', 'EUR']


@pytest.mark.parametrize(
    'resolution,key',
    [('PT15M', '15min'), ('PT30M', '30min'), ('PT60M', '60min')],
)
def test_parse_prices_with_currencies_supports_price_resolutions(
        resolution, key):
    xml = price_document(price_timeseries(
        '2026-09-25T21:00Z', '2026-09-25T22:00Z', resolution,
        [(1, 1)], 'EUR',
    ))

    parsed_row = parse_prices_with_currencies(xml)[key].iloc[0]
    assert parsed_row.tolist() == [1.0, 'EUR']


def test_parse_variable_sized_block_retains_currency():
    xml = price_document(price_timeseries(
        '2026-09-25T21:00Z', '2026-09-25T22:00Z', 'PT15M',
        [(1, 10), (3, 30)], 'UAH', curve_type='A03',
    ))

    frame = parse_prices_with_currencies(xml)['15min']

    assert frame['Price'].tolist() == [10.0, 10.0, 30.0, 30.0]
    assert frame['Currency'].tolist() == ['UAH'] * 4


def test_parse_missing_currency_does_not_default_to_eur():
    xml = price_document(price_timeseries(
        '2026-09-25T21:00Z', '2026-09-25T22:00Z', 'PT60M',
        [(1, 10)], currency=None,
    ))

    frame = parse_prices_with_currencies(xml)['60min']

    assert pd.isna(frame['Currency'].iloc[0])


def test_parse_overlapping_timeseries_keeps_price_currency_pairs():
    xml = price_document(
        price_timeseries(
            '2026-09-25T21:00Z', '2026-09-25T22:00Z', 'PT60M',
            [(1, 10)], 'EUR',
        ),
        price_timeseries(
            '2026-09-25T21:00Z', '2026-09-25T22:00Z', 'PT60M',
            [(1, 20)], 'UAH',
        ),
    )

    frame = parse_prices_with_currencies(xml)['60min']

    assert frame[['Price', 'Currency']].values.tolist() == [
        [10.0, 'EUR'], [20.0, 'UAH']
    ]
    assert frame.index.has_duplicates


def test_currency_client_matches_existing_price_query(monkeypatch):
    xml = price_document(price_timeseries(
        '2026-09-26T00:00Z', '2026-09-26T01:00Z', 'PT15M',
        [(1, 1), (2, 2), (3, 3), (4, 4)], 'UAH',
    ))
    calls = []

    def fake_query(self, country_code, start, end, offset=0, sequence=None):
        calls.append((start, end, offset, sequence))
        if offset:
            raise NoMatchingDataError
        return xml

    monkeypatch.setattr(EntsoeRawClient, 'query_day_ahead_prices', fake_query)
    client = EntsoePandasClient(api_key='test')
    start = pd.Timestamp('2026-09-26 02:00', tz='Europe/Amsterdam')
    end = pd.Timestamp('2026-09-26 02:30', tz='Europe/Amsterdam')

    prices = client.query_day_ahead_prices('NL', start=start, end=end)
    existing_call_count = len(calls)
    calls.clear()
    frame = client.query_day_ahead_prices_with_currencies(
        'NL', start=start, end=end
    )

    assert frame.columns.tolist() == ['Price', 'Currency']
    pd.testing.assert_index_equal(frame.index, prices.index)
    pd.testing.assert_series_equal(frame['Price'], prices, check_names=False)
    assert frame['Currency'].tolist() == ['UAH'] * len(frame)
    assert len(calls) == existing_call_count
    assert calls[0][0] == start - pd.Timedelta(days=1)
    assert calls[0][1] == end + pd.Timedelta(days=1)


def test_currency_client_document_deduplication_is_row_atomic(monkeypatch):
    documents = {
        0: price_document(price_timeseries(
            '2026-09-26T00:00Z', '2026-09-26T00:15Z', 'PT15M',
            [(1, 1)], 'EUR',
        )),
        100: price_document(price_timeseries(
            '2026-09-26T00:00Z', '2026-09-26T00:15Z', 'PT15M',
            [(1, 2)], currency=None,
        )),
    }

    def fake_query(self, country_code, start, end, offset=0, sequence=None):
        try:
            return documents[offset]
        except KeyError:
            raise NoMatchingDataError

    monkeypatch.setattr(EntsoeRawClient, 'query_day_ahead_prices', fake_query)
    client = EntsoePandasClient(api_key='test')
    timestamp = pd.Timestamp('2026-09-26 02:00', tz='Europe/Amsterdam')

    frame = client.query_day_ahead_prices_with_currencies(
        'NL', start=timestamp, end=timestamp
    )

    assert frame.loc[timestamp, 'Price'] == 2.0
    assert pd.isna(frame.loc[timestamp, 'Currency'])


def test_currency_client_deduplicates_using_last_valid_price_row(monkeypatch):
    documents = {
        0: price_document(price_timeseries(
            '2026-09-26T00:00Z', '2026-09-26T00:15Z', 'PT15M',
            [(1, 1)], 'EUR',
        )),
        100: price_document(price_timeseries(
            '2026-09-26T00:00Z', '2026-09-26T00:15Z', 'PT15M',
            [(1, 'NaN')], 'UAH',
        )),
    }

    def fake_query(self, country_code, start, end, offset=0, sequence=None):
        try:
            return documents[offset]
        except KeyError:
            raise NoMatchingDataError

    monkeypatch.setattr(EntsoeRawClient, 'query_day_ahead_prices', fake_query)
    client = EntsoePandasClient(api_key='test')
    timestamp = pd.Timestamp('2026-09-26 02:00', tz='Europe/Amsterdam')

    frame = client.query_day_ahead_prices_with_currencies(
        'NL', start=timestamp, end=timestamp
    )

    assert frame.loc[timestamp].tolist() == [1.0, 'EUR']


def test_currency_survives_year_splitting(monkeypatch):
    def fake_query(self, country_code, start, end, offset=0, sequence=None):
        if offset:
            raise NoMatchingDataError
        point_start = (start + pd.Timedelta(days=2)).tz_convert('UTC')
        point_end = point_start + pd.Timedelta(minutes=15)
        currency = 'UAH' if point_start.year == 2026 else 'EUR'
        return price_document(price_timeseries(
            point_start.isoformat(), point_end.isoformat(), 'PT15M',
            [(1, point_start.year)], currency,
        ))

    monkeypatch.setattr(EntsoeRawClient, 'query_day_ahead_prices', fake_query)
    client = EntsoePandasClient(api_key='test')
    start = pd.Timestamp('2026-01-02', tz='Europe/Amsterdam')
    end = pd.Timestamp('2027-01-03', tz='Europe/Amsterdam')

    frame = client.query_day_ahead_prices_with_currencies(
        'NL', start=start, end=end
    )

    assert frame['Currency'].tolist() == ['UAH', 'EUR']
    assert frame['Price'].tolist() == [2026.0, 2027.0]
