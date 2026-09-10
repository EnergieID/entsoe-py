from io import BytesIO
from itertools import product
from zipfile import ZipFile
import os
from dotenv import load_dotenv
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
import pandas as pd
import requests
import pytest

load_dotenv()

API_KEY = os.getenv("API_KEY")


@pytest.fixture
def client():
    yield EntsoePandasClient(api_key=API_KEY)


# this on purpose more then a month since entsoe has now a lot of "30 days" limits
STARTS = [pd.Timestamp("20260301", tz="Europe/Amsterdam")]
ENDS = [pd.Timestamp("20260402 23:59", tz="Europe/Amsterdam")]

COUNTRY_CODES = ["NL", "BE", "DE_LU", "FR"]
COUNTRY_CODES_FROM = ["NL"]
COUNTRY_CODES_TO = ["DE_LU", 'NO_2', 'BE']

BASIC_QUERIES_TIMESERIES = [
    "query_day_ahead_prices",
    "query_net_position",
    "query_load",
    "query_load_forecast",
    "query_wind_and_solar_forecast",
    "query_generation_forecast",
    "query_generation",
    "query_generation_per_plant",
    "query_installed_generation_capacity",
    "query_current_balancing_state"
]

BASIC_QUERIES = [
    "query_installed_generation_capacity_per_unit",
]

BASIC_QUERIES_TIMESERIES_ZIP = [
    "query_imbalance_prices",
    "query_imbalance_volumes",
]

CROSSBORDER_QUERIES = [
    "query_crossborder_flows",
    "query_scheduled_exchanges",
    "query_net_transfer_capacity_dayahead",
    "query_net_transfer_capacity_weekahead",
    "query_net_transfer_capacity_monthahead",
    #"query_net_transfer_capacity_yearahead",
]

def basic_checks(result, timeseries=True):
    assert isinstance(result, pd.Series) or isinstance(result, pd.DataFrame)
    assert not result.empty
    if timeseries:
        assert result.index.is_monotonic_increasing and result.index.is_unique

    if isinstance(result, pd.Series):
        assert not result.isna().all()
    elif isinstance(result, pd.DataFrame):
        assert not result.isna().all().all()

@pytest.mark.parametrize(
    "country_code, start, end, query",
    product(COUNTRY_CODES, STARTS, ENDS, BASIC_QUERIES_TIMESERIES),
)
def test_basic_queries_timeseries(client, query, country_code, start, end):
    if query == 'query_current_balancing_state' and country_code == 'DE_LU':
        country_code = 'DE_AMPRION'
    if query == 'query_generation_per_plant' and country_code == 'DE_LU':
        # this is bugged, ticket raised at entsoe
        return
    result = getattr(client, query)(country_code, start=start, end=end)
    basic_checks(result)

@pytest.mark.parametrize(
    "country_code, start, end, query",
    product(COUNTRY_CODES, STARTS, ENDS, BASIC_QUERIES),
)
def test_basic_queries(client, query, country_code, start, end):
    result = getattr(client, query)(country_code, start=start, end=end)
    basic_checks(result, timeseries=False)



@pytest.mark.parametrize(
    "country_code, start, end, query",
    product(['BE', 'FR'], STARTS, ENDS, BASIC_QUERIES_TIMESERIES_ZIP),
)
def test_basic_queries_zip(client, query, country_code, start, end):
    result = getattr(client, query)(country_code, start=start, end=end)
    basic_checks(result)



@pytest.mark.parametrize(
    "country_code_from, country_code_to, start, end, query",
    product(COUNTRY_CODES_FROM, COUNTRY_CODES_TO, STARTS, ENDS, CROSSBORDER_QUERIES),
)
def test_crossborder_queries(
    client, query, country_code_from, country_code_to, start, end
):
    result = getattr(client, query)(country_code_from, country_code_to, start=start, end=end)
    basic_checks(result)


def test_query_aggregate_water_reservoirs_and_hydro_storage(client):
    result = client.query_aggregate_water_reservoirs_and_hydro_storage('NO_2',
                                                                       start=STARTS[0], end=ENDS[0])
    basic_checks(result)


@pytest.mark.parametrize(
    "country_code_from, country_code_to, start, end, id_type",
    [x for x in product(["BE", "NL"], ["BE", "NL"], STARTS, ENDS, ['IDCT', 'IDA1', 'IDA2', 'IDA3']) if x[0] != x[1]],
)
def test_query_intraday_offered_capacity(client, country_code_from, country_code_to, start, end, id_type):
    result = client.query_intraday_offered_capacity(
        country_code_from, country_code_to, start=start, end=end, implicit=True, id_type=id_type
    )
    basic_checks(result)


# @pytest.mark.parametrize(
#     "country_code, process_type, start, end",
#     product([x if x != 'DE_LU' else 'DE_AMPRION' for x in COUNTRY_CODES], ['A51', 'A52', 'A47'], STARTS, ENDS),
# )
# def test_query_contracted_reserve_prices_procured_capacity(client, country_code, process_type, start, end):
#     # [O] A51 = Automatic frequency restoration reserve; A52 = Frequency containment reserve; A47 = Manual frequency restoration reserve; A46 = Replacement reserve
#     result = client.query_contracted_reserve_prices_procured_capacity(
#         country_code, start=start, end=end, process_type=process_type, type_marketagreement_type='A01'
#     )
#     basic_checks(result)


@pytest.mark.parametrize(
    "country_code, start, end",
    product(COUNTRY_CODES, STARTS, ENDS),
)
def test_query_unavailability_of_generation_units(client, country_code, start, end):
    result = client.query_unavailability_of_generation_units(country_code, start=start, end=end, docstatus=None, periodstartupdate=None, periodendupdate=None)
    basic_checks(result, timeseries=False)


@pytest.mark.parametrize(
    "country_code, start, end",
    product([x for x in COUNTRY_CODES if x != 'BE'], STARTS, ENDS),
)
def test_query_unavailability_of_production_units(client, country_code, start, end):
    result = client.query_unavailability_of_production_units(country_code, start=start, end=end, docstatus=None, periodstartupdate=None, periodendupdate=None)
    basic_checks(result, timeseries=False)

@pytest.mark.parametrize(
    "country_code_from, country_code_to, start, end",
    product(COUNTRY_CODES_FROM, [x for x in COUNTRY_CODES_TO if x != 'DE_LU'], STARTS, ENDS),
)
def test_query_unavailability_transmission(
    client, country_code_from, country_code_to, start, end
):
    result = client.query_unavailability_transmission(
        country_code_from, country_code_to, start=start, end=end, docstatus=None, periodstartupdate=None, periodendupdate=None
    )
    basic_checks(result, timeseries=False)

@pytest.mark.parametrize(
    "country_code, start, end",
    product(COUNTRY_CODES, STARTS, ENDS),
)
def test_query_withdrawn_unavailability_of_generation_units(
    client, country_code, start, end
):
    result = client.query_withdrawn_unavailability_of_generation_units(
        country_code, start, end,
    )
    basic_checks(result, timeseries=False)

# Offline A78 regression coverage; all documents below are synthetic.
_transmission_start = pd.Timestamp('2025-06-01', tz='UTC')
_transmission_end = pd.Timestamp('2025-06-02', tz='UTC')
_transmission_created = pd.Timestamp('2025-05-20T12:00:00Z')
_transmission_columns = ['docstatus', 'mrid', 'revision', 'businesstype', 'in_domain',
           'out_domain', 'qty_uom', 'curvetype', 'start', 'end',
           'resolution', 'pstn', 'avail_qty']


def _transmission_outage_zip(documents):
    """Each (mRID, revision) contributes one independently identifiable document."""
    result = BytesIO()
    with ZipFile(result, 'w') as archive:
        for document_id, revision in documents:
            archive.writestr(f'{document_id}-{revision}.xml', f'''\
<Unavailability_MarketDocument>
  <mRID>{document_id}</mRID><revisionNumber>{revision}</revisionNumber>
  <type>A78</type><createdDateTime>2025-05-20T12:00:00Z</createdDateTime>
  <docStatus><value>A05</value></docStatus>
  <TimeSeries><mRID>1</mRID><businessType>A53</businessType>
    <in_Domain.mRID>10YBE----------2</in_Domain.mRID>
    <out_Domain.mRID>10YNL----------L</out_Domain.mRID>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
    <curveType>A03</curveType><Available_Period>
      <timeInterval><start>2025-06-01T00:00Z</start>
        <end>2025-06-02T00:00Z</end></timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><quantity>100</quantity></Point>
    </Available_Period>
  </TimeSeries>
</Unavailability_MarketDocument>''')
    return result.getvalue()


def _transmission_response(content, status=200, content_type='application/zip'):
    result = requests.Response()
    result.status_code = status
    result._content = content
    result.headers['content-type'] = content_type
    return result


def _transmission_no_data():
    return _transmission_response(b'<Acknowledgement_MarketDocument><Reason><code>999</code>'
                    b'<text>No matching data found</text></Reason>'
                    b'</Acknowledgement_MarketDocument>', content_type='application/xml')


def _transmission_assert_projection(frame, expected):
    assert isinstance(frame, pd.DataFrame)
    assert list(frame.columns) == _transmission_columns
    assert frame.index.name == 'created_doc_time'
    assert str(frame.index.tz) == 'Europe/Amsterdam'
    assert all(value == _transmission_created for value in frame.index)
    assert sorted(zip(frame.mrid, frame.revision)) == sorted(expected)
    assert all(value == _transmission_start for value in frame['start'])
    assert all(value == _transmission_end for value in frame['end'])
    assert all(str(value.tz) == 'Europe/Amsterdam' for value in frame['start'])
    assert set(frame.qty_uom) == {'MAW'}
    assert set(frame.avail_qty) == {'100'}


class TestTransmissionUnavailabilityPagination:
    """Keep offline fixtures isolated from the existing live API tests."""

    @pytest.fixture(autouse=True)
    def forbid_network(self, monkeypatch):
        def fail(*args, **kwargs):
            raise AssertionError('Unexpected network access in offline test')
        monkeypatch.setattr(requests.Session, 'request', fail)

    @pytest.fixture
    def query(self, monkeypatch):
        def run(pages):
            calls = []
            client = EntsoePandasClient(api_key='synthetic-offline-token', retry_count=1, retry_delay=0)

            def get(url, params, **unused):
                calls.append(dict(params))
                offset = params['offset']
                assert offset in pages, f'Unexpected offset {offset}'
                value = pages[offset]
                if isinstance(value, Exception):
                    raise value
                return value

            monkeypatch.setattr(client.session, 'get', get)
            return client, calls
        return run

    def test_one_page_preserves_existing_projection(self, query):
        expected = [('outage-001', 3), ('outage-002', 1)]
        client, calls = query({0: _transmission_response(_transmission_outage_zip(expected)), 200: _transmission_no_data()})
        frame = client.query_unavailability_transmission('NL', 'BE', start=_transmission_start, end=_transmission_end)
        _transmission_assert_projection(frame, expected)

    def test_multiple_pages_preserve_distinct_documents_at_same_creation_time(self, query):
        first = [(f'outage-{number:03}', number % 3 + 1) for number in range(200)]
        last = [('outage-200', 7)]
        client, calls = query({0: _transmission_response(_transmission_outage_zip(first)),
                               200: _transmission_response(_transmission_outage_zip(last)), 400: _transmission_no_data()})
        frame = client.query_unavailability_transmission('NL', 'BE', start=_transmission_start, end=_transmission_end)
        _transmission_assert_projection(frame, first + last)
        assert [call['offset'] for call in calls] == [0, 200, 400]
        assert frame.index.has_duplicates  # Creation time is deliberately not a document key.

    def test_nonzero_offset_and_update_window_are_preserved_on_every_page(self, query):
        first = [(f'outage-{number:03}', 2) for number in range(200, 400)]
        last = [('outage-400', 4)]
        client, calls = query({200: _transmission_response(_transmission_outage_zip(first)),
                               400: _transmission_response(_transmission_outage_zip(last)), 600: _transmission_no_data()})
        frame = client.query_unavailability_transmission(
            'NL', 'BE', start=_transmission_start, end=_transmission_end, offset=200, docstatus='A05',
            periodstartupdate=pd.Timestamp('2025-05-20', tz='UTC'),
            periodendupdate=pd.Timestamp('2025-05-21', tz='UTC'))
        _transmission_assert_projection(frame, first + last)
        assert [call['offset'] for call in calls] == [200, 400, 600]
        for call in calls:
            assert call['documentType'] == 'A78'
            assert call['in_Domain'] == '10YBE----------2'
            assert call['out_Domain'] == '10YNL----------L'
            assert call['periodStart'] == '202506010000'
            assert call['periodEnd'] == '202506020000'
            assert call['periodStartUpdate'] == '202505200000'
            assert call['periodEndUpdate'] == '202505210000'
            assert call['docStatus'] == 'A05'

    @pytest.mark.parametrize('failure', [requests.ConnectionError('synthetic transport failure'),
                                        _transmission_response(b'synthetic upstream error', status=500)])
    def test_later_page_failure_must_not_return_partial_success(self, query, failure):
        first = [(f'outage-{number:03}', 1) for number in range(200)]
        client, calls = query({0: _transmission_response(_transmission_outage_zip(first)), 200: failure})
        with pytest.raises((requests.ConnectionError, requests.HTTPError)):
            client.query_unavailability_transmission('NL', 'BE', start=_transmission_start, end=_transmission_end)
        assert [call['offset'] for call in calls] == [0, 200]

    def test_initial_no_data_keeps_existing_error(self, query):
        client, calls = query({0: _transmission_no_data()})
        with pytest.raises(NoMatchingDataError):
            client.query_unavailability_transmission('NL', 'BE', start=_transmission_start, end=_transmission_end)
        assert [call['offset'] for call in calls] == [0]

    def test_success_at_existing_offset_ceiling_does_not_claim_completeness(self, query):
        # 4800 is the existing library ceiling, not a verified provider-wide limit.
        client, calls = query({4800: _transmission_response(_transmission_outage_zip([('outage-4800', 1)]))})
        with pytest.raises(RuntimeError, match='(?i)incomplete'):
            client.query_unavailability_transmission(
                'NL', 'BE', start=_transmission_start, end=_transmission_end, offset=4800)
        assert [call['offset'] for call in calls] == [4800]

    def test_no_data_at_existing_offset_ceiling_keeps_existing_error(self, query):
        client, calls = query({4800: _transmission_no_data()})
        with pytest.raises(NoMatchingDataError):
            client.query_unavailability_transmission(
                'NL', 'BE', start=_transmission_start, end=_transmission_end, offset=4800)
        assert [call['offset'] for call in calls] == [4800]

    def test_provider_rejection_of_offset_outside_existing_ceiling_propagates(self, query):
        client, calls = query({5000: _transmission_response(b'synthetic invalid offset', status=400)})
        with pytest.raises(requests.HTTPError) as error:
            client.query_unavailability_transmission(
                'NL', 'BE', start=_transmission_start, end=_transmission_end, offset=5000)
        assert error.value.response.status_code == 400
        assert [call['offset'] for call in calls] == [5000]
