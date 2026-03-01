"""
Shared XML builder helpers and Hypothesis strategies for entsoe-py tests.

XML builders produce structurally valid ENTSO-E XML that BeautifulSoup can parse.
All tag names are lowercase to match the actual ENTSO-E API responses, which is
what the parsers expect (they use lowercase find() calls).

Hypothesis strategies generate random valid inputs for property-based tests.
"""
import io
import zipfile
from typing import List, Tuple, Optional

import pandas as pd
import pytest
from hypothesis import strategies as st

from entsoe.mappings import Area

# ---------------------------------------------------------------------------
# ENTSO-E XML namespace
# ---------------------------------------------------------------------------
_NS = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"


def _wrap_document(inner_xml: str) -> str:
    """Wrap inner XML in a Publication_MarketDocument root element."""
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<Publication_MarketDocument xmlns="{_NS}">\n'
        f'{inner_xml}'
        f'</Publication_MarketDocument>'
    )


def _build_period_xml(period: dict) -> str:
    """Build a <period> element from a period spec dict.

    period keys:
        start: str  (ISO timestamp, e.g. '2023-01-01T00:00Z')
        end: str
        resolution: str  (e.g. 'PT60M')
        points: list of (position, value) tuples
        label: str  (tag name for the value, default 'quantity')
    """
    label = period.get('label', 'quantity')
    points_xml = '\n'.join(
        f'        <point>\n'
        f'          <position>{pos}</position>\n'
        f'          <{label}>{val}</{label}>\n'
        f'        </point>'
        for pos, val in period['points']
    )
    return (
        f'      <period>\n'
        f'        <timeinterval>\n'
        f'          <start>{period["start"]}</start>\n'
        f'          <end>{period["end"]}</end>\n'
        f'        </timeinterval>\n'
        f'        <resolution>{period["resolution"]}</resolution>\n'
        f'{points_xml}\n'
        f'      </period>'
    )


# ---------------------------------------------------------------------------
# XML Builder: Prices
# ---------------------------------------------------------------------------

def build_price_xml(periods: list) -> str:
    """Build valid price XML from period specs.

    Each period dict: {start, end, resolution, points: [(position, value)]}
    Points use the 'price.amount' label as expected by parse_prices.
    """
    timeseries_parts = []
    for period in periods:
        p = dict(period, label='price.amount')
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <curvetype>A01</curvetype>\n'
            f'{_build_period_xml(p)}\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


# ---------------------------------------------------------------------------
# XML Builder: Loads
# ---------------------------------------------------------------------------

def build_load_xml(periods: list, business_type: str = 'A04',
                   process_type: str = 'A01') -> str:
    """Build valid load XML with business type and process type.

    Each period dict: {start, end, resolution, points: [(position, value)]}
    """
    timeseries_parts = []
    for period in periods:
        p = dict(period, label='quantity')
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <businesstype>{business_type}</businesstype>\n'
            f'    <curvetype>A01</curvetype>\n'
            f'{_build_period_xml(p)}\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


# ---------------------------------------------------------------------------
# XML Builder: Generation
# ---------------------------------------------------------------------------

def build_generation_xml(
    periods: list,
    psr_type: str = 'B14',
    per_plant: bool = False,
    plant_name: Optional[str] = None,
    include_eic: bool = False,
    eic_code: Optional[str] = None,
    has_out_bidding_zone: bool = False,
    curve_type: str = 'A01',
) -> str:
    """Build valid generation XML with PSR type and plant metadata.

    Each period dict: {start, end, resolution, points: [(position, value)]}
    """
    timeseries_parts = []
    for period in periods:
        p = dict(period, label='quantity')

        # Build optional elements
        if per_plant and plant_name:
            eic_xml = ''
            if include_eic and eic_code:
                eic_xml = f'      <mrid codingscheme="A01">{eic_code}</mrid>\n'
            plant_xml = (
                f'    <mktpsrtype>\n'
                f'      <psrtype>{psr_type}</psrtype>\n'
                f'      <powersystemresources>\n'
                f'{eic_xml}'
                f'        <name>{plant_name}</name>\n'
                f'      </powersystemresources>\n'
                f'    </mktpsrtype>\n'
            )
        else:
            plant_xml = (
                f'    <mktpsrtype>\n'
                f'      <psrtype>{psr_type}</psrtype>\n'
                f'    </mktpsrtype>\n'
            )

        out_bz_xml = ''
        if has_out_bidding_zone:
            out_bz_xml = '    <outbiddingzone_domain.mrid>10YCZ-CEPS-----N</outbiddingzone_domain.mrid>\n'

        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <curvetype>{curve_type}</curvetype>\n'
            f'{plant_xml}'
            f'{out_bz_xml}'
            f'{_build_period_xml(p)}\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


# ---------------------------------------------------------------------------
# XML Builder: Generic TimeSeries (crossborder flows, net positions, etc.)
# ---------------------------------------------------------------------------

def build_timeseries_xml(periods: list, curve_type: str = 'A01') -> str:
    """Build generic timeseries XML for crossborder flows, etc.

    Each period dict: {start, end, resolution, points: [(position, value)]}
    """
    timeseries_parts = []
    for period in periods:
        p = dict(period, label='quantity')
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <curvetype>{curve_type}</curvetype>\n'
            f'{_build_period_xml(p)}\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


# ---------------------------------------------------------------------------
# XML Builder: Crossborder Flows
# ---------------------------------------------------------------------------

def build_crossborder_flow_xml(periods: list) -> str:
    """Build valid crossborder flow XML.

    Each period dict: {start, end, resolution, points: [(position, value)]}
    Uses the same structure as build_timeseries_xml since
    parse_crossborder_flows delegates to _parse_timeseries_generic_whole.
    """
    return build_timeseries_xml(periods, curve_type='A01')


# ---------------------------------------------------------------------------
# ZIP Builder: Unavailability
# ---------------------------------------------------------------------------

def _build_unavailability_xml(
    created_datetime: str,
    mrid: str,
    revision_number: int = 1,
    docstatus_value: Optional[str] = None,
    timeseries_xml: str = '',
) -> bytes:
    """Build a single unavailability XML document as bytes."""
    docstatus_xml = ''
    if docstatus_value:
        docstatus_xml = (
            f'  <docstatus>\n'
            f'    <value>{docstatus_value}</value>\n'
            f'  </docstatus>\n'
        )
    xml = (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<Publication_MarketDocument xmlns="{_NS}">\n'
        f'  <mrid>{mrid}</mrid>\n'
        f'  <revisionnumber>{revision_number}</revisionnumber>\n'
        f'  <createddatetime>{created_datetime}</createddatetime>\n'
        f'{docstatus_xml}'
        f'{timeseries_xml}'
        f'</Publication_MarketDocument>'
    )
    return xml.encode('utf-8')


def _build_gen_unavailability_ts(
    business_type: str = 'A54',
    bidding_zone_mrid: str = '10YDE-VE-------2',
    psr_type: str = 'B14',
    plant_name: str = 'TestPlant',
    plant_mrid: str = 'PLANT001',
    nominal_power: float = 500.0,
    available_periods: Optional[list] = None,
) -> str:
    """Build a generation unavailability timeseries element.

    available_periods: list of dicts with {start, end, resolution, points: [(position, quantity)]}
    """
    if available_periods is None:
        available_periods = [{
            'start': '2023-01-01T00:00Z',
            'end': '2023-01-02T00:00Z',
            'resolution': 'PT60M',
            'points': [(1, 100.0)],
        }]

    periods_xml = ''
    for ap in available_periods:
        points_xml = '\n'.join(
            f'          <point>\n'
            f'            <position>{pos}</position>\n'
            f'            <quantity>{qty}</quantity>\n'
            f'          </point>'
            for pos, qty in ap['points']
        )
        periods_xml += (
            f'      <available_period>\n'
            f'        <timeinterval>\n'
            f'          <start>{ap["start"]}</start>\n'
            f'          <end>{ap["end"]}</end>\n'
            f'        </timeinterval>\n'
            f'        <resolution>{ap["resolution"]}</resolution>\n'
            f'{points_xml}\n'
            f'      </available_period>\n'
        )

    return (
        f'  <timeseries>\n'
        f'    <businesstype>{business_type}</businesstype>\n'
        f'    <biddingzone_domain.mrid>{bidding_zone_mrid}</biddingzone_domain.mrid>\n'
        f'    <quantity_measure_unit.name>MAW</quantity_measure_unit.name>\n'
        f'    <curvetype>A01</curvetype>\n'
        f'    <production_registeredresource.mrid>{plant_mrid}</production_registeredresource.mrid>\n'
        f'    <production_registeredresource.name>{plant_name}</production_registeredresource.name>\n'
        f'    <production_registeredresource.psrtype.powersystemresources.name>{plant_name} Unit</production_registeredresource.psrtype.powersystemresources.name>\n'
        f'    <production_registeredresource.location.name>TestLocation</production_registeredresource.location.name>\n'
        f'    <production_registeredresource.psrtype.psrtype>{psr_type}</production_registeredresource.psrtype.psrtype>\n'
        f'    <production_registeredresource.psrtype.powersystemresources.nominalp>{nominal_power}</production_registeredresource.psrtype.powersystemresources.nominalp>\n'
        f'{periods_xml}'
        f'  </timeseries>\n'
    )


def build_unavailability_zip(
    entries: Optional[list] = None,
    doctype: str = 'A77',
) -> bytes:
    """Build an in-memory ZIP containing unavailability XML documents.

    entries: list of dicts, each with:
        created_datetime: str
        mrid: str
        revision_number: int (default 1)
        docstatus_value: str or None
        timeseries_xml: str (raw timeseries XML, or use defaults)

    If entries is None, creates a single default entry.
    """
    if entries is None:
        ts_xml = _build_gen_unavailability_ts()
        entries = [{
            'created_datetime': '2023-06-15T10:00Z',
            'mrid': 'DOC001',
            'revision_number': 1,
            'docstatus_value': 'A05',
            'timeseries_xml': ts_xml,
        }]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i, entry in enumerate(entries):
            xml_bytes = _build_unavailability_xml(
                created_datetime=entry['created_datetime'],
                mrid=entry.get('mrid', f'DOC{i:03d}'),
                revision_number=entry.get('revision_number', 1),
                docstatus_value=entry.get('docstatus_value'),
                timeseries_xml=entry.get('timeseries_xml', ''),
            )
            zf.writestr(f'outage_{i:03d}.xml', xml_bytes)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# ZIP Builder: Imbalance (prices and volumes)
# ---------------------------------------------------------------------------

def _build_imbalance_price_xml(periods: list) -> str:
    """Build imbalance price XML text.

    Each period dict: {start, end, resolution, points: [(position, amount, category)]}
    category is 'A04' (Long) or 'A05' (Short).
    """
    timeseries_parts = []
    for period in periods:
        points_xml = '\n'.join(
            f'        <point>\n'
            f'          <position>{pos}</position>\n'
            f'          <imbalance_price.amount>{amount}</imbalance_price.amount>\n'
            f'          <imbalance_price.category>{cat}</imbalance_price.category>\n'
            f'        </point>'
            for pos, amount, cat in period['points']
        )
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <curvetype>A01</curvetype>\n'
            f'      <period>\n'
            f'        <timeinterval>\n'
            f'          <start>{period["start"]}</start>\n'
            f'          <end>{period["end"]}</end>\n'
            f'        </timeinterval>\n'
            f'        <resolution>{period["resolution"]}</resolution>\n'
            f'{points_xml}\n'
            f'      </period>\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


def _build_imbalance_volume_xml(
    periods: list,
    flow_direction: Optional[str] = None,
) -> str:
    """Build imbalance volume XML text.

    Each period dict: {start, end, resolution, points: [(position, quantity)]}
    flow_direction: 'A01' (in), 'A02' (out), 'A03' (symmetric), or None.
    """
    timeseries_parts = []
    for period in periods:
        points_xml = '\n'.join(
            f'        <point>\n'
            f'          <position>{pos}</position>\n'
            f'          <quantity>{qty}</quantity>\n'
            f'        </point>'
            for pos, qty in period['points']
        )
        flow_xml = ''
        if flow_direction:
            flow_xml = f'    <flowdirection.direction>{flow_direction}</flowdirection.direction>\n'
        timeseries_parts.append(
            f'  <timeseries>\n'
            f'    <curvetype>A01</curvetype>\n'
            f'{flow_xml}'
            f'      <period>\n'
            f'        <timeinterval>\n'
            f'          <start>{period["start"]}</start>\n'
            f'          <end>{period["end"]}</end>\n'
            f'        </timeinterval>\n'
            f'        <resolution>{period["resolution"]}</resolution>\n'
            f'{points_xml}\n'
            f'      </period>\n'
            f'  </timeseries>'
        )
    return _wrap_document('\n'.join(timeseries_parts) + '\n')


def build_imbalance_zip(
    xml_contents: Optional[list] = None,
    kind: str = 'price',
) -> bytes:
    """Build an in-memory ZIP containing imbalance XML documents.

    xml_contents: list of raw XML strings. If None, creates a default.
    kind: 'price' or 'volume' — used for default content generation.
    """
    if xml_contents is None:
        if kind == 'price':
            xml_contents = [_build_imbalance_price_xml([{
                'start': '2023-01-01T00:00Z',
                'end': '2023-01-01T01:00Z',
                'resolution': 'PT15M',
                'points': [
                    (1, 50.0, 'A04'),
                    (2, 55.0, 'A04'),
                    (3, 52.0, 'A04'),
                    (4, 48.0, 'A04'),
                ],
            }])]
        else:
            xml_contents = [_build_imbalance_volume_xml([{
                'start': '2023-01-01T00:00Z',
                'end': '2023-01-01T01:00Z',
                'resolution': 'PT15M',
                'points': [(1, 100.0), (2, 200.0), (3, 150.0), (4, 175.0)],
            }], flow_direction='A01')]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i, xml_text in enumerate(xml_contents):
            if isinstance(xml_text, str):
                xml_bytes = xml_text.encode('utf-8')
            else:
                xml_bytes = xml_text
            zf.writestr(f'imbalance_{i:03d}.xml', xml_bytes)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Hypothesis Strategies
# ---------------------------------------------------------------------------

#: All known ENTSO-E resolution codes
KNOWN_RESOLUTIONS = ['PT60M', 'PT15M', 'PT30M', 'P1Y', 'P1D', 'P7D', 'P1M', 'PT1M']


@st.composite
def resolution_codes(draw):
    """Generate valid ENTSO-E resolution codes."""
    return draw(st.sampled_from(KNOWN_RESOLUTIONS))


@st.composite
def area_enums(draw):
    """Generate random Area enum members."""
    return draw(st.sampled_from(list(Area)))


@st.composite
def timestamp_pairs(draw, min_delta_hours=1, max_delta_hours=48):
    """Generate valid (start, end) timestamp pairs with UTC timezone.

    Returns a tuple of (start, end) pd.Timestamps with UTC timezone
    where start < end and the delta is between min_delta_hours and max_delta_hours.
    """
    base = draw(st.datetimes(
        min_value=pd.Timestamp('2020-01-01').to_pydatetime(),
        max_value=pd.Timestamp('2024-12-31').to_pydatetime(),
    ))
    delta_hours = draw(st.integers(min_value=min_delta_hours, max_value=max_delta_hours))
    start = pd.Timestamp(base, tz='UTC').floor('h')
    end = start + pd.Timedelta(hours=delta_hours)
    return start, end


@st.composite
def price_points(draw, n_points=None):
    """Generate lists of (position, price_value) tuples.

    Positions are 1-based and sequential. Price values are realistic floats.
    """
    if n_points is None:
        n_points = draw(st.integers(min_value=1, max_value=24))
    prices = draw(st.lists(
        st.floats(min_value=-500.0, max_value=5000.0, allow_nan=False, allow_infinity=False),
        min_size=n_points,
        max_size=n_points,
    ))
    return [(i + 1, round(p, 2)) for i, p in enumerate(prices)]
