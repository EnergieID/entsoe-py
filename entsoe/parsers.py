import sys
import zipfile
from io import BytesIO
from typing import Union
import warnings
import bs4
from bs4.builder import XMLParsedAsHTMLWarning
import pandas as pd
from lxml import etree

from .mappings import PSRTYPE_MAPPINGS, DOCSTATUS, BSNTYPE, Area

warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)

GENERATION_ELEMENT = "inBiddingZone_Domain.mRID"
CONSUMPTION_ELEMENT = "outBiddingZone_Domain.mRID"


def find(element, tag):
    print(tag)
    return next(element.iter('{*}'+tag)).text
    
def findall(element, tag):
    return element.iter('{*}'+tag)

def _extract_timeseries(xml_bytes):
    """
    Parameters
    ----------
    xml : bytes

    Yields
    -------
    lxml.element
    """
    if not xml_bytes:
        return
    for event, element in etree.iterparse(BytesIO(xml_bytes), tag='{*}TimeSeries'):
        yield element



def parse_prices(xml_bytes):
    """
    Parameters
    ----------
    xml_bytes : butes

    Returns
    -------
    pd.Series
    """
    series = {
        '15T': [],
        '30T': [],
        '60T': []
    }
    for element in _extract_timeseries(xml_bytes):
        element_series = _parse_price_timeseries(element)
        series[element_series.index.freqstr].append(element_series)

    for freq, freq_series in series.items():
        if len(freq_series) > 0:
            series[freq] = pd.concat(freq_series).sort_index()
    return series


def parse_netpositions(xml_bytes):
    """

    Parameters
    ----------
    xml_bytes : bytes

    Returns
    -------
    pd.Series
    """
    series = []
    for element in _extract_timeseries(xml_bytes):
        series.append(_parse_netposition_timeseries(element))
    series = pd.concat(series)
    series = series.sort_index()
    return series


def parse_loads(xml_bytes, process_type='A01'):
    """
    Parameters
    ----------
    xml_bytes: bytes

    Returns
    -------
    pd.DataFrame
    """
    if process_type == 'A01' or process_type == 'A16':
        series = []
        for element in _extract_timeseries(xml_bytes):
            series.append(_parse_load_timeseries(element))
        series = pd.concat(series)
        series = series.sort_index()
        return pd.DataFrame({
            'Forecasted Load' if process_type == 'A01' else 'Actual Load': series
        })
    else:
        series_min = pd.Series(dtype='object')
        series_max = pd.Series(dtype='object')
        for element in _extract_timeseries(xml_bytes):
            t = _parse_load_timeseries(element)
            if find(element, 'businessType') == 'A60':
                series_min = series_min.append(t)
            elif find(element, 'businessType') == 'A61':
                series_max = series_max.append(t)
            else:
                continue
        return pd.DataFrame({
            'Min Forecasted Load': series_min,
            'Max Forecasted Load': series_max
        })



def parse_generation(
        xml_bytes: str,
        per_plant: bool = False,
        include_eic: bool = False,
        nett: bool = False) -> Union[pd.DataFrame, pd.Series]:
    """
    Parameters
    ----------
    xml_bytes: bytes
    per_plant : bool
        Decide if you need the parser that can extract plant info as well.
    nett : bool
        If you want to condense generation and consumption of a plant into a
        nett number
    include_eic: bool
        If you want to include the eic code of a plan in the output

    Returns
    -------
    pd.DataFrame | pd.Series
    """
    all_series = dict()
    for element in _extract_timeseries(xml_bytes):
        ts = _parse_generation_timeseries(element, per_plant=per_plant, include_eic=include_eic)
        
        # check if we already have a series of this name
        series = all_series.get(ts.name)
        if series is None:
            # If not, we just save ts
            all_series[ts.name] = ts
        else:
            # If yes, we extend it
            series = pd.concat([series, ts])
            series.sort_index(inplace=True)
            all_series[series.name] = series
            
    # drop duplicates in all series
    for name in all_series:
        ts = all_series[name]
        all_series[name] = ts[~ts.index.duplicated(keep='first')]

    df = pd.DataFrame.from_dict(all_series)
    df.sort_index(inplace=True)

    df = _calc_nett_and_drop_redundant_columns(df, nett=nett)
    return df


def _calc_nett_and_drop_redundant_columns(
        df: pd.DataFrame, nett: bool) -> pd.DataFrame:
    def _calc_nett(_df):
        try:
            if set(['Actual Aggregated']).issubset(_df):
                if set(['Actual Consumption']).issubset(_df):
                    _new = _df['Actual Aggregated'].fillna(0) - _df[
                        'Actual Consumption'].fillna(0)
                else:
                    _new = _df['Actual Aggregated'].fillna(0)
            else:
                _new = -_df['Actual Consumption'].fillna(0)    
            
        except KeyError:
            print ('Netting production and consumption not possible. Column not found')
        return _new

    if hasattr(df.columns, 'levels'):
        if len(df.columns.levels[-1]) == 1:
            # Drop the extra header, if it is redundant
            df = df.droplevel(axis=1, level=-1)
        elif nett:
            frames = []
            for column in df.columns.levels[-2]:
                new = _calc_nett(df[column])
                new.name = column
                frames.append(new)
            df = pd.concat(frames, axis=1)
    else:
        if nett:
            df = _calc_nett(df)
        elif len(df.columns) == 1:
            df = df.squeeze()

    return df


def parse_installed_capacity_per_plant(xml_bytes):
    """
    Parameters
    ----------
    xml_bytes : bytes

    Returns
    -------
    pd.DataFrame
    """
    all_series = {}
    for element in _extract_timeseries(xml_bytes):
        s = _parse_installed_capacity_per_plant(element)
        series = all_series.get(s.name)
        if series is None:
            all_series[s.name] = s
        else:
            series = pd.concat([series, s])
            series.sort_index()
            all_series[series.name] = series

    for name in all_series:
        ts = all_series[name]
        all_series[name] = ts[~ts.index.duplicated(keep='first')]

    df = pd.DataFrame.from_dict(all_series).T
    df['Production Type'] = df['Production Type'].map(PSRTYPE_MAPPINGS)
    df['Name'] = df['Name'].str.encode('latin-1').str.decode('utf-8')
    #    df['Status'] = df['Status'].map(BSNTYPE)
    return df


def parse_water_hydro(xml_bytes, tz):
    """
    Parameters
    ----------
    xml_bytes: bytes

    Returns
    -------
    pd.Series
    """
    all_series = []
    for element in _extract_timeseries(xml_bytes):
        all_series.append(_parse_water_hydro_timeseries(element, tz=tz))

    series = pd.concat(all_series)

    return series


def parse_crossborder_flows(xml_bytes):
    """
    Parameters
    ----------
    xml_bytes: bytes

    Returns
    -------
    pd.Series
    """
    series = []
    for element in _extract_timeseries(xml_bytes):
        series.append(_parse_crossborder_flows_timeseries(element))
    series = pd.concat(series)
    series = series.sort_index()
    return series


def parse_imbalance_prices(xml_bytes):
    """
    Parameters
    ----------
    xml_bytes : bytes

    Returns
    -------
    pd.DataFrame
    """
    timeseries_blocks = _extract_timeseries(xml_bytes)
    frames = (_parse_imbalance_prices_timeseries(element)
              for element in timeseries_blocks)
    df = pd.concat(frames, axis=1)
    df = df.stack().unstack()  # ad-hoc fix to prevent column splitting by NaNs
    df.sort_index(inplace=True)
    return df


def parse_imbalance_volumes(xml_bytes):
    """
    Parameters
    ----------
    xml_bytes: bytes

    Returns
    -------
    pd.DataFrame
    """
    timeseries_blocks = _extract_timeseries(xml_bytes)
    frames = (_parse_imbalance_volumes_timeseries(element)
              for element in timeseries_blocks)
    df = pd.concat(frames, axis=1)
    df = df.stack().unstack()  # ad-hoc fix to prevent column splitting by NaNs
    df.sort_index(inplace=True)
    return df


def parse_procured_balancing_capacity(xml_bytes, tz):
    """
    Parameters
    ----------
    xml_bytes: bytes
    tz: str

    Returns
    -------
    pd.DataFrame
    """
    timeseries_blocks = _extract_timeseries(xml_bytes)
    frames = (_parse_procured_balancing_capacity(element, tz)
              for element in timeseries_blocks)
    df = pd.concat(frames, axis=1)

    df.sort_index(axis=0, inplace=True)
    df.sort_index(axis=1, inplace=True)
    return df


def _parse_procured_balancing_capacity(element, tz):
    """
    Parameters
    ----------
    element: lxml.element
    tz: str

    Returns
    -------
    pd.DataFrame
    """
    direction = {
        'A01': 'Up',
        'A02': 'Down'
    }

    flow_direction = direction[find(element, 'flowDirection.direction')]
    start = pd.to_datetime(find(element, 'start'))
    end = pd.to_datetime(find(element, 'end'))
    resolution = _resolution_to_timedelta(find(element, 'resolution'))
    tx = pd.date_range(start=start, end=end, freq=resolution, inclusive='left')
    points = findall(element, 'Point')
    df = pd.DataFrame(index=tx, columns=['Price', 'Volume'])

    for dt, point in zip(tx, points):
        df.loc[dt, 'Price'] = float(find(point, 'procurement_Price.amount'))
        df.loc[dt, 'Volume'] = float(find(point, 'quantity'))

    mrid = int(find(element, 'mRID'))
    df.columns = pd.MultiIndex.from_product(
        [[flow_direction], [mrid], df.columns],
        names=('direction', 'mrid', 'unit')
    )

    return df


def parse_contracted_reserve(xml_bytes, tz, label):
    """
    Parameters
    ----------
    xml_bytes: bytes
    tz: str
    label: str

    Returns
    -------
    pd.DataFrame
    """
    timeseries_blocks = _extract_timeseries(xml_bytes)
    frames = (_parse_contracted_reserve_series(element, tz, label)
              for element in timeseries_blocks)
    df = pd.concat(frames, axis=1)
    # Ad-hoc fix to prevent that columns are split by NaNs:
    df = df.groupby(axis=1, level = [0,1]).mean()
    df.sort_index(inplace=True)
    return df


def _parse_contracted_reserve_series(element, tz, label):
    """
    Parameters
    ----------
    element: lxml.element
    tz: str
    label: str (case sensitive!)

    Returns
    -------
    pd.Series
    """
    positions =  [int(x.text)   for x in findall(element, 'position')]
    prices = [float(x.text) for x in findall(element, label)]
    
    df = pd.DataFrame(data={'position': positions,
                            label: prices})
    df = df.set_index(['position'])
    df.sort_index(inplace=True)
    index = _parse_datetimeindex(element, tz)
    if len(index) > len(df.index):
        print("Shortening index", file=sys.stderr)
        df.index = index[:len(df.index)]
    else:
        df.index = index

    df.index.name = None
    df.columns.name = None
    direction_dico = {'A01': 'Up',
                      'A02': 'Down',
                      'A03': 'Symmetric'}

    # First column level: the type of reserve
    reserve_type = BSNTYPE[find(element, "businessType")]
    df.rename(columns={label: reserve_type}, inplace=True)

    # Second column level: the flow direction 
    direction = direction_dico[find(element, 'flowDirection.direction')]
    df.columns = pd.MultiIndex.from_product([df.columns, [direction]])
    return df


def parse_imbalance_prices_zip(zip_contents: bytes) -> pd.DataFrame:
    """
    Parameters
    ----------
    zip_contents : bytes

    Returns
    -------
    pd.DataFrame
    """
    def gen_frames(archive):
        with zipfile.ZipFile(BytesIO(archive), 'r') as arc:
            for f in arc.infolist():
                if f.filename.endswith('xml'):
                    #TODO this should generate bytes not xml text
                    frame = parse_imbalance_prices(xml_bytes=arc.read(f))
                    yield frame

    frames = gen_frames(zip_contents)
    df = pd.concat(frames)
    df.sort_index(inplace=True)
    return df


def _parse_imbalance_prices_timeseries(element) -> pd.DataFrame:
    """
    Parameters
    ----------
    element: lxml.element

    Returns
    -------
    pd.DataFrame
    """
    positions = []
    amounts = []
    categories = []
    for point in findall(element, 'Point'):
        positions.append(int(find(point, 'position')))
        amounts.append(float(find(point, 'imbalance_Price.amount')))
        if list(findall(point, 'imbalance_Price.category')):
            categories.append(find(point, 'imbalance_Price.category'))
        else:
            categories.append('None')

    df = pd.DataFrame(data={'position': positions,
                            'amount': amounts, 'category': categories})
    df = df.set_index(['position', 'category']).unstack()
    df.sort_index(inplace=True)
    df.index = _parse_datetimeindex(element)
    df = df.xs('amount', axis=1)
    df.index.name = None
    df.columns.name = None
    df.rename(columns={'A04': 'Long', 'A05': 'Short',
                       'None': 'Price for Consumption'}, inplace=True)

    return df


def parse_imbalance_volumes_zip(zip_contents: bytes) -> pd.DataFrame:
    """
    Parameters
    ----------
    zip_contents : bytes

    Returns
    -------
    pd.DataFrame
    """
    def gen_frames(archive):
        with zipfile.ZipFile(BytesIO(archive), 'r') as arc:
            for f in arc.infolist():
                if f.filename.endswith('xml'):
                    frame = parse_imbalance_volumes(xml_text=arc.read(f))
                    yield frame

    frames = gen_frames(zip_contents)
    df = pd.concat(frames)
    df.sort_index(inplace=True)
    return df


def _parse_imbalance_volumes_timeseries(element) -> pd.DataFrame:
    """
    Parameters
    ----------
    element: lxml.element

    Returns
    -------
    pd.DataFrame
    """
    flow_direction_factor = {
        'A01': 1, # in
        'A02': -1 # out
    }[find(element, 'flowDirection.direction')]

    df = pd.DataFrame(columns=['Imbalance Volume'])

    for period in findall(element, 'Period'):
        start = pd.to_datetime(find(period, 'start'))
        end = pd.to_datetime(find(period, 'end'))
        resolution = _resolution_to_timedelta(find(period, 'resolution'))
        tx = pd.date_range(start=start, end=end, freq=resolution, inclusive='left')
        points = findall(period, 'Point')

        for dt, point in zip(tx, points):
            df.loc[dt, 'Imbalance Volume'] = \
                float(find(point, 'quantity')) * flow_direction_factor

    df.set_index(['Imbalance Volume'])

    return df


def _parse_netposition_timeseries(element):
    """
    Parameters
    ----------
    element : lxml.element

    Returns
    -------
    pd.Series
    """
    positions = []
    quantities = []

    if 'REGION' in find(element, 'out_Domain.mRID'):
        factor = -1 # flow is import so negative
    else:
        factor = 1
    positions = [int(x.text) for x in findall(element, 'position')]
    quantities = [factor * float(x.text) for x in findall(element, 'quantity')]

    series = pd.Series(index=positions, data=quantities)
    series = series.sort_index()
    series.index = _parse_datetimeindex(element)

    return series


def _parse_price_timeseries(element):
    """
    Parameters
    ----------
    element : lxml.element

    Returns
    -------
    pd.Series
    """
    positions = [int(x.text) for x in findall(element, 'position')]
    prices = [float(x.text) for x in findall(element, 'price.amount')]

    series = pd.Series(index=positions, data=prices)
    series = series.sort_index()
    series.index = _parse_datetimeindex(element)

    return series


def _parse_load_timeseries(element):
    """
    Parameters
    ----------
    element : lxml.element

    Returns
    -------
    pd.Series
    """
    positions =  [int(x.text)   for x in findall(element, 'position')]
    prices = [float(x.text) for x in findall(element, 'quantity')]

    series = pd.Series(index=positions, data=prices)
    series = series.sort_index()
    series.index = _parse_datetimeindex(element)

    return series


def _parse_generation_timeseries(element, per_plant: bool = False, include_eic: bool = False) -> pd.Series:
    """
    Works for generation by type, generation forecast, and wind and solar
    forecast

    Parameters
    ----------
    element : lxml.element

    Returns
    -------
    pd.Series
    """
    positions =  [int(x.text)   for x in findall(element, 'position')]
    quantities = [float(x.text) for x in findall(element, 'quantity')]

    series = pd.Series(index=positions, data=quantities)
    series = series.sort_index()
    series.index = _parse_datetimeindex(element)

    # Check if there is a psrtype, if so, get it.
    _psrtype = list(findall(element, 'psrType'))
    if _psrtype:
        psrtype = find(element, 'psrType')
    else:
        psrtype = None

    # Check if the Direction is IN or OUT
    # If IN, this means Actual Consumption is measured
    # If OUT, this means Consumption is measured.
    # OUT means Consumption of a generation plant, eg. charging a pumped hydro plant
    if list(findall(element, CONSUMPTION_ELEMENT)):
        metric = 'Actual Consumption'
    else:
        metric = 'Actual Aggregated'

    name = [metric]

    # Set both psrtype and metric as names of the series
    if psrtype:
        psrtype_name = PSRTYPE_MAPPINGS[psrtype]
        name.append(psrtype_name)

    if per_plant:
        plantname = find(element, 'name')
        name.append(plantname)
        if include_eic:
            eic = find(element, 'mRID codingScheme="A01"')
            name.insert(0, eic)


    if len(name) == 1:
        series.name = name[0]
    else:
        # We give the series multiple names in a tuple
        # This will result in a multi-index upon concatenation
        name.reverse()
        series.name = tuple(name)

    return series


def _parse_water_hydro_timeseries(element, tz):
    """
    Parses timeseries for water reservoirs and hydro storage plants

    Parameters
    ----------
    element : lxml.element

    Returns
    -------
    pd.Series
    """

    positions =  [int(x.text)   for x in findall(element, 'position')]
    quantities = [float(x.text) for x in findall(element, 'quantity')]
    series = pd.Series(index=positions, data=quantities)
    series = series.sort_index()
    series.index = _parse_datetimeindex(element, tz)

    return series


def _parse_installed_capacity_per_plant(element):
    """
    Parameters
    ----------
    element : lxml.element

    Returns
    -------
    pd.Series
    """

    extract_vals = {'Name': 'registeredResource.name',
                    'Production Type': 'psrType',
                    'Bidding Zone': 'inBiddingZone_Domain.mRID',
                    # 'Status': 'businessType',
                    'Voltage Connection Level [kV]':
                        'voltage_PowerSystemResources.highVoltageLimit'}

    series = pd.Series(extract_vals).apply(lambda v: find(element, v))

    # extract only first point
    series['Installed Capacity [MW]'] = \
        float(find(element, 'quantity'))

    series.name = find(element, 'registeredResource.name')

    return series


def _parse_datetimeindex(element, tz=None):
    """
    Create a datetimeindex from a lxml element,
    given that it contains the elements 'start', 'end'
    and 'resolution'

    Parameters
    ----------
    element : lxml.element
    tz: str

    Returns
    -------
    pd.DatetimeIndex
    """
    start = pd.Timestamp(find(element, 'start'))
    end = pd.Timestamp(find(element, 'end'))
    if tz is not None:
        start = start.tz_convert(tz)
        end = end.tz_convert(tz)

    delta = _resolution_to_timedelta(res_text=find(element, 'resolution'))
    index = pd.date_range(start=start, end=end, freq=delta, inclusive='left')
    if tz is not None:
        dst_jump = len(set(index.map(lambda d: d.dst()))) > 1
        if dst_jump and delta == "7D":
            # For a weekly granularity, if we jump over the DST date in October,
            # date_range erronously returns an additional index element
            # because that week contains 169 hours instead of 168.
            index = index[:-1]
        index = index.tz_convert("UTC")

    return index


def _parse_crossborder_flows_timeseries(element):
    """
    Parameters
    ----------
    element : lxml.element

    Returns
    -------
    pd.Series
    """
    positions =  [int(x.text)   for x in findall(element, 'position')]
    flows = [float(x.text) for x in findall(element, 'quantity')]

    series = pd.Series(index=positions, data=flows)
    series = series.sort_index()
    series.index = _parse_datetimeindex(element)

    return series


def _resolution_to_timedelta(res_text: str) -> str:
    """
    Convert an Entsoe resolution to something that pandas can understand
    """
    resolutions = {
        'PT60M': '60min',
        'P1Y': '12M',
        'PT15M': '15min',
        'PT30M': '30min',
        'P1D': '1D',
        'P7D': '7D',
        'P1M': '1M',
    }
    delta = resolutions.get(res_text)
    if delta is None:
        raise NotImplementedError("Sorry, I don't know what to do with the "
                                  "resolution '{}', because there was no "
                                  "documentation to be found of this format. "
                                  "Everything is hard coded. Please open an "
                                  "issue.".format(res_text))
    return delta


# Define inverse bidding zone dico to look up bidding zone labels from the
# domain code in the unavailibility parsers:
_INV_BIDDING_ZONE_DICO = {area.code: area.name for area in Area}

#TODO cannot find some of these in https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html, such as revision, created_doc_time
HEADERS_UNAVAIL_GEN = ['created_doc_time',
                       'docStatus',
                       'mRID',
                       'revision',
                       'businesstype',
                       'biddingzone_domain',
                       'qty_uom',
                       'curvetype',
                       'production_resource_id',
                       'production_resource_name',
                       'production_resource_location',
                       'plant_type',
                       'nominal_power',
                       'start',
                       'end',
                       'resolution',
                       'pstn',
                       'avail_qty'
                       ]


def _unavailability_gen_ts(element) -> list:
    """
    Parser for generation unavailibility time-series
    Parameters
    ----------
    element : lxml.element
    tz : str

    Returns
    -------
    list
    """

    # Avoid attribute errors when some of the fields are void:
    get_attr = lambda attr: "" if list(findall(element, attr)) is None else find(element, attr)
    # When no nominal power is given, give default numeric value of 0:
    get_float = lambda val: float('NaN') if val == "" else float(val)

    f = [BSNTYPE[get_attr('businessType')],
         _INV_BIDDING_ZONE_DICO[get_attr('biddingZone_Domain.mRID')],
         get_attr('quantity_Measure_Unit.name'),
         get_attr('curveType'),
         get_attr('production_RegisteredResource.mRID'),
         get_attr('production_RegisteredResource.name'),
         get_attr('production_RegisteredResource.location.name'),
         PSRTYPE_MAPPINGS.get(get_attr(
             'production_RegisteredResource.pSRType.psrType'), ""),
         get_float(get_attr(
             'production_RegisteredResource.pSRType.powerSystemResources.nominalP'))]
    return [f + p for p in _available_period(element)]

#TODO
HEADERS_UNAVAIL_TRANSM = ['created_doc_time',
                          'docStatus',
                          'businesstype',
                          'in_domain',
                          'out_domain',
                          'qty_uom',
                          'curvetype',
                          'start',
                          'end',
                          'resolution',
                          'pstn',
                          'avail_qty'
                          ]


def _unavailability_tm_ts(element) -> list:
    """
    Parser for transmission unavailibility time-series

    Parameters
    ----------
    element: lxml.element
    tz : str

    Returns
    -------
    list
    """
    # Avoid attribute errors when some of the fields are void:
    get_attr = lambda attr: "" if list(findall(element, attr)) is None else find(element, attr)
    # When no nominal power is given, give default numeric value of 0:

    f = [BSNTYPE[get_attr('businessType')],
         _INV_BIDDING_ZONE_DICO[get_attr('in_Domain.mRID')],
         _INV_BIDDING_ZONE_DICO[get_attr('out_Domain.mRID')],
         get_attr('quantity_Measure_Unit.name'),
         get_attr('curveType'),
         ]
    return [f + p for p in _available_period(element)]


_UNAVAIL_PARSE_CFG = {'A77': (HEADERS_UNAVAIL_GEN, _unavailability_gen_ts),
                      'A78': (HEADERS_UNAVAIL_TRANSM, _unavailability_tm_ts),
                      'A80': (HEADERS_UNAVAIL_GEN, _unavailability_gen_ts)}


def parse_unavailabilities(response: bytes, doctype: str) -> pd.DataFrame:
    """
    Response for Unavailability of Generation Units is ZIP folder
    with one document inside it for each outage.
    This function parses all the files in the ZIP and returns a Pandas DataFrame.
    """
    # First, find out which parser and headers to use, based on the doc type:
    headers, ts_func = _UNAVAIL_PARSE_CFG[doctype]
    dfs = list()
    with zipfile.ZipFile(BytesIO(response), 'r') as arc:
        for f in arc.infolist():
            if f.filename.endswith('xml'):
                frame = _outage_parser(arc.read(f), headers, ts_func)
                dfs.append(frame)
    if len(dfs) == 0:
        df = pd.DataFrame(columns=headers)
    else:
        df = pd.concat(dfs, axis=0)
    df.set_index('created_doc_time', inplace=True)
    df.sort_index(inplace=True)
    return df


def _available_period(timeseries) -> list:
    # if not timeseries:
    #    return

    for period in findall(timeseries, 'Available_Period'):
        start, end = pd.Timestamp(find(period, 'start')), pd.Timestamp(
            find(period, 'end'))
        res = find(period, 'resolution')
        pstn, qty = find(period, 'position'), find(period, 'quantity')
        yield [start, end, res, pstn, qty]


def _outage_parser(xml_file: bytes, headers, ts_func) -> pd.DataFrame:
    # xml_text = xml_file.decode()
    # soup = bs4.BeautifulSoup(xml_text, 'html.parser')
    element = etree.parse(BytesIO(xml_file))
    
    
    
    mrid = find(element, 'mRID')
    revision_number = int(find(element, 'revisionNumber'))
    
    try:
        creation_date = pd.Timestamp(find(element, 'createdDateTime'))
    except AttributeError:
        creation_date = ""
        
    value = list(findall(element, 'value'))
    if value:
        docstatus = DOCSTATUS[find(element, 'value')]
    else:
        docstatus = None
    d = list()
    series = _extract_timeseries(xml_file)
    for ts in series:
        row = [creation_date, docstatus, mrid, revision_number]
        # ts_func may break since it will no longer receive a soup timeseries but a lxml element
        for t in ts_func(ts):
            d.append(row + t)
    df = pd.DataFrame.from_records(d, columns=headers)
    return df
