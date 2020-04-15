import bs4
import pandas as pd
from io import BytesIO
import zipfile
from .mappings import PSRTYPE_MAPPINGS, DOCSTATUS, BSNTYPE, BIDDING_ZONES

GENERATION_ELEMENT = "inBiddingZone_Domain.mRID"
CONSUMPTION_ELEMENT = "outBiddingZone_Domain.mRID"

def _extract_timeseries(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Yields
    -------
    bs4.element.tag
    """
    if not xml_text:
        return
    soup = bs4.BeautifulSoup(xml_text, 'html.parser')
    for timeseries in soup.find_all('timeseries'):
        yield timeseries


def parse_prices(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Returns
    -------
    pd.Series
    """
    series = pd.Series()
    for soup in _extract_timeseries(xml_text):
        series = series.append(_parse_price_timeseries(soup))
    series = series.sort_index()
    return series


def parse_loads(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Returns
    -------
    pd.Series
    """
    series = pd.Series()
    for soup in _extract_timeseries(xml_text):
        series = series.append(_parse_load_timeseries(soup))
    series = series.sort_index()
    return series


def parse_generation(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Returns
    -------
    pd.DataFrame
    """
    all_series = {}
    for soup in _extract_timeseries(xml_text):
        ts = _parse_generation_forecast_timeseries(soup)
        series = all_series.get(ts.name)
        if series is None:
            all_series[ts.name] = ts
        else:
            if (series.name == ts.name) & series.index.equals(ts.index):
                # For some production means the generation and consumption (e.g.
                # Pumped Storage, wind) are returned separately, with the same
                # index. The correct sign is ensured in the method
                # _parse_generation_forecast_timeseries.
                # Here we simply sum up the series to obtain the net generation,
                # i.e. generation - consumption.
                series = series + ts
            else:
                series = series.append(ts)
                series.sort_index()
            all_series[series.name] = series

    for name in all_series:
        ts = all_series[name]
        all_series[name] = ts[~ts.index.duplicated(keep='first')]

    df = pd.DataFrame.from_dict(all_series)
    return df

def parse_generation_per_plant(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Returns
    -------
    pd.DataFrame
    """
    all_series = {}
    for soup in _extract_timeseries(xml_text):
        ts = _parse_generation_forecast_timeseries_per_plant(soup)
        series = all_series.get(ts.name)
        if series is None:
            all_series[ts.name] = ts
        else:
            if (series.name == ts.name) & series.index.equals(ts.index):
                series = series + ts
            else:
                series = series.append(ts)
                series.sort_index()
            all_series[series.name] = series

    for name in all_series:
        ts = all_series[name]
        all_series[name] = ts[~ts.index.duplicated(keep='first')]

    df = pd.DataFrame.from_dict(all_series)
    return df

def parse_installed_capacity_per_plant(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Returns
    -------
    pd.DataFrame
    """
    all_series = {}
    for soup in _extract_timeseries(xml_text):
        s = _parse_installed_capacity_per_plant(soup)
        series = all_series.get(s.name)
        if series is None:
            all_series[s.name] = s
        else:
            series = series.append(s)
            series.sort_index()
            all_series[series.name] = series

    for name in all_series:
        ts = all_series[name]
        all_series[name] = ts[~ts.index.duplicated(keep='first')]

    df = pd.DataFrame.from_dict(all_series).T
    df['Production Type'] = df['Production Type'].map(PSRTYPE_MAPPINGS)
#    df['Status'] = df['Status'].map(BSNTYPE)
    return df

def parse_crossborder_flows(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Returns
    -------
    pd.Series
    """
    series = pd.Series()
    for soup in _extract_timeseries(xml_text):
        series = series.append(_parse_crossborder_flows_timeseries(soup))
    series = series.sort_index()
    return series


def parse_imbalance_prices(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Returns
    -------
    pd.DataFrame
    """
    timeseries_blocks = _extract_timeseries(xml_text)
    frames = (_parse_imbalance_prices_timeseries(soup)
              for soup in timeseries_blocks)
    df = pd.concat(frames, axis=1)
    df = df.stack().unstack() # ad-hoc fix to prevent column splitting by NaNs
    df.sort_index(inplace=True)
    return df


def parse_contracted_reserve(xml_text, tz, label):
    """
    Parameters
    ----------
    xml_text : str
    tz: str
    label: str
    
    Returns
    -------
    pd.DataFrame
    """
    timeseries_blocks = _extract_timeseries(xml_text)
    frames = (_parse_contracted_reserve_series(soup, tz, label)
              for soup in timeseries_blocks)
    df = pd.concat(frames, axis = 1)
    # df = df.stack().unstack() # ad-hoc fix to prevent column splitting by NaNs
    df = df.groupby(by = df.columns, axis = 1).mean()
    df.sort_index(inplace = True)
    return df


def _parse_contracted_reserve_series(soup, tz, label):
    """
    Parameters
    ----------
    soup : bs4.element.tag
    tz: str
    label: str

    Returns
    -------
    pd.Series
    """
    positions = []
    prices = []
    for point in soup.find_all('point'):
        positions.append(int(point.find('position').text))
        prices.append(float(point.find(label).text))

    df = pd.DataFrame(data = {'position': positions,
                            label: prices})
    df = df.set_index(['position'])
    df.sort_index(inplace = True)
    index = _parse_datetimeindex(soup, tz)
    if len(index) > len(df.index):
        print("Shortening index")
        df.index = index[:len(df.index)]
    else:
        df.index = index

    df.index.name = None
    df.columns.name = None
    direction_dico = {'A01': 'Up',
                      'A02': 'Down',
                      'A03' : 'Symmetric'}

    reserve_type = BSNTYPE[soup.find("businesstype").text]
    direction = direction_dico[soup.find("flowdirection.direction").text]

    df.rename(columns = {label: "%s - %s" % (reserve_type, direction)},
              inplace = True)
    return df


def _parse_imbalance_prices_timeseries(soup):
    """
    Parameters
    ----------
    soup : bs4.element.tag

    Returns
    -------
    pd.Series
    """
    positions = []
    amounts = []
    categories = []
    for point in soup.find_all('point'):
        positions.append(int(point.find('position').text))
        amounts.append(float(point.find('imbalance_price.amount').text))
        if point.find('imbalance_price.category'):
            categories.append(point.find('imbalance_price.category').text)
        else:
            categories.append('None')

    df = pd.DataFrame(data={'position': positions,
                            'amount': amounts, 'category': categories})
    df = df.set_index(['position', 'category']).unstack()
    df.sort_index(inplace=True)
    df.index = _parse_datetimeindex(soup)
    df = df.xs('amount', axis=1)
    df.index.name = None
    df.columns.name = None
    df.rename(columns={'A04': 'Long', 'A05': 'Short',
                       'None' : 'Price for Consumption'}, inplace=True)

    return df


def _parse_price_timeseries(soup):
    """
    Parameters
    ----------
    soup : bs4.element.tag

    Returns
    -------
    pd.Series
    """
    positions = []
    prices = []
    for point in soup.find_all('point'):
        positions.append(int(point.find('position').text))
        prices.append(float(point.find('price.amount').text))

    series = pd.Series(index=positions, data=prices)
    series = series.sort_index()
    series.index = _parse_datetimeindex(soup)

    return series


def _parse_load_timeseries(soup):
    """
    Parameters
    ----------
    soup : bs4.element.tag

    Returns
    -------
    pd.Series
    """
    positions = []
    prices = []
    for point in soup.find_all('point'):
        positions.append(int(point.find('position').text))
        prices.append(float(point.find('quantity').text))

    series = pd.Series(index=positions, data=prices)
    series = series.sort_index()
    series.index = _parse_datetimeindex(soup)

    return series


def _parse_generation_forecast_timeseries(soup):
    """
    Parameters
    ----------
    soup : bs4.element.tag

    Returns
    -------
    pd.Series
    """
    psrtype = soup.find('psrtype').text
    positions = []
    quantities = []
    for point in soup.find_all('point'):
        positions.append(int(point.find('position').text))
        quantity = point.find('quantity')
        if quantity is None:
            raise LookupError(f'No quantity found in this point, it should have one: {point}')
        quantities.append(float(quantity.text))

    series = pd.Series(index=positions, data=quantities)
    series = series.sort_index()
    series.index = _parse_datetimeindex(soup)

    series.name = PSRTYPE_MAPPINGS[psrtype]
    if soup.find(CONSUMPTION_ELEMENT.lower()):
        # https://entsoe.zendesk.com/hc/en-us/articles/115005485123-Resful-API-How-to-differentiate-TimeSeries-for-Scheduled-Generation-from-Scheduled-Consumption-
        # For Consumption, we should find the entry "outBiddingZone_Domain.mRID".
        # This allows to distinguish Hydro Pumped Storage Generation and
        # Hydro Pumped Storage Load from each other and avoid non-unique index errors.
        series = -series

    return series

def _parse_generation_forecast_timeseries_per_plant(soup):
    """
    Parameters
    ----------
    soup : bs4.element.tag

    Returns
    -------
    pd.Series
    """
    plantname = soup.find('name').text
    positions = []
    quantities = []
    for point in soup.find_all('point'):
        positions.append(int(point.find('position').text))
        quantities.append(float(point.find('quantity').text))

    series = pd.Series(index=positions, data=quantities)
    series = series.sort_index()
    series.index = _parse_datetimeindex(soup)

    series.name = plantname
    if soup.find(CONSUMPTION_ELEMENT.lower()):
        # https://entsoe.zendesk.com/hc/en-us/articles/115005485123-Resful-API-How-to-differentiate-TimeSeries-for-Scheduled-Generation-from-Scheduled-Consumption-
        # For Consumption, we should find the entry "outBiddingZone_Domain.mRID".
        # This allows to distinguish Hydro Pumped Storage Generation and
        # Hydro Pumped Storage Load from each other and avoid non-unique index errors.
        series = -series

    return series

def _parse_installed_capacity_per_plant(soup):
    """
    Parameters
    ----------
    soup : bs4.element.tag

    Returns
    -------
    pd.Series
    """
    extract_vals = {'Name':'registeredresource.name',
                    'Production Type': 'psrtype',
                    'Bidding Zone': 'inbiddingzone_domain.mrid',
                    # 'Status': 'businesstype',
                    'Voltage Connection Level [kV]':
                        'voltage_powersystemresources.highvoltagelimit'}
    series = pd.Series(extract_vals).apply(lambda v: soup.find(v).text)

    #extract only first point
    series['Installed Capacity [MW]'] = \
        soup.find_all('point')[0].find('quantity').text

    series.name = soup.find('registeredresource.mrid').text

    return series


def _parse_datetimeindex(soup, tz = None):
    """
    Create a datetimeindex from a parsed beautifulsoup,
    given that it contains the elements 'start', 'end'
    and 'resolution'

    Parameters
    ----------
    soup : bs4.element.tag
    tz: str

    Returns
    -------
    pd.DatetimeIndex
    """
    start = pd.Timestamp(soup.find('start').text)
    end = pd.Timestamp(soup.find('end').text)
    if tz is not None:
        start = start.tz_convert(tz)
        end = end.tz_convert(tz)

    delta = _resolution_to_timedelta(res_text=soup.find('resolution').text)
    index = pd.date_range(start=start, end=end, freq=delta, closed='left')
    if tz is not None:
        dst_jump = len(set(index.map(lambda d:d.dst()))) > 1
        if dst_jump and delta == "7D":
            # For a weekly granularity, if we jump over the DST date in October,
            # date_range erronously returns an additional index element
            # because that week contains 169 hours instead of 168.
            index = index[:-1]
        index = index.tz_convert("UTC")

    return index


def _parse_crossborder_flows_timeseries(soup):
    """
    Parameters
    ----------
    soup : bs4.element.tag

    Returns
    -------
    pd.Series
    """
    positions = []
    flows = []
    for point in soup.find_all('point'):
        positions.append(int(point.find('position').text))
        flows.append(float(point.find('quantity').text))

    series = pd.Series(index=positions, data=flows)
    series = series.sort_index()
    series.index = _parse_datetimeindex(soup)

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
_INV_BIDDING_ZONE_DICO = {k: v for (v, k) in BIDDING_ZONES.items()}

HEADERS_UNAVAIL_GEN = ['created_doc_time',
                       'docstatus',
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


def _unavailability_gen_ts(soup: bs4.BeautifulSoup) -> list:
    '''
    Parser for generation unavailibility time-series
    Parameters
    ----------
    soup : bs4.element.tag
    tz: str

    Returns
    -------
    list
    '''

    # Avoid attribute errors when some of the fields are void:
    get_attr = lambda attr: "" if soup.find(attr) is None else soup.find(attr).text
    # When no nominal power is given, give default numeric value of 0:
    get_float = lambda val: float('NaN') if val == "" else float(val)

    f = [BSNTYPE[get_attr('businesstype')],
         _INV_BIDDING_ZONE_DICO[get_attr('biddingzone_domain.mrid')],
         get_attr('quantity_measure_unit.name'),
         get_attr('curvetype'),
         get_attr('production_registeredresource.mrid'),
         get_attr('production_registeredresource.name'),
         get_attr('production_registeredresource.location.name'),
         PSRTYPE_MAPPINGS.get(get_attr(
             'production_registeredresource.psrtype.psrtype'), ""),
         get_float(get_attr('production_registeredresource.psrtype.powersystemresources.nominalp'))]
    return [f + p for p in _available_period(soup)]

HEADERS_UNAVAIL_TRANSM = ['created_doc_time',
                   'docstatus',
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


def _unavailability_tm_ts(soup: bs4.BeautifulSoup) -> list:
    '''
    Parser for transmission unavailibility time-series
    Parameters
    ----------
    soup : bs4.element.tag
    tz: str

    Returns
    -------
    list
    '''
    # Avoid attribute errors when some of the fields are void:
    get_attr = lambda attr: "" if soup.find(attr) is None else soup.find(attr).text
    # When no nominal power is given, give default numeric value of 0:

    f = [BSNTYPE[get_attr('businesstype')],
         _INV_BIDDING_ZONE_DICO[get_attr('in_domain.mrid')],
         _INV_BIDDING_ZONE_DICO[get_attr('out_domain.mrid')],
         get_attr('quantity_measure_unit.name'),
         get_attr('curvetype'),
         ]
    return [f + p for p in _available_period(soup)]


_UNAVAIL_PARSE_CFG = {}
_UNAVAIL_PARSE_CFG['A77'] = (HEADERS_UNAVAIL_GEN, _unavailability_gen_ts)
_UNAVAIL_PARSE_CFG['A78'] = (HEADERS_UNAVAIL_TRANSM, _unavailability_tm_ts)
_UNAVAIL_PARSE_CFG['A80'] = (HEADERS_UNAVAIL_GEN, _unavailability_gen_ts)


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
    df = pd.concat(dfs, axis = 0)
    df.set_index('created_doc_time', inplace = True)
    df.sort_index(inplace = True)
    return df


def _available_period(timeseries: bs4.BeautifulSoup) -> list:
    # if not timeseries:
    #    return
    for period in timeseries.find_all('available_period'):
        start, end = pd.Timestamp(period.timeinterval.start.text), pd.Timestamp(period.timeinterval.end.text)
        res = period.resolution.text
        pstn, qty = period.point.position.text, period.point.quantity.text
        yield [start, end, res, pstn, qty]


def _outage_parser(xml_file: bytes, headers, ts_func) -> pd.DataFrame:
    xml_text = xml_file.decode()

    soup = bs4.BeautifulSoup(xml_text, 'html.parser')
    try:
        creation_date = pd.Timestamp(soup.createddatetime.text)
    except AttributeError:
        creation_date = ""

    try:
        docstatus = DOCSTATUS[soup.docstatus.value.text]
    except AttributeError:
        docstatus = None
    d = list()
    series = _extract_timeseries(xml_text)
    for ts in series:
        row = [creation_date, docstatus]
        for t in ts_func(ts):
            d.append(row + t)
    df = pd.DataFrame.from_records(d, columns = headers)
    return df
