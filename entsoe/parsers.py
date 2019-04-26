import bs4
import pandas as pd
from io import BytesIO
import zipfile
from .mappings import PSRTYPE_MAPPINGS, DOCSTATUS, BSNTYPE, BIDDING_ZONES


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
            series = series.append(ts)
            series.sort_index()
            all_series[series.name] = series

    for name in all_series:
        ts = all_series[name]
        all_series[name] = ts[~ts.index.duplicated(keep='first')]

    df = pd.DataFrame.from_dict(all_series)
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
        quantities.append(float(point.find('quantity').text))

    series = pd.Series(index=positions, data=quantities)
    series = series.sort_index()
    series.index = _parse_datetimeindex(soup)

    series.name = PSRTYPE_MAPPINGS[psrtype]
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
    return series


def _parse_datetimeindex(soup):
    """
    Create a datetimeindex from a parsed beautifulsoup,
    given that it contains the elements 'start', 'end'
    and 'resolution'

    Parameters
    ----------
    soup : bs4.element.tag

    Returns
    -------
    pd.DatetimeIndex
    """
    start = pd.Timestamp(soup.find('start').text)
    end = pd.Timestamp(soup.find('end').text)
    delta = _resolution_to_timedelta(res_text=soup.find('resolution').text)
    index = pd.date_range(start=start, end=end, freq=delta, closed='left')
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
        'PT30M': '30min'
    }
    delta = resolutions.get(res_text)
    if delta is None:
        raise NotImplementedError("Sorry, I don't know what to do with the "
                                  "resolution '{}', because there was no "
                                  "documentation to be found of this format. "
                                  "Everything is hard coded. Please open an "
                                  "issue.".format(res_text))
    return delta


def parse_unavailabilities(response: bytes) -> pd.DataFrame:
    """
    Response for Unavailability of Generation Units is ZIP folder
    with one document inside it for each outage.
    This function parses all the files in the ZIP and returns a Pandas DataFrame.
    """
    dfs = list()
    with zipfile.ZipFile(BytesIO(response), 'r') as arc:
        for f in arc.infolist():
            if f.filename.endswith('xml'):
                frame = _outage_parser(arc.read(f))
                dfs.append(frame)
    df = pd.concat(dfs, axis=0)
    df.set_index('created_doc_time', inplace=True)
    df.sort_index(inplace=True)
    return df


def _available_period(timeseries: bs4.BeautifulSoup) -> list:
    #if not timeseries:
    #    return
    for period in timeseries.find_all('available_period'):
        start, end = pd.Timestamp(period.timeinterval.start.text), pd.Timestamp(period.timeinterval.end.text)
        res = period.resolution.text
        pstn, qty = period.point.position.text, period.point.quantity.text
        yield [start, end, res, pstn, qty]


def _unavailability_timeseries(soup: bs4.BeautifulSoup) -> list:

    # Avoid attribute errors when some of the fields are void: 
    get_attr = lambda attr: "" if soup.find(attr) is None else soup.find(attr).text 
    # When no nominal power is given, give default numeric value of 0:
    get_float = lambda val: float('NaN') if val == "" else float(val)

    dm = {k: v for (v, k) in BIDDING_ZONES.items()}
    f = [BSNTYPE[get_attr('businesstype')],
         dm[get_attr('biddingzone_domain.mrid')],
         get_attr('quantity_measure_unit.name'),
         get_attr('curvetype'),
         get_attr('production_registeredresource.mrid'),
         get_attr('production_registeredresource.name'),
         get_attr('production_registeredresource.location.name'),
         PSRTYPE_MAPPINGS.get(get_attr(
             'production_registeredresource.psrtype.psrtype'), ""),
         get_float(get_attr('production_registeredresource.psrtype.powersystemresources.nominalp'))]
    return [f + p for p in _available_period(soup)]


def _outage_parser(xml_file: bytes) -> pd.DataFrame:
    xml_text = xml_file.decode()
    #if not(xml_text):
        #return
    headers = ['created_doc_time',
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
        for t in _unavailability_timeseries(ts):
            d.append(row + t)
    df = pd.DataFrame.from_records(d, columns=headers)
    return df
