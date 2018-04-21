import bs4
import pandas as pd

from .entsoe import PSRTYPE_MAPPINGS


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
    frames = (_parse_imbalance_prices_timeseries(soup) for soup in timeseries_blocks)
    df = pd.concat(frames, axis=1)
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
        categories.append(point.find('imbalance_price.category').text)

    df = pd.DataFrame(data={'position': positions, 'amount': amounts, 'category': categories})
    df = df.set_index(['position', 'category']).unstack()
    df.sort_index(inplace=True)
    df.index = _parse_datetimeindex(soup)
    df = df.xs('amount', axis=1)
    df.index.name = None
    df.columns.name = None
    df.rename(columns={'A04': 'Generation', 'A05': 'Load'}, inplace=True)

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


def _resolution_to_timedelta(res_text):
    """
    Convert an Entsoe resolution to something that pandas can understand

    Parameters
    ----------
    res_text : str

    Returns
    -------
    str
    """
    if res_text == 'PT60M':
        delta = '60min'
    elif res_text == 'P1Y':
        delta = '12M'
    elif res_text == "PT15M":
        delta = '15min'
    else:
        raise NotImplementedError("Sorry, I don't know what to do with the "
                                  "resolution '{}', because there was no "
                                  "documentation to be found of this format. "
                                  "Everything is hard coded. Please open an "
                                  "issue.".format(res_text))
    return delta
