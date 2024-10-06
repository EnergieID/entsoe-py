import bs4
import pandas as pd
from functools import reduce

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

def _resolution_to_timedelta(res_text: str) -> str:
    """
    Convert an Entsoe resolution to something that pandas can understand
    """
    resolutions = {
        'PT60M': '60min',
        'P1Y': '12MS',
        'PT15M': '15min',
        'PT30M': '30min',
        'P1D': '1D',
        'P7D': '7D',
        'P1M': '1MS',
    }
    delta = resolutions.get(res_text)
    if delta is None:
        raise NotImplementedError("Sorry, I don't know what to do with the "
                                  "resolution '{}', because there was no "
                                  "documentation to be found of this format. "
                                  "Everything is hard coded. Please open an "
                                  "issue.".format(res_text))
    return delta

def _parse_datetimeindex(soup, tz=None):
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
  end = pd.Timestamp(soup.find_all('end')[-1].text)
  if tz is not None:
      start = start.tz_convert(tz)
      end = end.tz_convert(tz)

  delta = _resolution_to_timedelta(res_text=soup.find('resolution').text)
  index = pd.date_range(start=start, end=end, freq=delta, inclusive='left')
  if tz is not None:
      dst_jump = len(set(index.map(lambda d: d.dst()))) > 1
      if dst_jump and delta == "7D":
          # For a weekly granularity, if we jump over the DST date in October,
          # date_range erronously returns an additional index element
          # because that week contains 169 hours instead of 168.
          index = index[:-1]
      index = index.tz_convert("UTC")
  elif index.to_series().diff().min() >= pd.Timedelta('1D') and end.hour == start.hour + 1:
      # For a daily or larger granularity, if we jump over the DST date in October,
      # date_range erronously returns an additional index element
      # because the period contains one extra hour.
      index = index[:-1]

  return index

def _parse_timeseries_generic(soup, label='quantity', to_float=True):
    periods = []
    for period in soup.find_all('period'):
        period_soup = bs4.BeautifulSoup(str(period), 'html.parser')
        periods.append(_parse_timeseries_generic_period(period_soup, soup, label, to_float))
    return reduce(lambda s1, s2: s1.combine_first(s2), periods)

def _parse_timeseries_generic_period(soup, whole_soup, label='quantity', to_float=True):
    data = {}
    for point in soup.find_all('point'):
        value = point.find(label).text
        if to_float:
            value = value.replace(',', '')
        data[int(point.find('position').text)] = value

    series = pd.Series(data)
    series.sort_index()
    index = _parse_datetimeindex(soup)
    if whole_soup.find('curvetype').text == 'A03':
        # with A03 its possible that positions are missing, this is when values are repeated
        # see docs: https://eepublicdownloads.entsoe.eu/clean-documents/EDI/Library/cim_based/Introduction_of_different_Timeseries_possibilities__curvetypes__with_ENTSO-E_electronic_document_v1.4.pdf
        # so lets do reindex on a continious range which creates gaps if positions are missing
        # then forward fill, so repeat last valid value, to fill the gaps
        series = series.reindex(list(range(1, len(index)+1))).ffill()

    series.index = index
    if to_float:
        series = series.astype(float)

    return series

def _parse_timeseries_generic_whole(xml_text, label='quantity', to_float=True):
    series_all = []
    for soup in _extract_timeseries(xml_text):
        series_all.append(_parse_timeseries_generic(soup, label=label, to_float=to_float))

    series_all = pd.concat(series_all).sort_index()
    return series_all