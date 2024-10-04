import bs4
import pandas as pd


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


def _timeseries_frequencies_to_timedelta(freq_text: str) -> str:
    time_mapping = {
        '15T': '15min',
        '30T': '30min',
        '1H': '60min',
        '1h': '60min',
        'h': '60min',
        '0.25H': '15min',
        '0.5H': '30min',
    }

    delta = time_mapping.get(freq_text)
    if delta is None:
        raise NotImplementedError("Sorry, I don't know what to do with the "
                                  "resolution '{}', because there was no "
                                  "documentation to be found of this format. "
                                  "Everything is hard coded. Please open an "
                                  "issue.".format(freq_text))
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
    # Create a list to store all time series data

    # Fun fact: the <timeinterval> don't seem to be sorted. So earlier time intervals are not necessarily before
    # later time intervals. Weird!

    all_data = []

    overall_start = pd.Timestamp(year=2300, month=1, day=1).tz_localize("utc")
    overall_end = pd.Timestamp(year=1900, month=1, day=1).tz_localize("utc")

    # Iterate over each period
    for period in soup.find_all("period"):

        # For each period, I'm checking which start time it corresponds to.
        start_time_str = period.find("start").text
        end_time_str = period.find("end").text
        resolution_str = period.find("resolution").text  # PT6H, PT30M, etc.
        start_time = pd.to_datetime(start_time_str)
        end_time = pd.to_datetime(end_time_str)


        # I'm keeping track of the overall intended start and the intended end point, incase the data for the intended
        # start/end has gone missing.
        #
        if start_time < overall_start:
            overall_start = start_time

        if end_time > overall_end:
            overall_end = end_time

        # Convert ISO 8601 duration to a pandas Timedelta
        resolution_timedelta = pd.to_timedelta(resolution_str)

        # Loop over each point and extract position
        for point in period.find_all("point"):
            position = int(point.find("position").text)
            value = point.find(label).text
            if to_float:
                value = float(value)

            # Calculate the timestamp for this point based on the position and resolution
            timestamp = start_time + resolution_timedelta * (position - 1)

            # Append the data
            all_data.append([timestamp, value])

    # Create a DataFrame from the combined data
    df_combined = pd.DataFrame(all_data, columns=['Timestamp', label])

    # Reindex the DataFrame to include the complete range
    df_combined.set_index('Timestamp', inplace=True)

    if soup.find('curvetype').text == 'A03':
        # with A03 its possible that positions are missing, this is when values are repeated
        # see docs: https://eepublicdownloads.entsoe.eu/clean-documents/EDI/Library/cim_based/Introduction_of_different_Timeseries_possibilities__curvetypes__with_ENTSO-E_electronic_document_v1.4.pdf
        # I am creating a range from the

        # Create a complete date range for the specified periods using the maximum resolution. If I hadn't logged
        # the overall start and end, the index would have been the wrong length if either of these edge cases
        # were missing in the data returned from Entsoe. Given that we are using a forward fill, if the first value is
        # missing, it should show as NaN.

        complete_range = pd.date_range(start=overall_start,
                                       end=overall_end,
                                       freq=resolution_timedelta)

        df_combined = df_combined.reindex(complete_range)

        # Forward fill missing values
        df_combined[label] = df_combined[label].ffill()

    series = df_combined[label]
    series.name = None
    return series

def _parse_timeseries_generic_whole(xml_text, label='quantity', to_float=True):
    series_all = []
    for soup in _extract_timeseries(xml_text):
        series_all.append(_parse_timeseries_generic(soup, label=label, to_float=to_float))

    series_all = pd.concat(series_all).sort_index()
    return series_all