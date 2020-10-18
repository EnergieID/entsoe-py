from itertools import tee
import pytz

import pandas as pd
from dateutil import rrule


def year_blocks(start, end):
    """
    Create pairs of start and end with max a year in between, to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.YEARLY

    res = []
    for day in rrule.rrule(rule, dtstart=start, until=end):
        res.append(pd.Timestamp(day))
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def month_blocks(start, end):
    """
    Create pairs of start and end with max a month in between, to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.MONTHLY

    res = []
    for day in rrule.rrule(rule, dtstart=start, until=end):
        res.append(pd.Timestamp(day))
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def day_blocks(start, end):
    """
    Create pairs of start and end with max a day in between, to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.DAILY

    res = []
    for day in rrule.rrule(rule, dtstart=start, until=end):
        res.append(pd.Timestamp(day))
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def pairwise(iterable):
    """
    Create pairs to iterate over
    eg. [A, B, C, D] -> ([A, B], [B, C], [C, D])

    Parameters
    ----------
    iterable : iterable

    Returns
    -------
    iterable
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def datetime_to_str(dtm: pd.Timestamp) -> str:
    """
    Convert a datetime object to a string in UTC
    of the form YYYYMMDDhh00

    Parameters
    ----------
    dtm : pd.Timestamp
        Recommended to use a timezone-aware object!
        If timezone-naive, UTC is assumed

    Returns
    -------
    str
    """
    if dtm.tzinfo is not None and dtm.tzinfo != pytz.UTC:
        dtm = dtm.tz_convert("UTC")
    fmt = '%Y%m%d%H00'
    ret_str = dtm.strftime(fmt)
    return ret_str


def datetime_to_iso8601_z_notation(dtm: pd.Timestamp) -> str:
    """
    Convert a datetime to an ISO8601 in Z notation
    Timezone-naive inputs are assumed UTC
    """
    if dtm.tzinfo is None:
        dtm = dtm.tz_localize('UTC')
    dtm = dtm.tz_convert('UTC')
    ret_str = dtm.isoformat().replace('+00:00', 'Z')
    return ret_str


def start_end_to_timeinterval(start: pd.Timestamp, end: pd.Timestamp) -> str:
    """Convert start and end to an ISO8601 time interval"""
    start_str = datetime_to_iso8601_z_notation(start)
    end_str = datetime_to_iso8601_z_notation(end)
    timeinterval = f'{start_str}/{end_str}'
    return timeinterval
