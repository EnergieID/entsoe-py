import pandas as pd
from dateutil import rrule
from itertools import tee

def period_splitter(start, end, days_thresh = 7):
    days_requested = (end - start).days
    request_bins = []
    rolling_start = start
    if days_requested > days_thresh:
        rolling_end = rolling_start + pd.Timedelta(days_thresh, 'D')
        while rolling_end < end:
            request_bins.append((rolling_start, rolling_end))
            rolling_start += pd.Timedelta(days_thresh, 'D')
            rolling_end += pd.Timedelta(days_thresh, 'D')

        request_bins.append((rolling_start, end))
    else:
        request_bins = [(start, end)]

    return request_bins


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
