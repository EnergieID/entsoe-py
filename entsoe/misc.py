import pandas as pd
import datetime as dt
from dateutil import rrule
from itertools import tee
from functools import partial
from typing import Union, Iterable, Tuple


get_distinct_sorted_pairs = lambda list: pairwise(sorted(set(list)))

def _blocks(rrule_freq: int,
            start: Union[pd.Timestamp, dt.datetime], end: Union[pd.Timestamp, dt.datetime]
            ) -> Iterable[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Create pairs of start and end with max `rrule_freq` in between, to deal with usage
    restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule_freq

    res = [start] + list(map(pd.Timestamp, rrule.rrule(rule, dtstart=start, until=end))) + [end]
    return get_distinct_sorted_pairs(res)

year_blocks = partial(_blocks, rrule_freq = rrule.YEARLY)
month_blocks = partial(_blocks, rrule_freq = rrule.MONTHLY)
day_blocks = partial(_blocks, rrule_freq = rrule.DAILY)

def pairwise(iterable: Iterable) -> zip:
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
