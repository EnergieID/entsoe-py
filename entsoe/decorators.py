import sys
from socket import gaierror
from time import sleep
import requests
from functools import wraps
from .exceptions import NoMatchingDataError, PaginationError
import pandas as pd
import logging

from .misc import year_blocks, day_blocks


def retry(func):
    """Catches connection errors, waits and retries"""

    @wraps(func)
    def retry_wrapper(*args, **kwargs):
        self = args[0]
        error = None
        for _ in range(self.retry_count):
            try:
                result = func(*args, **kwargs)
            except (requests.ConnectionError, gaierror) as e:
                error = e
                print("Connection Error, retrying in {} seconds".format(
                    self.retry_delay), file=sys.stderr)
                sleep(self.retry_delay)
                continue
            else:
                return result
        else:
            raise error

    return retry_wrapper


def paginated(func):
    """Catches a PaginationError, splits the requested period in two and tries
    again. Finally it concatenates the results"""

    @wraps(func)
    def pagination_wrapper(*args, start, end, **kwargs):
        try:
            df = func(*args, start=start, end=end, **kwargs)
        except PaginationError:
            pivot = start + (end - start) / 2
            df1 = pagination_wrapper(*args, start=start, end=pivot, **kwargs)
            df2 = pagination_wrapper(*args, start=pivot, end=end, **kwargs)
            df = pd.concat([df1, df2])
        return df

    return pagination_wrapper


def year_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def year_wrapper(*args, start, end, **kwargs):
        blocks = year_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                logging.debug(f"NoMatchingDataError: between {_start} and {_end}")
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames, sort=True)
        df = df.loc[~df.index.duplicated(keep='first')]
        return df

    return year_wrapper


def day_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def day_wrapper(*args, start, end, **kwargs):
        blocks = day_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                print(f"NoMatchingDataError: between {_start} and {_end}", file=sys.stderr)
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames)
        return df

    return day_wrapper
