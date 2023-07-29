import tqdm
import sys
from socket import gaierror
from time import sleep
import requests
from functools import wraps
from .exceptions import NoMatchingDataError, PaginationError
import pandas as pd
import logging
from .misc import year_blocks, day_blocks, period_splitter

class ProgressBar:
    show = False
    batch_days = 7

    @staticmethod
    def progress_bar(func):
        """ Forces a weekly progress bar, if query range more than 7 days (adjustable interval)"""
        @wraps(func)
        def progress_wrapper(*args, **kwargs):
            if not ProgressBar.show:
                result = func(*args, **kwargs)
                return result

            start = kwargs['start']
            end = kwargs['end']

            query_bins = period_splitter(start, end, days_thresh=ProgressBar.batch_days)

            if len(query_bins) <= 1:
                result = func(*args, **kwargs)

            else:

                tqdm.tqdm._instances.clear()
                pbar = tqdm.tqdm(range(len(query_bins)), desc="\tDownload Progress")
                results = []
                for i in pbar:
                    pair = query_bins[i]
                    pair_kwargs = kwargs.copy()
                    pair_kwargs['start'] = pair[0]
                    pair_kwargs['end'] = pair[1]
                    results.append(func(*args, **pair_kwargs ))
                    pbar.set_postfix_str(s="Reached: {}".format(pair[1].date()))

                result = pd.concat([*results], axis = 0)

            return result

        return progress_wrapper
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

def documents_limited(n):
    def decorator(func):
        """Deals with calls where you cannot query more than n documents at a time, by offsetting per n documents"""

        @wraps(func)
        def documents_wrapper(*args, **kwargs):
            frames = []
            for offset in range(0, 4800 + n, n):
                try:
                    frame = func(*args, offset=offset, **kwargs)
                    frames.append(frame)
                except NoMatchingDataError:
                    logging.debug(f"NoMatchingDataError: for offset {offset}")
                    break

            if len(frames) == 0:
                # All the data returned are void
                raise NoMatchingDataError

            df = pd.concat(frames, sort=True)
            df = df.loc[~df.index.duplicated(keep='first')]
            return df
        return documents_wrapper
    return decorator


def year_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def year_wrapper(*args, start=None, end=None, **kwargs):
        if start is None or end is None:
            raise Exception('Please specify the start and end date explicity with start=<date> when calling this '
                            'function')
        if type(start) != pd.Timestamp or type(end) != pd.Timestamp:
            raise Exception('Please use a timezoned pandas object for start and end')
        if start.tzinfo is None or end.tzinfo is None:
            raise Exception('Please use a timezoned pandas object for start and end')

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
